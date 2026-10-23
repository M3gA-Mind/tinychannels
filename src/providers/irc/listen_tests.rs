//! Socket-level tests for the IRC channel: a local TLS listener plays the IRC
//! server, scripting the registration, SASL and message exchange line by line.

use super::*;
use std::time::Duration;
use tinychannels_conformance::{
    AdapterFixture, ConformanceFeature, ExpectedScope, WireObservation, check_all_features,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

/// Hang guard only: every exchange below is driven by explicit reads, so a
/// healthy run never comes near it.
const GUARD: Duration = Duration::from_secs(10);

fn acceptor(tls12_only: bool) -> TlsAcceptor {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert = include_bytes!("../../../tests/fixtures/tls/cert.der");
    let key = include_bytes!("../../../tests/fixtures/tls/key.der");
    let builder = if tls12_only {
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS12])
    } else {
        rustls::ServerConfig::builder()
    };
    let config = builder
        .with_no_client_auth()
        .with_single_cert(
            vec![rustls::pki_types::CertificateDer::from(cert.to_vec())],
            rustls::pki_types::PrivatePkcs8KeyDer::from(key.to_vec()).into(),
        )
        .unwrap();
    TlsAcceptor::from(Arc::new(config))
}

async fn bind() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

fn config(port: u16) -> IrcChannelConfig {
    IrcChannelConfig {
        server: "127.0.0.1".into(),
        port,
        nickname: "bot".into(),
        username: Some("botuser".into()),
        channels: vec![],
        allowed_users: vec!["*".into()],
        server_password: None,
        nickserv_password: None,
        sasl_password: None,
        verify_tls: false,
    }
}

/// The server side of one IRC connection.
struct Peer {
    reader: BufReader<tokio::io::ReadHalf<TlsStream<TcpStream>>>,
    writer: tokio::io::WriteHalf<TlsStream<TcpStream>>,
}

impl Peer {
    async fn accept(listener: &TcpListener, acceptor: &TlsAcceptor) -> Self {
        let (socket, _) = tokio::time::timeout(GUARD, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let tls = tokio::time::timeout(GUARD, acceptor.accept(socket))
            .await
            .unwrap()
            .unwrap();
        let (reader, writer) = tokio::io::split(tls);
        Self {
            reader: BufReader::new(reader),
            writer,
        }
    }

    async fn next_line(&mut self) -> String {
        let mut line = String::new();
        tokio::time::timeout(GUARD, self.reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        line.trim_end_matches(['\r', '\n']).to_string()
    }

    async fn is_closed(&mut self) -> bool {
        let mut line = String::new();
        match tokio::time::timeout(GUARD, self.reader.read_line(&mut line))
            .await
            .expect("peer should observe listener cleanup")
        {
            Ok(0) => true,
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => true,
            Ok(_) => false,
            Err(error) => panic!("unexpected TLS peer read error: {error}"),
        }
    }

    async fn expect(&mut self, want: &str) {
        assert_eq!(self.next_line().await, want);
    }

    async fn say(&mut self, line: &str) {
        self.writer
            .write_all(format!("{line}\r\n").as_bytes())
            .await
            .unwrap();
        self.writer.flush().await.unwrap();
    }

    /// Close the connection cleanly (TLS `close_notify`), which the client
    /// reads as end of stream.
    async fn close(mut self) {
        self.writer.shutdown().await.unwrap();
    }
}

async fn recv(
    rx: &mut mpsc::UnboundedReceiver<tinychannels_bus::ChannelIngress>,
) -> tinychannels_bus::channel::ChannelInboundEnvelope {
    let ingress = tokio::time::timeout(GUARD, rx.recv())
        .await
        .unwrap()
        .expect("listener dropped its sender");
    let tinychannels_bus::ChannelIngress::Inbound(envelope) = ingress else {
        panic!("IRC listener should emit inbound messages");
    };
    envelope
}

#[derive(Clone)]
struct InboundSink(mpsc::UnboundedSender<tinychannels_bus::ChannelIngress>);

#[async_trait::async_trait]
impl crate::channel::ChannelInboundSink for InboundSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.0
            .send(ingress)
            .map_err(|_| crate::channel::ChannelSendError {
                kind: crate::channel::SendErrorKind::Transient,
                message: "test sink closed".into(),
                retryable: true,
                ..Default::default()
            })
    }
}

struct IrcConformanceFixture {
    channel: Arc<dyn crate::LongTailNative>,
    peer: tokio::sync::Mutex<Peer>,
    observations: std::sync::Mutex<Vec<WireObservation>>,
}

#[async_trait::async_trait]
impl AdapterFixture for IrcConformanceFixture {
    fn adapter(&self) -> &dyn crate::channel::ChannelAdapter {
        self.channel.as_ref()
    }

    fn expected_scope(&self, _: ConformanceFeature) -> ExpectedScope {
        ExpectedScope {
            channel: tinychannels_bus::ChannelRef {
                id: "irc".into(),
                account_id: None,
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Channel,
                id: "#ops".into(),
                ..Default::default()
            },
            reply_to_id: None,
            thread_id: None,
        }
    }

    fn receipt_required(&self, _: ConformanceFeature) -> bool {
        false
    }

    fn clear_observations(&self) {
        self.observations.lock().unwrap().clear();
    }

    fn observations(&self) -> Vec<WireObservation> {
        self.observations.lock().unwrap().clone()
    }

    async fn exercise(
        &self,
        feature: ConformanceFeature,
    ) -> Result<
        Option<tinychannels_bus::channel::ChannelSendOutcome>,
        tinychannels_bus::channel::ChannelSendError,
    > {
        let expected = self.expected_scope(feature);
        let intent = |payload| {
            let mut intent = tinychannels_bus::ChannelOutboundIntent {
                idempotency_key: "irc-conformance".into(),
                channel_id: "irc".into(),
                conversation_id: "#ops".into(),
                channel_ref: Some(expected.channel.clone()),
                conversation_ref: Some(expected.conversation.clone()),
                payload,
                ..Default::default()
            };
            if feature == ConformanceFeature::ReplyAddressing {
                intent.reply_to_id = Some("parent-message".into());
            }
            if feature == ConformanceFeature::ThreadAddressing {
                intent.thread_id = Some("thread-channel".into());
            }
            intent
        };
        use tinychannels_bus::{ContentPart, MediaKind, MediaReference, OutboundContentPart};
        let rich = |part| tinychannels_bus::OutboundPayload::Parts {
            parts: vec![OutboundContentPart::Content { part }],
        };
        let media = |kind| MediaReference {
            opaque_id: Some(format!("irc-{kind:?}")),
            kind,
            ..Default::default()
        };
        let text = || tinychannels_bus::OutboundPayload::Text {
            text: "conformance".into(),
        };

        if feature == ConformanceFeature::Text {
            let outcome =
                crate::channel::ChannelAdapter::send_outcome(self.channel.as_ref(), intent(text()))
                    .await?;
            let line = self.peer.lock().await.next_line().await;
            assert_eq!(line, "PRIVMSG #ops :conformance");
            self.observations.lock().unwrap().push(WireObservation {
                feature,
                operation: "send".into(),
                channel: expected.channel,
                conversation: expected.conversation,
                reply_to_id: None,
                thread_id: None,
                payload_kinds: vec!["text".into()],
                platform_message_ids: Vec::new(),
                request: serde_json::json!({"line": line}),
            });
            return Ok(Some(outcome));
        }

        match feature {
            ConformanceFeature::Typing => {
                crate::channel::ChannelTyping::start_typing(self.channel.as_ref(), "#ops").await?;
            }
            ConformanceFeature::Reactions => {
                crate::channel::LegacyChannelReaction::send_reaction(
                    self.channel.as_ref(),
                    &expected.channel,
                    &expected.conversation,
                    "message-1",
                    &tinychannels_bus::channel::ChannelReaction::default(),
                    false,
                )
                .await?;
            }
            ConformanceFeature::Edit => {
                crate::channel::ChannelEdit::edit(
                    self.channel.as_ref(),
                    &tinychannels_bus::channel::MessageReceipt::default(),
                    intent(text()),
                )
                .await?;
            }
            ConformanceFeature::Delete => {
                crate::channel::ChannelDelete::delete(
                    self.channel.as_ref(),
                    &tinychannels_bus::channel::MessageReceipt::default(),
                )
                .await?;
            }
            ConformanceFeature::StreamingDraft => {
                crate::channel::ChannelStreamingDraft::upsert_draft(
                    self.channel.as_ref(),
                    None,
                    intent(text()),
                )
                .await?;
            }
            ConformanceFeature::VoiceIn
            | ConformanceFeature::InboundReactions
            | ConformanceFeature::ReadReceipts => {
                return Err(tinychannels_bus::channel::ChannelSendError {
                    kind: tinychannels_bus::channel::SendErrorKind::BadFormat,
                    message: "IRC has no inbound operation for this feature".into(),
                    unsupported: true,
                    ..Default::default()
                });
            }
            feature => {
                let payload = match feature {
                    ConformanceFeature::Text
                    | ConformanceFeature::ReplyAddressing
                    | ConformanceFeature::ThreadAddressing => text(),
                    ConformanceFeature::Image => rich(ContentPart::Image {
                        media: media(MediaKind::Image),
                        caption: None,
                        alt_text: None,
                    }),
                    ConformanceFeature::Video => rich(ContentPart::Video {
                        media: media(MediaKind::Video),
                        caption: None,
                        duration_ms: None,
                        thumbnail: None,
                    }),
                    ConformanceFeature::Audio => rich(ContentPart::Audio {
                        media: media(MediaKind::Audio),
                        codec: None,
                        duration_ms: None,
                        title: None,
                        transcript: None,
                    }),
                    ConformanceFeature::File => rich(ContentPart::File {
                        media: media(MediaKind::Document),
                        file_name: None,
                        caption: None,
                    }),
                    ConformanceFeature::Sticker => rich(ContentPart::Sticker {
                        media: media(MediaKind::Image),
                        pack_id: None,
                        sticker_id: None,
                        emoji: None,
                        alt_text: None,
                    }),
                    ConformanceFeature::Location => rich(ContentPart::Location {
                        latitude: 1.0,
                        longitude: 2.0,
                        label: None,
                        address: None,
                        accuracy_meters: None,
                        live_until_unix_ms: None,
                    }),
                    ConformanceFeature::Contact => rich(ContentPart::Contact {
                        display_name: "Conformance".into(),
                        phone_numbers: Vec::new(),
                        emails: Vec::new(),
                        organization: None,
                    }),
                    ConformanceFeature::Buttons => rich(ContentPart::Buttons {
                        rows: vec![vec![tinychannels_bus::ButtonAction {
                            id: "action".into(),
                            label: "Go".into(),
                            style: None,
                            disabled: false,
                        }]],
                    }),
                    ConformanceFeature::Poll => rich(ContentPart::Poll {
                        question: "Question".into(),
                        options: vec!["Yes".into()],
                        multiple_choice: false,
                        closes_at_unix_ms: None,
                        provider_poll_id: None,
                    }),
                    ConformanceFeature::Presentation => rich(ContentPart::Presentation {
                        blocks: serde_json::json!({"kind": "approval"}),
                    }),
                    ConformanceFeature::NativeData => {
                        tinychannels_bus::OutboundPayload::NativeChannelData {
                            data: serde_json::json!({"action": "create_thread"}),
                        }
                    }
                    ConformanceFeature::VoiceOut => tinychannels_bus::OutboundPayload::Voice {
                        voice: tinychannels_bus::OutboundVoice::Media {
                            media: media(MediaKind::Audio),
                            codec: Some("ogg_opus".into()),
                            duration_ms: Some(1000),
                        },
                    },
                    ConformanceFeature::VoiceSynthesis => {
                        tinychannels_bus::OutboundPayload::Voice {
                            voice: tinychannels_bus::OutboundVoice::Synthesize {
                                text: "speak".into(),
                                voice: None,
                            },
                        }
                    }
                    _ => unreachable!("handled as an optional operation above"),
                };
                let outcome = crate::channel::ChannelAdapter::send_outcome(
                    self.channel.as_ref(),
                    intent(payload),
                )
                .await?;
                return Ok(Some(outcome));
            }
        }
        Ok(None)
    }
}

#[tokio::test]
async fn shared_conformance_checks_irc_text_against_a_real_peer_without_fabricated_receipts() {
    let (listener, port) = bind().await;
    let acceptor = acceptor(false);
    let config = tinychannels_bus::ChannelsConfig {
        irc: Some(tinychannels_bus::config::IrcConfig {
            server: "127.0.0.1".into(),
            port,
            nickname: "bot".into(),
            username: Some("botuser".into()),
            channels: vec![],
            allowed_users: vec!["*".into()],
            server_password: None,
            nickserv_password: None,
            sasl_password: None,
            verify_tls: Some(false),
        }),
        ..Default::default()
    };
    let channel = crate::factory::build_long_tail_channel(
        &config,
        &crate::factory::DefaultHttpClients,
        "irc",
    )
    .expect("configured IRC adapter from the production factory");
    let (tx, _rx) = mpsc::unbounded_channel();
    let sink = InboundSink(tx);
    let mut start = tokio::spawn({
        let channel = channel.clone();
        async move { crate::channel::ChannelAdapter::start(channel.as_ref(), &sink).await }
    });
    let mut peer = tokio::select! {
        result = &mut start => panic!("IRC adapter start returned before accepting a peer: {result:?}"),
        peer = Peer::accept(&listener, &acceptor) => peer,
    };
    peer.expect("NICK bot").await;
    peer.expect("USER botuser 0 * :OpenHuman").await;
    peer.say(":srv 001 bot :Welcome").await;

    let fixture = IrcConformanceFixture {
        channel: channel.clone(),
        peer: tokio::sync::Mutex::new(peer),
        observations: std::sync::Mutex::new(Vec::new()),
    };
    let results = check_all_features(&fixture).await;
    assert_eq!(results.len(), ConformanceFeature::ALL.len());
    let failures = results
        .iter()
        .filter_map(|(feature, result)| {
            result
                .as_ref()
                .err()
                .map(|error| format!("{feature:?}: {error}"))
        })
        .collect::<Vec<_>>();
    assert!(
        failures.is_empty(),
        "IRC conformance failures: {failures:#?}"
    );
    crate::channel::ChannelAdapter::stop(channel.as_ref())
        .await
        .unwrap();
    tokio::time::timeout(GUARD, start)
        .await
        .expect("stop must join the public listener")
        .expect("listener task must not panic")
        .expect("listener must stop cleanly");
}

struct RejectingSink;

#[async_trait::async_trait]
impl crate::channel::ChannelInboundSink for RejectingSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(
        &self,
        _: tinychannels_bus::ChannelIngress,
    ) -> Result<(), crate::channel::ChannelSendError> {
        Err(crate::channel::ChannelSendError {
            kind: crate::channel::SendErrorKind::Transient,
            message: "private sink failure detail".into(),
            retryable: true,
            ..Default::default()
        })
    }
}

#[tokio::test]
async fn public_adapter_start_owns_typed_listener_until_stop_joins_it() {
    let (listener, port) = bind().await;
    let acceptor = acceptor(false);
    let channel = Arc::new(IrcChannel::new(IrcChannelConfig {
        channels: vec!["#ops".into()],
        allowed_users: vec!["alice".into()],
        ..config(port)
    }));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sink = InboundSink(tx);
    let mut start = tokio::spawn({
        let channel = channel.clone();
        let sink = sink.clone();
        async move { crate::channel::ChannelAdapter::start(channel.as_ref(), &sink).await }
    });
    // `start` must establish and own its listener instead of returning as the
    // current adapter macro does. Racing the real TLS peer against completion
    // avoids relying on scheduler timing or an arbitrary startup delay.
    let mut peer = tokio::select! {
        result = &mut start => panic!("IRC adapter start returned before accepting a peer: {result:?}"),
        peer = Peer::accept(&listener, &acceptor) => peer,
    };
    peer.expect("NICK bot").await;
    peer.expect("USER botuser 0 * :OpenHuman").await;
    peer.say(":srv 001 bot :Welcome").await;
    peer.expect("JOIN #ops").await;

    peer.say(":mallory!m@host PRIVMSG #ops :denied").await;
    peer.say(":alice!a@host PRIVMSG #ops :hello native listener")
        .await;
    let ingress = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("public start should publish admitted IRC messages")
        .expect("public start should keep the supplied sink connected");
    let tinychannels_bus::ChannelIngress::Inbound(envelope) = ingress else {
        panic!("IRC message should use the inbound envelope route");
    };
    assert_eq!(envelope.channel.id, "irc");
    assert_eq!(
        envelope.conversation.kind,
        tinychannels_bus::ConversationKind::Channel
    );
    assert_eq!(envelope.conversation.id, "#ops");
    assert_eq!(envelope.sender.id, "alice");
    assert!(envelope.message_id.starts_with("irc_"));
    assert!(envelope.text.contains("hello native listener"));
    assert!(
        rx.try_recv().is_err(),
        "denied senders must not reach the sink"
    );

    crate::channel::ChannelAdapter::stop(channel.as_ref())
        .await
        .unwrap();
    tokio::time::timeout(GUARD, start)
        .await
        .expect("stop must join the public listener")
        .expect("listener task must not panic")
        .expect("listener must stop cleanly");
    let final_line = peer.next_line().await;
    if final_line.starts_with("QUIT ") {
        assert!(
            peer.next_line().await.is_empty(),
            "QUIT must close the IRC socket"
        );
    } else {
        assert!(final_line.is_empty(), "stop must close the IRC socket");
    }
}

#[tokio::test]
async fn public_adapter_cleans_up_after_sink_failure_and_can_restart() {
    let (listener, port) = bind().await;
    let acceptor = acceptor(false);
    let channel = Arc::new(IrcChannel::new(IrcChannelConfig {
        channels: vec!["#ops".into()],
        allowed_users: vec!["alice".into()],
        ..config(port)
    }));
    let mut start = tokio::spawn({
        let channel = channel.clone();
        async move { crate::channel::ChannelAdapter::start(channel.as_ref(), &RejectingSink).await }
    });
    let mut peer = tokio::select! {
        result = &mut start => panic!("IRC start returned before accepting a peer: {result:?}"),
        peer = Peer::accept(&listener, &acceptor) => peer,
    };
    peer.expect("NICK bot").await;
    peer.expect("USER botuser 0 * :OpenHuman").await;
    peer.say(":srv 001 bot :Welcome").await;
    peer.expect("JOIN #ops").await;
    peer.say(":alice!a@host PRIVMSG #ops :reject this event")
        .await;

    let error = tokio::time::timeout(GUARD, &mut start)
        .await
        .expect("a failing event sink should stop the listener")
        .expect("listener task must not panic")
        .expect_err("the typed sink rejected the admitted event");
    assert_eq!(error.kind, crate::channel::SendErrorKind::Transient);
    assert!(error.retryable);
    assert_eq!(error.message, "IRC listener stopped");
    assert!(!error.message.contains("private sink failure detail"));
    assert!(
        peer.is_closed().await,
        "the failed listener must close its socket"
    );

    // A second start on the same adapter proves the failed run released its
    // single-listener registration and dropped the old writer.
    let (tx, _rx) = mpsc::unbounded_channel();
    let sink = InboundSink(tx);
    let mut restarted = tokio::spawn({
        let channel = channel.clone();
        let sink = sink.clone();
        async move { crate::channel::ChannelAdapter::start(channel.as_ref(), &sink).await }
    });
    let mut peer = tokio::select! {
        result = &mut restarted => panic!("restarted listener returned before accepting a peer: {result:?}"),
        peer = Peer::accept(&listener, &acceptor) => peer,
    };
    peer.expect("NICK bot").await;
    peer.expect("USER botuser 0 * :OpenHuman").await;
    peer.say(":srv 001 bot :Welcome").await;
    peer.expect("JOIN #ops").await;
    crate::channel::ChannelAdapter::stop(channel.as_ref())
        .await
        .unwrap();
    tokio::time::timeout(GUARD, restarted)
        .await
        .expect("stop must join the restarted listener")
        .expect("listener task must not panic")
        .expect("restarted listener must stop cleanly");
}

#[tokio::test]
async fn public_adapter_stop_cancels_tls_handshake_and_releases_listener() {
    let (listener, port) = bind().await;
    let channel = Arc::new(IrcChannel::new(config(port)));
    let (tx, _rx) = mpsc::unbounded_channel();
    let sink = InboundSink(tx);
    let mut start = tokio::spawn({
        let channel = channel.clone();
        let sink = sink.clone();
        async move { crate::channel::ChannelAdapter::start(channel.as_ref(), &sink).await }
    });
    let (_socket, _) = tokio::time::timeout(GUARD, listener.accept())
        .await
        .expect("listener should establish TCP before the TLS handshake")
        .unwrap();
    crate::channel::ChannelAdapter::stop(channel.as_ref())
        .await
        .unwrap();
    tokio::time::timeout(GUARD, &mut start)
        .await
        .expect("stop must cancel an in-progress TLS handshake")
        .expect("listener task must not panic")
        .expect("listener cancellation should be clean");
    let registration = channel
        .begin_listener()
        .expect("cancellation must release single-listener ownership");
    drop(registration);
    channel.end_listener();
}

#[tokio::test]
async fn public_adapter_rejects_a_second_concurrent_listener() {
    let (listener, port) = bind().await;
    let acceptor = acceptor(false);
    let channel = Arc::new(IrcChannel::new(config(port)));
    let (tx, _rx) = mpsc::unbounded_channel();
    let sink = InboundSink(tx);
    let mut start = tokio::spawn({
        let channel = channel.clone();
        let sink = sink.clone();
        async move { crate::channel::ChannelAdapter::start(channel.as_ref(), &sink).await }
    });
    let _peer = Peer::accept(&listener, &acceptor).await;
    let second = crate::channel::ChannelAdapter::start(channel.as_ref(), &sink).await;
    let error = second.expect_err("the provider must have one listener owner");
    assert!(
        !error.retryable,
        "concurrent listener conflict is not transient"
    );

    crate::channel::ChannelAdapter::stop(channel.as_ref())
        .await
        .unwrap();
    tokio::time::timeout(GUARD, &mut start)
        .await
        .expect("stop must join the first listener")
        .expect("listener task must not panic")
        .expect("listener cancellation should be clean");
}

#[tokio::test]
async fn listen_registers_authenticates_and_forwards_allowed_privmsgs() {
    let (listener, port) = bind().await;
    let acceptor = acceptor(false);
    let channel = Arc::new(IrcChannel::new(IrcChannelConfig {
        channels: vec!["#ops".into(), "&local".into()],
        allowed_users: vec!["alice".into()],
        server_password: Some("srv-pw".into()),
        nickserv_password: Some("ns-pw".into()),
        sasl_password: Some("sasl-pw".into()),
        ..config(port)
    }));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sink = InboundSink(tx);
    let listen = tokio::spawn({
        let channel = channel.clone();
        let sink = sink.clone();
        async move { crate::channel::ChannelAdapter::start(channel.as_ref(), &sink).await }
    });

    let mut peer = Peer::accept(&listener, &acceptor).await;
    peer.expect("CAP REQ :sasl").await;
    peer.expect("PASS srv-pw").await;
    peer.expect("NICK bot").await;
    peer.expect("USER botuser 0 * :OpenHuman").await;

    peer.say("PING :tok-1").await;
    peer.expect("PONG :tok-1").await;

    // A CAP line that does not mention SASL changes nothing.
    peer.say(":srv CAP * LS :multi-prefix").await;
    peer.say(":srv CAP * ACK :sasl").await;
    peer.expect("AUTHENTICATE PLAIN").await;
    peer.say("AUTHENTICATE +").await;
    peer.expect(&format!(
        "AUTHENTICATE {}",
        encode_sasl_plain("bot", "sasl-pw")
    ))
    .await;
    peer.say(":srv 903 bot :SASL authentication successful")
        .await;
    peer.expect("CAP END").await;

    // Messages before RPL_WELCOME are dropped, as are blank lines.
    peer.say(":alice!a@host PRIVMSG #ops :too early").await;
    peer.say("").await;

    peer.say(":srv 433 * bot :Nickname is already in use").await;
    peer.expect("NICK bot_").await;

    peer.say(":srv 001 bot_ :Welcome").await;
    peer.expect("PRIVMSG NickServ :IDENTIFY ns-pw").await;
    peer.expect("JOIN #ops").await;
    peer.expect("JOIN &local").await;

    // Service bots and users outside the allowlist are ignored.
    peer.say(":NickServ!s@services PRIVMSG bot_ :You are now identified")
        .await;
    peer.say(":ChanServ!s@services PRIVMSG #ops :topic").await;
    peer.say(":mallory!m@host PRIVMSG #ops :let me in").await;
    peer.say(":srv NOTICE * :unrelated").await;
    peer.say(":Alice!a@host PRIVMSG #ops :hello channel").await;
    peer.say(":alice!a@host PRIVMSG bot_ :hello dm").await;

    let channel_msg = recv(&mut rx).await;
    assert_eq!(channel_msg.sender.id, "Alice");
    assert_eq!(channel_msg.conversation.id, "#ops");
    assert_eq!(
        channel_msg.text,
        format!("{IRC_STYLE_PREFIX}<Alice> hello channel")
    );
    assert_eq!(channel_msg.channel.id, "irc");
    assert!(channel_msg.message_id.starts_with("irc_"));

    let dm = recv(&mut rx).await;
    assert_eq!(dm.sender.id, "alice");
    assert_eq!(dm.conversation.id, "alice");
    assert_eq!(dm.text, format!("{IRC_STYLE_PREFIX}hello dm"));
    assert_ne!(dm.message_id, channel_msg.message_id);

    // Typed send writes through the connection the adapter listener registered.
    crate::channel::ChannelAdapter::send_outcome(
        channel.as_ref(),
        tinychannels_bus::ChannelOutboundIntent {
            channel_id: "irc".into(),
            conversation_id: "#ops".into(),
            idempotency_key: "irc-socket-send".into(),
            payload: tinychannels_bus::OutboundPayload::Text {
                text: "line one\n\nline two".into(),
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    peer.expect("PRIVMSG #ops :line one").await;
    peer.expect("PRIVMSG #ops :line two").await;

    peer.say(":srv 464 * :Password incorrect").await;
    let error = tokio::time::timeout(GUARD, listen)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.message, "IRC listener stopped");
    assert!(rx.try_recv().is_err(), "no other message was forwarded");
}

#[tokio::test]
async fn listen_continues_without_sasl_when_the_server_refuses_it() {
    let (listener, port) = bind().await;
    let acceptor = acceptor(false);
    let channel = IrcChannel::new(IrcChannelConfig {
        username: None,
        sasl_password: Some("sasl-pw".into()),
        ..config(port)
    });
    let (tx, _rx) = mpsc::unbounded_channel();
    let sink = InboundSink(tx);
    let listen =
        tokio::spawn(async move { crate::channel::ChannelAdapter::start(&channel, &sink).await });

    let mut peer = Peer::accept(&listener, &acceptor).await;
    peer.expect("CAP REQ :sasl").await;
    peer.expect("NICK bot").await;
    peer.expect("USER bot 0 * :OpenHuman").await;

    peer.say(":srv CAP * NAK :sasl").await;
    peer.expect("CAP END").await;

    // SASL is no longer pending, so a credential request is not answered:
    // the next line the client writes is the PONG.
    peer.say("AUTHENTICATE +").await;
    peer.say("PING").await;
    peer.expect("PONG :").await;

    peer.close().await;
    let error = tokio::time::timeout(GUARD, listen)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.message, "IRC listener stopped");
}

#[tokio::test]
async fn listen_ends_sasl_on_failure_and_stops_when_the_receiver_is_gone() {
    let (listener, port) = bind().await;
    let acceptor = acceptor(false);
    let channel = IrcChannel::new(IrcChannelConfig {
        sasl_password: Some("sasl-pw".into()),
        ..config(port)
    });
    let (tx, rx) = mpsc::unbounded_channel();
    let sink = InboundSink(tx);
    let listen =
        tokio::spawn(async move { crate::channel::ChannelAdapter::start(&channel, &sink).await });

    let mut peer = Peer::accept(&listener, &acceptor).await;
    peer.expect("CAP REQ :sasl").await;
    peer.expect("NICK bot").await;
    peer.expect("USER botuser 0 * :OpenHuman").await;

    peer.say(":srv 904 bot :SASL authentication failed").await;
    peer.expect("CAP END").await;

    // Without NickServ or channels configured, welcome sends nothing; the
    // next client line is the PONG.
    peer.say(":srv 001 bot :Welcome").await;
    peer.say("PING :after-welcome").await;
    peer.expect("PONG :after-welcome").await;

    drop(rx);
    peer.say(":alice!a@host PRIVMSG #ops :anyone there?").await;
    tokio::time::timeout(GUARD, listen)
        .await
        .unwrap()
        .unwrap()
        .expect_err("the typed sink reports closure to the adapter");
}

#[tokio::test]
async fn send_fails_before_listen_has_connected() {
    let channel = IrcChannel::new(config(1));
    let error = crate::channel::ChannelAdapter::send_outcome(
        &channel,
        tinychannels_bus::ChannelOutboundIntent {
            channel_id: "irc".into(),
            conversation_id: "#ops".into(),
            idempotency_key: "irc-offline-send".into(),
            payload: tinychannels_bus::OutboundPayload::Text {
                text: "hello".into(),
            },
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.message, "IRC is not connected");
    assert!(!error.sent_before_error);
}

#[tokio::test]
async fn stop_recovers_a_poisoned_listener_lock_and_signals_cancellation() {
    let channel = std::sync::Arc::new(IrcChannel::new(config(1)));
    let (cancel, mut cancelled) = tokio::sync::watch::channel(false);
    *channel.listener_cancel.lock().unwrap() = Some(cancel);

    let poisoner = channel.clone();
    let poisoned = std::thread::spawn(move || {
        let _guard = poisoner.listener_cancel.lock().unwrap();
        panic!("poison listener cancellation state for recovery test");
    })
    .join()
    .is_err();
    assert!(poisoned, "the test must exercise a genuinely poisoned lock");

    crate::channel::ChannelAdapter::stop(channel.as_ref())
        .await
        .expect("stop recovers the lock and cancels the owned listener");
    tokio::time::timeout(GUARD, cancelled.changed())
        .await
        .expect("stop should publish cancellation")
        .expect("the cancellation sender remains registered");
    assert!(*cancelled.borrow());
}

#[tokio::test]
async fn typed_send_uses_the_scoped_conversation_on_an_active_connection() {
    let (listener, port) = bind().await;
    let acceptor = acceptor(false);
    let channel = Arc::new(IrcChannel::new(config(port)));
    let (tx, _rx) = mpsc::unbounded_channel();
    let sink = InboundSink(tx);
    let mut start = tokio::spawn({
        let channel = channel.clone();
        async move { crate::channel::ChannelAdapter::start(channel.as_ref(), &sink).await }
    });
    let mut peer = tokio::select! {
        result = &mut start => panic!("IRC start returned before connecting: {result:?}"),
        peer = Peer::accept(&listener, &acceptor) => peer,
    };
    peer.expect("NICK bot").await;
    peer.expect("USER botuser 0 * :OpenHuman").await;
    peer.say(":srv 001 bot :Welcome").await;

    let outcome = crate::channel::ChannelAdapter::send_outcome(
        channel.as_ref(),
        tinychannels_bus::ChannelOutboundIntent {
            idempotency_key: "irc-connected-send".into(),
            channel_id: "irc".into(),
            conversation_id: "#fallback".into(),
            conversation_ref: Some(tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Channel,
                id: "#scoped".into(),
                ..Default::default()
            }),
            payload: tinychannels_bus::OutboundPayload::Text {
                text: "hello from the typed adapter".into(),
            },
            ..Default::default()
        },
    )
    .await
    .expect("the active connection should accept typed text");
    assert!(outcome.receipt.parts.is_empty());
    peer.expect("PRIVMSG #scoped :hello from the typed adapter")
        .await;

    crate::channel::ChannelAdapter::stop(channel.as_ref())
        .await
        .unwrap();
    tokio::time::timeout(GUARD, start)
        .await
        .expect("stop should join the typed listener")
        .unwrap()
        .unwrap();
    // The listener stop already closed its TLS side, so asking the peer to
    // send a second close_notify can race into ConnectionAborted here.
    drop(peer);
}

#[tokio::test]
async fn health_check_completes_a_tls12_handshake_and_quits() {
    let (listener, port) = bind().await;
    let acceptor = acceptor(true);
    let server = tokio::spawn(async move {
        let mut peer = Peer::accept(&listener, &acceptor).await;
        peer.next_line().await
    });

    assert!(IrcChannel::new(config(port)).health_check().await);
    let line = tokio::time::timeout(GUARD, server).await.unwrap().unwrap();
    assert_eq!(line, "QUIT :health check");
}

#[tokio::test]
async fn verified_tls_rejects_an_untrusted_certificate() {
    let (listener, port) = bind().await;
    let acceptor = acceptor(false);
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            // The client aborts the handshake; the server-side error is expected.
            let _ = acceptor.accept(socket).await;
        }
    });

    let channel = IrcChannel::new(IrcChannelConfig {
        verify_tls: true,
        ..config(port)
    });
    assert!(!channel.health_check().await);
    let (tx, _rx) = mpsc::unbounded_channel();
    let sink = InboundSink(tx);
    assert!(
        crate::channel::ChannelAdapter::start(&channel, &sink)
            .await
            .is_err()
    );
    tokio::time::timeout(GUARD, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn health_check_reports_an_unreachable_server() {
    let (listener, port) = bind().await;
    drop(listener);
    assert!(!IrcChannel::new(config(port)).health_check().await);
}
