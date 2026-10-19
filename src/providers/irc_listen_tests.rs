//! Socket-level tests for the IRC channel: a local TLS listener plays the IRC
//! server, scripting the registration, SASL and message exchange line by line.

use super::*;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

/// Hang guard only: every exchange below is driven by explicit reads, so a
/// healthy run never comes near it.
const GUARD: Duration = Duration::from_secs(10);

fn acceptor(tls12_only: bool) -> TlsAcceptor {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert = include_bytes!("../../tests/fixtures/tls/cert.der");
    let key = include_bytes!("../../tests/fixtures/tls/key.der");
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

async fn recv(rx: &mut mpsc::Receiver<ChannelMessage>) -> ChannelMessage {
    tokio::time::timeout(GUARD, rx.recv())
        .await
        .unwrap()
        .expect("listener dropped its sender")
}

#[derive(Clone)]
struct InboundSink(mpsc::UnboundedSender<tinychannels_bus::ChannelIngress>);

#[async_trait::async_trait]
impl crate::channel::ChannelInboundSink for InboundSink {
    async fn push(
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
    let (tx, mut rx) = mpsc::channel(8);
    let listen = tokio::spawn({
        let channel = channel.clone();
        async move { channel.listen(tx).await }
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
    assert_eq!(channel_msg.sender, "Alice");
    assert_eq!(channel_msg.reply_target, "#ops");
    assert_eq!(
        channel_msg.content,
        format!("{IRC_STYLE_PREFIX}<Alice> hello channel")
    );
    assert_eq!(channel_msg.channel, "irc");
    assert!(channel_msg.id.starts_with("irc_"));

    let dm = recv(&mut rx).await;
    assert_eq!(dm.sender, "alice");
    assert_eq!(dm.reply_target, "alice");
    assert_eq!(dm.content, format!("{IRC_STYLE_PREFIX}hello dm"));
    assert_ne!(dm.id, channel_msg.id);

    // send() writes through the connection listen() registered.
    channel
        .send(&SendMessage::new("line one\n\nline two", "#ops"))
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
    assert!(error.to_string().contains("password mismatch"), "{error}");
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
    let (tx, _rx) = mpsc::channel(1);
    let listen = tokio::spawn(async move { channel.listen(tx).await });

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
    assert!(error.to_string().contains("closed by server"), "{error}");
}

#[tokio::test]
async fn listen_ends_sasl_on_failure_and_stops_when_the_receiver_is_gone() {
    let (listener, port) = bind().await;
    let acceptor = acceptor(false);
    let channel = IrcChannel::new(IrcChannelConfig {
        sasl_password: Some("sasl-pw".into()),
        ..config(port)
    });
    let (tx, rx) = mpsc::channel(1);
    let listen = tokio::spawn(async move { channel.listen(tx).await });

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
        .expect("a closed receiver ends listen cleanly");
}

#[tokio::test]
async fn send_fails_before_listen_has_connected() {
    let channel = IrcChannel::new(config(1));
    let error = channel
        .send(&SendMessage::new("hello", "#ops"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not connected"), "{error}");
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
    let (tx, _rx) = mpsc::channel(1);
    assert!(channel.listen(tx).await.is_err());
    tokio::time::timeout(GUARD, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn health_check_reports_an_unreachable_server() {
    let (listener, port) = bind().await;
    drop(listener);
    assert!(!IrcChannel::new(config(port)).health_check().await);
}
