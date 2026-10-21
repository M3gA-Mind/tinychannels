use super::*;
use crate::providers::slack::fixtures::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, ResponseTemplate};

type Socket = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

fn envelope(kind: &str, id: &str, payload: serde_json::Value) -> Message {
    Message::Text(
        json!({"type": kind, "envelope_id": id, "payload": payload})
            .to_string()
            .into(),
    )
}

fn message_event(ts: &str, text: &str) -> serde_json::Value {
    json!({"event": {"type": "message", "channel": "C1", "ts": ts, "user": "U1", "text": text}})
}

async fn expect_ack(socket: &mut Socket, id: &str) {
    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let ack: serde_json::Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
    assert_eq!(ack, json!({"envelope_id": id}));
}

/// Serve scripted connections; returns the `ws://` URL.
async fn serve(
    script: impl Fn(usize, Socket) -> futures_util::future::BoxFuture<'static, ()>
    + Send
    + Sync
    + 'static,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let counter = AtomicUsize::new(0);
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            script(counter.fetch_add(1, Ordering::SeqCst), socket).await;
        }
    });
    url
}

async fn open_returns(server: &wiremock::MockServer, url: &str) {
    Mock::given(method("POST"))
        .and(path("/api/apps.connections.open"))
        .and(header("authorization", "Bearer app-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok(json!({"url": url}))))
        .mount(server)
        .await;
}

#[tokio::test]
async fn socket_mode_acks_envelopes_dispatches_events_and_reconnects_after_a_disconnect() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    // The first open is refused, the second points at a dead port, then it works.
    Mock::given(method("POST"))
        .and(path("/api/apps.connections.open"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"ok": false, "error": "invalid_auth"})),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/apps.connections.open"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(ok(json!({"url": "ws://127.0.0.1:1"}))),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = seen.clone();
    let url = serve(move |index, mut socket| {
        let recorded = recorded.clone();
        Box::pin(async move {
            recorded.lock().unwrap().push(index);
            if index == 0 {
                socket.send(Message::Binary(vec![1, 2, 3].into())).await.unwrap();
                socket.send(Message::Text("not json".into())).await.unwrap();
                socket.send(Message::Text(json!({"type": "hello"}).to_string().into())).await.unwrap();
                socket.send(envelope("events_api", "e1", message_event("1.1", "first"))).await.unwrap();
                expect_ack(&mut socket, "e1").await;
                let press = json!({
                    "type": "block_actions", "trigger_id": "t1", "user": {"id": "U1"},
                    "channel": {"id": "C1"}, "message": {"ts": "1.1"}, "actions": [{"action_id": "go"}],
                });
                socket.send(envelope("interactive", "e2", press)).await.unwrap();
                expect_ack(&mut socket, "e2").await;
                socket.send(envelope("slash_commands", "e3", json!({}))).await.unwrap();
                expect_ack(&mut socket, "e3").await;
                socket.send(Message::Text(json!({"type": "disconnect", "reason": "refresh_requested"}).to_string().into())).await.unwrap();
            } else {
                socket.send(envelope("events_api", "e4", message_event("2.2", "second"))).await.unwrap();
                expect_ack(&mut socket, "e4").await;
                let _ = socket.close(None).await;
            }
        })
    })
    .await;
    open_returns(&server, &url).await;
    let events = Arc::new(RecordingEvents {
        deliver: false,
        ..Default::default()
    });
    let channel = channel_for(&server, &["U1"]).with_events(events.clone());
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let listener = tokio::spawn(async move { channel.listen_socket(&tx, "app-token").await });
    assert_eq!(rx.recv().await.unwrap().content, "first");
    assert_eq!(rx.recv().await.unwrap().content, "second");
    listener.abort();
    assert!(
        seen.lock().unwrap().len() >= 2,
        "a disconnect request must trigger a reconnect"
    );
    assert_eq!(
        events.events().len(),
        1,
        "the button press became a callback event"
    );
    assert_eq!(events.inbound().len(), 2);
}

#[tokio::test]
async fn socket_mode_stops_when_the_receiver_is_gone() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    let url = serve(|_, mut socket| {
        Box::pin(async move {
            socket
                .send(envelope("events_api", "e1", message_event("1.1", "orphan")))
                .await
                .unwrap();
            let _ = socket.next().await;
        })
    })
    .await;
    open_returns(&server, &url).await;
    let channel = channel_for(&server, &["*"]);
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    drop(rx);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        channel.listen_socket(&tx, "app-token"),
    )
    .await;
    assert!(result.unwrap().is_ok());
}

#[tokio::test]
async fn a_server_that_hangs_up_mid_ack_just_ends_the_connection() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    let url = serve(|_, mut socket| {
        Box::pin(async move {
            socket
                .send(envelope("events_api", "e1", message_event("1.1", "x")))
                .await
                .unwrap();
            drop(socket);
        })
    })
    .await;
    open_returns(&server, &url).await;
    let channel = channel_for(&server, &["*"]);
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let result = tokio::time::timeout(
        Duration::from_millis(300),
        channel.listen_socket(&tx, "app-token"),
    )
    .await;
    assert!(result.is_err(), "the listener keeps reconnecting");
    while rx.try_recv().is_ok() {}
}

#[tokio::test]
async fn polling_delivers_oldest_first_filters_and_advances_the_cursor() {
    let server = mock_api().await;
    mount_auth(&server, "bot").await;
    let polls = Arc::new(AtomicUsize::new(0));
    let counter = polls.clone();
    Mock::given(method("GET"))
        .and(path("/api/conversations.history"))
        .and(query_param("channel", "room"))
        .respond_with(move |request: &wiremock::Request| {
            let oldest = request
                .url
                .query_pairs()
                .find(|(k, _)| k == "oldest")
                .map(|(_, v)| v.into_owned());
            match counter.fetch_add(1, Ordering::SeqCst) {
                0 => ResponseTemplate::new(200).set_body_string("invalid json"),
                1 => ResponseTemplate::new(200).set_body_json(json!({})),
                2 => {
                    assert_eq!(oldest, None);
                    ResponseTemplate::new(200).set_body_json(json!({"messages": [
                        {"ts": "003", "user": "alice", "text": "second", "thread_ts": "001"},
                        {"ts": "002", "user": "alice", "text": "first"},
                        {"ts": "001", "user": "alice", "text": ""},
                        {"ts": "001", "user": "mallory", "text": "blocked"},
                        {"ts": "001", "user": "bot", "text": "own message"},
                        {}
                    ]}))
                }
                _ => {
                    assert_eq!(oldest.as_deref(), Some("003"));
                    ResponseTemplate::new(200).set_body_json(json!({"messages": [
                        {"ts": "004", "user": "alice", "text": "receiver closed"},
                        {"ts": "003", "user": "alice", "text": "duplicate"}
                    ]}))
                }
            }
        })
        .mount(&server)
        .await;
    let channel = SlackChannel::new("t".into(), Some("room".into()), vec!["alice".into()])
        .with_api_base(format!("{}/api", server.uri()))
        .with_timing(Duration::from_millis(5), Duration::from_millis(5));
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let task = tokio::spawn(async move { channel.listen_polling(&tx, "room").await });
    let first = rx.recv().await.unwrap();
    let second = rx.recv().await.unwrap();
    assert_eq!(
        (first.content.as_str(), second.content.as_str()),
        ("first", "second")
    );
    assert_eq!(second.thread_ts.as_deref(), Some("001"));
    assert_eq!(first.reply_target, "room");
    drop(rx);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );
}

#[tokio::test]
async fn polling_survives_transport_errors() {
    let channel = SlackChannel::new("t".into(), Some("room".into()), vec!["*".into()])
        .with_api_base("http://127.0.0.1:1/api")
        .with_timing(Duration::from_millis(5), Duration::from_millis(5));
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        channel.listen_polling(&tx, "room"),
    )
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn adapter_start_delivers_socket_mode_messages_to_the_typed_sink() {
    use crate::channel::{ChannelAdapter, ChannelInboundSink, ChannelSendError};
    use tinychannels_bus::ChannelIngress;

    struct Sink(tokio::sync::mpsc::UnboundedSender<ChannelIngress>);

    #[async_trait::async_trait]
    impl ChannelInboundSink for Sink {
        async fn push(&self, ingress: ChannelIngress) -> Result<(), ChannelSendError> {
            self.0.send(ingress).map_err(|_| ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Transient,
                message: "test sink closed".into(),
                retryable: true,
                ..Default::default()
            })
        }
    }

    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    let url = serve(|_, mut socket| {
        Box::pin(async move {
            socket
                .send(envelope(
                    "events_api",
                    "e-typed-1",
                    json!({"event": {
                        "type": "message",
                        "channel": "C1",
                        "channel_type": "channel",
                        "ts": "9.4",
                        "thread_ts": "9.1",
                        "user": "U1",
                        "text": "typed Slack ingress"
                    }}),
                ))
                .await
                .unwrap();
            expect_ack(&mut socket, "e-typed-1").await;
            let _ = socket.close(None).await;
        })
    })
    .await;
    open_returns(&server, &url).await;
    let channel = channel_for(&server, &["U1"]).with_app_token(Some("app-token".into()));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = Sink(tx);
    let start = ChannelAdapter::start(&channel, &sink);
    tokio::pin!(start);
    let ingress = tokio::select! {
        result = &mut start => panic!("Slack adapter start returned before typed delivery: {result:?}"),
        ingress = tokio::time::timeout(Duration::from_secs(2), rx.recv()) => {
            ingress.expect("typed Slack ingress timeout").expect("typed sink closed")
        }
    };
    let ChannelIngress::Inbound(envelope) = ingress else {
        panic!("expected a Slack inbound envelope, got {ingress:?}");
    };
    assert_eq!(envelope.channel.id, "slack");
    assert_eq!(envelope.conversation.id, "C1");
    assert_eq!(envelope.conversation.thread_id.as_deref(), Some("9.1"));
    assert_eq!(envelope.message_id, "slack_C1_9.4");
    assert_eq!(envelope.text, "typed Slack ingress");
    ChannelAdapter::stop(&channel).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), &mut start)
        .await
        .expect("Slack adapter observes stop")
        .unwrap();
}
