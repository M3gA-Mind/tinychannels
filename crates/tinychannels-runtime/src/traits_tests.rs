use super::*;

#[test]
fn channel_message_clone_preserves_fields() {
    let message = ChannelMessage {
        id: "42".into(),
        sender: "alice".into(),
        reply_target: "alice".into(),
        content: "ping".into(),
        channel: "dummy".into(),
        timestamp: 999,
        topic_id: None,
        reply_to_id: None,
        thread_ts: None,
        sender_name: None,
    };

    let cloned = message.clone();
    assert_eq!(cloned.id, "42");
    assert_eq!(cloned.sender, "alice");
    assert_eq!(cloned.reply_target, "alice");
    assert_eq!(cloned.content, "ping");
    assert_eq!(cloned.channel, "dummy");
    assert_eq!(cloned.timestamp, 999);
}

#[test]
fn send_message_generates_deterministic_idempotency_key() {
    let message = SendMessage::new("hello", "alice")
        .in_thread(Some("thread-1".to_string()))
        .with_deterministic_idempotency_key("telegram");
    let again = SendMessage::new("hello", "alice")
        .in_thread(Some("thread-1".to_string()))
        .with_deterministic_idempotency_key("telegram");

    assert_eq!(message.idempotency_key, again.idempotency_key);
    assert!(
        message
            .idempotency_key
            .as_deref()
            .unwrap()
            .starts_with("legacy-send:telegram:")
    );
}

#[test]
fn explicit_send_message_idempotency_key_is_preserved() {
    let message = SendMessage::new("hello", "alice")
        .with_idempotency_key("caller-key")
        .with_deterministic_idempotency_key("telegram");

    assert_eq!(message.idempotency_key.as_deref(), Some("caller-key"));
}

use super::*;

struct DummyChannel;

#[async_trait]
impl Channel for DummyChannel {
    fn name(&self) -> &str {
        "dummy"
    }

    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        tx.send(ChannelMessage {
            id: "1".into(),
            sender: "tester".into(),
            reply_target: "tester".into(),
            content: "hello".into(),
            channel: "dummy".into(),
            timestamp: 123,
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))
    }
}

struct Recording(std::sync::Mutex<Vec<String>>);

#[async_trait]
impl Channel for Recording {
    fn name(&self) -> &str {
        "recording"
    }
    async fn send(&self, m: &SendMessage) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(format!("send:{}", m.content));
        Ok(())
    }
    async fn listen(&self, _tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        Ok(())
    }
    async fn send_idempotent(&self, m: &SendMessage) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(format!("idem:{}", m.content));
        Ok(())
    }
}

#[tokio::test]
async fn default_trait_methods_return_success() {
    let channel = DummyChannel;
    assert!(channel.health_check().await);
    assert!(channel.start_typing("bob").await.is_ok());
    assert!(channel.stop_typing("bob").await.is_ok());
    assert!(channel.send(&SendMessage::new("hi", "bob")).await.is_ok());
    assert_eq!(channel.proactive_target(), None);
    assert_eq!(channel.begin_typing_refresh("bob", 4).await.unwrap(), None);
    assert!(channel.end_typing_refresh("h").await.is_ok());
    assert!(!channel.supports_reactions());
}

#[tokio::test]
async fn default_draft_methods_return_success() {
    let channel = DummyChannel;
    assert!(!channel.supports_draft_updates());
    assert!(
        channel
            .send_draft(&SendMessage::new("d", "bob"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(channel.update_draft("bob", "m", "t").await.is_ok());
    assert!(channel.finalize_draft("bob", "m", "t", None).await.is_ok());
}

#[tokio::test]
async fn listen_sends_message_to_channel() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    DummyChannel.listen(tx).await.unwrap();
    let received = rx.recv().await.expect("message should be sent");
    assert_eq!(received.sender, "tester");
    assert_eq!(received.channel, "dummy");
}

#[tokio::test]
async fn send_with_outbound_intent_routes_through_send_idempotent() {
    let channel = Recording(Default::default());
    channel
        .send_with_outbound_intent(&SendMessage::new("x", "bob"))
        .await
        .unwrap();
    assert_eq!(*channel.0.lock().unwrap(), vec!["idem:x".to_string()]);
}

#[tokio::test]
async fn send_idempotent_defaults_to_send() {
    let channel = Recording(Default::default());
    // Dummy has no override; Recording overrides, so check the default via a dyn object.
    let dummy: Box<dyn Channel> = Box::new(DummyChannel);
    assert!(
        dummy
            .send_idempotent(&SendMessage::new("x", "b"))
            .await
            .is_ok()
    );
    drop(channel);
}

#[test]
fn capabilities_default_to_configured_instance_facts() {
    let capabilities = DummyChannel.capabilities();
    assert_eq!(
        capabilities.version,
        tinychannels_bus::ChannelCapabilities::VERSION
    );
    assert!(capabilities.content.text.inbound);
    assert!(capabilities.content.text.outbound);
    assert!(
        capabilities
            .actions
            .supported
            .iter()
            .any(|action| action == "send")
    );
    assert!(!capabilities.progressive_edits);
}
