use super::super::*;
use super::*;
use async_trait::async_trait;

struct Capabilities;
#[async_trait]
impl SpeechSynthesizer for Capabilities {
    async fn synthesize(&self, _: SpeechRequest) -> anyhow::Result<SpeechResult> {
        Ok(SpeechResult::default())
    }
}
#[async_trait]
impl ReactionGate for Capabilities {
    async fn should_react(&self, _: ReactionQuery) -> anyhow::Result<ReactionDecision> {
        Ok(ReactionDecision::default())
    }
}
#[async_trait]
impl ConversationStore for Capabilities {
    async fn history(&self, _: &str, _: usize) -> anyhow::Result<Vec<ConversationMessage>> {
        Ok(vec![])
    }
    async fn append(&self, _: &str, _: ConversationMessage) -> anyhow::Result<()> {
        Ok(())
    }
}
#[async_trait]
impl Memory for Capabilities {
    async fn recall(&self, _: &str, _: usize) -> anyhow::Result<Vec<crate::context::MemoryEntry>> {
        Ok(vec![])
    }
}
#[async_trait]
impl EventSink for Capabilities {
    async fn publish(&self, _: &str, _: &str, _: serde_json::Value) -> anyhow::Result<()> {
        Ok(())
    }
}
impl LifecycleRegistry for Capabilities {
    fn register_shutdown(&self, _: &str, _: ShutdownHook) {}
}
#[async_trait]
impl RunLedger for Capabilities {
    async fn upsert_run(&self, _: RunUpsert) -> anyhow::Result<()> {
        Ok(())
    }
    async fn append_event(&self, _: RunEventAppend) -> anyhow::Result<()> {
        Ok(())
    }
    async fn record_telemetry(&self, _: RunTelemetry) -> anyhow::Result<()> {
        Ok(())
    }
}
#[async_trait]
impl AllowlistStore for Capabilities {
    async fn persist_allowed_identity(&self, _: &str, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

#[test]
fn builder_preserves_capability_identity_and_last_registration_wins() {
    let fixture = Arc::new(Capabilities);
    let speech: Arc<dyn SpeechSynthesizer> = fixture.clone();
    let reaction: Arc<dyn ReactionGate> = fixture.clone();
    let conversations: Arc<dyn ConversationStore> = fixture.clone();
    let memory: Arc<dyn Memory> = fixture.clone();
    let events: Arc<dyn EventSink> = fixture.clone();
    let lifecycle: Arc<dyn LifecycleRegistry> = fixture.clone();
    let ledger: Arc<dyn RunLedger> = fixture.clone();
    let allowlist: Arc<dyn AllowlistStore> = fixture;
    let host = ChannelHostBuilder::new()
        .synthesizer(Arc::new(Capabilities))
        .synthesizer(speech.clone())
        .reactions(reaction.clone())
        .conversations(conversations.clone())
        .memory(memory.clone())
        .events(events.clone())
        .lifecycle(lifecycle.clone())
        .ledger(ledger.clone())
        .allowlist(allowlist.clone())
        .build();
    assert!(Arc::ptr_eq(&host.synthesizer().unwrap(), &speech));
    assert!(Arc::ptr_eq(&host.reactions().unwrap(), &reaction));
    assert!(Arc::ptr_eq(&host.conversations().unwrap(), &conversations));
    assert!(Arc::ptr_eq(&host.memory().unwrap(), &memory));
    assert!(Arc::ptr_eq(&host.events().unwrap(), &events));
    assert!(Arc::ptr_eq(&host.lifecycle().unwrap(), &lifecycle));
    assert!(Arc::ptr_eq(&host.ledger().unwrap(), &ledger));
    assert!(Arc::ptr_eq(&host.allowlist().unwrap(), &allowlist));
    let caps = host.capabilities();
    assert_eq!(caps, HostCapabilities::probe(host.as_ref()));
    assert!(caps.tts && caps.reaction_gate && caps.conversation_store && caps.memory_recall);
    assert!(caps.event_sink && caps.lifecycle && caps.run_ledger && caps.allowlist_store);
    assert!(!caps.turn_dispatch && !caps.stt && !caps.approvals);
    assert!(!caps.is_lean());
}
