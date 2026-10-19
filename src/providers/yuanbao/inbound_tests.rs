use super::*;

fn cfg(bot_id: &str) -> YuanbaoConfig {
    let mut c = YuanbaoConfig::default();
    c.app_key = "ak".into();
    c.ws_domain = "wss://x".into();
    c.token = "tok".into();
    c.bot_id = bot_id.into();
    c.bot_name = "bot".into();
    c.dm_access = "open".into();
    c.group_access = "open".into();
    c
}

fn ctx_with(msg: InboundMessage) -> PipelineCtx {
    PipelineCtx {
        msg,
        source: Source::default(),
        text: String::new(),
        image_urls: Vec::new(),
        is_at_bot: false,
        is_owner_command: false,
        kind: MessageKind::Text,
    }
}

#[tokio::test]
async fn dedup_skips_repeat() {
    let state = PipelineState::new(&cfg("bot1"), "bot1".into());
    let mw = DedupMw;
    let msg = InboundMessage {
        msg_id: "m1".into(),
        ..Default::default()
    };
    let mut c1 = ctx_with(msg.clone());
    assert!(matches!(
        mw.process(&state, &mut c1).await,
        MwResult::Continue
    ));
    let mut c2 = ctx_with(msg);
    assert!(matches!(
        mw.process(&state, &mut c2).await,
        MwResult::Skip(_)
    ));
}

#[tokio::test]
async fn access_guard_open() {
    let state = PipelineState::new(&cfg("bot1"), "bot1".into());
    let mw = AccessGuardMw;
    let mut c = ctx_with(InboundMessage {
        from_account: "alice".into(),
        ..Default::default()
    });
    c.source.is_group = false;
    c.source.from_account = "alice".into();
    assert!(matches!(
        mw.process(&state, &mut c).await,
        MwResult::Continue
    ));
}

#[tokio::test]
async fn full_dm_dispatch() {
    let mut config = cfg("bot1");
    config.group_at_required = false;
    let state = PipelineState::new(&config, "bot1".into());
    let pipeline = InboundPipeline::new(state);
    let msg = InboundMessage {
        from_account: "alice".into(),
        to_account: "bot1".into(),
        msg_id: "hi".into(),
        msg_body: vec![MsgBodyElement {
            msg_type: "TIMTextElem".into(),
            msg_content: MsgContent {
                text: Some("hello".into()),
                ..Default::default()
            },
        }],
        ..Default::default()
    };
    let body = crate::providers::yuanbao::proto::encode_msg_body_element(&msg.msg_body[0]);
    // Synthesize an InboundMessagePush from scratch:
    use crate::providers::yuanbao::proto;
    let mut buf = Vec::new();
    let put_str = |fnum: u32, s: &str, b: &mut Vec<u8>| {
        proto::encode_varint(((fnum as u64) << 3) | 2, b);
        proto::encode_varint(s.len() as u64, b);
        b.extend_from_slice(s.as_bytes());
    };
    put_str(2, &msg.from_account, &mut buf);
    put_str(3, &msg.to_account, &mut buf);
    put_str(12, &msg.msg_id, &mut buf);
    proto::encode_varint(((13u64) << 3) | 2, &mut buf);
    proto::encode_varint(body.len() as u64, &mut buf);
    buf.extend_from_slice(&body);

    let outcome = pipeline.process(&buf).await;
    assert!(
        matches!(outcome, PipelineOutcome::Dispatch(_)),
        "got {:?}",
        outcome
    );
}

#[tokio::test]
async fn sound_element_with_a_download_url_survives_the_empty_text_filter() {
    let mut config = cfg("bot1");
    config.dm_access = "open".into();
    let pipeline = InboundPipeline::new(PipelineState::new(&config, "bot1".into()));
    let body = serde_json::json!({
        "from_account": "alice",
        "to_account": "bot1",
        "msg_id": "voice-message-7",
        "msg_body": [{
            "msg_type": "TIMSoundElem",
            "msg_content": {
                "sound": "voice-uuid-7",
                "url": "https://media.example.test/voice.ogg",
                "file_size": 321,
                "file_name": "voice.ogg"
            }
        }]
    })
    .to_string();

    match pipeline.process(body.as_bytes()).await {
        PipelineOutcome::Dispatch(ctx) => {
            assert_eq!(ctx.msg.msg_id, "voice-message-7");
            assert_eq!(ctx.kind, MessageKind::Voice);
            assert_eq!(
                ctx.msg.msg_body[0].msg_content.sound.as_deref(),
                Some("voice-uuid-7")
            );
            assert_eq!(
                ctx.msg.msg_body[0].msg_content.url.as_deref(),
                Some("https://media.example.test/voice.ogg")
            );
        }
        outcome => panic!("downloadable TIMSoundElem should dispatch, got {outcome:?}"),
    }
}

#[test]
fn pipeline_orders_trust_and_filter_stages_before_dispatch_enrichment() {
    let pipeline = InboundPipeline::new(PipelineState::new(&cfg("bot1"), "bot1".into()));
    let names: Vec<_> = pipeline.stages.iter().map(|stage| stage.name()).collect();
    assert_eq!(
        names,
        vec![
            "decode",
            "extract_fields",
            "recall_guard",
            "dedup",
            "skip_self",
            "chat_routing",
            "access_guard",
            "auto_set_home",
            "extract_content",
            "placeholder_filter",
            "owner_command",
            "build_source",
            "group_at_guard",
            "group_attribution",
            "classify_msg_type",
            "quote_context",
            "media_resolve",
        ]
    );
}

#[test]
fn dedup_evicts_oldest_at_capacity_and_expired_ids_can_be_reinserted() {
    let mut cache = DedupCache::new(2, 60);
    assert!(!cache.check_and_insert("first"));
    assert!(!cache.check_and_insert("second"));
    assert!(!cache.check_and_insert("third"));
    assert!(!cache.index.contains("first"));
    assert!(cache.check_and_insert("second"));
    cache.order.front_mut().unwrap().1 = Instant::now() - Duration::from_secs(61);
    assert!(!cache.check_and_insert("second"));
    assert_eq!(
        cache
            .order
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<Vec<_>>(),
        vec!["third", "second"]
    );
}

#[test]
fn closed_access_aliases_disable_inbound_access() {
    for policy in ["closed", "disabled", "none", "CLOSED"] {
        assert_eq!(AccessPolicy::parse(policy), AccessPolicy::Closed);
    }
    assert_eq!(AccessPolicy::parse("unknown"), AccessPolicy::Allowlist);
}

struct AbortStage;

#[async_trait]
impl Middleware for AbortStage {
    fn name(&self) -> &'static str {
        "abort"
    }
    async fn process(&self, _state: &PipelineState, _ctx: &mut PipelineCtx) -> MwResult {
        MwResult::Abort(YuanbaoError::AuthFailed("middleware rejected".into()))
    }
}

#[tokio::test]
async fn middleware_abort_preserves_error_and_malformed_json_fails_before_stages() {
    let mut pipeline = InboundPipeline::new(PipelineState::new(&cfg("bot1"), "bot1".into()));
    pipeline.stages = vec![Box::new(AbortStage)];
    match pipeline.process(b"{}").await {
        PipelineOutcome::Failed(YuanbaoError::AuthFailed(message)) => {
            assert_eq!(message, "middleware rejected")
        }
        outcome => panic!("unexpected outcome: {outcome:?}"),
    }
    assert!(matches!(
        pipeline.process(b"{invalid").await,
        PipelineOutcome::Failed(YuanbaoError::ProtoDecode(_))
    ));
}
