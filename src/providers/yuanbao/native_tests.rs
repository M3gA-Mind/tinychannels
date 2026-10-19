use super::*;
use crate::factory::{
    DefaultHttpClients, build_long_tail_channel, build_long_tail_channel_with_host,
};
use crate::host::ChannelHostBuilder;
use crate::host::{MediaScope, MediaStoreRequest, StoredMedia};
use crate::providers::native::outbound::{plan, target_of};
use async_trait::async_trait;
use parking_lot::Mutex;
use tinychannels_bus::channel::{ChannelOutboundMessage, ContentPart, MediaKind, MediaReference};

#[derive(Default)]
struct MediaRepo {
    last_scope: Mutex<Option<MediaScope>>,
}

#[async_trait]
impl MediaRepository for MediaRepo {
    async fn store_media(&self, _request: MediaStoreRequest) -> anyhow::Result<MediaReference> {
        anyhow::bail!("not used")
    }

    async fn read_media(
        &self,
        scope: MediaScope,
        opaque_id: String,
        _max_bytes: u64,
    ) -> anyhow::Result<StoredMedia> {
        *self.last_scope.lock() = Some(scope.clone());
        Ok(StoredMedia {
            media: MediaReference {
                opaque_id: Some(opaque_id),
                kind: MediaKind::Image,
                ..Default::default()
            },
            content_type: "image/png".into(),
            file_name: Some("image.png".into()),
            bytes: b"image-bytes".to_vec(),
        })
    }
}

fn channel() -> YuanbaoChannel {
    let mut config = crate::providers::yuanbao::YuanbaoConfig::default();
    config.app_key = "app".into();
    config.token = "token".into();
    config.ws_domain = "wss://example.invalid".into();
    YuanbaoChannel::new(config).unwrap()
}

#[test]
fn media_is_advertised_only_when_scoped_host_storage_is_injected() {
    let without_host = channel();
    let caps = NativeProvider::declared_capabilities(&without_host);
    assert!(!caps.content.image.outbound);
    assert!(!caps.content.file.outbound);
    assert!(!caps.voice_out.supported);

    let repository = Arc::new(MediaRepo::default());
    let with_host = channel().with_media_repository(repository);
    let caps = NativeProvider::declared_capabilities(&with_host);
    assert!(caps.content.image.outbound);
    assert!(caps.content.file.outbound);
    assert!(!caps.content.image.inbound);
    assert!(!caps.voice_out.supported);
}

#[test]
fn inbound_voice_is_advertised_with_store_for_the_supplied_typed_sink() {
    let store_only = channel().with_voice_media_store(Arc::new(
        crate::providers::native::fixtures::StubVoice::default(),
    ));
    assert!(
        NativeProvider::declared_capabilities(&store_only)
            .voice_in
            .supported
    );

    let host_ready = store_only;
    let capabilities = NativeProvider::declared_capabilities(&host_ready);
    assert!(capabilities.voice_in.supported);
    assert!(
        capabilities
            .voice_in
            .accepted_codecs
            .contains(&"ogg_opus".into())
    );
}

#[test]
fn configured_media_limit_does_not_exceed_yuanbao_wire_size() {
    let mut config = crate::providers::yuanbao::YuanbaoConfig::default();
    config.app_key = "app".into();
    config.token = "token".into();
    config.ws_domain = "wss://example.invalid".into();
    config.max_media_mb = u32::MAX;
    let channel = YuanbaoChannel::new(config)
        .unwrap()
        .with_media_repository(Arc::new(MediaRepo::default()));

    let limits = NativeProvider::limits(&channel);
    assert_eq!(limits.file_bytes, u64::from(u32::MAX));
    let caps = NativeProvider::declared_capabilities(&channel);
    assert_eq!(caps.content.image.max_size_bytes, Some(u64::from(u32::MAX)));
    assert_eq!(caps.content.file.max_size_bytes, Some(u64::from(u32::MAX)));
}

#[tokio::test]
async fn media_planning_reads_the_exact_host_scope_and_opaque_reference() {
    let repository = Arc::new(MediaRepo::default());
    let channel = channel().with_media_repository(repository.clone());
    let message = ChannelOutboundMessage {
        channel_id: "yuanbao".into(),
        conversation_id: "group-7".into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "yuanbao".into(),
            account_id: Some("account-2".into()),
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            id: "group-7".into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-9".into()),
        idempotency_key: "send-3".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Content {
            part: ContentPart::Image {
                media: MediaReference {
                    opaque_id: Some("opaque-4".into()),
                    kind: MediaKind::Image,
                    ..Default::default()
                },
                caption: Some("caption".into()),
                alt_text: None,
            },
        }],
    };
    let target = target_of(&message, "yuanbao", "Yuanbao").unwrap();

    let planned = plan(&channel, &message, &target).await.unwrap();
    assert_eq!(planned.len(), 1);
    let Step::File(file) = &planned[0] else {
        panic!("image must be prepared as one media upload")
    };
    assert_eq!(file.name, "image.png");
    assert_eq!(file.bytes, b"image-bytes");
    assert_eq!(file.caption.as_deref(), Some("caption"));
    assert_eq!(
        repository.last_scope.lock().as_ref(),
        Some(&tinychannels_bus::HostVoiceScope {
            configured_instance: "account-2".into(),
            channel: tinychannels_bus::ChannelRef {
                id: "yuanbao".into(),
                account_id: Some("account-2".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                id: "group-7".into(),
                ..Default::default()
            },
            turn_id: "turn-9".into(),
        })
    );
}

#[test]
fn host_aware_factory_injects_media_without_changing_legacy_factory_behavior() {
    let mut yuanbao = crate::providers::yuanbao::YuanbaoConfig::default();
    yuanbao.app_key = "app".into();
    yuanbao.token = "token".into();
    yuanbao.ws_domain = "wss://example.invalid".into();
    let config = tinychannels_bus::ChannelsConfig {
        yuanbao: Some(yuanbao),
        ..Default::default()
    };
    let without_host = build_long_tail_channel(&config, &DefaultHttpClients, "yuanbao").unwrap();
    assert!(!without_host.declared_capabilities().content.image.outbound);

    let repository = Arc::new(MediaRepo::default());
    let host = ChannelHostBuilder::new()
        .media_repository(repository)
        .build();
    let with_host =
        build_long_tail_channel_with_host(&config, &host, &DefaultHttpClients, "yuanbao").unwrap();
    assert!(with_host.declared_capabilities().content.image.outbound);
    assert!(with_host.declared_capabilities().content.file.outbound);
}

#[test]
fn upload_failure_and_disconnected_send_have_distinct_delivery_certainty() {
    let upload = upload_not_delivered();
    assert!(upload.retryable);
    assert!(!upload.sent_before_error);

    let disconnected =
        final_send_error(crate::providers::yuanbao::errors::YuanbaoError::NotConnected);
    assert!(disconnected.retryable);
    assert!(!disconnected.sent_before_error);

    let uncertain = final_send_error(crate::providers::yuanbao::errors::YuanbaoError::Timeout(
        "private request id must not escape".into(),
    ));
    assert!(!uncertain.retryable);
    assert!(uncertain.sent_before_error);
    assert_eq!(uncertain.message, "Yuanbao send outcome is uncertain");
}
