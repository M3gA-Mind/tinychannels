//! Owned provider listener, lifecycle hooks and ordered inbound forwarding.
use crate::host::HostChannels;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tinybus::{Error, Result};
use tinychannels::channel::ChannelAdapter;
use tinychannels::host::{LifecycleRegistry, ShutdownHook};
use tinychannels::{Channel, ChannelHostBuilder};
use tinychannels_bus::{
    ChannelSupervisionPolicy, ChannelsConfig, HostEventPublication, HostTranscriptionRequest,
};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

struct AbortOnDrop<T>(JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

enum ProviderOperation {
    SendTyped(tinychannels_bus::ChannelOutboundMessage),
    NativeDraft(
        Option<tinychannels_bus::channel::MessageReceipt>,
        tinychannels_bus::ChannelOutboundIntent,
    ),
    Interaction(tinychannels_bus::ChannelInteraction),
}

#[cfg(test)]
pub(crate) fn typed_test_message(
    channel_id: &str,
    text: &str,
    conversation_id: &str,
) -> tinychannels_bus::ChannelOutboundMessage {
    let intent = tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: format!("test:{channel_id}:{conversation_id}:{text}"),
        channel_id: channel_id.to_owned(),
        conversation_id: conversation_id.to_owned(),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            id: conversation_id.to_owned(),
            ..Default::default()
        }),
        payload: tinychannels_bus::OutboundPayload::Text {
            text: text.to_owned(),
        },
        ..Default::default()
    };
    tinychannels_bus::ChannelOutboundMessage::from_intent(&intent)
}

struct TypingRefreshTask {
    recipient: String,
    cancel: watch::Sender<bool>,
    task: JoinHandle<()>,
}

type TypingRefreshes = HashMap<String, TypingRefreshTask>;

const MAX_TYPING_REFRESH_RECIPIENTS: usize = 64;
const MAX_TYPING_REFRESH_INTERVAL_SECS: u64 = 60;

async fn dispatch_interaction(
    channel: Option<Arc<dyn Channel>>,
    native: Option<NativePorts>,
    interaction: tinychannels_bus::ChannelInteraction,
    typing: &mut TypingRefreshes,
) -> Result<tinychannels_bus::ChannelInteractionResult> {
    if let Some(native) = &native {
        match &interaction {
            tinychannels_bus::ChannelInteraction::Describe => {
                return Ok(crate::interaction::describe_adapter(
                    native.adapter.as_ref(),
                    true,
                ));
            }
            tinychannels_bus::ChannelInteraction::DescribeV2 => {
                return Ok(crate::interaction::describe_adapter(
                    native.adapter.as_ref(),
                    false,
                ));
            }
            _ => {}
        }
        if let Some(result) = crate::interaction::dispatch_native_draft(
            native.adapter.as_ref(),
            native.drafts.as_ref(),
            interaction.clone(),
        )
        .await
        {
            return result;
        }
    }
    use tinychannels_bus::ChannelInteraction as Op;
    match interaction {
        describe @ (Op::Describe | Op::DescribeV2) => {
            if let Some(native) = &native {
                Ok(crate::interaction::describe_adapter(
                    native.adapter.as_ref(),
                    matches!(describe, Op::Describe),
                ))
            } else {
                crate::interaction::dispatch(
                    channel
                        .as_deref()
                        .ok_or_else(|| Error::failed("legacy provider port is unavailable"))?,
                    describe,
                )
                .await
            }
        }
        Op::StartTypingRefresh {
            recipient,
            interval_secs,
        } => {
            if interval_secs == 0 || interval_secs > MAX_TYPING_REFRESH_INTERVAL_SECS {
                return Err(Error::failed(
                    "typing refresh interval is outside its limit",
                ));
            }
            if typing.len() >= MAX_TYPING_REFRESH_RECIPIENTS {
                return Err(Error::failed("typing refresh capacity is full"));
            }
            let (cancel, mut canceled) = watch::channel(false);
            let refresh_channel = channel.clone();
            let refresh_native = native.clone();
            let refresh_recipient = recipient.clone();
            let interval = Duration::from_secs(interval_secs);
            let task = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = async { let _ = canceled.wait_for(|value| *value).await; } => break,
                        _ = tokio::time::sleep(interval) => {
                            let refresh = async {
                                if let Some(native) = &refresh_native {
                                    native.typing.start_typing(&refresh_recipient).await.map_err(|error| anyhow::anyhow!(error.message))
                                } else if let Some(channel) = &refresh_channel {
                                    channel.start_typing(&refresh_recipient).await
                                } else {
                                    anyhow::bail!("provider typing port is unavailable")
                                }
                            };
                            tokio::select! {
                                biased;
                                _ = async { let _ = canceled.wait_for(|value| *value).await; } => break,
                                result = refresh => if let Err(error) = result {
                                tracing::debug!("provider typing refresh failed: {error}");
                                }
                            }
                        }
                    }
                }
            });
            let handle = loop {
                let candidate = uuid::Uuid::new_v4().simple().to_string();
                if !typing.contains_key(&candidate) {
                    break candidate;
                }
            };
            typing.insert(
                handle.clone(),
                TypingRefreshTask {
                    recipient,
                    cancel,
                    task,
                },
            );
            Ok(tinychannels_bus::ChannelInteractionResult::TypingRefresh { handle })
        }
        Op::StopTypingRefresh { handle } => {
            stop_typing_refresh(&handle, typing).await;
            Ok(tinychannels_bus::ChannelInteractionResult::Completed)
        }
        Op::StartTyping { recipient } => {
            if let Some(native) = &native {
                native
                    .typing
                    .start_typing(&recipient)
                    .await
                    .map_err(|_| Error::failed("provider typing operation failed"))?;
                Ok(tinychannels_bus::ChannelInteractionResult::Completed)
            } else {
                crate::interaction::dispatch(
                    channel
                        .as_deref()
                        .ok_or_else(|| Error::failed("legacy provider port is unavailable"))?,
                    Op::StartTyping { recipient },
                )
                .await
            }
        }
        Op::StopTyping { recipient } => {
            if let Some(native) = &native {
                native
                    .typing
                    .stop_typing(&recipient)
                    .await
                    .map_err(|_| Error::failed("provider typing operation failed"))?;
                Ok(tinychannels_bus::ChannelInteractionResult::Completed)
            } else {
                crate::interaction::dispatch(
                    channel
                        .as_deref()
                        .ok_or_else(|| Error::failed("legacy provider port is unavailable"))?,
                    Op::StopTyping { recipient },
                )
                .await
            }
        }
        Op::SendReaction {
            channel: channel_ref,
            conversation,
            provider_message_id,
            reaction,
            remove,
        } => {
            if let Some(native) = &native {
                native
                    .reactions
                    .send_reaction(
                        &channel_ref,
                        &conversation,
                        &provider_message_id,
                        &reaction,
                        remove,
                    )
                    .await
                    .map_err(|_| Error::failed("provider reaction failed"))?;
                Ok(tinychannels_bus::ChannelInteractionResult::Completed)
            } else {
                crate::interaction::dispatch(
                    channel
                        .as_deref()
                        .ok_or_else(|| Error::failed("legacy provider port is unavailable"))?,
                    Op::SendReaction {
                        channel: channel_ref,
                        conversation,
                        provider_message_id,
                        reaction,
                        remove,
                    },
                )
                .await
            }
        }
        Op::EditMessage { receipt, intent } => {
            if let Some(native) = &native {
                let receipt = native
                    .edits
                    .edit(&receipt, intent)
                    .await
                    .map_err(|_| Error::failed("provider edit failed"))?;
                Ok(tinychannels_bus::ChannelInteractionResult::Sent(Box::new(
                    tinychannels_bus::channel::ChannelSendOutcome {
                        receipt,
                        error: None,
                    },
                )))
            } else {
                crate::interaction::dispatch(
                    channel
                        .as_deref()
                        .ok_or_else(|| Error::failed("legacy provider port is unavailable"))?,
                    Op::EditMessage { receipt, intent },
                )
                .await
            }
        }
        Op::DeleteMessage { receipt } => {
            if let Some(native) = &native {
                native
                    .deletes
                    .delete(&receipt)
                    .await
                    .map_err(|_| Error::failed("provider delete failed"))?;
                Ok(tinychannels_bus::ChannelInteractionResult::Completed)
            } else {
                crate::interaction::dispatch(
                    channel
                        .as_deref()
                        .ok_or_else(|| Error::failed("legacy provider port is unavailable"))?,
                    Op::DeleteMessage { receipt },
                )
                .await
            }
        }
        interaction => {
            crate::interaction::dispatch(
                channel
                    .as_deref()
                    .ok_or_else(|| Error::failed("legacy provider port is unavailable"))?,
                interaction,
            )
            .await
        }
    }
}

async fn stop_typing_refresh(handle: &str, typing: &mut TypingRefreshes) {
    if let Some(refresh) = typing.remove(handle) {
        refresh.cancel.send_replace(true);
        if let Err(error) = refresh.task.await {
            tracing::debug!(%handle, recipient = %refresh.recipient, "typing refresh task join failed: {error}");
        }
    }
}

async fn stop_all_typing_refreshes(typing: &mut TypingRefreshes) {
    let handles = typing.keys().cloned().collect::<Vec<_>>();
    for handle in handles {
        stop_typing_refresh(&handle, typing).await;
    }
}

struct SendCommand {
    operation: ProviderOperation,
    reply: oneshot::Sender<Result<tinychannels_bus::ChannelInteractionResult>>,
    started: oneshot::Sender<()>,
}

#[derive(Debug)]
pub(crate) enum RunningSendError {
    NotDispatched(String),
    AfterDispatch(String),
}

impl std::fmt::Display for RunningSendError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotDispatched(message) | Self::AfterDispatch(message) => {
                formatter.write_str(message)
            }
        }
    }
}

/// The stable account authority durable media is scoped by.
///
/// Most providers have it from configuration alone, so it is fixed when they
/// start. A provider that only learns it from its authenticated session (a
/// linked WhatsApp Web device) binds itself as the source and the scope is
/// read from it on every use: unknown until linking, withdrawn for good if the
/// session later reports another account, never taken from a mutable label.
#[derive(Clone, Default)]
pub(crate) struct MediaScope(Arc<MediaScopeInner>);

#[derive(Default)]
struct MediaScopeInner {
    fixed: Option<String>,
    source: std::sync::OnceLock<Arc<dyn Channel>>,
}

impl From<Option<String>> for MediaScope {
    fn from(scope: Option<String>) -> Self {
        Self::fixed(scope)
    }
}

impl std::fmt::Debug for MediaScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The scope names an account authority; never print it.
        formatter.write_str("MediaScope(..)")
    }
}

impl MediaScope {
    pub(crate) fn fixed(scope: Option<String>) -> Self {
        Self(Arc::new(MediaScopeInner {
            fixed: scope,
            source: std::sync::OnceLock::new(),
        }))
    }

    /// Resolve the scope from `channel`'s authenticated identity from now on.
    pub(crate) fn bind_source(&self, channel: Arc<dyn Channel>) {
        let _ = self.0.source.set(channel);
    }

    /// The scope now, if the account is known.
    pub(crate) fn current(&self) -> Option<String> {
        if let Some(fixed) = &self.0.fixed {
            return Some(fixed.clone());
        }
        let channel = self.0.source.get()?;
        let identity = channel.media_account_identity()?;
        media_scope_digest(
            tinychannels_bus::provider_id(channel.name()),
            &["linked-session".to_owned(), identity],
        )
    }
}

fn media_scope_digest(provider: &str, identity: &[String]) -> Option<String> {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"tinychannels-media-account-v1\0");
    digest.update((provider.len() as u64).to_le_bytes());
    digest.update(provider.as_bytes());
    for part in identity {
        digest.update((part.len() as u64).to_le_bytes());
        digest.update(part.as_bytes());
    }
    Some(format!(
        "{provider}:media-v1:{}",
        hex::encode(digest.finalize())
    ))
}

/// Native hooks registered while the selected provider starts.
#[derive(Default)]
pub(crate) struct Hooks {
    hooks: StdMutex<Vec<ShutdownHook>>,
    configured_instance: StdMutex<Option<String>>,
    media_account_scope: StdMutex<MediaScope>,
}
impl LifecycleRegistry for Hooks {
    fn register_shutdown(&self, _name: &str, hook: ShutdownHook) {
        self.hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(hook);
    }
}
impl Hooks {
    pub(crate) fn host(
        self: &Arc<Self>,
        callbacks: HostChannels,
        configured_instance: String,
        media_account_scope: impl Into<MediaScope>,
    ) -> Arc<dyn tinychannels::host::ChannelHost> {
        let media_account_scope = media_account_scope.into();
        *self
            .configured_instance
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(configured_instance.clone());
        *self
            .media_account_scope
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = media_account_scope.clone();
        ChannelHostBuilder::new()
            .lifecycle(self.clone())
            .transcriber(Arc::new(ModuleTranscriber(
                callbacks.clone(),
                configured_instance.clone(),
            )))
            .synthesizer(Arc::new(ModuleSynthesizer(
                callbacks.clone(),
                configured_instance.clone(),
            )))
            .voice_media_store(Arc::new(ModuleVoiceMediaStore(
                callbacks.clone(),
                configured_instance.clone(),
                media_account_scope.clone(),
            )))
            .media_repository(Arc::new(ModuleMediaRepository(
                callbacks.clone(),
                configured_instance.clone(),
                media_account_scope,
            )))
            .voice_codec(Arc::new(ModuleVoiceCodec(callbacks.clone())))
            .events(Arc::new(ModuleEvents(
                callbacks.clone(),
                configured_instance,
                self.media_account_scope
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone(),
            )))
            .storage(Arc::new(ModuleStorage(callbacks.clone())))
            .allowlist(Arc::new(ModuleAllowlist(callbacks)))
            .build()
    }
    async fn drain(&self) {
        let hooks = std::mem::take(&mut *self.hooks.lock().unwrap_or_else(|e| e.into_inner()));
        for hook in hooks {
            hook().await;
        }
    }
}

pub(crate) struct ModuleVoiceCodec(pub(crate) HostChannels);

#[async_trait::async_trait]
impl tinychannels::host::VoiceCodec for ModuleVoiceCodec {
    async fn transcode(
        &self,
        request: tinychannels::host::VoiceCodecRequest,
    ) -> anyhow::Result<tinychannels::host::VoiceCodecResult> {
        use tinyvoice_bus::codec::{AudioCodec, CodecTranscodeRequest};
        let parse = |format: &str| match format {
            "wav_pcm16" | "audio/wav" => Ok(AudioCodec::WavPcm16),
            "ogg_opus" | "audio/ogg" => Ok(AudioCodec::OggOpus),
            "m4a_aac" | "audio/mp4" => Ok(AudioCodec::M4aAac),
            "mp3" | "audio/mpeg" => Ok(AudioCodec::Mp3),
            "amr_nb" | "audio/amr" => Ok(AudioCodec::AmrNb),
            _ => anyhow::bail!("unsupported TinyVoice codec format"),
        };
        let result = self
            .0
            .transcode_voice(CodecTranscodeRequest {
                audio_base64: request.audio_base64,
                from: parse(&request.from)?,
                target: parse(&request.target)?,
            })
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let info = self
            .0
            .probe_voice(tinyvoice_bus::codec::CodecInput {
                audio_base64: result.audio_base64.clone(),
            })
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let waveform = self
            .0
            .waveform_voice(tinyvoice_bus::codec::WaveformRequest {
                audio_base64: result.audio_base64.clone(),
                buckets: 64,
            })
            .await
            .ok()
            .map(|result| {
                result
                    .peaks
                    .into_iter()
                    .map(|peak| f32::from(peak) / 255.0)
                    .collect()
            });
        Ok(tinychannels::host::VoiceCodecResult {
            audio_base64: result.audio_base64,
            duration_ms: Some(info.duration_ms),
            waveform,
        })
    }
}

struct ModuleStorage(HostChannels);

#[async_trait::async_trait]
impl tinychannels::host::StorageProvider for ModuleStorage {
    async fn documents(&self) -> anyhow::Result<Arc<dyn tinystoragedrivers_core::DocumentStore>> {
        self.0
            .documents()
            .await
            .map_err(|error| anyhow::anyhow!("host storage: {:?}", error.kind()))
    }

    async fn blobs(&self) -> anyhow::Result<Arc<dyn tinystoragedrivers_core::BlobStore>> {
        Ok(Arc::clone(self.0.storage().await?.blobs()))
    }
}

/// Return a stable, opaque media authority for the selected provider's
/// configured identity material. Mutable policy fields are deliberately
/// excluded so changing allowed users or presentation settings does not
/// orphan durable media. A credential rotation creates a new authority unless
/// the provider later exposes an authenticated stable account identifier.
pub(crate) fn configured_media_account_scope(
    instance: &str,
    config: &ChannelsConfig,
) -> Option<String> {
    let provider = tinychannels_bus::provider_id(instance);
    let identity: Vec<String> = match provider {
        "telegram" => {
            let c = config.telegram.as_ref()?;
            if c.bot_token.trim().is_empty() {
                return None;
            }
            vec![c.bot_token.clone()]
        }
        "discord" => {
            let c = config.discord.as_ref()?;
            if c.bot_token.trim().is_empty() {
                return None;
            }
            vec![c.bot_token.clone()]
        }
        "slack" => {
            let c = config.slack.as_ref()?;
            if c.bot_token.trim().is_empty() {
                return None;
            }
            // Socket Mode's app token is an optional transport credential; the
            // bot token identifies the account that owns media.
            vec![c.bot_token.clone()]
        }
        "mattermost" => {
            let c = config.mattermost.as_ref()?;
            if c.url.trim().is_empty() || c.bot_token.trim().is_empty() {
                return None;
            }
            vec![c.url.clone(), c.bot_token.clone()]
        }
        // Signal has no secret. The daemon URL and the registered account it
        // serves together identify the account that owns media, so two
        // accounts (or two daemons) never share a scope.
        "signal" => {
            let c = config.signal.as_ref()?;
            if c.http_url.trim().is_empty() || c.account.trim().is_empty() {
                return None;
            }
            vec![c.http_url.clone(), c.account.clone()]
        }
        "email" => {
            let c = config.email.as_ref()?;
            if c.imap_host.trim().is_empty()
                || c.username.trim().is_empty()
                || c.password.trim().is_empty()
            {
                return None;
            }
            vec![
                c.imap_host.clone(),
                c.imap_port.to_string(),
                c.username.clone(),
                c.password.clone(),
            ]
        }
        "whatsapp" => {
            let c = config.whatsapp.as_ref()?;
            let phone_number_id = c.phone_number_id.as_deref()?;
            let access_token = c.access_token.as_deref()?;
            if phone_number_id.trim().is_empty() || access_token.trim().is_empty() {
                return None;
            }
            vec![
                phone_number_id.to_owned(),
                access_token.to_owned(),
                c.app_secret.clone().unwrap_or_default(),
            ]
        }
        "linq" => {
            let c = config.linq.as_ref()?;
            if c.api_token.trim().is_empty() {
                return None;
            }
            vec![c.from_phone.clone(), c.api_token.clone()]
        }
        "irc" => {
            let c = config.irc.as_ref()?;
            if c.server.trim().is_empty() {
                return None;
            }
            let principal = if c
                .sasl_password
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty())
            {
                c.username.as_deref().unwrap_or(&c.nickname)
            } else if c
                .nickserv_password
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty())
            {
                &c.nickname
            } else {
                return None;
            };
            let credential = c
                .sasl_password
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .or(c.nickserv_password.as_deref())?;
            vec![
                c.server.clone(),
                c.port.to_string(),
                principal.to_owned(),
                credential.to_owned(),
            ]
        }
        "lark" => {
            let c = config.lark.as_ref()?;
            if c.app_id.trim().is_empty() || c.app_secret.trim().is_empty() {
                return None;
            }
            vec![
                c.app_id.clone(),
                c.app_secret.clone(),
                if c.use_feishu {
                    "feishu".into()
                } else {
                    "lark".into()
                },
            ]
        }
        "dingtalk" => {
            let c = config.dingtalk.as_ref()?;
            if c.client_id.trim().is_empty() || c.client_secret.trim().is_empty() {
                return None;
            }
            vec![c.client_id.clone(), c.client_secret.clone()]
        }
        "qq" => {
            let c = config.qq.as_ref()?;
            if c.app_id.trim().is_empty() || c.app_secret.trim().is_empty() {
                return None;
            }
            vec![c.app_id.clone(), c.app_secret.clone()]
        }
        "yuanbao" => {
            let c = config.yuanbao.as_ref()?;
            if c.app_key.trim().is_empty() || c.app_secret.trim().is_empty() {
                return None;
            }
            vec![
                c.api_domain.clone(),
                c.env.clone(),
                c.route_env.clone(),
                c.app_key.clone(),
                c.app_secret.clone(),
            ]
        }
        // The current iMessage configuration has no stable account identity.
        // Do not silently turn a provider name into cross-runtime authority.
        _ => return None,
    };
    media_scope_digest(provider, &identity)
}

#[derive(Clone)]
pub(crate) struct ModuleMediaRepository(
    pub(crate) HostChannels,
    pub(crate) String,
    pub(crate) MediaScope,
);

fn bind_configured_media_scope(
    scope: &mut tinychannels_bus::HostVoiceScope,
    instance: &str,
    media_account_scope: Option<&str>,
) -> anyhow::Result<()> {
    let account_scope = media_account_scope
        .ok_or_else(|| anyhow::anyhow!("configured account has no stable media identity"))?;
    bind_configured_voice_callback_scope(scope, instance);
    scope.channel.account_id = Some(account_scope.to_owned());
    Ok(())
}

fn bind_configured_voice_callback_scope(
    scope: &mut tinychannels_bus::HostVoiceScope,
    instance: &str,
) {
    scope.configured_instance = instance.to_owned();
    scope.channel.id = tinychannels_bus::provider_id(instance).to_owned();
    scope.channel.account_id = Some(instance.to_owned());
}

impl ModuleMediaRepository {
    fn bind_scope(&self, scope: &mut tinychannels::host::MediaScope) -> anyhow::Result<()> {
        bind_configured_media_scope(scope, &self.1, self.2.current().as_deref())
    }

    async fn scoped_store(&self) -> anyhow::Result<tinychannels::host::ScopedMediaStore> {
        let storage = self.0.storage().await.map_err(|error| {
            anyhow::anyhow!("host scoped storage is unavailable: {:?}", error.kind())
        })?;
        Ok(tinychannels::host::ScopedMediaStore::new(
            Arc::clone(storage.documents()),
            Arc::clone(storage.blobs()),
        ))
    }
}

#[async_trait::async_trait]
impl crate::media::MediaPersistence for ModuleMediaRepository {
    async fn store_public_media(
        &self,
        request: tinychannels::host::MediaStoreRequest,
    ) -> tinybus::Result<tinychannels_bus::MediaReference> {
        <Self as tinychannels::host::MediaRepository>::store_media(self, request)
            .await
            .map_err(|_| tinybus::Error::failed("media could not be stored"))
    }
}

#[async_trait::async_trait]
impl tinychannels::host::MediaRepository for ModuleMediaRepository {
    async fn store_media(
        &self,
        mut request: tinychannels::host::MediaStoreRequest,
    ) -> anyhow::Result<tinychannels_bus::MediaReference> {
        let scope = &mut request.metadata.scope;
        if scope.conversation.id.trim().is_empty() || scope.turn_id.trim().is_empty() {
            anyhow::bail!("media storage requires a conversation and turn identity");
        }
        self.bind_scope(scope)?;
        self.scoped_store().await?.store(request).await
    }

    async fn read_media(
        &self,
        mut scope: tinychannels::host::MediaScope,
        opaque_id: String,
        max_bytes: u64,
    ) -> anyhow::Result<tinychannels::host::StoredMedia> {
        if scope.conversation.id.trim().is_empty() || scope.turn_id.trim().is_empty() {
            anyhow::bail!("media resolution requires a conversation and turn identity");
        }
        self.bind_scope(&mut scope)?;
        self.scoped_store()
            .await?
            .read(scope, &opaque_id, max_bytes)
            .await
    }
}

struct ModuleAllowlist(HostChannels);

#[async_trait::async_trait]
impl tinychannels::host::AllowlistStore for ModuleAllowlist {
    async fn persist_allowed_identity(&self, channel: &str, identity: &str) -> anyhow::Result<()> {
        self.0
            .persist_allowed_identity(channel, identity)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }
}

struct ModuleTranscriber(HostChannels, String);

#[async_trait::async_trait]
impl tinychannels::host::Transcriber for ModuleTranscriber {
    fn name(&self) -> &str {
        "module-host"
    }

    async fn transcribe(
        &self,
        request: tinychannels::host::TranscriptionRequest,
    ) -> anyhow::Result<tinychannels::host::TranscriptionResult> {
        let result = self
            .0
            .transcribe(HostTranscriptionRequest {
                audio_base64: request.audio_base64,
                mime_type: request.mime_type,
                file_name: request.file_name,
                language: request.language,
            })
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(tinychannels::host::TranscriptionResult {
            text: result.text,
            language: result.language,
            duration_secs: result.duration_secs,
        })
    }

    async fn transcribe_scoped(
        &self,
        scope: tinychannels_bus::HostVoiceScope,
        request: tinychannels::host::TranscriptionRequest,
    ) -> anyhow::Result<tinychannels::host::TranscriptionResult> {
        validate_voice_scope(&scope)?;
        let mut scope = scope;
        bind_configured_voice_callback_scope(&mut scope, &self.1);
        let result = self
            .0
            .transcribe_voice(tinychannels_bus::HostVoiceTranscriptionRequest {
                scope,
                audio_base64: request.audio_base64,
                mime_type: request.mime_type,
                file_name: request.file_name,
                language: request.language,
            })
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(tinychannels::host::TranscriptionResult {
            text: result.text,
            language: result.language,
            duration_secs: result.duration_secs,
        })
    }
}

pub(crate) struct ModuleSynthesizer(pub(crate) HostChannels, pub(crate) String);

#[async_trait::async_trait]
impl tinychannels::host::SpeechSynthesizer for ModuleSynthesizer {
    async fn synthesize(
        &self,
        _request: tinychannels::host::SpeechRequest,
    ) -> anyhow::Result<tinychannels::host::SpeechResult> {
        anyhow::bail!("scoped voice synthesis requires a conversation and turn identity")
    }

    async fn synthesize_scoped(
        &self,
        scope: tinychannels_bus::HostVoiceScope,
        request: tinychannels::host::SpeechRequest,
    ) -> anyhow::Result<tinychannels::host::SpeechResult> {
        validate_voice_scope(&scope)?;
        let mut scope = scope;
        bind_configured_voice_callback_scope(&mut scope, &self.1);
        let result = self
            .0
            .synthesize_speech(tinychannels_bus::HostSpeechSynthesisRequest {
                scope,
                text: request.text,
                voice: request.voice,
                format: request.format,
            })
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(tinychannels::host::SpeechResult {
            audio_base64: result.audio_base64,
            mime_type: result.mime_type,
            duration_ms: result.duration_ms,
            visemes: None,
        })
    }
}

pub(crate) struct ModuleVoiceMediaStore(
    pub(crate) HostChannels,
    pub(crate) String,
    pub(crate) MediaScope,
);

#[async_trait::async_trait]
impl tinychannels::host::VoiceMediaStore for ModuleVoiceMediaStore {
    async fn store_voice(
        &self,
        request: tinychannels::host::VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        validate_voice_scope(&request.scope)?;
        let mut projection_scope = request.scope;
        bind_configured_voice_callback_scope(&mut projection_scope, &self.1);
        let mut storage_scope = projection_scope.clone();
        bind_configured_media_scope(&mut storage_scope, &self.1, self.2.current().as_deref())?;
        use base64::Engine;
        let audio = base64::engine::general_purpose::STANDARD
            .decode(request.audio_base64.as_bytes())
            .map_err(|_| anyhow::anyhow!("voice media payload is invalid"))?;
        if audio.is_empty() || audio.len() > 8 * 1024 * 1024 {
            anyhow::bail!("voice media exceeds the configured size limit");
        }
        let storage = self.0.storage().await.map_err(|error| {
            anyhow::anyhow!("host scoped storage is unavailable: {:?}", error.kind())
        })?;
        let media_store = tinychannels::host::ScopedMediaStore::new(
            Arc::clone(storage.documents()),
            Arc::clone(storage.blobs()),
        );
        let media = media_store
            .store(tinychannels::host::MediaStoreRequest {
                metadata: tinychannels::host::MediaMetadata {
                    scope: storage_scope.clone(),
                    kind: tinychannels_bus::MediaKind::Audio,
                    content_type: request.mime_type,
                    file_name: request.file_name,
                    duration_ms: request.duration_ms,
                    codec: request.codec,
                    voice_direction: Some(request.direction),
                    dedupe_key: None,
                    max_bytes: 8 * 1024 * 1024,
                },
                bytes: audio,
            })
            .await?;
        let projection = match self
            .0
            .store_voice_media(tinychannels_bus::HostStoreVoiceMediaRequest {
                scope: projection_scope.clone(),
                direction: request.direction,
                audio_base64: request.audio_base64,
                mime_type: media.content_type.clone().unwrap_or_default(),
                codec: media.codec.clone(),
                duration_ms: media.duration_ms,
                file_name: media.file_name.clone(),
                media_reference: Some(media.clone()),
            })
            .await
        {
            Ok(projection) => projection,
            Err(_) => {
                // A denied publication must not strand a Ready blob that the caller never received.
                // `delete` leaves its durable cleanup tombstone if the blob backend is transiently down.
                let _ = media_store
                    .delete(
                        storage_scope,
                        media.opaque_id.as_deref().unwrap_or_default(),
                    )
                    .await;
                return Err(anyhow::anyhow!("host voice media publication failed"));
            }
        };
        let accessible_url = projection
            .accessible_url
            .filter(|url| !url.trim().is_empty());
        let mut media = media;
        if projection.media.opaque_id == media.opaque_id {
            media.local_cache_path = projection.media.local_cache_path;
        }
        Ok(tinychannels_bus::HostStoreVoiceMediaResult {
            media,
            accessible_url,
        })
    }

    async fn read_voice(
        &self,
        mut scope: tinychannels_bus::HostVoiceScope,
        opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        validate_voice_scope(&scope)?;
        if opaque_id.trim().is_empty() {
            anyhow::bail!("voice media reference is empty");
        }
        bind_configured_voice_callback_scope(&mut scope, &self.1);
        if opaque_id.starts_with("tinychannels_media_v1_") {
            let mut storage_scope = scope;
            bind_configured_media_scope(&mut storage_scope, &self.1, self.2.current().as_deref())?;
            return read_scoped_voice_media(&self.0, storage_scope, opaque_id, 8 * 1024 * 1024)
                .await;
        }
        read_scoped_voice_media(&self.0, scope, opaque_id, 8 * 1024 * 1024).await
    }
}

fn validate_voice_scope(scope: &tinychannels_bus::HostVoiceScope) -> anyhow::Result<()> {
    if scope.conversation.id.trim().is_empty() || scope.turn_id.trim().is_empty() {
        anyhow::bail!("voice callback requires a conversation and turn identity");
    }
    Ok(())
}

async fn read_scoped_voice_media(
    host: &HostChannels,
    scope: tinychannels_bus::HostVoiceScope,
    opaque_id: String,
    max_bytes: u64,
) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
    if !opaque_id.starts_with("tinychannels_media_v1_") {
        return host
            .read_voice_media(tinychannels_bus::HostReadVoiceMediaRequest {
                scope,
                opaque_id,
                max_bytes,
            })
            .await
            .map_err(|_| anyhow::anyhow!("legacy scoped voice media is unavailable"));
    }
    let storage = host.storage().await.map_err(|error| {
        anyhow::anyhow!("host scoped storage is unavailable: {:?}", error.kind())
    })?;
    let media_store = tinychannels::host::ScopedMediaStore::new(
        Arc::clone(storage.documents()),
        Arc::clone(storage.blobs()),
    );
    let stored = media_store.read(scope, &opaque_id, max_bytes).await?;
    use base64::Engine;
    Ok(tinychannels_bus::HostReadVoiceMediaResult {
        audio_base64: base64::engine::general_purpose::STANDARD.encode(stored.bytes),
        mime_type: stored.content_type,
        file_name: stored.file_name,
    })
}

pub(crate) struct ModuleEvents(
    pub(crate) HostChannels,
    pub(crate) String,
    pub(crate) MediaScope,
);

#[async_trait::async_trait]
impl tinychannels::channel::ChannelInboundSink for ModuleEvents {
    async fn push(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> std::result::Result<(), tinychannels::channel::ChannelSendError> {
        tinychannels::host::EventSink::publish_ingress(self, ingress)
            .await
            .map(|_| ())
            .map_err(|error| tinychannels::channel::ChannelSendError {
                kind: tinychannels::channel::SendErrorKind::Unknown,
                message: error.to_string(),
                retryable: true,
                ..Default::default()
            })
    }
}

#[async_trait::async_trait]
impl tinychannels::host::EventSink for ModuleEvents {
    async fn publish(
        &self,
        domain: &str,
        kind: &str,
        payload: serde_json::Value,
    ) -> anyhow::Result<()> {
        self.0
            .publish_event(HostEventPublication {
                domain: domain.to_owned(),
                kind: kind.to_owned(),
                payload,
            })
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    async fn publish_channel_event(
        &self,
        mut event: tinychannels_bus::ChannelEvent,
    ) -> anyhow::Result<()> {
        // The module start call is the authenticated source of instance scope;
        // provider payloads cannot choose or spoof their account identity.
        event.channel.id = tinychannels_bus::provider_id(&self.1).to_owned();
        event.channel.account_id = Some(self.1.clone());
        let kind = match &event.event {
            tinychannels_bus::ChannelEventKind::Typing { .. } => "typing",
            tinychannels_bus::ChannelEventKind::ReadReceipt { .. } => "read_receipt",
            tinychannels_bus::ChannelEventKind::DeliveredReceipt { .. } => "delivered_receipt",
            tinychannels_bus::ChannelEventKind::ReactionAdded { .. } => "reaction_added",
            tinychannels_bus::ChannelEventKind::ReactionRemoved { .. } => "reaction_removed",
            tinychannels_bus::ChannelEventKind::OutboundReaction { .. } => "outbound_reaction",
            tinychannels_bus::ChannelEventKind::MessageUpdated { .. } => "message_updated",
            tinychannels_bus::ChannelEventKind::MessageDeleted { .. } => "message_deleted",
            tinychannels_bus::ChannelEventKind::ButtonCallback { .. } => "button_callback",
            tinychannels_bus::ChannelEventKind::Presence { .. } => "presence",
            tinychannels_bus::ChannelEventKind::Lifecycle { .. } => "lifecycle",
        };
        event.event_id = format!("{}:{}:{kind}", self.1, event.event_id);
        self.0
            .deliver_event(event)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    async fn publish_ingress(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> anyhow::Result<bool> {
        match ingress {
            tinychannels_bus::ChannelIngress::Inbound(mut envelope) => {
                envelope.channel.id = tinychannels_bus::provider_id(&self.1).to_owned();
                envelope.channel.account_id = Some(self.1.clone());
                for part in &mut envelope.parts {
                    let tinychannels_bus::ContentPart::VoiceNote { voice_note } = part else {
                        continue;
                    };
                    if voice_note.transcript.as_ref().is_some_and(|transcript| {
                        transcript.state == tinychannels_bus::VoiceTranscriptState::Complete
                            && !transcript.text.trim().is_empty()
                    }) {
                        // Completed provider/host transcripts are already
                        // authoritative. Avoid reading and retranscribing the
                        // media during replay.
                        continue;
                    }
                    let scope = tinychannels_bus::HostVoiceScope {
                        configured_instance: self.1.clone(),
                        channel: envelope.channel.clone(),
                        conversation: envelope.conversation.clone(),
                        turn_id: envelope.message_id.clone(),
                    };
                    let request = if let Some(opaque_id) = voice_note.media.opaque_id.clone() {
                        let mut storage_scope = scope.clone();
                        if opaque_id.starts_with("tinychannels_media_v1_") {
                            bind_configured_media_scope(
                                &mut storage_scope,
                                &self.1,
                                self.2.current().as_deref(),
                            )?;
                        }
                        match read_scoped_voice_media(
                            &self.0,
                            storage_scope,
                            opaque_id,
                            8 * 1024 * 1024,
                        )
                        .await
                        {
                            Ok(audio) => Some(tinychannels::host::TranscriptionRequest {
                                audio_base64: audio.audio_base64,
                                mime_type: Some(audio.mime_type),
                                file_name: audio.file_name,
                                language: None,
                            }),
                            Err(_) => None,
                        }
                    } else {
                        None
                    };
                    tinychannels::host::enrich_voice_note(
                        voice_note,
                        scope,
                        request,
                        Some(Arc::new(ModuleTranscriber(self.0.clone(), self.1.clone()))),
                    )
                    .await;
                }
                self.0.deliver_inbound(&self.1, envelope).await;
                Ok(true)
            }
            tinychannels_bus::ChannelIngress::Event(event) => {
                self.publish_channel_event(event).await?;
                Ok(true)
            }
        }
    }

    async fn publish_inbound(
        &self,
        message: tinychannels_bus::ChannelMessage,
    ) -> anyhow::Result<bool> {
        let mut envelope = tinychannels::inbound_envelope_from_legacy_message(&message);
        if tinychannels_bus::provider_id(&self.1) == "telegram" {
            if let Some((chat, topic)) = message.reply_target.split_once(':') {
                envelope.conversation.id = chat.to_owned();
                envelope.conversation.topic_id = Some(topic.to_owned());
                envelope.conversation.thread_id = None;
            }
            let raw_message_id = message.id.strip_prefix("telegram_").and_then(|id| {
                id.rsplit_once('_')
                    .map(|(_, message_id)| message_id.to_owned())
            });
            envelope.raw = Some(serde_json::json!({"providerMessageId": raw_message_id}));
        }
        self.publish_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }
}

/// Reconnect execution stays in the module; raw provider errors never cross callbacks.
async fn listen_with_supervision(
    channel: Arc<dyn Channel>,
    tx: mpsc::Sender<tinychannels::ChannelMessage>,
    host: HostChannels,
    policy: Option<ChannelSupervisionPolicy>,
) -> anyhow::Result<()> {
    let Some(policy) = policy else {
        return channel.listen(tx).await;
    };
    let name = channel.name().to_owned();
    let mut backoff = policy.initial_backoff_secs.max(1);
    let maximum = policy.max_backoff_secs.max(backoff);
    loop {
        let result = channel.listen(tx.clone()).await;
        if tx.is_closed() {
            return result;
        }
        host.report_status(&name, "restarting", Some("provider disconnected".into()))
            .await;
        tokio::time::sleep(
            Duration::from_secs(backoff)
                + Duration::from_millis(tinychannels::runtime::jitter_millis(backoff)),
        )
        .await;
        backoff = backoff.saturating_mul(2).min(maximum);
        host.report_status(&name, "connecting", None).await;
    }
}

async fn listen_adapter_with_supervision(
    adapter: Arc<dyn tinychannels::channel::ChannelAdapter>,
    sink: Arc<ModuleEvents>,
    host: HostChannels,
    name: String,
    policy: Option<ChannelSupervisionPolicy>,
    mut cancellation: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    if *cancellation.borrow() {
        return Ok(());
    }
    let (mut backoff, maximum) = policy
        .map(|policy| {
            let initial = policy.initial_backoff_secs.max(1);
            (Some(initial), policy.max_backoff_secs.max(initial))
        })
        .unwrap_or((None, 0));
    loop {
        let mut start = Box::pin(adapter.start(sink.as_ref()));
        let result = tokio::select! { biased;
            // Poll start first so providers can synchronously claim their
            // listener before stop is sent if cancellation raced task startup.
            result = &mut start => result.map_err(|error| anyhow::anyhow!(error.message)),
            _ = async { let _ = cancellation.wait_for(|value| *value).await; } => {
                let _ = adapter.stop().await;
                let _ = (&mut start).await;
                return Ok(());
            }
        };
        let _ = adapter.stop().await;
        let Some(delay) = backoff else {
            return result;
        };
        host.report_status(&name, "restarting", Some("provider disconnected".into()))
            .await;
        tokio::select! { biased;
            _ = async { let _ = cancellation.wait_for(|value| *value).await; } => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(delay) + Duration::from_millis(tinychannels::runtime::jitter_millis(delay))) => {}
        }
        backoff = Some(delay.saturating_mul(2).min(maximum));
        host.report_status(&name, "connecting", None).await;
        if let Err(error) = result {
            tracing::debug!(channel = %name, %error, "native listener disconnected");
        }
    }
}

/// A resource is retained until its owning task acknowledges cleanup.
#[derive(Clone)]
pub(crate) struct NativePorts {
    /// Display name used in operation errors ("Telegram", "Slack").
    provider: &'static str,
    adapter: Arc<dyn tinychannels::channel::ChannelAdapter>,
    typed_send: Arc<dyn tinychannels::channel::ChannelTypedSend>,
    typing: Arc<dyn tinychannels::channel::ChannelTyping>,
    reactions: Arc<dyn tinychannels::channel::ChannelReaction>,
    edits: Arc<dyn tinychannels::channel::ChannelEdit>,
    deletes: Arc<dyn tinychannels::channel::ChannelDelete>,
    drafts: Arc<dyn tinychannels::channel::ChannelStreamingDraft>,
    listener_mode: LongTailListenerMode,
}

/// Which listener implementation owns inbound delivery for a long-tail
/// provider. Migrated providers run their typed adapter directly; the others
/// keep their existing legacy listener until their own migration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LongTailListenerMode {
    Legacy,
    Adapter,
}

impl NativePorts {
    pub(crate) fn native(
        channel: Arc<dyn tinychannels::LongTailNative>,
        provider: &'static str,
    ) -> Self {
        Self {
            provider,
            adapter: channel.clone(),
            typed_send: channel.clone(),
            typing: channel.clone(),
            reactions: channel.clone(),
            edits: channel.clone(),
            deletes: channel.clone(),
            drafts: channel,
            listener_mode: LongTailListenerMode::Legacy,
        }
    }

    /// Ports over one long-tail provider object (Lark, Mattermost, IRC, QQ,
    /// DingTalk, Yuanbao); `provider` is its display name in errors.
    fn long_tail(
        channel: Arc<dyn tinychannels::LongTailNative>,
        provider: &'static str,
        listener_mode: LongTailListenerMode,
    ) -> Self {
        Self {
            provider,
            adapter: channel.clone(),
            typed_send: channel.clone(),
            typing: channel.clone(),
            reactions: channel.clone(),
            edits: channel.clone(),
            deletes: channel.clone(),
            drafts: channel,
            listener_mode,
        }
    }

    fn telegram(channel: Arc<tinychannels::TelegramChannel>) -> Self {
        let mut ports = Self::native(channel, "Telegram");
        ports.listener_mode = LongTailListenerMode::Legacy;
        ports
    }

    fn slack(channel: Arc<tinychannels::SlackChannel>) -> Self {
        let mut ports = Self::native(channel, "Slack");
        ports.listener_mode = LongTailListenerMode::Legacy;
        ports
    }
}

pub(crate) struct Running {
    pub(crate) channel: Option<Arc<dyn Channel>>,
    provider_name: String,
    native_ports: Option<NativePorts>,
    pub(crate) media_account_scope: MediaScope,
    configured_instance: String,
    send: mpsc::Sender<SendCommand>,
    cancel: watch::Sender<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
    finished: Arc<AtomicBool>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdapterMode {
    LegacyBacked,
    DiscordNativeV2,
    TelegramNativeV2,
    SlackNativeV2,
    SignalNativeV2,
    IMessageNativeV2,
    LinqNativeV2,
    EmailNativeV2,
    WhatsAppNativeV2,
    LongTailNativeV2,
}

pub(crate) struct ConfiguredAdapterHandle {
    pub(crate) configured_instance: String,
    #[cfg(test)]
    pub(crate) mode: AdapterMode,
    adapter: Arc<dyn tinychannels::channel::ChannelAdapter>,
    #[cfg(test)]
    native: Option<Arc<NativeAdapter>>,
}

impl ConfiguredAdapterHandle {
    #[cfg(test)]
    pub(crate) fn adapter(&self) -> &dyn tinychannels::channel::ChannelAdapter {
        self.adapter.as_ref()
    }

    pub(crate) fn descriptor(&self) -> tinychannels_bus::channel::ChannelDescriptor {
        debug_assert_eq!(
            self.adapter.descriptor().account_id.as_deref(),
            Some(self.configured_instance.as_str())
        );
        self.adapter.descriptor()
    }

    pub(crate) fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        self.adapter.declared_capabilities()
    }

    pub(crate) fn proactive_target(&self) -> Option<String> {
        self.adapter.proactive_target()
    }

    pub(crate) async fn send(
        &self,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> std::result::Result<
        tinychannels::channel::ChannelSendOutcome,
        tinychannels::channel::ChannelSendError,
    > {
        self.adapter.send(intent).await
    }

    #[cfg(test)]
    pub(crate) fn reactions(&self) -> Option<&dyn tinychannels::channel::ChannelReaction> {
        self.native
            .as_deref()
            .map(|adapter| adapter as &dyn tinychannels::channel::ChannelReaction)
    }

    #[cfg(test)]
    pub(crate) fn edits(&self) -> Option<&dyn tinychannels::channel::ChannelEdit> {
        self.native
            .as_deref()
            .map(|adapter| adapter as &dyn tinychannels::channel::ChannelEdit)
    }

    #[cfg(test)]
    pub(crate) fn deletes(&self) -> Option<&dyn tinychannels::channel::ChannelDelete> {
        self.native
            .as_deref()
            .map(|adapter| adapter as &dyn tinychannels::channel::ChannelDelete)
    }
}

pub(crate) struct ConfiguredAdapterFactory;

/// Configured native v2 surface (Telegram, Slack). The provider's native typed sender remains
/// the implementation authority; this adapter submits every operation to the
/// running provider's single ordered operation queue.
struct NativeAdapter {
    running: Arc<Running>,
}

impl NativeAdapter {
    fn provider(&self) -> &'static str {
        self.running
            .native_ports
            .as_ref()
            .map_or("Provider", |ports| ports.provider)
    }
}

fn adapter_operation_error(
    provider: &str,
    error: RunningSendError,
) -> tinychannels::channel::ChannelSendError {
    tinychannels::channel::ChannelSendError {
        kind: tinychannels::channel::SendErrorKind::Unknown,
        message: format!("{provider} operation failed"),
        retryable: matches!(error, RunningSendError::NotDispatched(_)),
        sent_before_error: matches!(error, RunningSendError::AfterDispatch(_)),
        ..Default::default()
    }
}

#[async_trait::async_trait]
impl tinychannels::channel::ChannelAdapter for NativeAdapter {
    fn descriptor(&self) -> tinychannels_bus::channel::ChannelDescriptor {
        let mut descriptor = self
            .running
            .native_ports
            .as_ref()
            .expect("native ports")
            .adapter
            .descriptor();
        descriptor.account_id = Some(self.running.configured_instance.clone());
        descriptor.metadata.insert(
            "configuredInstance".into(),
            self.running.configured_instance.clone(),
        );
        descriptor
    }

    fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        self.running
            .native_ports
            .as_ref()
            .expect("native ports")
            .adapter
            .declared_capabilities()
    }

    fn proactive_target(&self) -> Option<String> {
        self.running.proactive_target()
    }

    fn receive_ack_policy(&self) -> tinychannels::channel::ChannelReceiveAckPolicy {
        tinychannels::channel::ChannelReceiveAckPolicy::AfterReceiveRecord
    }

    async fn start(
        &self,
        _sink: &(dyn tinychannels::channel::ChannelInboundSink + Send + Sync),
    ) -> std::result::Result<(), tinychannels::channel::ChannelSendError> {
        Err(tinychannels::channel::ChannelSendError {
            kind: tinychannels::channel::SendErrorKind::Forbidden,
            message: format!(
                "{} polling is already owned by the configured module runtime",
                self.provider()
            ),
            retryable: false,
            ..Default::default()
        })
    }

    async fn stop(&self) -> std::result::Result<(), tinychannels::channel::ChannelSendError> {
        self.running
            .stop()
            .await
            .map_err(|_error| tinychannels::channel::ChannelSendError {
                kind: tinychannels::channel::SendErrorKind::Transient,
                message: format!("{} runtime stop failed", self.provider()),
                retryable: false,
                sent_before_error: true,
                ..Default::default()
            })
    }

    async fn send(
        &self,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> std::result::Result<
        tinychannels::channel::ChannelSendOutcome,
        tinychannels::channel::ChannelSendError,
    > {
        tinychannels::channel::ChannelAdapter::send(self.running.as_ref(), intent).await
    }

    async fn status(
        &self,
    ) -> std::result::Result<
        tinychannels::controllers::ChannelAccountSnapshot,
        tinychannels::channel::ChannelSendError,
    > {
        tinychannels::channel::ChannelAdapter::status(self.running.as_ref()).await
    }
}

#[async_trait::async_trait]
impl tinychannels::channel::ChannelTyping for NativeAdapter {
    async fn start_typing(
        &self,
        conversation_id: &str,
    ) -> std::result::Result<(), tinychannels::channel::ChannelSendError> {
        self.running
            .interact(tinychannels_bus::ChannelInteraction::StartTyping {
                recipient: conversation_id.into(),
            })
            .await
            .map(|_| ())
            .map_err(|error| adapter_operation_error(self.provider(), error))
    }

    async fn stop_typing(
        &self,
        conversation_id: &str,
    ) -> std::result::Result<(), tinychannels::channel::ChannelSendError> {
        self.running
            .interact(tinychannels_bus::ChannelInteraction::StopTyping {
                recipient: conversation_id.into(),
            })
            .await
            .map(|_| ())
            .map_err(|error| adapter_operation_error(self.provider(), error))
    }
}

#[async_trait::async_trait]
impl tinychannels::channel::ChannelReaction for NativeAdapter {
    async fn send_reaction(
        &self,
        channel: &tinychannels_bus::channel::ChannelRef,
        conversation: &tinychannels_bus::channel::ConversationRef,
        provider_message_id: &str,
        reaction: &tinychannels_bus::channel::ChannelReaction,
        remove: bool,
    ) -> std::result::Result<(), tinychannels::channel::ChannelSendError> {
        self.running
            .interact(tinychannels_bus::ChannelInteraction::SendReaction {
                channel: channel.clone(),
                conversation: conversation.clone(),
                provider_message_id: provider_message_id.into(),
                reaction: reaction.clone(),
                remove,
            })
            .await
            .map(|_| ())
            .map_err(|error| adapter_operation_error(self.provider(), error))
    }
}

#[async_trait::async_trait]
impl tinychannels::channel::ChannelEdit for NativeAdapter {
    async fn edit(
        &self,
        receipt: &tinychannels_bus::channel::MessageReceipt,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> std::result::Result<
        tinychannels_bus::channel::MessageReceipt,
        tinychannels::channel::ChannelSendError,
    > {
        match self
            .running
            .interact(tinychannels_bus::ChannelInteraction::EditMessage {
                receipt: receipt.clone(),
                intent,
            })
            .await
            .map_err(|error| adapter_operation_error(self.provider(), error))?
        {
            tinychannels_bus::ChannelInteractionResult::Sent(outcome) => Ok(outcome.receipt),
            _ => Err(tinychannels::channel::ChannelSendError {
                kind: tinychannels::channel::SendErrorKind::Unknown,
                message: format!("{} edit returned no message receipt", self.provider()),
                retryable: false,
                ..Default::default()
            }),
        }
    }
}

#[async_trait::async_trait]
impl tinychannels::channel::ChannelDelete for NativeAdapter {
    async fn delete(
        &self,
        receipt: &tinychannels_bus::channel::MessageReceipt,
    ) -> std::result::Result<(), tinychannels::channel::ChannelSendError> {
        self.running
            .interact(tinychannels_bus::ChannelInteraction::DeleteMessage {
                receipt: receipt.clone(),
            })
            .await
            .map(|_| ())
            .map_err(|error| adapter_operation_error(self.provider(), error))
    }
}

#[async_trait::async_trait]
impl tinychannels::channel::ChannelStreamingDraft for NativeAdapter {
    async fn upsert_draft(
        &self,
        previous: Option<&tinychannels_bus::channel::MessageReceipt>,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> std::result::Result<
        tinychannels_bus::channel::MessageReceipt,
        tinychannels::channel::ChannelSendError,
    > {
        self.running
            .upsert_native_draft(previous.cloned(), intent)
            .await
            .map_err(|error| adapter_operation_error(self.provider(), error))
    }
}

impl ConfiguredAdapterFactory {
    pub(crate) fn legacy(running: Arc<Running>) -> ConfiguredAdapterHandle {
        let configured_instance = running.configured_instance.clone();
        let adapter: Arc<dyn tinychannels::channel::ChannelAdapter> = running;
        ConfiguredAdapterHandle {
            configured_instance,
            #[cfg(test)]
            mode: AdapterMode::LegacyBacked,
            adapter,
            #[cfg(test)]
            native: None,
        }
    }

    pub(crate) fn telegram(running: Arc<Running>) -> ConfiguredAdapterHandle {
        Self::native(
            running,
            #[cfg(test)]
            AdapterMode::TelegramNativeV2,
        )
    }

    pub(crate) fn discord(running: Arc<Running>) -> ConfiguredAdapterHandle {
        Self::native(
            running,
            #[cfg(test)]
            AdapterMode::DiscordNativeV2,
        )
    }

    pub(crate) fn whatsapp(running: Arc<Running>) -> ConfiguredAdapterHandle {
        Self::native(
            running,
            #[cfg(test)]
            AdapterMode::WhatsAppNativeV2,
        )
    }

    pub(crate) fn long_tail(running: Arc<Running>) -> ConfiguredAdapterHandle {
        Self::native(
            running,
            #[cfg(test)]
            AdapterMode::LongTailNativeV2,
        )
    }

    pub(crate) fn slack(running: Arc<Running>) -> ConfiguredAdapterHandle {
        Self::native(
            running,
            #[cfg(test)]
            AdapterMode::SlackNativeV2,
        )
    }

    pub(crate) fn native(
        running: Arc<Running>,
        #[cfg(test)] mode: AdapterMode,
    ) -> ConfiguredAdapterHandle {
        let configured_instance = running.configured_instance.clone();
        let native = Arc::new(NativeAdapter { running });
        let adapter: Arc<dyn tinychannels::channel::ChannelAdapter> = native.clone();
        ConfiguredAdapterHandle {
            configured_instance,
            #[cfg(test)]
            mode,
            adapter,
            #[cfg(test)]
            native: Some(native),
        }
    }
}

impl Running {
    pub(crate) fn provider_name(&self) -> &str {
        &self.provider_name
    }

    pub(crate) fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        self.native_ports.as_ref().map_or_else(
            || {
                self.channel
                    .as_ref()
                    .map_or_else(Default::default, |channel| channel.capabilities())
            },
            |ports| ports.adapter.declared_capabilities(),
        )
    }

    pub(crate) fn proactive_target(&self) -> Option<String> {
        self.native_ports.as_ref().map_or_else(
            || {
                self.channel
                    .as_ref()
                    .and_then(|channel| channel.proactive_target())
            },
            |ports| ports.adapter.proactive_target(),
        )
    }

    pub(crate) fn adapter_handle(self: &Arc<Self>) -> ConfiguredAdapterHandle {
        if let Some(ports) = &self.native_ports {
            match ports.provider {
                "Discord" => ConfiguredAdapterFactory::discord(self.clone()),
                "Slack" => ConfiguredAdapterFactory::slack(self.clone()),
                "Signal" => ConfiguredAdapterFactory::native(
                    self.clone(),
                    #[cfg(test)]
                    AdapterMode::SignalNativeV2,
                ),
                "iMessage" => ConfiguredAdapterFactory::native(
                    self.clone(),
                    #[cfg(test)]
                    AdapterMode::IMessageNativeV2,
                ),
                "Linq" => ConfiguredAdapterFactory::native(
                    self.clone(),
                    #[cfg(test)]
                    AdapterMode::LinqNativeV2,
                ),
                "Email" => ConfiguredAdapterFactory::native(
                    self.clone(),
                    #[cfg(test)]
                    AdapterMode::EmailNativeV2,
                ),
                "WhatsApp" => ConfiguredAdapterFactory::whatsapp(self.clone()),
                "Telegram" => ConfiguredAdapterFactory::telegram(self.clone()),
                _ => ConfiguredAdapterFactory::long_tail(self.clone()),
            }
        } else {
            ConfiguredAdapterFactory::legacy(self.clone())
        }
    }

    #[cfg(test)]
    pub(crate) fn start(
        channel: Arc<dyn Channel>,
        hooks: Arc<Hooks>,
        host: HostChannels,
    ) -> Arc<Self> {
        let name = channel.name().to_owned();
        Self::start_inner(Some(channel), name, hooks, host, None, false, None)
    }
    #[cfg(test)]
    pub(crate) fn start_supervised(
        channel: Arc<dyn Channel>,
        hooks: Arc<Hooks>,
        host: HostChannels,
        policy: ChannelSupervisionPolicy,
    ) -> Arc<Self> {
        let name = channel.name().to_owned();
        Self::start_inner(Some(channel), name, hooks, host, Some(policy), false, None)
    }
    #[cfg(test)]
    pub(crate) fn start_outbound(
        channel: Arc<dyn Channel>,
        hooks: Arc<Hooks>,
        host: HostChannels,
    ) -> Arc<Self> {
        let name = channel.name().to_owned();
        Self::start_inner(Some(channel), name, hooks, host, None, true, None)
    }
    pub(crate) fn start_telegram(
        channel: Arc<tinychannels::TelegramChannel>,
        hooks: Arc<Hooks>,
        host: HostChannels,
        supervision: Option<ChannelSupervisionPolicy>,
        outbound_only: bool,
    ) -> Arc<Self> {
        let provider_name = channel.descriptor().id;
        Self::start_inner(
            Some(channel.clone()),
            provider_name,
            hooks,
            host,
            supervision,
            outbound_only,
            Some(NativePorts::telegram(channel)),
        )
    }
    /// Start a configured provider with typed operations and its provider-owned
    /// legacy listener. Signal opts into its typed SSE listener explicitly;
    /// providers with webhook, polling or session lifecycles retain theirs.
    pub(crate) fn start_native<T>(
        provider: &'static str,
        channel: Arc<T>,
        hooks: Arc<Hooks>,
        host: HostChannels,
        supervision: Option<ChannelSupervisionPolicy>,
        outbound_only: bool,
    ) -> Arc<Self>
    where
        T: tinychannels::LongTailNative + Channel + 'static,
    {
        let provider_name = channel.descriptor().id;
        let legacy: Arc<dyn Channel> = channel.clone();
        let adapter: Arc<dyn tinychannels::LongTailNative> = channel;
        let listener_mode = if matches!(provider_name.as_str(), "signal" | "email") {
            LongTailListenerMode::Adapter
        } else {
            LongTailListenerMode::Legacy
        };
        Self::start_inner(
            Some(legacy),
            provider_name,
            hooks,
            host,
            supervision,
            outbound_only,
            Some(NativePorts::long_tail(adapter, provider, listener_mode)),
        )
    }

    /// Start one long-tail text provider whose object is both the listener and
    /// the v2 adapter.
    pub(crate) fn start_long_tail(
        provider: &'static str,
        channel: Arc<dyn tinychannels::LongTailNative>,
        hooks: Arc<Hooks>,
        host: HostChannels,
        supervision: Option<ChannelSupervisionPolicy>,
        outbound_only: bool,
        listener_mode: LongTailListenerMode,
    ) -> Arc<Self> {
        Self::start_inner(
            None,
            channel.descriptor().id,
            hooks,
            host,
            supervision,
            outbound_only,
            Some(NativePorts::long_tail(channel, provider, listener_mode)),
        )
    }

    pub(crate) fn start_slack(
        channel: Arc<tinychannels::SlackChannel>,
        hooks: Arc<Hooks>,
        host: HostChannels,
        supervision: Option<ChannelSupervisionPolicy>,
        outbound_only: bool,
    ) -> Arc<Self> {
        let provider_name = channel.descriptor().id;
        Self::start_inner(
            Some(channel.clone()),
            provider_name,
            hooks,
            host,
            supervision,
            outbound_only,
            Some(NativePorts::slack(channel)),
        )
    }

    fn start_inner(
        channel: Option<Arc<dyn Channel>>,
        provider_name: String,
        hooks: Arc<Hooks>,
        host: HostChannels,
        supervision: Option<ChannelSupervisionPolicy>,
        outbound_only: bool,
        native_ports: Option<NativePorts>,
    ) -> Arc<Self> {
        let (cancel, mut canceled) = watch::channel(false);
        let send_cancel = cancel.subscribe();
        let (send, mut sends) = mpsc::channel::<SendCommand>(256);
        let finished = Arc::new(AtomicBool::new(false));
        let task_finished = finished.clone();
        let listen_channel = channel.clone();
        let send_channel = channel.clone();
        let configured_instance = hooks
            .configured_instance
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .unwrap_or_else(|| provider_name.clone());
        let media_account_scope = hooks
            .media_account_scope
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let worker_cancel = cancel.clone();
        let worker_instance = configured_instance.clone();
        let worker_scope = media_account_scope.clone();
        let worker_native = native_ports.clone();
        let worker_provider_name = provider_name.clone();
        let native_listener = worker_native
            .as_ref()
            .is_some_and(|ports| ports.listener_mode == LongTailListenerMode::Adapter);
        let worker = tokio::spawn(async move {
            let name = worker_provider_name;
            host.report_status(&name, "connecting", None).await;
            let (tx, rx) = if native_listener {
                (None, None)
            } else {
                let (tx, rx) = tokio::sync::mpsc::channel(256);
                (Some(tx), Some(rx))
            };
            let mut forwarder = if native_listener {
                None
            } else {
                let forward_host = host.clone();
                let forward_name = name.clone();
                Some(AbortOnDrop(tokio::spawn(async move {
                    let mut rx = rx.expect("legacy listener owns its inbound receiver");
                    while let Some(message) = rx.recv().await {
                        let envelope = tinychannels::inbound_envelope_from_legacy_message(&message);
                        forward_host.deliver_inbound(&forward_name, envelope).await;
                    }
                })))
            };
            let listener_host = host.clone();
            let listener_cancel = worker_cancel.subscribe();
            let listener_native = worker_native.clone();
            let listener_instance = worker_instance.clone();
            let listener_scope = worker_scope.clone();
            let listener_name = name.clone();
            let mut listener = AbortOnDrop(tokio::spawn(async move {
                if native_listener {
                    let Some(native) = listener_native else {
                        return Err(anyhow::anyhow!("native listener ports are unavailable"));
                    };
                    let sink = Arc::new(ModuleEvents(
                        listener_host.clone(),
                        listener_instance,
                        listener_scope,
                    ));
                    let adapter = Arc::clone(&native.adapter);
                    let mut listener_cancel = listener_cancel;
                    if outbound_only {
                        let _ = listener_cancel.wait_for(|value| *value).await;
                        let _ = adapter.stop().await;
                        Ok(())
                    } else {
                        listen_adapter_with_supervision(
                            Arc::clone(&adapter),
                            sink,
                            listener_host.clone(),
                            listener_name,
                            supervision,
                            listener_cancel,
                        )
                        .await
                    }
                } else if outbound_only {
                    // Keep the inbound sender alive until cancellation so the forwarder
                    // has the same explicit cleanup path as native listeners.
                    let _inbound = tx.expect("legacy listener owns its inbound sender");
                    std::future::pending::<anyhow::Result<()>>().await
                } else {
                    listen_with_supervision(
                        listen_channel.expect("legacy listener has a legacy provider"),
                        tx.expect("legacy listener owns its inbound sender"),
                        listener_host,
                        supervision,
                    )
                    .await
                }
            }));
            let mut send_cancel = send_cancel;
            let mut sender = AbortOnDrop(tokio::spawn(async move {
                let mut typing = TypingRefreshes::new();
                loop {
                    let command = tokio::select! {biased;
                        _=async {let _=send_cancel.wait_for(|value|*value).await;}=>break,
                        command=sends.recv()=>match command {Some(command)=>command,None=>break},
                    };
                    let _ = command.started.send(());
                    let result = match command.operation {
                        ProviderOperation::SendTyped(message) => {
                            let sent = if let Some(native) = &worker_native {
                                match native.typed_send.send_typed(&message).await {
                                    Ok(outcome) => Ok(outcome),
                                    Err(error) => {
                                        Ok(tinychannels_bus::channel::ChannelSendOutcome {
                                            receipt: tinychannels_bus::channel::MessageReceipt {
                                                idempotency_key: Some(
                                                    message.idempotency_key.clone(),
                                                ),
                                                channel: Some(
                                                    message.channel_ref.clone().unwrap_or_else(
                                                        || tinychannels_bus::ChannelRef {
                                                            id: tinychannels_bus::provider_id(
                                                                &worker_instance,
                                                            )
                                                            .to_owned(),
                                                            account_id: Some(
                                                                worker_instance.clone(),
                                                            ),
                                                        },
                                                    ),
                                                ),
                                                conversation: Some(
                                                    message
                                                        .conversation_ref
                                                        .clone()
                                                        .unwrap_or_else(|| {
                                                            tinychannels_bus::ConversationRef {
                                                                id: message.conversation_id.clone(),
                                                                thread_id: message
                                                                    .thread_id
                                                                    .clone(),
                                                                ..Default::default()
                                                            }
                                                        }),
                                                ),
                                                thread_id: message.thread_id.clone(),
                                                reply_to_id: message.reply_to_id.clone(),
                                                ..Default::default()
                                            },
                                            error: Some(error),
                                        })
                                    }
                                }
                            } else {
                                match &send_channel {
                                    Some(channel) => {
                                        channel.send_typed_with_receipt(&message).await
                                    }
                                    None => Err(anyhow::anyhow!(
                                        "provider typed-send port is unavailable"
                                    )),
                                }
                            };
                            sent.map(|mut outcome| {
                                outcome.receipt.idempotency_key =
                                    Some(message.idempotency_key.clone());
                                outcome.receipt.channel = Some(tinychannels_bus::ChannelRef {
                                    id: tinychannels_bus::provider_id(&worker_instance).to_owned(),
                                    account_id: Some(worker_instance.clone()),
                                });
                                if outcome.receipt.conversation.is_none() {
                                    outcome.receipt.conversation =
                                        Some(message.conversation_ref.clone().unwrap_or_else(
                                            || tinychannels_bus::ConversationRef {
                                                id: message.conversation_id.clone(),
                                                ..Default::default()
                                            },
                                        ));
                                }
                                if outcome.receipt.thread_id.is_none() {
                                    outcome.receipt.thread_id =
                                        message.thread_id.clone().or_else(|| {
                                            message
                                                .conversation_ref
                                                .as_ref()
                                                .and_then(|r| r.thread_id.clone())
                                        });
                                }
                                tinychannels_bus::ChannelInteractionResult::Sent(Box::new(outcome))
                            })
                            .map_err(|_| Error::failed("provider typed send failed"))
                        }
                        ProviderOperation::NativeDraft(previous, intent) => {
                            if let Some(native) = &worker_native {
                                match native
                                    .drafts
                                    .upsert_draft(previous.as_ref(), intent.clone())
                                    .await
                                {
                                    Ok(receipt) => Ok(
                                        tinychannels_bus::ChannelInteractionResult::Sent(Box::new(
                                            tinychannels_bus::channel::ChannelSendOutcome {
                                                receipt,
                                                error: None,
                                            },
                                        )),
                                    ),
                                    Err(error) => Ok(
                                        tinychannels_bus::ChannelInteractionResult::Sent(Box::new(
                                            tinychannels_bus::channel::ChannelSendOutcome {
                                                receipt:
                                                    tinychannels_bus::channel::MessageReceipt {
                                                        idempotency_key: Some(
                                                            intent.idempotency_key,
                                                        ),
                                                        channel: intent.channel_ref,
                                                        conversation: intent.conversation_ref,
                                                        ..Default::default()
                                                    },
                                                error: Some(error),
                                            },
                                        )),
                                    ),
                                }
                            } else {
                                Err(Error::failed("native draft port is unavailable"))
                            }
                        }
                        ProviderOperation::Interaction(interaction) => {
                            dispatch_interaction(
                                send_channel.clone(),
                                worker_native.clone(),
                                interaction,
                                &mut typing,
                            )
                            .await
                        }
                    };
                    let _ = command.reply.send(result);
                }
                stop_all_typing_refreshes(&mut typing).await;
                while let Ok(command) = sends.try_recv() {
                    let _ = command
                        .reply
                        .send(Err(Error::failed("provider is stopping")));
                }
            }));
            let outcome = if native_listener {
                tokio::select! { biased;
                    _ = async { let _ = canceled.wait_for(|value| *value).await; } => {
                        let _ = (&mut listener.0).await;
                        None
                    }
                    outcome = &mut listener.0 => Some(outcome),
                }
            } else {
                tokio::select! { biased;
                    _ = async { let _ = canceled.wait_for(|value| *value).await; } => {
                        listener.0.abort();
                        let _ = (&mut listener.0).await;
                        None
                    }
                    outcome = &mut listener.0 => Some(outcome),
                }
            };
            worker_cancel.send_replace(true);
            // Join listener first: it cannot register another native hook later.
            hooks.drain().await;
            let _ = (&mut sender.0).await;
            if let Some(forwarder) = &mut forwarder {
                let _ = (&mut forwarder.0).await;
            }
            host.report_status(
                &name,
                if outcome.is_some_and(|outcome| !matches!(outcome, Ok(Ok(())))) {
                    "failed"
                } else {
                    "stopped"
                },
                None,
            )
            .await;
            task_finished.store(true, Ordering::SeqCst);
        });
        Arc::new(Self {
            channel,
            provider_name,
            native_ports,
            media_account_scope,
            configured_instance,
            send,
            cancel,
            worker: Mutex::new(Some(worker)),
            finished,
        })
    }
    fn native_provider(&self) -> &'static str {
        self.native_ports
            .as_ref()
            .map_or("Provider", |ports| ports.provider)
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }
    pub(crate) async fn send_typed(
        &self,
        message: tinychannels_bus::ChannelOutboundMessage,
    ) -> std::result::Result<tinychannels_bus::channel::ChannelSendOutcome, RunningSendError> {
        match self.submit(ProviderOperation::SendTyped(message)).await? {
            tinychannels_bus::ChannelInteractionResult::Sent(receipt) => Ok(*receipt),
            _ => Err(RunningSendError::AfterDispatch(
                "typed send returned no receipt".into(),
            )),
        }
    }

    fn adapter_descriptor(&self) -> tinychannels_bus::channel::ChannelDescriptor {
        let display_name = self.provider_name();
        let channel_id = tinychannels_bus::provider_id(display_name).to_owned();
        tinychannels_bus::channel::ChannelDescriptor {
            id: channel_id,
            display_name: display_name.to_owned(),
            account_id: Some(self.configured_instance.clone()),
            metadata: std::collections::BTreeMap::from([
                (
                    "configuredInstance".into(),
                    self.configured_instance.clone(),
                ),
                (
                    "adapterMode".into(),
                    if self.native_ports.is_some() {
                        "native-v2"
                    } else {
                        "legacy-backed"
                    }
                    .into(),
                ),
            ]),
            ..Default::default()
        }
    }

    fn validate_adapter_intent(
        &self,
        intent: &tinychannels_bus::ChannelOutboundIntent,
    ) -> std::result::Result<(), tinychannels::channel::ChannelSendError> {
        let expected_channel = tinychannels_bus::provider_id(self.provider_name());
        if tinychannels_bus::provider_id(&intent.channel_id) != expected_channel
            || intent.channel_ref.as_ref().is_some_and(|reference| {
                reference.id != expected_channel
                    || reference
                        .account_id
                        .as_deref()
                        .is_some_and(|account_id| account_id != self.configured_instance.as_str())
            })
        {
            return Err(adapter_scope_error(
                "channel account does not match configured instance",
            ));
        }
        if intent.conversation_id.trim().is_empty()
            || intent
                .conversation_ref
                .as_ref()
                .is_some_and(|conversation| {
                    conversation.id != intent.conversation_id
                        || (intent.thread_id.is_some()
                            && conversation.thread_id.is_some()
                            && conversation.thread_id != intent.thread_id)
                })
        {
            return Err(adapter_scope_error(
                "conversation address is incomplete or inconsistent",
            ));
        }
        Ok(())
    }

    pub(crate) async fn upsert_native_draft(
        &self,
        previous: Option<tinychannels_bus::channel::MessageReceipt>,
        mut intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> std::result::Result<tinychannels_bus::channel::MessageReceipt, RunningSendError> {
        if self.native_ports.is_none() {
            return Err(RunningSendError::NotDispatched(
                "native draft port is unavailable".into(),
            ));
        }
        self.validate_adapter_intent(&intent)
            .map_err(|e| RunningSendError::NotDispatched(e.message))?;
        let expected_channel = tinychannels_bus::ChannelRef {
            id: tinychannels_bus::provider_id(&self.configured_instance).to_owned(),
            account_id: Some(self.configured_instance.clone()),
        };
        intent.channel_ref = Some(expected_channel.clone());
        let conversation =
            intent
                .conversation_ref
                .clone()
                .unwrap_or_else(|| tinychannels_bus::ConversationRef {
                    id: intent.conversation_id.clone(),
                    thread_id: intent.thread_id.clone(),
                    ..Default::default()
                });
        intent.conversation_ref = Some(conversation.clone());
        if let Some(receipt) = &previous {
            let valid_id = receipt
                .primary_platform_message_id
                .as_deref()
                .is_some_and(|id| !id.trim().is_empty())
                || receipt
                    .parts
                    .iter()
                    .any(|part| !part.platform_message_id.trim().is_empty());
            if receipt.channel.as_ref() != Some(&expected_channel)
                || receipt.conversation.as_ref() != Some(&conversation)
                || !valid_id
                || receipt
                    .parts
                    .iter()
                    .any(|part| part.platform_message_id.trim().is_empty())
            {
                return Err(RunningSendError::NotDispatched(format!(
                    "draft receipt scope does not match {} intent",
                    self.native_provider()
                )));
            }
        }
        match self
            .submit(ProviderOperation::NativeDraft(previous, intent))
            .await?
        {
            tinychannels_bus::ChannelInteractionResult::Sent(outcome) => {
                if let Some(error) = outcome.error {
                    Err(RunningSendError::AfterDispatch(error.message))
                } else {
                    Ok(outcome.receipt)
                }
            }
            _ => Err(RunningSendError::AfterDispatch(
                "native draft returned no receipt".into(),
            )),
        }
    }

    pub(crate) async fn interact(
        &self,
        interaction: tinychannels_bus::ChannelInteraction,
    ) -> std::result::Result<tinychannels_bus::ChannelInteractionResult, RunningSendError> {
        let expected_channel = tinychannels_bus::ChannelRef {
            id: tinychannels_bus::provider_id(self.provider_name()).to_owned(),
            account_id: Some(self.configured_instance.clone()),
        };
        let valid_receipt = |receipt: &tinychannels_bus::channel::MessageReceipt| {
            let has_message_id = receipt
                .primary_platform_message_id
                .as_deref()
                .is_some_and(|message_id| !message_id.trim().is_empty())
                || receipt
                    .parts
                    .iter()
                    .any(|part| !part.platform_message_id.trim().is_empty());
            receipt.channel.as_ref() == Some(&expected_channel)
                && receipt.conversation.as_ref().is_some_and(|conversation| {
                    !conversation.id.trim().is_empty()
                        && receipt
                            .parts
                            .iter()
                            .all(|part| !part.platform_message_id.trim().is_empty())
                })
                && has_message_id
        };
        if let tinychannels_bus::ChannelInteraction::UpsertDraft { previous, intent } = &interaction
        {
            let receipt = self
                .upsert_native_draft(previous.clone(), intent.clone())
                .await?;
            return Ok(tinychannels_bus::ChannelInteractionResult::Sent(Box::new(
                tinychannels_bus::channel::ChannelSendOutcome {
                    receipt,
                    error: None,
                },
            )));
        }
        match &interaction {
            tinychannels_bus::ChannelInteraction::SendReaction {
                channel,
                conversation,
                provider_message_id,
                ..
            } if channel != &expected_channel
                || conversation.id.trim().is_empty()
                || provider_message_id.trim().is_empty() =>
            {
                return Err(RunningSendError::NotDispatched(
                    "provider reaction scope is incomplete or does not match configured account"
                        .into(),
                ));
            }
            tinychannels_bus::ChannelInteraction::MarkRead {
                channel,
                conversation,
                provider_message_id,
            } if channel != &expected_channel
                || conversation.id.trim().is_empty()
                || provider_message_id.trim().is_empty() =>
            {
                return Err(RunningSendError::NotDispatched(
                    "provider read receipt scope is incomplete or does not match configured account"
                        .into(),
                ));
            }
            tinychannels_bus::ChannelInteraction::EditMessage { receipt, intent }
                if !valid_receipt(receipt)
                    || self.validate_adapter_intent(intent).is_err()
                    || intent.conversation_ref.as_ref() != receipt.conversation.as_ref() =>
            {
                return Err(RunningSendError::NotDispatched(
                    "provider edit receipt and intent scope do not match configured account".into(),
                ));
            }
            tinychannels_bus::ChannelInteraction::DeleteMessage { receipt }
                if !valid_receipt(receipt) =>
            {
                return Err(RunningSendError::NotDispatched(
                    "provider delete receipt does not match configured account".into(),
                ));
            }
            _ => {}
        }
        self.submit(ProviderOperation::Interaction(interaction))
            .await
    }

    /// Run a webhook interaction directly on the provider. Ingesting a
    /// delivery can download media for seconds; it must not hold up the
    /// ordered send queue, and its ordering against sends does not matter.
    pub(crate) async fn webhook(
        &self,
        interaction: tinychannels_bus::ChannelInteraction,
    ) -> Result<tinychannels_bus::ChannelInteractionResult> {
        crate::interaction::dispatch(
            self.channel
                .as_deref()
                .ok_or_else(|| Error::failed("legacy provider port is unavailable"))?,
            interaction,
        )
        .await
    }

    async fn submit(
        &self,
        operation: ProviderOperation,
    ) -> std::result::Result<tinychannels_bus::ChannelInteractionResult, RunningSendError> {
        let (reply, wait) = oneshot::channel();
        let (started, dispatch) = oneshot::channel();
        self.send
            .try_send(SendCommand {
                operation,
                reply,
                started,
            })
            .map_err(|_| {
                RunningSendError::NotDispatched("provider send queue is full or closed".into())
            })?;
        match wait.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => match dispatch.await {
                Ok(()) => Err(RunningSendError::AfterDispatch(error.to_string())),
                Err(_) => Err(RunningSendError::NotDispatched(error.to_string())),
            },
            Err(_) => match dispatch.await {
                Ok(()) => Err(RunningSendError::AfterDispatch(
                    "provider send worker stopped after dispatch".into(),
                )),
                Err(_) => Err(RunningSendError::NotDispatched(
                    "provider send worker stopped before dispatch".into(),
                )),
            },
        }
    }
    pub(crate) async fn stop(&self) -> Result<()> {
        self.cancel.send_replace(true);
        let mut worker = self.worker.lock().await;
        if let Some(task) = worker.as_mut() {
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .map_err(|_| Error::failed("provider cleanup is pending; retry StopChannel"))?
                .map_err(|_| Error::failed("provider cleanup worker failed"))?;
        }
        worker.take();
        Ok(())
    }
}

fn adapter_scope_error(message: &str) -> tinychannels::channel::ChannelSendError {
    tinychannels::channel::ChannelSendError {
        kind: tinychannels::channel::SendErrorKind::Forbidden,
        message: message.into(),
        retryable: false,
        ..Default::default()
    }
}

/// The module's configured runtime handle is the adapter factory's concrete
/// legacy-backed entry until its provider receives a native adapter port.
/// It reuses the already-running listener and typed send worker.
#[async_trait::async_trait]
impl tinychannels::channel::ChannelAdapter for Running {
    fn descriptor(&self) -> tinychannels_bus::channel::ChannelDescriptor {
        self.adapter_descriptor()
    }

    fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        Running::declared_capabilities(self)
    }

    fn proactive_target(&self) -> Option<String> {
        Running::proactive_target(self)
    }

    async fn start(
        &self,
        _sink: &(dyn tinychannels::channel::ChannelInboundSink + Send + Sync),
    ) -> std::result::Result<(), tinychannels::channel::ChannelSendError> {
        Err(tinychannels::channel::ChannelSendError {
            kind: tinychannels::channel::SendErrorKind::Forbidden,
            message: "configured adapter is already started by the module runtime".into(),
            retryable: false,
            ..Default::default()
        })
    }

    async fn stop(&self) -> std::result::Result<(), tinychannels::channel::ChannelSendError> {
        Running::stop(self)
            .await
            .map_err(|error| tinychannels::channel::ChannelSendError {
                kind: tinychannels::channel::SendErrorKind::Transient,
                message: error.to_string(),
                retryable: true,
                sent_before_error: true,
                ..Default::default()
            })
    }

    async fn send(
        &self,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> std::result::Result<
        tinychannels::channel::ChannelSendOutcome,
        tinychannels::channel::ChannelSendError,
    > {
        self.validate_adapter_intent(&intent)?;
        let capabilities = self.declared_capabilities();
        if !adapter_payload_supported(&intent.payload, &capabilities) {
            return Err(tinychannels::channel::ChannelSendError {
                kind: tinychannels::channel::SendErrorKind::BadFormat,
                message: "configured provider does not advertise this outbound content kind".into(),
                unsupported: true,
                retryable: false,
                ..Default::default()
            });
        }
        let mut intent = intent;
        let expected_channel = tinychannels_bus::provider_id(&self.configured_instance).to_owned();
        intent
            .channel_ref
            .get_or_insert_with(|| tinychannels_bus::ChannelRef {
                id: expected_channel,
                account_id: Some(self.configured_instance.clone()),
            });
        if let Some(channel_ref) = &mut intent.channel_ref
            && channel_ref.account_id.is_none()
        {
            channel_ref.account_id = Some(self.configured_instance.clone());
        }
        intent
            .conversation_ref
            .get_or_insert_with(|| tinychannels_bus::ConversationRef {
                id: intent.conversation_id.clone(),
                thread_id: intent.thread_id.clone(),
                ..Default::default()
            });
        let message = tinychannels_bus::ChannelOutboundMessage::from_intent(&intent);
        let mut outcome = self.send_typed(message).await.map_err(|error| {
            let sent_before_error = matches!(error, RunningSendError::AfterDispatch(_));
            tinychannels::channel::ChannelSendError {
                kind: tinychannels::channel::SendErrorKind::Unknown,
                message: if self.native_ports.is_some() {
                    format!("{} send failed", self.native_provider())
                } else {
                    error.to_string()
                },
                retryable: !sent_before_error,
                sent_before_error,
                ..Default::default()
            }
        })?;
        if let Some(error) = &mut outcome.error {
            let partial = error.sent_before_error
                || outcome.receipt.primary_platform_message_id.is_some()
                || !outcome.receipt.platform_message_ids.is_empty()
                || !outcome.receipt.parts.is_empty()
                || !error.continuation_message_ids.is_empty()
                || error.partial_overflow.is_some();
            let uncertain = error.kind == tinychannels::channel::SendErrorKind::Unknown;
            if partial || uncertain {
                error.unsupported = false;
            }
            error.sent_before_error |= partial || uncertain;
            if partial || uncertain {
                error.retryable = false;
            }
        }
        Ok(outcome)
    }

    async fn status(
        &self,
    ) -> std::result::Result<
        tinychannels::controllers::ChannelAccountSnapshot,
        tinychannels::channel::ChannelSendError,
    > {
        let healthy = if let Some(native) = &self.native_ports {
            native
                .adapter
                .status()
                .await
                .is_ok_and(|snapshot| snapshot.health_state.as_deref() == Some("healthy"))
        } else if let Some(channel) = &self.channel {
            channel.health_check().await
        } else {
            false
        };
        Ok(tinychannels::controllers::ChannelAccountSnapshot {
            channel_id: tinychannels_bus::provider_id(self.provider_name()).into(),
            account_id: Some(self.configured_instance.clone()),
            states: vec![
                tinychannels::controllers::ChannelAccountState::Configured,
                tinychannels::controllers::ChannelAccountState::Enabled,
            ],
            health_state: Some(if healthy { "healthy" } else { "unhealthy" }.into()),
            ..Default::default()
        })
    }
}

fn adapter_payload_supported(
    payload: &tinychannels_bus::OutboundPayload,
    capabilities: &tinychannels_bus::ChannelCapabilities,
) -> bool {
    use tinychannels_bus::{ContentPart, OutboundContentPart, OutboundPayload};
    let supports_voice = |voice: &tinychannels_bus::OutboundVoice| match voice {
        tinychannels_bus::OutboundVoice::Media { .. } => capabilities.voice_out.supported,
        tinychannels_bus::OutboundVoice::Synthesize { .. } => {
            capabilities.voice_out.supported && capabilities.voice_out.synthesis
        }
    };
    let supports_part = |part: &ContentPart| match part {
        ContentPart::Text { .. } => capabilities.content.text.outbound,
        ContentPart::Image { .. } => capabilities.content.image.outbound,
        ContentPart::Video { .. } => capabilities.content.video.outbound,
        ContentPart::Audio { .. } => capabilities.content.audio.outbound,
        ContentPart::File { .. } => capabilities.content.file.outbound,
        ContentPart::VoiceNote { .. } => capabilities.voice_out.supported,
        ContentPart::Sticker { .. } => capabilities.content.sticker.outbound,
        ContentPart::Location { .. } => capabilities.content.location.outbound,
        ContentPart::Contact { .. } => capabilities.content.contact.outbound,
        ContentPart::Buttons { .. } => capabilities.content.buttons.outbound,
        ContentPart::Poll { .. } => capabilities.content.poll.outbound,
        ContentPart::Presentation { .. } => capabilities.content.presentation.outbound,
        ContentPart::Native { .. } => capabilities.content.native.outbound,
    };
    match payload {
        OutboundPayload::Text { .. } => capabilities.content.text.outbound,
        OutboundPayload::Media { text, media_urls } => {
            text.as_ref()
                .is_none_or(|_| capabilities.content.text.outbound)
                && (media_urls.is_empty() || capabilities.content.image.outbound)
        }
        OutboundPayload::Voice { voice } => supports_voice(voice),
        OutboundPayload::Files { .. } => capabilities.content.file.outbound,
        OutboundPayload::Poll { .. } => capabilities.content.poll.outbound,
        OutboundPayload::PresentationBlocks { .. } => capabilities.content.presentation.outbound,
        OutboundPayload::NativeChannelData { .. } => capabilities.content.native.outbound,
        OutboundPayload::Parts { parts } => parts.iter().all(|part| match part {
            OutboundContentPart::Content { part } => supports_part(part),
            OutboundContentPart::Voice { voice } => supports_voice(voice),
        }),
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.get_mut().take() {
            worker.abort();
        }
    }
}

#[cfg(test)]
#[path = "provider_tests.rs"]
mod tests;
