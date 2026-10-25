# Shared voice pipeline

TinyChannels owns the provider-neutral voice flow. Providers download inbound
audio once and persist it through the host-granted scoped `BlobStore` media
route, then deliver a normalized voice note containing a durable opaque media
ID. Metadata binds that ID to the configured instance, account, full
conversation, and originating turn. The `tinychannels_media_v1_` prefix marks
references owned by this store; arbitrary unprefixed IDs remain authenticated
legacy host references and resolve through `ReadVoiceMedia`. A malformed or
out-of-scope ID in the new namespace never falls back to the legacy callback.
The shared module ingress preserves the audio note and enriches it with the
host STT transcript. Provider transcripts take precedence, and completed host
transcripts survive replay. A failed or unavailable STT callback marks the
transcript unavailable without removing the stored audio.

The blob key is restricted to `channels/media/`; metadata lives in
`channels.media_metadata`. The bus schema declares the exact no-index
collection spec used by the module store, and the module validates the
complete stored scope on
every read and applies content-type, byte-size, and digest checks. Pending
writes and deletions retain durable state for cleanup retries. OpenHuman's
host adapter must grant only that blob prefix and metadata collection through
`ScopedStorage`; that narrow grant remains downstream integration work. Hosts
also own local-cache materialization: `MediaReference.local_cache_path` must
be an actual path produced by host storage after its protected-path and symlink
guards, never a blob key or provider URL. The OpenHuman guarded cache projection
is likewise downstream work.

The current module exposes this store to provider and shared voice routes, but
the TinyChannels bus has no host-to-module operation for creating a general
typed-media reference. Host applications must not link the implementation
crate or duplicate its metadata schema to upload media. A bounded, scope-bound
module operation for host media ingestion is required before host artifact or
frontend upload consumers can use this namespace; it must return the same
opaque reference and keep configured account, full conversation, and origin
turn validation inside the module.

For completed assistant turns, the module asks the host's scoped
`GetVoiceReplyPolicy` callback for its mode and verified turn facts. Automatic
mirror, always, never, and speak-reply decisions apply only to final messages;
explicit `Synthesize` parts are honored when sent through the typed transport.
Approval, tool, and clarification prompts are not automatically voiced. The
provider's advertised output codecs and size/duration limits constrain
synthesis. Speech is converted and probed by the loaded TinyVoice module
through `tinyvoice-bus`, then persisted by `StoreVoiceMedia`. The durable opaque
ID remains the intent identity. The host's existing voice publication callback
receives the shared media reference so it can authorize a recipient link or
materialize a guarded local cache path without storing a second copy. A separate
`accessibleUrl` is retained only when the host has authorized a link that the
recipient can open; it must not be a local path, provider token URL, or
temporary cache location. Hosts own access checks for opaque IDs and link
expiry/revocation.

Providers resolve opaque IDs through a scoped `ReadVoiceMedia` call for native
uploads. Telegram emits native voice for Ogg Opus and native audio for its
advertised M4A/MP3 formats. Transcript text accompanies native voice as a
caption; transcripts above Telegram's caption limit are delivered as follow-up
text chunks, and each accepted message ID is retained in order if a later chunk
has an uncertain outcome. Reply/thread addressing is carried in the upload and
text requests and receipts. Other providers advertise no voice synthesis unless
their own native transport and host ports support it.

`ChannelOutboundIntent.turnId` and `ChannelOutboundMessage.turnId` preserve the
originating host turn independently from their per-send `idempotencyKey`. A host
mapping `ProgressiveTransportRequest::SendTyped` into a provider message must
copy that request's `turnId`, channel/account reference, and full conversation
reference. Telegram uses those exact address fields for `ReadVoiceMedia`; it
never substitutes the send idempotency hash for the turn authority. Durable
replay preserves the intent's originating turn as well as its idempotency key.

The inline and callback limits are deliberately separate:

| Boundary | Limit | Payload |
| --- | ---: | --- |
| Legacy host callbacks | 128 KiB serialized JSON | Small status, event, and compatibility callbacks |
| Progressive controller callback | 1 MiB serialized JSON | Text and typed media references; never large audio bytes |
| Scoped STT/TTS/media callbacks | 12 MiB serialized JSON request and response | Bounded base64 payloads, checked in both directions |
| TinyBus frame | 16 MiB serialized message | Contract transport ceiling, not an audio budget |
| Telegram media | 8 MiB decoded bytes | Local native upload/download cap |

The module applies 60-second timeouts and bounds active host callbacks. A
scoped progressive feed is also bounded by its host turn lease; expiry aborts
in-flight voice preparation and memoizes the terminal result so an identical
feed cannot trigger a second synthesis. Once a provider send callback has been
dispatched, receipt uncertainty remains terminal and must not cause a retry. Persisted send intents contain only
compact references and metadata, so normal voice media does not exceed the
existing 128 KiB durable intent/receipt boundary.
