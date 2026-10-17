# Shared voice pipeline

TinyChannels owns the provider-neutral voice flow. Providers download inbound
audio once, store it through `StoreVoiceMedia`, and deliver a normalized voice
note containing the durable opaque media ID. The shared module ingress resolves
that ID through `ReadVoiceMedia`, preserves the audio note, and enriches it with
the host STT transcript. Provider transcripts take precedence, and completed
host transcripts survive replay. A failed or unavailable STT callback marks
the transcript unavailable without removing the stored audio.

For completed assistant turns, the module asks the host's scoped
`GetVoiceReplyPolicy` callback for its mode and verified turn facts. Mirror,
always, never, and speak-reply are applied only to final messages. The provider's
advertised output codecs and size/duration limits constrain synthesis. Speech
is converted and probed by the loaded TinyVoice module through `tinyvoice-bus`,
then persisted by `StoreVoiceMedia`. The durable opaque ID remains the intent
identity. A separate `accessibleUrl` is retained only when the host has
authorized a link that the recipient can open; it must not be a local path,
provider token URL, or temporary cache location. Hosts own access checks for
opaque IDs and link expiry/revocation.

Providers resolve opaque IDs through a scoped `ReadVoiceMedia` call for native
uploads. Telegram emits native voice for Ogg Opus and native audio for its
advertised M4A/MP3 formats. Transcript text accompanies native voice as a
caption, with reply/thread addressing carried in the upload request and receipt.
Other providers advertise no voice synthesis unless their own native transport
and host ports support it.

The inline and callback limits are deliberately separate:

| Boundary | Limit | Payload |
| --- | ---: | --- |
| Legacy host callbacks | 128 KiB serialized JSON | Small status, event, and compatibility callbacks |
| Progressive controller callback | 1 MiB serialized JSON | Text and typed media references; never large audio bytes |
| Scoped STT/TTS/media callbacks | 12 MiB serialized JSON request and response | Bounded base64 payloads, checked in both directions |
| TinyBus frame | 16 MiB serialized message | Contract transport ceiling, not an audio budget |
| Telegram media | 8 MiB decoded bytes | Local native upload/download cap |

The module applies 60-second timeouts and bounds active host callbacks. A
cancelled waiter does not cancel an already dispatched callback; the tracked
operation remains owned until completion or runtime shutdown. Provider sends
are not retried after an unknown receipt. Persisted send intents contain only
compact references and metadata, so normal voice media does not exceed the
existing 128 KiB durable intent/receipt boundary.
