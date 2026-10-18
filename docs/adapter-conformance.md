# Channel adapter conformance

The adapter contract is being adopted in stages. The module's
`ConfiguredAdapterFactory` currently returns an explicitly `LegacyBacked`
handle around the already-running provider actor. This preserves listener
ordering and routes both durable and best-effort typed sends through the same
adapter dispatch seam. The handle binds the configured account identity,
validates supplied channel and conversation addresses, and preserves the full
typed send outcome. Rich content is sent only when the configured provider's
capability facts advertise that content kind.

This bridge is not a provider conformance claim. Its lifecycle is owned by the
module runtime, and it does not expose optional typing, reaction, edit, delete,
or streaming-draft traits. Capability facts continue to come from the
configured provider and are marked `adapterMode=legacy-backed` on the
descriptor.

The legacy `start_channel` service currently selects providers by their bare
provider name and uses that name as the configured instance identity; it does
not create separate account aliases for multiple accounts of one provider.
At the bridge boundary, a missing `ChannelRef.account_id` is filled from the
running instance, while an explicit account must match that instance. Tests
that construct a `Running` actor with an explicit instance exercise the
adapter contract but do not imply that the legacy service can start aliased
accounts.

## Current automated coverage

The reusable `tinychannels-conformance` crate defines a 24-case feature matrix
and checks declared capability behavior against wire observations. Its own
tests reject authentication failures mislabeled as unsupported, missing or
mis-scoped receipts, wrong payloads, mismatched response IDs, partial effects
reported as unsupported, and voice ingress without both audio and transcript
evidence. Non-message operations can be observed without manufacturing a
message receipt; streaming drafts must report create, update, finalize, and
cancel-cleanup operations.

The module tests exercise account mismatch rejection before dispatch, partial
receipt retry safety, raw voice-synthesis capability gating, and real
configured Running adapters that send through Telegram's Bot API against
wiremock servers. The conformance kit observes the actual `sendMessage`
request and fixture response ID, then verifies configured account and
conversation scope. An unconfigured Telegram channel rejects image content
before platform I/O; a configured scoped-media repository enables typed media
and the Running adapter is tested sending an image read from the module's
host-granted storage bus. CI runs these tests and the conformance-kit
self-tests through `scripts/test-adapter-conformance.sh`.

The shared media foundation also tests a connected Telegram inbound photo
flow: a wiremock `getFile` response and authenticated file download pass through
MIME/body validation into the host-scoped blob store, then appear as an ordered
typed image with scope-bound readback. The configured Running Telegram test
also seeds that same host storage bus, reads the opaque reference through the
configured repository, and verifies native `sendPhoto` multipart content and
its actual receipt. Typed audio title/transcript/duration and partial-send
receipt safety have focused wire tests. This foundation does not claim final
conformance for other providers. Native upload request/receipt evidence remains
required from each provider port before final issue acceptance.

Telegram-specific preflight advertises MP3/M4A for native `sendAudio`, MPEG-4
for native `sendVideo`, and a 10 MiB photo cap; unsupported audio containers are
rejected before platform I/O and non-MPEG-4 video uses `sendDocument`. The
Bot API's photo dimensions and caption constraints are checked before upload.
See the [Telegram Bot API](https://core.telegram.org/bots/api#sendphoto),
[sendAudio](https://core.telegram.org/bots/api#sendaudio), and
[sendVideo](https://core.telegram.org/bots/api#sendvideo) limits.

## Discord

`DiscordChannel` implements the v2 contract natively (`ChannelAdapter`,
`ChannelTypedSend`, `ChannelTyping`, `ChannelReaction`, `ChannelEdit`,
`ChannelDelete`, `ChannelStreamingDraft`) and mirrors it on the legacy
`Channel` trait, so the module's legacy-backed bridge reaches the same code.
`src/providers/discord/conformance_tests.rs` runs the whole 24-case matrix
against a wiremock Discord. Observations are parsed from the requests the mock
received and message ids from the responses it returned.

- **Addressing.** `ConversationRef.id` is the parent channel and `thread_id`
  the thread channel; a send posts to the thread when one is set, and replies
  use `message_reference`. Threads are resolved from `THREAD_CREATE`,
  `GUILD_CREATE` or one cached `GET /channels/{id}`.
- **Attachments.** Image, video, audio and file parts upload as multipart
  `files[n]` from host media references (10 MiB budget, 10 files). Inbound
  attachments are downloaded unauthenticated from the CDN, bounded, and stored
  through the host media repository. Both directions are declared only when the
  host supplied a repository.
- **Voice messages.** Inbound messages with flag 8192 are stored through the
  host voice store, transcribed with the host STT, and delivered as a
  `VoiceNote` carrying duration and the decoded waveform. Outbound voice is
  sent alone with flag 8192, `duration_secs` and a base64 `waveform`; the
  TinyVoice codec measures both, and a missing codec falls back to the Ogg
  granule position and a flat waveform. Non-Ogg audio is transcoded or
  rejected.
- **Presentation.** Buttons and approvals become action rows (5x5, custom ids
  up to 100 bytes); presentation text becomes an embed. A button press is
  acknowledged with a deferred update and published as an opaque
  `ButtonCallback`. Mentions are limited to users, so model output cannot
  ping `@everyone`.
- **Threads, edits, streaming.** A `Native` part
  `{"action":"create_thread","name":...}` creates a thread (from a message or
  standalone). Edits, deletes and reactions use the receipt; streaming drafts
  edit the first 2000 characters in place and the final edit posts the rest,
  rate limited per channel.
- **Idempotency.** Every created message carries a deterministic `nonce` with
  `enforce_nonce`, so a retried send cannot duplicate a message.
- **Events.** `MESSAGE_UPDATE`, `MESSAGE_DELETE` and reaction add/remove are
  published as `message_updated`, `message_deleted`, `reaction_added` and
  `reaction_removed`, filtered by the same guild, channel and allowlist scope
  as messages.

Not supported through the bot API, and reported as explicit unsupported
results: stickers, locations, contacts, polls and read receipts.

General host-to-module media ingestion is still missing from the TinyChannels
bus. Host consumers must wait for the owning bus/module extension rather than
linking TinyChannels implementation code or duplicating its metadata schema.

| Behavior | Foundation status | Final provider coverage |
| --- | --- | --- |
| Text | Running factory and Telegram Bot API mock-wire request/receipt tested | Pending full provider port coverage |
| Image, video, audio, files, stickers, locations, contacts, buttons, polls | Capability-gated at adapter dispatch | Pending per-provider wire assertions |
| Voice in and out | Raw synthesis is rejected unless configured synthesis is declared | Pending native adapter fixtures and ingress lifecycle coverage |
| Reply and thread addressing | Full refs preserved and mismatches rejected | Pending per-provider wire assertions |
| Typing, reactions, edit, delete | Not exposed by the bridge | Pending native adapter traits |
| Read receipts | Not exposed by the bridge | Pending provider ports |
| Streaming draft lifecycle | Not exposed by the bridge | Pending native adapter traits |
| Capability honesty | Kit validates unsupported outcomes and real configured Telegram text/image cases | Pending observed wire behavior for every declared feature |

Provider leaves #106–#111 must add mock-platform fixtures that assert the
actual request and receipt for each declared operation. The final matrix and
legacy trait removal remain open until those fixtures pass for every provider.
