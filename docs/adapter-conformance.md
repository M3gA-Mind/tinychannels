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

General host-to-module media ingestion is still missing from the TinyChannels
bus. Host consumers must wait for the owning bus/module extension rather than
linking TinyChannels implementation code or duplicating its metadata schema.

## Slack

`SlackChannel` is a native v2 adapter (`adapterMode=native-v2`). It implements
`ChannelAdapter`, `ChannelTypedSend`, `ChannelTyping`, `ChannelReaction`,
`ChannelEdit`, `ChannelDelete` and `ChannelStreamingDraft`, and the module runs
it through `Running::start_slack` on the same ordered operation queue as
Telegram. Its conformance test
(`slack_adapter_passes_the_full_conformance_matrix_against_the_mock_platform`)
runs all 24 kit features against a mock Web API: declared features must reach
the platform with the right addressing and receipt IDs, and undeclared ones
(stickers, locations, contacts, polls, native data, voice synthesis without a
configured synthesizer, read receipts) must return an explicit unsupported
result without any request.

- **Inbound:** Socket Mode when `app_token` is set (every envelope is
  acknowledged before it is handled, `disconnect` frames reconnect), otherwise
  `conversations.history` polling of `channel_id`. Messages, `app_mention`,
  edits, deletes, reactions and Block Kit button presses are normalized; a
  repeated `channel:ts` is dropped.
- **Files:** inbound files are downloaded with the bot token (only from
  `files.slack.com`) into the scoped media repository; outbound files use
  `files.getUploadURLExternal`, a raw upload and `files.completeUploadExternal`.
- **Voice:** Slack has no native voice-note API. Inbound audio clips become
  voice notes carrying Slack's own transcript when it is finished. Outbound
  voice is an audio file upload with the transcript as its comment; the
  descriptor advertises this as `voiceOut=file`.
- **Typing:** `assistant.threads.setStatus` for a `channel:thread_ts`
  recipient; a recipient without a thread returns an explicit unsupported
  result.
- **Receipts:** every text, Block Kit and draft message returns Slack's `ts`.
  An upload whose message timestamp Slack does not report returns the file ID,
  which can be deleted (`files.delete`) but not edited.

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
