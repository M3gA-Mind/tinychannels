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

## Signal

`SignalChannel` is a native v2 adapter (`adapterMode=native-v2`) built on the
shared `providers::native` plumbing that Signal, iMessage, Linq and Email use:
a provider implements `NativeProvider` (deliver one resolved step, optionally
typing, reactions, edits and deletes) and `native_v2_adapter!` supplies the whole
adapter trait family. Anything a provider does not implement answers an
explicit, non-retryable `unsupported` result before any platform I/O. The module
runs it through `Running::start_native`. Its conformance test
(`signal_adapter_passes_the_full_conformance_matrix_against_the_mock_daemon`)
runs all 24 kit features against a mock signal-cli daemon.

- **Inbound:** the daemon's SSE stream. Messages (with attachments), reactions,
  remote deletes and read/delivery receipts are normalized onto the ordered
  ingress path; a message the sink does not take falls back to the legacy
  receiver. A repeated `sender:timestamp` is dropped.
- **Attachments and voice:** inbound bytes come from signal-cli's
  `getAttachment` JSON-RPC method into the scoped media repository; audio the
  daemon flags `voiceNote` becomes a voice note transcribed with the host's
  speech to text. Outbound attachments are `data:` URIs in the `send` call.
  signal-cli has no voice-note flag on send, so outbound voice is an audio
  attachment captioned with the transcript (descriptor `voiceOut=attachment`).
- **Reactions, edits, deletes, typing:** `sendReaction`, `send` with
  `editTimestamp`, `remoteDelete` and `sendTyping` (`stop` to clear). Message
  ids are Signal millisecond timestamps. A reaction in a group needs the target
  author (`author:timestamp`); a bare timestamp means the peer in a direct chat.
- **Declared unsupported:** threads, buttons, presentations, stickers,
  locations, contacts, polls, native data and streaming drafts.
- **Media scope:** Signal has no secret, so the media account scope is derived
  from the daemon URL and the registered account.

## iMessage

`IMessageChannel` is a native v2 adapter on the same `providers::native`
plumbing as Signal (macOS only: `chat.db` in, `osascript` out). Its conformance
test (`imessage_adapter_passes_the_full_conformance_matrix_against_a_fake_messages_folder`)
runs all 24 kit features against a scratch Messages folder: a `chat.db` with the
real table layout, a recording AppleScript runner, and a stand-in that records a
sent row for every script, the way Messages does.

- **Inbound:** the database is polled read-only. Text, attachments and group
  chats (addressed by chat guid) are published on the ordered ingress path;
  a tapback becomes `reaction_added` or `reaction_removed`; a message of ours the
  recipient read becomes `read_receipt`. A repeated guid is dropped, and a
  message the sink does not take falls back to the legacy receiver.
- **Audio messages:** Apple's CAF voice messages are converted to AAC with
  `afconvert`, stored as voice notes and transcribed with the host's speech to
  text. Outbound voice is an audio attachment (descriptor `voiceOut=attachment`).
- **Outbound ids:** AppleScript returns none, so the new row is read back from
  `chat.db`. A send Messages never records is an uncertain outcome
  (`sent_before_error`), not a failure to retry.
- **Declared unsupported:** AppleScript cannot tapback, edit, unsend, show typing
  or thread, so reactions out, edits, deletes, typing, replies and threads answer
  `unsupported` before any script runs.
- **One reader or two:** the shell's `imessage_scanner` also reads `chat.db`, but
  in the desktop process, to ingest history into memory. This reader runs inside
  the channels module and follows new messages for a live conversation; the
  module cannot depend on the app, so each keeps a small reader over the same
  documented schema.
## Linq

`LinqChannel` is a native v2 adapter (iMessage, RCS and SMS through the Linq
Partner V3 API) on the same `providers::native` plumbing. Its conformance test
(`linq_adapter_passes_the_full_conformance_matrix_against_the_mock_api`) runs all
24 kit features against a mock Linq API and media host.

- **Inbound is a signed webhook.** The host forwards each raw POST as
  `DeliverWebhook`; the module authenticates the exact bytes and ingests it
  (before this the module refused Linq's inbound entirely, so nothing was ever
  delivered). The signature argument is `t=<X-Webhook-Timestamp>,v1=<X-Webhook-Signature>`,
  verified as HMAC-SHA256 over `"{timestamp}.{body}"` with `[linq] webhook_secret`
  and a 300 second window. Without a secret every delivery is refused. Only
  `message.received` is ingested; the sender allowlist and `is_from_me` filter
  apply, and a repeated message id counts as a duplicate. A message the host
  does not take is an error, so Linq redelivers it.
- **Media and voice in:** `media`, `image`, `audio` and `video` parts are
  downloaded (credential-free: the URLs are pre-signed; https, or the API origin)
  into the scoped media repository. A part typed `voice`, `voice_memo` or
  `audio_message`, an audio part flagged `voice_memo`, or one named
  `Audio Message*` is stored as a voice note and transcribed with the host's
  speech to text.
- **Outbound:** text goes to `POST /chats/{id}/messages` (a phone number with no
  chat yet opens one with `POST /chats`). Files and voice clips are uploaded
  through `POST /attachments` and sent by `attachment_id`; voice is an audio
  attachment (descriptor `voiceOut=attachment`). The receipt id is Linq's message
  id. Typing uses `POST`/`DELETE /chats/{id}/typing`.
- **Declared unsupported:** reactions, edits, deletes, replies, threads and
  inbound events other than messages.

## Mattermost

Mattermost uses the v4 REST API. With both the scoped media repository and
ordered event sink installed, inbound posts publish typed text and files;
file bytes are fetched from `GET /api/v4/files/{id}` and file metadata comes
from the post's `metadata.files` or `GET /api/v4/files/{id}/info`. Downloads
are bounded to the provider's 50 MiB limit before they enter the media store.
The post `root_id` is retained as the conversation thread and reply reference.
Audio attachments remain ordinary audio: Mattermost's file metadata has no
voice-note marker, so the adapter does not claim voice-note ingress or run
speech-to-text on arbitrary audio files. Outbound voice is sent as an audio
file attachment (`voiceOut=attachment`) when the host supplies voice storage.

## Email

`EmailChannel` (feature `email`) is a native v2 adapter on the same
`providers::native` plumbing. Its conformance test
(`email_adapter_passes_the_full_conformance_matrix_against_a_capturing_relay`)
runs all 24 kit features against a capturing SMTP relay, and drives inbound mail
through the same dispatch the IMAP loop uses.

- **Envelope:** an inbound message keeps its `Subject:` (the text is
  `Subject: ...` then the body, as before), `Message-ID` (the message id),
  `In-Reply-To` (`reply_to_id`) and the thread root (`conversation.thread_id`:
  the first `References` id, else `In-Reply-To`, else the message itself). The
  conversation is the sender's address.
- **Outbound is one message per reply.** The planned text becomes the body,
  files and voice clips become attachments (`bundles_steps`), so text plus an
  image is one email, not two. The receipt id is the `Message-ID` the adapter
  generated for the send. `reply_to_id` and `thread_id` become `In-Reply-To` and
  `References`; the subject is the leading `Subject:` line if there is one, else
  `Re:` the subject of the thread's inbound mail, else `OpenHuman Message`.
- **Voice:** any inbound audio attachment is stored as a voice note and
  transcribed with the host's speech to text. Outbound voice is an audio
  attachment (descriptor `voiceOut=attachment`).
- **Failures:** a recipient that is not a single address (whitespace, a comma,
  angle brackets, control characters) is rejected before the relay; a relay
  refusal or timeout is a retryable transient failure (`MailError::TimedOut` is
  documented to mean "not accepted").
- **Declared unsupported:** reactions, edits, deletes, typing, drafts and
  inbound events.

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

## WhatsApp (Cloud API and Web)

Both WhatsApp backends are native v2 adapters (`adapterMode=native-v2`, with
`backend=cloud` or `backend=web` on the descriptor). Each implements
`ChannelAdapter`, `ChannelTypedSend`, `ChannelTyping`, `ChannelReaction`,
`ChannelEdit`, `ChannelDelete` and `ChannelStreamingDraft`, and the module runs
either through `Running::start_native` on the same ordered operation queue as
Telegram and Slack. Capability facts are derived from the exact adapter and the
ports it was built with (`whatsapp_shared::capabilities`): media kinds only
with scoped host media storage, voice only with the host voice store, edit and
unsend only on Web, a voice waveform only on Web.

Each backend runs all 24 kit features, and every observation is read from what
the platform double actually received:

- `the_cloud_adapter_passes_the_whole_conformance_matrix_against_its_wire`
  records the Graph API requests a mock server received (message bodies,
  multipart media uploads, the `typing_indicator` read call) and feeds inbound
  features through a signed webhook.
- `the_web_adapter_passes_the_whole_conformance_matrix_against_its_session`
  records the protocol messages, reactions, edits, revokes and receipts a
  linked-session double was asked to perform and feeds inbound features through
  the adapter's real event handler.

Declared features (text, image, video, audio, file, sticker, location, contact,
voice in and out, reply addressing, typing, reactions both ways, read receipts;
plus edit and delete on Web) must reach the wire with the configured account
and conversation. Undeclared ones (buttons, polls, presentations, native data,
voice synthesis, thread addressing, streaming drafts; plus edit and delete on
the Cloud API, which cannot do either) must fail with an explicit
`unsupported` error before any platform call. Two tests pin the declared set
itself, so an adapter cannot quietly stop claiming a protocol-supported
feature.

Cloud webhook ingress is authenticated before it is parsed. The module tests in
`whatsapp_native` drive signed deliveries through `Running::webhook` and the
`channel_interaction` method: a delivery with no signature, a wrong one, or a
verify token in its place is refused with the client-error name; a delivery
for another `phone_number_id` is refused rather than routed; redeliveries are
published once; and an inbound push-to-talk note is stored, then transcribed by
the module. `compiled_artifact_authenticates_whatsapp_cloud_webhooks_and_runs_native_v2`
repeats the signed-webhook and native-send path against the compiled module.

Voice notes in WhatsApp are OGG/Opus. Inbound notes keep only the host's opaque
media reference (provider URLs and handles never reach history) and, on Web,
the sender's waveform. Outbound notes are sent as push-to-talk audio; on Web
the waveform the shared voice pipeline produced (`OutboundVoice::Media.waveform`)
is resampled to WhatsApp's 64 buckets. The Cloud API has no waveform field, so
that backend reports `nativeWaveform=false`.

WhatsApp Web scopes durable media by the linked account's phone-number JID,
learned from the live session (`media_account_identity`) rather than from the
mutable `session_path` label. Until the device is linked, media operations fail
closed; if a later connection reports a different account the identity is
withdrawn for the life of that runtime and inbound traffic is dropped.
