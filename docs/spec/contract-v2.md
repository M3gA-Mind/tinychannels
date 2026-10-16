# TinyChannels Channel Contract v2

**Status: proposed contract.** This document defines the target portable channel
boundary for TinyChannels and OpenHuman. It is a specification, not a claim
that every field or behavior is implemented. Existing Rust types and wire
surfaces are inventoried in [Current implementation](#current-implementation).
The owning implementations and adapters may land in later changes.

## Goals and boundaries

The contract gives channel providers one transport-neutral representation for
inbound messages, outbound content, identity, delivery results, and runtime
capabilities. It preserves provider facts that affect routing or security and
allows hosts to adapt richer content to less capable providers.

TinyChannels owns portable DTOs, provider adapters, normalization, provider
wire behavior, and the web-surface event adapter in the TinyChannels runtime.
The host owns credentials and secret storage, authorization policy, agent
execution, durable storage, UI rendering, and application event-bus wiring.
Provider-specific data may cross the contract as opaque native data, but must
not replace portable fields when portable fields apply.

## Wire conventions

- JSON object fields use camelCase; enum values use snake_case.
- Addressing fields retain the established wire spellings reply_to_id and
  thread_id in every v21 compatibility shape and in every adapter projection.
  They are stable exceptions to camelCase and must never be renamed to
  replyToId/threadId during migration.
- Discriminated unions use a stable kind or type field. Unknown additive
  object fields are ignored when safe. Unknown enum variants must fail closed
  where they affect authorization or delivery; otherwise adapters preserve them
  as unknown/native data when the schema permits.
- Identifiers are opaque strings. Do not parse provider IDs for business
  meaning or normalize their case.
- Optional values are absent or null; empty strings are not a substitute for
  missing identifiers.
- Timestamps use Unix milliseconds unless a field explicitly says otherwise.
- Secret values never appear in DTOs, receipts, logs, or event payloads. Use a
  host-owned secret reference where configuration must name a credential.
- A serialized field's name, type, meaning, and omission behavior are wire
  compatibility commitments.

## Content model

Messages contain ordered ContentPart values. Preserve order and part
boundaries through normalization and delivery. An empty parts list is invalid.
Each variant has a kind discriminator and typed fields:

| Kind | Required data | Optional data and behavior |
| --- | --- | --- |
| text | text | markdown dialect (plain, CommonMark, or provider-markup tag); formatting remains text, never HTML authority. |
| image | media reference | caption, alt text, width, height. |
| video | media reference | caption, duration, width, height, thumbnail reference. |
| audio | media reference | codec, duration, title, transcript; ordinary audio is distinct from a voice note. |
| file | media reference | file name, byte size, caption. |
| voice_note | VoiceNote (below) | Transcript and waveform may be absent. |
| sticker | media reference | pack/id, emoji, alt text. |
| location | latitude, longitude | label, address, accuracy, live-update expiry. |
| contact | display name | phone numbers, emails, organization; each contact method is typed. |
| buttons | ordered rows of typed actions | labels, styles, disabled state; action IDs are opaque and scoped to the conversation. |
| poll | question, ordered options | multiple-choice flag, close time, provider poll ID. |
| presentation | typed blocks | provider-neutral blocks or an explicitly identified native block set. |
| native | provider and opaque data | Used only when no portable kind can represent the provider feature. |

All media references can carry a URL, host-local path, media type, codec where
applicable, file name, byte size, duration, and provider message identifier.
Absent fields are omitted. A provider must not turn an opaque remote URL into a
trusted local path. An adapter must preserve media identity and order.

Text parts may declare their markdown dialect: plain, CommonMark, HTML,
Slack mrkdwn, Discord markdown, Telegram MarkdownV2, or an open provider tag.
The declared dialect describes formatting and does not grant permission to
execute markup. When converting dialects, preserve text and report unsupported
formatting rather than silently stripping meaningful content.

VoiceNote contains an audio media reference; optional codec and durationMs;
optional normalized waveform samples; pushToTalk; and an optional transcript.
Waveform is a bounded ordered list of normalized amplitude samples, not a
provider-specific binary encoding. Transcript contains text, optional language,
state (pending, complete, unavailable), and source (provider or host_stt).
Transcription is enrichment, not a replacement for audio. Keep the audio even
when a transcript exists.

Outbound Voice is a tagged union with exactly two forms: media, which carries
an existing audio media reference and its metadata, or synthesize, which carries
text and an optional requested voice identifier. Synthesis occurs only when
explicitly requested and supported by host policy and the provider. A provider
must not silently replace requested text with generated speech. Receipts
identify the actual delivered voice part and whether it was media or synthesis.

Rust types and JSON Schema become authoritative when implemented. If a
destination cannot deliver a part, the adapter returns a typed
unsupported-content failure or applies an explicit caller-selected fallback.
It must not silently discard content. A v21 text-only adapter joins text parts
in order and rejects non-text parts unless the caller explicitly permits a
textual fallback.

## Addresses and message envelopes

Channel and conversation addressing is lossless. A channel reference contains
provider/channel id and optional accountId. A conversation reference contains
kind, opaque id, and optional scopeId, parentId, threadId, and topicId.
scopeId distinguishes tenants, servers, workspaces, or guilds; parentId
identifies a parent conversation; thread and topic are separate because
provider semantics differ. A sender reference contains opaque id, ordered
altIds, optional display name, isBot, and provider roles.

ChannelInboundEnvelope contains:

| Field | Meaning |
| --- | --- |
| channel | Channel/account reference. |
| messageId | Provider message identifier, stable in that channel/account. |
| conversation | Lossless conversation and scope address. |
| sender | Sender identity facts as provided by the channel. |
| parts | Ordered content parts; text is retained as a compatibility projection during migration. |
| reply_to_id | Optional source message being replied to. Preserve the exact provider ID. |
| thread_id | Optional provider thread identifier. Preserve separately from topic_id. |
| access | Ingress facts and upstream authorization result; not host policy. |
| raw | Optional provider payload for diagnostics/adaptation; private data must be redacted before logging. |
| timestamp | Optional source timestamp in Unix milliseconds. |

The legacy implementation has a top-level text and media vector. The v21
compatibility shim projects text parts by joining their text values in order
with one U+000A LINE FEED between adjacent text parts. It does not trim or
escape text. Non-text parts are not included in text; a send containing
non-text content must fail as unsupported unless the caller explicitly chose a
fallback before conversion. The canonical v22 representation is parts; never
concatenate both forms in one consumer.

access records dmDecision (allow, pairing, deny), groupPolicy (open,
allowlist, disabled), mentionGate (explicit mention, bot reply, thread
participant, native command, or none), and commandAuthorized (default false).
These are source-side facts, not a grant from the host. deliveredViaUpstreamRelay
is authenticated transport provenance held out-of-band and is never accepted
from serialized input. Authorization decisions remain host owned.

Outbound requests address channel/account, conversation, optional
reply_to_id and thread_id, ordered parts, idempotency key, and durability policy
(required, best_effort, disabled). Retries for a given idempotency key must not
intentionally create a second logical delivery. required means the host's
durable queue must accept the request before provider send; providers must not
claim durable acceptance themselves.

Session keys derive from namespace, channel, account, conversation kind,
scope, conversation, topic, thread, and optionally participant according to
explicit session policy. Include topic_id and thread_id as distinct segments
when present; a topic-only address remains distinct when thread_id is absent.
When both are present, include both in that order. Keep scope, topic and thread
distinct. A migration must preserve the existing key lookup path; do not switch
identities by selecting recent files or inventing timestamps.

## Events and delivery receipts

The provider-neutral event stream is ordered per runtime and uses a tagged
type discriminator. It includes:

| type | Required data | Purpose |
| --- | --- | --- |
| stream_open | turnId, conversation, optional messageId | Starts one agent-output stream. |
| stream_delta | turnId, delta text, optional sequence | Incremental text; never a committed message. |
| stream_finalize | turnId, final ordered parts, receipt | Commits the final agent output. |
| message_received | Full inbound envelope | New inbound message. |
| message_updated | Address, message ID, updated parts, source timestamp | Provider edit/update. |
| message_deleted | Address, message ID, source timestamp | Provider deletion. |
| reaction_added / reaction_removed | Address, message ID, actor, typed reaction | Inbound reaction change. |
| outbound_reaction | Address, message ID, reaction, result | Host requested reaction and provider result. |
| read_receipt / delivered_receipt | Address, message ID, actor, timestamp | Provider read/delivered acknowledgment. |
| presence | Account/sender, state, optional expiry | Online, offline, away, or provider-defined presence. |
| typing | Address, actor, active state, optional expiry | Ephemeral typing state. |
| lifecycle | Provider/account, state, timestamp, optional sanitized reason | Connect, disconnect, health, restart. |
| delivery_status | Idempotency key, status, optional receipt/error | Outbound progress/result. |
| approval | turnId, requestId, prompt, typed choices, state | Approval request and resolution. |
| tool_progress | turnId, toolId, state, optional content-free summary | Tool execution progress. |
| subagent | turnId, agentId, state, optional content-free summary | Child-agent lifecycle. |
| usage | turnId, input/output/cached token counts when known | Turn usage facts. |
| artifact | turnId, artifact reference and operation | Artifact created/updated. |
| queue | item reference, state, optional position | Delivery/turn queue changes. |
| suggestion | typed suggestion and state | User-facing follow-up suggestion. |

Each event carries a stable event ID when the provider supplies one; otherwise
the adapter creates a deterministic ID scoped to the source event where
possible. Preserve source timestamps and provider IDs. A replayed event must
remain identifiable as the same event. Events do not carry credentials or
unsanitized exception text.

A send returns MessageReceipt: the idempotency key, sent time, primary
platform message ID when available, and ordered parts. Each receipt part
identifies the platform message ID, logical content kind, zero-based part
index, optional thread_id/reply_to_id, optional edit_token and delete_token,
and optional opaque raw source result.
Multi-message sends return every emitted platform message. A partial send must
report the parts already delivered and a typed failure for the remainder; it
must never fabricate all-or-nothing success.

edit_token and delete_token, when required by a provider, are opaque and scoped
to the receipt part. Receipt raw data is for round-tripping only; callers use
the normalized fields. Receipt part thread_id and reply_to_id retain those
exact spellings.

## Unified capabilities

Relay and local providers reuse the same ChannelCapabilities type and schema.
It is assembled for a configured provider/account and combines static
features, presentation limits, action support, content/event support, delivery
semantics, and runtime state. Static catalog facts and per-account/runtime
facts are distinguished within the descriptor; a provider-level true does not
mean the current account is configured or available. A legacy relay v1
handshake may use an adapter for its frozen wire shape during the one-release
compatibility window; v2 uses the shared descriptor directly.

The descriptor has explicit fields:

| Field | Meaning |
| --- | --- |
| content_parts | Supported ContentPart kinds: text, image, video, audio, file, voice_note, sticker, location, contact, buttons, poll, presentation, native. |
| markdown_dialects | Accepted/emitted markup dialects for text parts. |
| voice_in | Whether voice notes are accepted; accepted codecs, max_duration_ms, max_size_bytes, normalized_waveform, native_waveform, and transcript sources (provider/host_stt). |
| voice_out | Whether Voice media and Voice synthesize are supported; accepted codecs, max_duration_ms, max_size_bytes, normalized_waveform, native_waveform, available voice identifiers, and synthesis support. |
| events | Supported normalized event types: stream_open/delta/finalize, inbound message, edit/delete, inbound/outbound reaction, read/delivered receipt, presence, typing, lifecycle, delivery_status, approval, tool_progress, subagent, usage, artifact, queue, suggestion. |
| conversation_types | Supported dm, group, channel, and thread conversation kinds. |
| addressing | Support for account_id, scope_id, parent_id, topic_id, reply_to_id, and thread_id, with provider restrictions represented per operation. |
| actions | Supported send/reply/thread/edit/delete/reaction/read/typing/draft/poll/buttons/native-command actions. |
| text | text_limit, length_unit (characters, utf8_bytes, utf16_units), markdown dialect, and chunk constraints. |
| image, video, audio, file, sticker | Accepted/emitted codec or media types, maximum size, and applicable duration/dimension limits. |
| location, contact, buttons, poll, presentation, native | Support flags plus typed size/count/action limits where applicable. |
| delivery | Durable-final support, idempotency, batch, partial result, and unknown-send reconciliation facts. |
| account | linked/not_linked/configured/not_configured/enabled/disabled state, health and reconnect facts. |

An absent kind or event means unsupported. Omitted codec/media-type lists mean
the adapter does not advertise a restricted list, not that arbitrary values
are guaranteed. Unknown additive fields are ignored. The supported durable
final-delivery keys are text, media, poll, payload, silent, replyTo, thread,
nativeQuote, messageSendingHooks, batch, reconcileUnknownSend,
afterSendSuccess, and afterCommit.

The existing relay handshake version 1 remains frozen and additive-only until
the versioned v2 relay handshake is negotiated.

## Web surface events

The TinyChannels web adapter consumes the same envelopes, parts, receipts, and
ChannelCapabilities as other transports. It emits a versioned tagged union:

| type | Payload |
| --- | --- |
| message | Full inbound or committed outbound envelope. |
| message_updated, message_deleted | Address, message ID, updated parts or deletion, source timestamp. |
| stream_open, stream_delta, stream_finalize | Turn identifier, ordered deltas/final parts, and final receipt. |
| tool, subagent | Typed identifiers, state, and bounded progress data. |
| usage | Input, output, and cached token counts when known. |
| approval | Request identifier, prompt, typed choices, and pending/resolved state. |
| artifact | Artifact reference and create/update/delete operation. |
| queue | Queue item reference, state, and optional position. |
| suggestion | Typed suggestion and state. |
| reaction_added, reaction_removed, outbound_reaction | Address, message ID, actor/reaction, and result where applicable. |
| read_receipt, delivered_receipt | Address, message ID, actor, timestamp. |
| presence, typing, lifecycle, delivery_status | The corresponding normalized channel event payload. |

Every event carries schemaVersion, conversation/runtime scope, and a
monotonically increasing sequence number for replay. Text deltas are ephemeral
and are not committed messages or receipts. Finalization carries the committed
parts and receipt. Outbound user messages and agent replies use the same
ContentPart array. The host authenticates subscriptions and authorizes
commands; event payloads never establish authority. The web adapter preserves
unknown native content as opaque data; rendering requires an explicit safe
renderer. Attachment URLs must be host-authorized, and replay cannot cross
account/conversation scope.

## Migration and compatibility window

The v2 content model advances the current TinyBus wire contract revision 21 to
revision 22 and the module operation version 1.21 to 1.22. v21 is the current
wire revision, not a Cargo semver major version. Keep existing member names,
arities, and serialized meaning unchanged.

The migration has one full release compatibility window:

1. Revision 22 adds ContentPart, Voice, normalized event, and shared capability
   DTOs. It retains ChannelMessage, SendMessage, the Channel trait and all
   existing TinyBus members.
2. Ship a single v21 compatibility shim at the boundary. It maps legacy text
   to one text part and back, preserves reply_to_id/thread_id exactly, and
   passes through address fields when available. It rejects or explicitly
   degrades rich parts according to caller policy; it never fabricates them.
3. During the first released revision-22 host/module pair, the shim is
   supported and covered in both directions. New v22 paths use parts and the
   unified descriptor directly. Do not dual-write two competing content
   representations indefinitely.
4. After that one full release, remove the shim and legacy trait/projections
   only in a separately announced breaking revision. No revision may silently
   change the meaning or serialization of an existing TinyBus member.

The compatibility window begins when a released host and module both support
revision 22. A v21 consumer continues using v21 unchanged; it does not infer
v22 support from an unknown field.

## Feature and provider matrix

This matrix distinguishes Cargo build features from portable contract
capabilities. A provider build does not imply every connected account supports
every capability; query the configured descriptor.

| Feature / provider | Build gate today | Contract coverage required in v2 | Current caveat |
| --- | --- | --- | --- |
| Telegram | always-on (Cargo default is empty) | Text, media, voice notes, threads/topics, receipts, reactions, typing, edits, native commands | Voice transcription currently projects to legacy text; v2 must retain audio and transcript together. |
| Discord | always-on (Cargo default is empty) | Text, media/files, threads, receipts, reactions, edits, native interactions | Account and guild facts are runtime-specific. |
| Slack | always-on (Cargo default is empty) | Text, files/media, threads, receipts, reactions, edits, typing, drafts | Workspace scope must remain in address/session identity. |
| Signal | always-on (Cargo default is empty) | Text, attachments, voice notes, receipts, reactions where supported | Provider capability varies by runtime. |
| WhatsApp Cloud | always-on (Cargo default is empty) | Text, media/files, voice notes, receipts, reactions where supported | Preserve provider message IDs for idempotency/reconciliation. |
| WhatsApp Web | whatsapp-web | Same portable content and addressing as supported by runtime | Session persistence is currently in-memory; not a contract guarantee. |
| Email | email | Text, files/media, receipts, reply/reference addressing | Send-only build uses the separate gate below. |
| Email send-only | email-send | Text and file attachments; no inbound/listen capability | No Channel listener implementation by design. |
| Lark/Feishu | lark | Text, media/files, threads, cards/native content, receipts | Optional provider dependency. |
| IRC, iMessage, Mattermost, QQ, Yuanbao/DingTalk, Linq | always-on (Cargo default is empty) | Text and the provider-supported subset of addressing/events/receipts | Capability is reported per provider/account; no blanket rich-media promise. |
| Web surface adapter | tinychannels-runtime adapter using host-supplied transport | All portable content/events, subject to host authorization | Host supplies transport, authentication, authorization, and application event wiring. |
| Relay connector | relay-websocket for WebSocket I/O | Projection of portable events, content and capability facts | Existing descriptor wire v1 remains frozen. |

Contract feature coverage is summarized separately from build features:

| Contract feature | Part family | Event family | Capability fields |
| --- | --- | --- | --- |
| Rich text | text, markdown dialect | stream_open/delta/finalize | text, markdown_dialects, length_unit, text_limit |
| Attachments | image, video, audio, file, sticker | message, delivery_status | per-kind media types/codecs, max_size_bytes, duration/dimensions |
| Voice | voice_note; Voice media/synthesize | message, stream_finalize | voice_in, voice_out, accepted codecs, max duration/size, waveform, transcript source |
| Structured interaction | buttons, poll, presentation, native | outbound_reaction, approval, artifact, suggestion | actions, per-kind support and limits |
| Identity and addressing | sender, channel, conversation, scope, reply_to_id, thread_id | message, reaction, receipts, presence, typing | conversation types, address features |
| Delivery | ordered parts and idempotency key | delivery_status, read_receipt, delivered_receipt | durability, partial results, reconcileUnknownSend |
| Runtime and harness | all applicable parts | lifecycle, tools, subagent, usage, queue | events, account state, health, reconnect facts |

The provider-to-Cargo-feature assignments remain those in Cargo.toml. This
provider matrix is a capability planning guide; query the configured
descriptor at runtime rather than inferring account support from build flags.

## Current implementation

The contract crate currently exposes ChannelInboundEnvelope with text/media,
ChannelOutboundIntent with text/media/voice/files/poll/presentation/native
variants, MessageReceipt, separate static and presentation capability structs
plus durable-final keys, legacy Channel, and relay CapabilityDescriptor v1.
The harness exposes turn admission, inbound lifecycle stages, and output
events. Provider voice paths may transcribe audio before legacy dispatch. The
full ordered ContentPart model, first-class voice-note metadata, unified
descriptor, normalized provider event family, and shared web event wire
described above remain v2 requirements. Wire revision 21 and module operation
version 1.21 are current implementation facts; revision 22 and operation
version 1.22 in this document are the proposed next contract increment.

Track implementation status in code changes and release notes, not by editing
this normative vocabulary to imply that a partially migrated producer has
finished the contract.

## Superseded material

This contract supersedes the earlier channel-model and migration claims in
[tinychannels-execution-plan.md](tinychannels-execution-plan.md),
[openclaw-hermes-channel-porting.md](openclaw-hermes-channel-porting.md), and
[porting.md](../porting.md). Those documents remain historical research and
migration context; their proposed shapes and status statements are not
authoritative where they conflict with this contract.
