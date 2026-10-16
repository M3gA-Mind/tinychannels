# TinyChannels Channel Contract v2

**Status: proposed contract.** This document defines the target portable channel
boundary for TinyChannels and OpenHuman. It is a specification, not a claim
that every field or behavior is implemented. Existing Rust types and wire
surfaces are inventoried in Current implementation. The owning implementations
and adapters may land in later changes.

## Goals and boundaries

The contract gives channel providers one transport-neutral representation for
inbound messages, outbound content, identity, delivery results, and runtime
capabilities. It preserves provider facts that affect routing or security and
allows hosts to adapt richer content to less capable providers.

TinyChannels owns portable DTOs, provider adapters, normalization and provider
wire behavior. The host owns credentials and secret storage, authorization
policy, agent execution, durable storage, and product event-bus wiring. The web
provider and web UI remain host consumers. Provider-specific data may cross the
contract as opaque native data, but must not replace portable fields when
portable fields apply.

## Wire conventions

- JSON object fields use camelCase; enum values use snake_case.
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
boundaries through normalization and delivery. An empty parts list is invalid;
plain text is represented by a text part. A message may contain more than one
text, media, or attachment part.

Illustrative JSON shapes:

    { "kind": "text", "text": "Hello" }
    { "kind": "media", "media": { "url": "https://…", "mediaType": "image/png" }, "caption": "Chart" }
    { "kind": "file", "file": { "url": "https://…", "name": "report.pdf", "mediaType": "application/pdf", "sizeBytes": 1234 } }
    { "kind": "voice_note", "audio": { "url": "https://…", "mediaType": "audio/ogg", "codec": "opus", "durationMs": 2400 }, "waveform": [0, 18, 42], "pushToTalk": true, "transcript": { "text": "Turn left", "language": "en", "state": "complete" } }
    { "kind": "poll", "question": "Lunch?", "options": ["A", "B"], "allowsMultiple": false }
    { "kind": "presentation", "format": "provider-neutral-or-native", "data": {} }
    { "kind": "native", "provider": "telegram", "data": {} }

The schema above is illustrative; Rust types and JSON Schema are authoritative
when implemented. Every part has a stable discriminator and typed payload.
Media references can carry a URL, host-local path, media type, file name, byte
size, and provider message identifier where known. A provider must not turn an
opaque remote URL into a trusted local path.

voice_note is a first-class part, not merely an audio attachment or a string
transcript. It carries audio reference and media type, with optional codec and
duration in milliseconds; optional normalized waveform samples (ordered,
bounded amplitude values, not provider-specific binary encoding); pushToTalk,
distinguishing voice-note capture from ordinary audio; and optional transcript
with text, optional language, and state pending, complete, or unavailable.

Transcription is enrichment, not a replacement for audio. Keep the audio even
when a transcript exists. Missing waveform, codec, duration, or transcript is
valid. Invalid or unavailable optional metadata is omitted. Outbound voice
delivery can either send supplied audio or synthesize audio from text when the
host/provider advertises the relevant capability. Synthesis must be explicit in
the send request or selected by host policy; a provider must not silently
replace requested text with generated speech. Receipts identify the actual
delivered voice/media parts.

If a destination cannot deliver a part, the adapter reports a typed
unsupported-content failure or applies an explicit host-selected fallback.
It must not silently discard content. Text-only legacy providers receive text
parts joined in order using the compatibility projection described below;
non-text content is surfaced as unsupported unless the caller explicitly
permits a textual fallback.

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
| access | Ingress facts and upstream authorization result; not host policy. |
| raw | Optional provider payload for diagnostics/adaptation; private data must be redacted before logging. |
| timestamp | Optional source timestamp in Unix milliseconds. |

The legacy implementation has a top-level text and media vector. During
migration, producers populate both parts and those legacy projections when
they can represent the same content. The canonical v2 representation is parts;
consumers prefer it whenever present and must not concatenate it with the
projections.

access records dmDecision (allow, pairing, deny), groupPolicy (open,
allowlist, disabled), mentionGate (explicit mention, bot reply, thread
participant, native command, or none), and commandAuthorized (default false).
These are source-side facts, not a grant from the host. deliveredViaUpstreamRelay
is authenticated transport provenance held out-of-band and is never accepted
from serialized input. Authorization decisions remain host owned.

Outbound requests address channel/account, conversation, optional
replyToId/threadId, ordered parts, idempotency key, and durability policy
(required, best_effort, disabled). Retries for a given idempotency key must not
intentionally create a second logical delivery. required means the host's
durable queue must accept the request before provider send; providers must not
claim durable acceptance themselves.

Session keys derive from namespace, channel, account, conversation kind,
scope, conversation, thread, and optionally participant according to explicit
session policy. Keep scope and topic distinct. A migration must preserve the
existing key lookup path; do not switch identities by selecting recent files or
inventing timestamps.

## Events and delivery receipts

The provider-neutral event stream is ordered per runtime and includes these
event families:

| Event | Required data | Purpose |
| --- | --- | --- |
| message_received | Full inbound envelope | New inbound message. |
| message_updated | Address, message ID, updated parts, source timestamp | Provider edit/update. |
| message_deleted | Address, message ID, source timestamp | Provider deletion. |
| reaction | Address, message ID, actor, reaction, add/remove | Reaction change. |
| typing | Address, actor, active state | Ephemeral typing state. |
| lifecycle | Provider/account, state, timestamp, optional sanitized reason | Connect, disconnect, health, restart. |
| delivery_status | Idempotency key, status, optional receipt/error | Outbound progress/result. |

Each event carries a stable event ID when the provider supplies one; otherwise
the adapter creates a deterministic ID scoped to the source event where
possible. Preserve source timestamps and provider IDs. A replayed event must
remain identifiable as the same event. Events do not carry credentials or
unsanitized exception text.

A send returns MessageReceipt: the idempotency key, sent time, primary
platform message ID when available, and ordered parts. Each receipt part
identifies the platform message ID, logical content kind, zero-based part
index, optional thread/reply IDs, and optional opaque raw source result.
Multi-message sends return every emitted platform message. A partial send must
report the parts already delivered and a typed failure for the remainder; it
must never fabricate all-or-nothing success.

Edit/delete tokens, when required by a provider, are opaque and scoped to the
receipt part. Receipt raw data is for round-tripping only; callers use the
normalized fields.

## Unified capabilities

Expose one ChannelCapabilities descriptor assembled for a configured
provider/account. It combines the currently separate static features,
presentation limits, action support, outbound content support, delivery
semantics, and runtime state. Static catalog facts and per-account/runtime
facts are distinguishable within the same descriptor; consumers must not
assume a provider-level true means the current account is configured or
available.

The descriptor includes supported conversation types and content kinds
(including voice note, voice synthesis, files, polls, and presentation/native
content); supported actions such as reply, thread, edit, delete/unsend,
reactions, typing, draft update, and native commands; text limit and length
unit (characters, utf8_bytes, utf16_units), markup dialect, and
presentation/action limits; durable-final delivery support keyed by the stable
capability set below; and current account state (linked, not_linked,
configured, not_configured, enabled, disabled), health, and reconnect facts.

The durable-final keys are text, media, poll, payload, silent, replyTo, thread,
nativeQuote, messageSendingHooks, batch, reconcileUnknownSend,
afterSendSuccess, and afterCommit. Unknown additive capability keys are
ignored. An omitted capability means unsupported. false is not inferred from
missing runtime state. A relay descriptor is a projection of this unified
descriptor, not a competing capability vocabulary. The existing relay
handshake version 1 remains frozen and additive-only; projection must preserve
its current field names/defaults until a separately versioned relay wire change
is approved.

## Web surface events

The OpenHuman web surface consumes the same envelopes, parts, receipts, and
capabilities as other providers. Its application event adapter emits typed
events with a versioned type and payload; the event stream is scoped to the
authenticated conversation/runtime and carries a monotonically increasing
sequence number for reconnect replay.

The web event set maps to message_received, message_updated, message_deleted,
reaction, typing, lifecycle, and delivery_status. Outbound user messages and
agent replies use the same ordered ContentPart array. Text-delta streaming
remains an ephemeral harness event and is not mistaken for a committed message
or receipt. Finalization produces the normal message and receipt events. The
host authenticates and authorizes subscriptions and commands; event payloads
never establish authority by themselves.

Provider-independent web payloads preserve unknown native content as opaque
data but render it only through an explicit safe renderer. Attachment URLs
must be host-authorized, and event replay must not expose another account's
conversation.

## Migration and compatibility window

The v2 content model is introduced additively on the current TinyBus wire
contract revision 21. The existing ChannelMessage, SendMessage, Channel trait,
legacy top-level text/media projections, and current TinyBus members remain
supported for one full release after v2 consumers and producers are available.
This is the v21 compatibility window:

1. Add v2 content parts and envelopes without changing legacy field meaning.
2. Dual-write representable fields. Legacy consumers continue receiving
   ChannelMessage; v2 consumers read parts when present.
3. Add adapters between Channel and v2. send maps text-only requests to a
   text part; listen maps legacy messages to envelopes while preserving
   provider IDs and address facts that exist. Rich content cannot be fabricated
   from a legacy text message.
4. During the next release, hosts may opt into v2 and report compatibility
   use. Keep both directions tested.
5. After one full released compatibility window, remove the old trait and
   projections only in a separately announced breaking release and contract
   revision. Never silently change the meaning or serialization of an existing
   TinyBus member.

Here v21 is the current TinyBus contract revision, not a Cargo semver major
version. The current module operation version is 1.21. Implementation must
assign the additive revision and operation-version increments before changing
the wire. The compatibility window begins only when a released host and module
both support v2.

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
| Web surface | host-owned, no TinyChannels provider feature | All portable content/events, subject to host authorization | Remains an OpenHuman consumer, not a TinyChannels provider. |
| Relay connector | relay-websocket for WebSocket I/O | Projection of portable events, content and capability facts | Existing descriptor wire v1 remains frozen. |

The exact provider-to-feature assignments are the crate's Cargo feature
definitions. This table is a capability planning matrix; do not infer feature
availability from it when building a host.

## Current implementation

The contract crate currently exposes ChannelInboundEnvelope with text/media,
ChannelOutboundIntent with text/media/voice/files/poll/presentation/native
variants, MessageReceipt, separate static and presentation capability structs
plus durable-final keys, legacy Channel, and relay CapabilityDescriptor v1.
The harness exposes turn admission, inbound lifecycle stages, and output
events. Provider voice paths may transcribe audio before legacy dispatch. The
full ordered ContentPart model, first-class voice-note metadata, unified
descriptor, normalized provider event family, and shared web event wire
described above remain v2 requirements.

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

