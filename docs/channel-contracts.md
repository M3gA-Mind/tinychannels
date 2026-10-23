# Channel Contracts

TinyChannels separates portable channel data from provider implementation.
Hosts exchange requests and results with the loaded module through
`tinychannels-bus`; provider transports and normalization logic remain inside
TinyChannels. The bus DTOs are the host boundary, not provider objects or an
application-specific callback trait.

## Compatibility DTOs and module boundary

`ChannelMessage` and `SendMessage` remain compatibility shapes for paths that
still require them. They do not define the normalized v2 content model. Hosts
that construct or load providers use bus contracts and module operations;
provider implementation details stay behind that boundary. The
`tinychannels-runtime` crate contains shared provider algorithms, and
`tinychannels-module` exposes configured behavior through TinyBus.

The production provider factory returns `BuiltChannel` values backed by the
`ChannelAdapter` runtime contract. Optional adapter ports add provider-specific
operations such as typing, reactions, edits, deletes, and streaming drafts. The
older `Channel` and `ChannelBackend` APIs remain compatibility surfaces; the
module does not use the legacy test adapter as its production provider path.

Compatibility conversion is explicit. Legacy `thread_ts` maps to the
appropriate thread or topic address without changing its serialized field.
Text projections preserve text-part order, and non-text content must not be
silently dropped by a compatibility path.

## Normalized Inbound Envelope

`ChannelInboundEnvelope` is the inbound bus contract. It includes:

- `channel` - `ChannelRef` with channel id and optional account id.
- `message_id` - provider message id.
- `conversation` - `ConversationRef` with kind, id, scope, parent, thread, and
  topic fields.
- `sender` - `SenderRef` with ids, name, bot flag, and roles.
- `parts` - ordered typed content; `text` and `media` remain compatibility
  projections.
- `access` - DM/group/mention authorization facts.
- `media` - ordered media references.
- `raw` - optional provider payload for diagnostics or adapter-specific needs.

`PrepareChannelInbound` asks the module to normalize a configured provider
envelope and prepare provider-derived history, thread, client, and session
identity inputs. The host passes authenticated provenance separately;
serialized input cannot assert that it arrived through a trusted relay.
Telegram topic ids remain distinct from generic thread ids.

## Normalized Outbound Intent

`ChannelOutboundIntent` is the provider-neutral logical outbound send request:

- `idempotency_key`
- `channel_id`
- `conversation_id`
- `reply_to_id`
- `thread_id`
- `durability`
- `payload`

The canonical `Parts` payload carries ordered typed content, including text,
images, video, audio, files, voice notes, stickers, locations, contacts,
buttons, polls, presentation blocks, and provider-native data. Simpler legacy
payload variants remain available for compatibility. Unsupported content must
produce an explicit result or use a caller-selected fallback; it is not
silently discarded.

## Receipts

`MessageReceipt` normalizes platform send results:

- primary platform message id
- all platform message ids produced by a logical send
- per-part receipts for split or multi-part messages
- thread and reply linkage
- edit and delete tokens
- raw source results

The receipt helpers accept heterogeneous provider results and extract stable
ids from message, chat, room, conversation, JID, and poll fields.

## Capabilities

Provider capability data is split by concern:

- `ChannelStaticCapabilities` describes feature support such as polls,
  reactions, edits, threads, media, and native commands.
- `ChannelPresentationCapabilities` describes text limits, length unit,
  markdown dialect, edit support, and action layout constraints.
- `DurableFinalDeliveryCapability` describes whether a provider can guarantee
  durable final delivery for text, media, polls, payloads, replies, threads,
  reconciliation, hooks, batch sends, and commit timing.

Configured adapter descriptors are the runtime source of truth. The provider
matrix documents declared and tested support; it does not promise that every
provider or account supports every content kind or action.

## Provider conformance

Provider adapters declare capabilities and are checked against the shared
`tinychannels-conformance` feature matrix. Tests observe actual mock-platform
requests and returned identifiers; an undeclared feature must return an
unsupported result without performing a platform operation. The matrix covers
text and media kinds, voice, addressing, typing, reactions, edits, deletes,
read receipts, and streaming drafts. Provider-specific suites define the
tested subset for each adapter.
