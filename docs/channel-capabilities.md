# Capabilities

This page is the master index of TinyChannels functionality.

## Host and provider contracts

- `ChannelInboundEnvelope` - normalized inbound event with ordered `ContentPart`
  values and provider address facts.
- `ChannelOutboundIntent` - normalized outbound request with typed parts,
  address, durability, and idempotency key.
- `MessageReceipt` - structured result for a provider operation.
- `ChannelCapabilities` - declared support for content, voice, addressing,
  reactions, edits, receipts, and streaming drafts.
- `PrepareChannelInbound` and `PrepareChannelIdentity` - module operations that
  normalize provider input and prepare provider-specific identity values.
- `BeginWebTurn`, `ProjectWebOutput`, and `EndWebTurn` - scoped module
  operations for projecting canonical agent output onto web events.

See [Channel Contracts](channel-contracts.md).

## Provider conformance

- `tinychannels-conformance` - a shared 24-case behavior matrix for typed
  content, voice, addressing, typing, reactions, edits, deletes, read receipts,
  and streaming drafts.
- Provider suites compare declared capabilities with actual mock-wire
  observations and require undeclared behavior to fail without platform I/O.

## Host Capabilities

- `ChannelHost` - optional capability aggregator.
- `ProviderContext` - standard provider construction context.
- `HostCapabilities` - cheap capability descriptor.
- `TurnDispatcher` - run a channel turn through the host harness.
- `Transcriber` and `SpeechSynthesizer` - voice input/output.
- `ApprovalGate` and `ReactionGate` - human approval and response gating.
- `ConversationStore` - durable per-session history.
- `EventSink` - host domain events.
- `LifecycleRegistry` - provider shutdown hooks.
- `RunLedger` - run and telemetry records.

The module executes provider operations through TinyBus. OpenHuman supplies
scoped host callbacks and retains authorization, account policy, and durable
conversation sessions.

## Relay Contracts

- `CapabilityDescriptor` - connector handshake descriptor.
- `RelayPlatformEntry` and `RelayDescriptorOptions` - descriptor projection
  inputs.
- `GatewayToConnectorFrame` - gateway frames: hello, outbound, interrupt,
  going idle, inbound ack.
- `ConnectorToGatewayFrame` - connector frames: descriptor, inbound,
  outbound result, interrupt inbound, going idle ack, passthrough forward.
- Relay auth helpers - signed upgrade and delivery payload verification.
- `relay_send_action_from_outbound_intent` - outbound intent to relay `send`
  action projection.
- `RelayTransport` and related traits - reconnectable frame transport.

See the relay section in [Channel Contracts](channel-contracts.md) and the
repository's relay module documentation.

## Delivery Capabilities

- `DeliveryDurability` - required, best effort, or disabled.
- `DurableFinalDeliveryCapability` - text, media, poll, payload, reply, thread,
  reconciliation, after-send, and after-commit capability flags.
- `negotiate_delivery_durability` - downgrade missing durable capabilities to
  best effort.
- `compute_backoff_ms` - OpenClaw-compatible recovery backoff.
- `recover_pending_deliveries` - drain pending entries in enqueue order.
- Unknown-send reconciliation for crash or transport ambiguity after platform
  send starts.

See [Delivery](channel-delivery.md).

## Controller capabilities

- `ChannelController` - typed TinyBus operations for setup, status, health,
  and channel management.
- `ChannelDefinition`, `ChannelAuthMode`, and `ChannelCapability` - setup and
  connection metadata exchanged with the host.
- `ChannelManager` and `ChannelBackend` remain compatibility APIs inside the
  library; OpenHuman's host integration uses the module operations instead of
  implementing this backend trait.

## Built-In Providers

Exported providers include:

- `DingTalkChannel`
- `DiscordChannel`
- `EmailChannel`
- `IMessageChannel`
- `IrcChannel`
- `LarkChannel`
- `LinqChannel`
- `MattermostChannel`
- `QQChannel`
- `SignalChannel`
- `SlackChannel`
- `WhatsAppChannel`
- `YuanbaoChannel`

See [Providers](channel-providers.md).
