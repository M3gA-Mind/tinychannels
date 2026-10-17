# tinychannels-bus

Serialized channel vocabulary, configuration/schema declarations, interface
names, relay frames and structured errors. Normal dependencies are Serde,
JSON/schema/error derivation, and Base64 used only by the existing
`PassthroughForward.body` custom serializer (`bodyB64`). It carries only wire
DTOs, names, constants, and errors. Provider traits and history helpers live in
`tinychannels-runtime`; this crate has no async runtime, async-trait macro,
`anyhow`, callbacks, scheduler, crypto, entropy, stateful locks, transport,
session derivation or delivery algorithms.

DTOs are defined once here. `tinychannels-runtime` and `tinychannels` re-export
these same Rust types. Hosts should name wire values through this crate and
execute behavior through the compiled module. The root library compatibility
facade is for existing implementation consumers, not a host dependency shortcut.

The method constants, public `METHODS` array, and compiled module export list
come from one registry in `names.rs`; host callbacks keep their own opposite
direction registry. The artifact test compares the compiled manifest with the
registry including declaration order.

`python3 scripts/check-bus-purity.py --offline` audits independent default and
all-feature normal/build dependency closures and rejects runtime source seams.
The approved 21-package closure includes Base64 solely for serialization;
crypto and UUID generation remain implementation dependencies.

The original module and host callback names, method arities, JSON field names,
optional fields and persisted configuration values are unchanged. See the
[relocation specification](../../docs/spec/pure-contract-relocation.md) for
necessary Rust source API migrations and subsequent module-operation slices.

`ChannelInteraction(request)` adds instance capability discovery, typing start/stop,
and draft creation, updates and finalization. It is a confidential call. The
module serializes these operations with ordinary sends in its bounded provider
queue; stopping a provider joins admitted work and rejects subsequent calls.
Contract version 6 adds the member without changing existing method arity.

Contract version 16 adds confidential `StartRuntimeWithDispatch(handle,
ChannelRuntimeDispatchPolicy)`. The listener count selects the existing module
admission rule (four turns per listener, clamped to 8–64). This opt-in runtime
calls its immutable host object's `ProcessInbound(channel, message)` for each
turn with bounded concurrency. The host supplies only its turn execution and
product policy; the compiled module owns the queue, admission, completion
frontier, failures, and drain. Like the existing `DeliverInbound` host callback,
`ProcessInbound` is a normal callback to the host process, not a confidential
call to an attested module recipient.

Existing `StartRuntime` and ordered `DeliverInbound` behavior remain unchanged.
Dispatch mode and effective limit cannot change after activation. Flush waits
for preceding accepted turns; stop freezes admission and retains pending work
across canceled waiters. The existing module cleanup deadline reports pending
cleanup after ten seconds while retaining the owner for a later stop retry.
Contract version 22 carries `ChannelInboundEnvelope` content parts through
`DeliverInbound` and `ProcessInbound`, and sends `ChannelOutboundMessage` values
with ordered typed parts through `SendChannelIntent`. The legacy `ChannelMessage`
and `SendMessage` conversions are deprecated, one-release compatibility
projections. Their outbound adapter accepts text parts only; rich payloads
remain available to providers that implement `Channel::send_typed`. Binding is
exact-version: a v21 host cannot bind a v22 module, so this shim is for source
migration and same-version legacy provider adapters, not cross-version callback
compatibility.

Contract version 23 adds confidential `BeginMediaUpload`,
`AppendMediaUploadChunk`, `FinishMediaUpload`, and `AbortMediaUpload` calls for
host-to-module media. An upload is accepted only on its owning runtime object
while the named configured provider instance is active. The caller supplies
the complete conversation and originating-turn scope; the module replaces
caller-selected provider/account identity with the active configured instance
before storing through its scoped media repository. A successful finalize
returns the existing opaque `MediaReference` DTO, which can be passed directly
to typed provider sends.

Each runtime accepts at most two active uploads. Each upload is at most 20 MiB,
split into ordered chunks of at most 512 KiB (40 chunks maximum); serialized
begin metadata is limited to 64 KiB. Incomplete uploads expire after one
minute. Finalization has a one-minute persistence deadline, retains its
reservation while storage is pending, and retries use the same deterministic
storage key. At most 128 terminal handle results are retained for five minutes.
Chunk retries are idempotent only when bytes match; conflicting retries fail.
The implementation validates media kind, MIME type, body and size through
`ScopedMediaStore` before returning a reference. This contract adds no host
implementation-library dependency or host-side media metadata schema.

This source contract still requires a compatible published native module before
a downstream host can claim released composition.

Provider notifications use `ChannelEvent`, a versioned DTO with a `type`
discriminator, stable event/source IDs, source time, `ChannelRef`, and the full
optional `ConversationRef`. The module sends it through `ChannelsHost.DeliverEvent`
in the same ordered callback queue as inbound messages. `PublishEvent` remains
available for older, unrelated domain publications.

Providers that support v22 ordered ingress submit `ChannelIngress` items through
their `EventSink`: normalized inbound envelopes and typed events share one
awaited path. Telegram uses this path for messages, edits and reactions, so a
later edit/reaction cannot overtake its message. The legacy message-channel
bridge remains for providers still migrating. Event message references retain
the stable module message ID while `providerMessageId` preserves the source ID;
Telegram topic IDs remain separate from chat IDs in `ConversationRef`.

Typed send outcomes may carry multiple provider message IDs and per-part edit
or delete tokens. Tokens are opaque operation inputs and are only populated
when the provider's actual response returns the corresponding message IDs.
Required delivery first persists the unknown-send state, then attaches any
partial receipt in a follow-up save; both writes preserve the pre-I/O crash
marker. Terminal receipts are included in idempotent replays.

The controller vocabulary includes complete Discord guild, text-channel and
permission DTOs. Provider APIs re-export these types at their existing paths;
REST calls and permission calculations remain implementation behavior.

The deployed OpenHuman web chat vocabulary is available from
`tinychannels_bus::web`. It is a distinct snake-case `event`-tagged protocol,
separate from provider `ChannelEvent`'s `type` tag and the progressive output
event's `type` tag. The web DTOs retain optional-field omission and accept the
historical `chat_error` cancellation shape alongside `chat_cancelled`.
Tenant/profile stamps stay in the host. Ordered message `parts` use
`OutboundContentPart`, including typed media and voice notes; `full_response`
remains a compatibility projection. Socket event aliases are transport names
and never rewrite the payload discriminator. Alias generation suppresses
parent and subagent stream deltas, matching the deployed socket server.

Regenerate the checked-in JSON Schema for TypeScript generation with
`cargo run -p tinychannels-bus --example web_schema > crates/tinychannels-bus/schema/web-channel-event.schema.json`;
the public `web_event_schema_json()` function exposes the same schema to
codegen tools.
The transport-free helpers also carry the web payload cap and timing
calculations for migration into host adapters. Logging and clock acquisition
remain host responsibilities. This contract does not claim that OpenHuman's
current host, TUI, or frontend has migrated to these types; those integrations
belong to their dependent epic leaves.

`src/fixtures/web/` contains hand-authored historical compatibility fixtures,
not captured runtime transcripts. Their shapes were derived from the deployed
host at OpenHuman base `ba83482245`, including constructors in
`crates/openhuman-core/src/web_chat/presentation.rs`,
`crates/openhuman-core/src/web_chat/ops/`,
`crates/openhuman-core/src/channels/proactive.rs`,
`crates/openhuman-core/src/flows/ops/streaming.rs`,
`crates/openhuman-core/src/cron/scheduler/`,
`crates/openhuman-core/src/agent/orchestration/background_delivery.rs`,
`crates/openhuman-core/src/agent/tinyagents/host/progress_sink.rs`,
`crates/openhuman-core/src/channels/host/adapters.rs`,
`crates/openhuman-core/src/channels/providers/relay/channel.rs`,
and the RPC Socket.IO adapter. These fixtures pin serialized field presence,
omission, nulls, and nested payloads against that source audit. Some valid
representable states are not emitted by the current constructors (for example,
an empty citations list); those are compatibility-shape fixtures, not claims
of observed runtime output. `historical_events.json` includes the per-variant
field shapes for the remaining deployed event names plus the two legacy names
still accepted by clients.
