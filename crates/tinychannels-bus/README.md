# tinychannels-bus

Serialized channel vocabulary, configuration/schema declarations, interface
names, relay frames and structured errors. Normal dependencies are Serde,
JSON/schema/error derivation, and Base64 used only by the existing
`PassthroughForward.body` custom serializer (`bodyB64`). This crate has no
provider callbacks, scheduler, crypto, entropy, stateful locks, transport,
session derivation or delivery algorithms.

DTOs are defined once here. `tinychannels-runtime` and `tinychannels` re-export
these same Rust types. Hosts should name wire values through this crate and
execute behavior through the compiled module. The root library compatibility
facade is for existing implementation consumers, not a host dependency shortcut.

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

Contract version 13 adds confidential `StartRuntimeWithDispatch(handle,
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
This source contract still requires a compatible published native module before
a downstream host can claim released composition.

The controller vocabulary includes complete Discord guild, text-channel and
permission DTOs. Provider APIs re-export these types at their existing paths;
REST calls and permission calculations remain implementation behavior.
