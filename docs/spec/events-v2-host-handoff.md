# Channel events v2 host handoff

TinyChannels now carries provider events through one normalized contract and a
typed callback. The bus contract adds the `DeliverEvent(ChannelEvent)` member
to `ai.tinyhumans.tinychannels.ChannelsHost`. A provider uses
`ChannelIngress::{Inbound, Event}` at its listener boundary; the module stamps
the configured channel/account identity, serializes the ingress through the
same ordered host callback path, and invokes `DeliverInbound` or `DeliverEvent`.
The new DTOs add no transport behavior or dependencies. The bus crate still has
legacy async/runtime dependencies through its existing provider trait; removal
of that edge remains tracked by #101/#103. Legacy
`Channel::listen(Sender<ChannelMessage>)` providers remain supported by the
module's compatibility bridge during the provider migration window.

`ChannelEvent` is the v2 vocabulary from contract-v2: its flattened `type`
discriminator covers `typing`, `read_receipt`, `delivered_receipt`,
`reaction_added`, `reaction_removed`, `outbound_reaction`, `message_updated`,
`message_deleted`, `presence`, and `lifecycle`. It retains configured account,
conversation kind and full topic/thread/parent address, stable adapter event
ID, provider source event ID and source timestamp where available. Telegram's
real listener now reports edits and reaction-set changes as typed events;
message IDs correlate with inbound messages while `providerMessageId` retains
the raw platform identifier. Outbound Telegram reactions use the same canonical
message reference and preserve the raw ID; the legacy reaction operation only
receives chat/message IDs, so its event cannot claim a topic/thread address.
Native operation migration in #105 can add that address when the caller has it.

Receipts include their idempotency key, provider message IDs, full address and
per-part `editToken`/`deleteToken` only where the provider returned an ID and
supports that operation. Multipart partial results remain attached to the
typed failure and durable queue record. A dispatched send without a confirmed
provider ID is recorded as ambiguous and is not blindly retried. Optional new
receipt fields decode older persisted records that do not contain them.

The old v21 `ChannelMessage` and `Channel::listen` path remain as a one-release
mechanical compatibility surface. The adapter does not invent v2 address
facts from synthetic message IDs; providers that have not migrated can only
expose the information available in their legacy message. New providers should
use `ChannelIngress` and construct the complete address at the source.

This change supplies typed module-to-host delivery and provider-facing DTOs; it
does not add host rendering. OpenHuman #7969 owns the subscriber/projection and
removal of the old `ChannelReactionReceived`/`ChannelReactionSent` events.
OpenHuman #7971 owns thread/web rendering, including reaction and receipt
presentation. Broader provider conformance and provider migrations remain in
#103 and #105; this change's real provider coverage is Telegram's currently
supported update/send paths. Do not describe other providers as event-capable
until their source callbacks and operation support are migrated and tested.
