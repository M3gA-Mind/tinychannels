# Progressive output engine

`ProgressiveReply` is the single per-turn delivery engine for canonical
`ChannelOutputEvent` values. It owns text accumulation, thinking/tool status,
typing refresh, draft lifecycle, typed media/voice dispatch, configured text
segmentation, receipt aggregation, and ephemeral cleanup. The TinyBus module
keeps bounded command admission, callback adaptation, terminal replay, and
worker lifetime; it delegates event and delivery decisions to this engine.

## Public surface

`tinychannels::ProgressiveReply` and `tinychannels::ProgressiveSender` are
re-exported from the crate root. Use `ProgressiveReply::new_scoped` with the
host's configured address, conversation, and turn identity. The unscoped
constructor and provider-wide edit latch remain for the one-release legacy
path. Scoped turns keep edit capability state per turn and preserve complete
conversation scope on each send/edit/delete/typing operation.

Hosts feed ordinary events through `apply_output_event` and terminate with
`finalize_output_event`. Provider sends return `ChannelSendOutcome`; a receipt
with an error or no identifying receipt is retained as partial/unknown and is
never retried automatically. Text-only final output edits an existing draft
when possible. Without edit support, text-only final output is split according
to the configured account's `textLimit` and `lengthUnit`. Rich ordered content
is never flattened for this fallback.

## Constraints

Output observation (`ProgressiveSender::output_event`) is not a provider send
acknowledgement. A host must preserve event sequence and scope in its callback
and must implement the receipt-aware Edit/Delete request fields if it supports
opaque provider tokens. A multi-message draft is not edited through one message
ID; its complete known receipt is used to edit or clean up every ephemeral
part. Thinking and filler bubbles also retain every receipt part and opaque
delete token. A missing or partial receipt disables retries for that ephemeral
stream but does not suppress the independent final reply. An explicit
`Unavailable` result means no attempt occurred and may be retried on a later
tick. An ambiguous edit or delete does not trigger a replacement send.
Filler status sends stop after eight attempts per turn, independently of the
host-selected scoped output lease, and all known receipt parts remain available
for cleanup.

The host supplies a scoped output lease that covers its turn backstop and closes
the handle on completion/error. The module validates that the nonzero deadline
is representable and uses it for worker cleanup; it does not add a second
scoped turn timeout. Legacy opens retain their 180-second compatibility
lifetime. Active and retained handle counts remain bounded, and completed
terminal results are process-local replay data that can be evicted at capacity.

Approval and clarification prompts are delivered as text for native channels;
their original correlation data remains in the typed output observation. Tool
progress uses an active status marker; completion is represented separately.
Model input/output capture data is not turned into user-facing output here.

The bus operation and adapter details are specified in
[`../../../docs/spec/progress-output.md`](../../../docs/spec/progress-output.md).
