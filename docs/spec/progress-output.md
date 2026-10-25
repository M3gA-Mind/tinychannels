# Harness progress output contract

`ExecuteProgressive` is the confidential module member (`module operation
version 1.25`); hosts need this minimum version for `OpenOutput`, `FeedOutput`,
typed sends, and receipt-aware edit/delete. The legacy `Feed` command keeps its
one-release `{event,value}` wire shape. New producers use `FeedOutput`.

`ChannelOutputEvent` is the portable output vocabulary at the channel host
boundary. A host adapts its concrete harness enum by an exhaustive match and
constructs these DTOs; it must not serialize or debug-print the source enum.
The host remains responsible for capture and audience policy. Model-call input
and output are observability payloads and are not channel output.

`FeedOutput` sends each event, in order, through `OutputEvent` as an observation
callback, then applies it to the module-owned progressive reply. Observation
acknowledgement is not a provider-send receipt. Typed `Media`, `VoiceNote`, and
terminal `FinalMessage` content use `SendTyped`; only its `ChannelSendOutcome`
acknowledges delivery. An explicitly unsupported typed payload returns
`DeliveryUnsupported` with the original ordered parts. Other failures remain
failures, and the engine does not retry an ambiguous send. A terminal feed is
unique and last in its batch, closes the handle, and rejects later feeds. Its
result includes every receipt and any partial-send error. Multi-event terminal
batches retain prior outcomes in order; an uncertain or partial event stops
the batch before later provider sends. Approval and clarification events are
shown as native text prompts, while observation retains the original
correlation ID. Tool progress stays active until a completion event arrives;
opaque call IDs are not rendered as user text.

Thinking and filler messages retain complete receipts and per-part edit/delete
tokens. A missing or partial ephemeral receipt fences later sends for that
ephemeral stream without blocking the final reply. Explicit `Unavailable`
means no provider attempt occurred and may be retried on a later timer tick.
`Close` and turn cancellation delete every known draft, thinking, and filler
part using its receipt authority.

Scoped opens carry the host-selected turn ID, configured `ChannelRef` including
account identity, complete `ConversationRef`, and required `lifetimeMs` lease.
The host chooses a lease that covers its runner backstop and closes the handle
when the turn completes or errors; the module rejects zero or unrepresentable
deadlines and does not impose another scoped turn timeout. Legacy `Open` keeps
its 180-second compatibility lifetime. The module checks the configured
account key and provider before opening; send, edit, delete, typing, and
observation requests retain that scope. Bridge idempotency keys include the
event sequence and full channel/conversation scope. Replaying one event must
reuse its sequence; each new event must advance it.

Typed provider sends include a stable per-turn scoped sequence key. A terminal
feed is fingerprinted before dispatch; an identical retry waits for or returns
the retained terminal result without sending again, while changed terminal
content is rejected. `Close` and shutdown remove ephemeral state without
sending a synthetic cancellation message. On a module-sequenced handle
(`OpenOutput`/`OpenHostedOutput`), nonterminal `FeedOutput` has no
caller-supplied batch identity or retained replay contract: a repeated call
after a lost reply is a new delivery and must not be blindly retried. Such a
handle rejects a host `sequence`, and any `turnId`, `channelRef` or
`conversation` the request asserts must equal the scope fixed at open.

Handles from `Open`/`OpenScoped` are host-sequenced instead: every
`FeedOutput` carries `sequence`, the first event's stable position in the
turn, and repeats the turn scope bound at open (or by the first accepted batch
of an unscoped `Open`); a changed scope is rejected. Resending the last batch
with the same sequence and events returns its retained `OutputUpdated` result
and retries only the failed observation callbacks; a reused sequence with
different events, or a stale one, is rejected. Such a handle starts typing only
once a scoped batch has bound its transport scope. A host
may retry the exact typed transport request with its stable key, but it must
reconcile ambiguous nonterminal feed results before resubmitting events. This
is in-process terminal retention, not crash recovery. Scoped opens currently
resolve a local configured channel instance. Managed/relay provider
descriptors and web surface registration are supplied by their owning host
integrations. Open handles are bounded by active and retained-entry limits;
completed entries may be evicted when retention is at capacity. Filler status
sends are independently limited to eight attempts per turn, while all known
receipt parts remain retained for cleanup; this limit does not cap the host
lease or final delivery.
If the harness `Agent.run` call returns an error before `TurnCompleted`, the
host closes that turn with `Close`; this control cleans up owned drafts and
ephemeral status messages without sending a replacement reply. `Lifecycle`
stage strings are observational metadata and do not imply terminal control.

When `FinalMessage.parts` is non-empty, that ordered list is authoritative.
`text` is a display/compatibility projection and must not be appended as another
part or deduplicated by comparing content. If the parts contain only text and
exceed the configured account's length limit, the engine splits each text part
at UTF-8 scalar boundaries using its declared Characters, Utf8Bytes, or
Utf16Units unit. Part order and markdown dialect are preserved. Mixed rich
content is delivered intact; if the provider cannot accept it, the complete
ordered payload is returned as unsupported rather than flattened. Each
segment's idempotency key includes its event sequence and segment index.

The TinyAgents `AgentProgress` source variants map as follows:

| Source event | Channel output projection |
| --- | --- |
| `TurnStarted`, `IterationStarted` | `Lifecycle` with stage and iteration metadata. |
| `TextDelta` | `TextDelta`, preserving delta text and iteration. |
| `ThinkingDelta` | `ThinkingDelta`; expose only where the host's recipient policy permits reasoning. |
| `ToolCallArgsDelta` | `ToolCallArgsDelta`, preserving call ID, tool name, fragment, and iteration. |
| `ToolCallStarted` | `ToolCallStarted`, preserving call ID, name, arguments, iteration, label, and detail. |
| `ToolCallProgress` | `ToolCallProgress`, preserving call ID and status message. |
| `ToolCallCompleted` | `ToolCallCompleted`, preserving call ID, status, lengths, elapsed time, iteration, allowed arguments/output, labels, and structured result. |
| `SubagentSpawned`, `SubagentCompleted`, `SubagentFailed`, `SubagentAwaitingUser`, `SubagentIterationStarted` | `Subagent`, preserving agent/task identity, state, iteration, and user-facing question or summary. Preserve capped child output and authorized worktree path/changed-file/dirty-status facts used by the UI; preserve `extended_policy` for child iteration rendering. Prompt/input capture remains excluded. |
| `SubagentToolCallStarted`, `SubagentToolCallCompleted` | `Subagent` with nested call identity and allowed tool facts; preserve task and call IDs. |
| `SubagentTextDelta`, `SubagentThinkingDelta` | `Subagent` with task identity and the corresponding delta, subject to the same text/reasoning policy as parent output. |
| `TurnCostUpdated` | `Usage`, preserving cumulative model and token/cost totals. |
| `ModelCallCompleted` | No channel event. Its captured model input/output remains in the observability path. Usage already has a dedicated projection. |
| `TurnContent` | No channel event. It duplicates user/model content captured for observability, not channel output. |
| `TurnCompleted` | Lifecycle completion precedes `FinalMessage` in the same batch, when canonical final content is available; terminal content closes the progressive handle. Early-stop metadata belongs in lifecycle state. A host-only lifecycle notification may occur outside the closed progressive stream. |

This table is the conformance checklist for the OpenHuman adapter. Fields
marked as allowed are still subject to OpenHuman's current capture gates and
must be bounded before rendering. `checkpoint_path` remains a host ledger and
resume side effect; #7967 must retain it natively without projecting it to the
generic UI event. This crate does not claim that the host adapter has migrated;
that integration and its exhaustive-match tests belong to OpenHuman #7967.
