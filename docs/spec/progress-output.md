# Harness progress output contract

`ChannelOutputEvent` is the portable output vocabulary at the channel host
boundary. A host adapts its concrete harness enum by an exhaustive match and
constructs these DTOs; it must not serialize or debug-print the source enum.
The host remains responsible for capture and audience policy. Model-call input
and output are observability payloads and are not channel output.

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
| `SubagentSpawned`, `SubagentCompleted`, `SubagentFailed`, `SubagentAwaitingUser`, `SubagentIterationStarted` | `Subagent`, preserving agent/task identity, state, iteration, and user-facing question or summary. Do not forward trace-only prompt/output or filesystem details. |
| `SubagentToolCallStarted`, `SubagentToolCallCompleted` | `Subagent` with nested call identity and allowed tool facts; preserve task and call IDs. |
| `SubagentTextDelta`, `SubagentThinkingDelta` | `Subagent` with task identity and the corresponding delta, subject to the same text/reasoning policy as parent output. |
| `TurnCostUpdated` | `Usage`, preserving cumulative model and token/cost totals. |
| `ModelCallCompleted` | No channel event. Its captured model input/output remains in the observability path. Usage already has a dedicated projection. |
| `TurnContent` | No channel event. It duplicates user/model content captured for observability, not channel output. |
| `TurnCompleted` | `FinalMessage` when canonical final content is available, followed by lifecycle completion; early-stop metadata belongs in lifecycle state. |

This table is the conformance checklist for the OpenHuman adapter. Fields
marked as allowed are still subject to OpenHuman's current capture gates and
must be bounded before rendering. This crate does not claim that the host
adapter has migrated; that integration and its exhaustive-match tests belong
to OpenHuman #7967.
