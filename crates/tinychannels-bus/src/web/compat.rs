//! Pure Socket.IO event-name compatibility helpers.

use super::events::WebChannelEvent;

impl WebChannelEvent {
    /// Apply the historical typed-projector normalization before host delivery.
    /// Directly-authored web events retain their serialized `null` and empty
    /// values; only events passing through the old typed adapter used this rule.
    pub fn normalize_projected_wire_fields(&mut self) {
        use WebChannelEvent as E;
        let (tool_name, args) = match self {
            E::ToolArgsDelta { tool_name, .. } => (Some(tool_name), None),
            E::ToolCall {
                tool_name, args, ..
            }
            | E::SubagentToolCall {
                tool_name, args, ..
            }
            | E::ApprovalRequest {
                tool_name, args, ..
            }
            | E::PlanReviewRequest {
                tool_name, args, ..
            } => (Some(tool_name), Some(args)),
            E::ToolResult {
                tool_name, args, ..
            }
            | E::SubagentToolResult {
                tool_name, args, ..
            } => (Some(tool_name), Some(args)),
            E::SubagentSpawned { tool_name, .. }
            | E::SubagentCompleted { tool_name, .. }
            | E::SubagentFailed { tool_name, .. }
            | E::SubagentAwaitingUser { tool_name, .. }
            | E::SubagentIterationStart { tool_name, .. }
            | E::ApprovalDecided { tool_name, .. }
            | E::PlanReviewDecided { tool_name, .. } => (Some(tool_name), None),
            E::ArtifactReady { args, .. }
            | E::ArtifactFailed { args, .. }
            | E::ArtifactPending { args, .. }
            | E::MemoryActivity { args, .. }
            | E::ExternalTransferPending { args, .. } => (None, Some(args)),
            _ => (None, None),
        };
        if let Some(tool_name) = tool_name
            && tool_name.as_deref() == Some("")
        {
            *tool_name = None;
        }
        if let Some(args) = args
            && args.as_ref().is_some_and(serde_json::Value::is_null)
        {
            *args = None;
        }
    }
}

/// Pure Socket.IO event-name compatibility mapping. The returned string is a
/// transport event name; callers must emit the unchanged payload whose
/// `event` discriminator remains canonical. Streaming aliases stay suppressed.
pub fn legacy_event_alias(name: &str, enabled: bool) -> Option<String> {
    if !enabled {
        return None;
    }
    const STREAMING: &[&str] = &["text_delta", "thinking_delta", "tool_args_delta"];
    let normalized = name.replace(':', "_");
    let base = normalized.strip_prefix("subagent_").unwrap_or(&normalized);
    if STREAMING.contains(&base) {
        return None;
    }
    if name.contains('_') {
        Some(name.replace('_', ":"))
    } else if name.contains(':') {
        Some(name.replace(':', "_"))
    } else {
        None
    }
}
