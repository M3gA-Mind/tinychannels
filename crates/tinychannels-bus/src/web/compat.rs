//! Pure Socket.IO event-name compatibility helpers.

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
