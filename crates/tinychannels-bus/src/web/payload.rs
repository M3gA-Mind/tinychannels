//! Payload-size limits and projection helpers for web clients.

/// Upper bound for subagent tool arguments and results sent to web clients.
pub const MAX_WEB_TOOL_PAYLOAD_BYTES: usize = 256 * 1024;
const WEB_TRUNCATION_MARKER_BUDGET: usize = 80;

/// Bound a textual web payload on a UTF-8 boundary, counting its marker.
pub fn cap_web_output(output: String) -> String {
    if output.len() <= MAX_WEB_TOOL_PAYLOAD_BYTES {
        return output;
    }
    let mut end = MAX_WEB_TOOL_PAYLOAD_BYTES.saturating_sub(WEB_TRUNCATION_MARKER_BUDGET);
    while end > 0 && !output.is_char_boundary(end) {
        end -= 1;
    }
    let omitted = output.len() - end;
    format!(
        "{}\n…[truncated {omitted} bytes of tool output]",
        &output[..end]
    )
}

/// Bound serialized tool arguments; oversized values become a marker string.
pub fn cap_web_args(args: Option<serde_json::Value>) -> Option<serde_json::Value> {
    let value = args?;
    if value.is_null() {
        return None;
    }
    let rendered = value.to_string();
    if rendered.len() <= MAX_WEB_TOOL_PAYLOAD_BYTES {
        Some(value)
    } else {
        Some(serde_json::Value::String(cap_web_output(rendered)))
    }
}
