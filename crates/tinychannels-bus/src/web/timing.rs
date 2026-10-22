//! Monotonic turn timing and cost-update helpers.

use super::types::WebTurnTimingPayload;

/// Compute the displayed output rate when the turn has a non-zero duration.
pub fn tokens_per_second(output_tokens: Option<u64>, total_ms: Option<u64>) -> Option<f64> {
    match (output_tokens, total_ms) {
        (Some(tokens), Some(ms)) if ms > 0 => Some(tokens as f64 / (ms as f64 / 1000.0)),
        _ => None,
    }
}

/// Elapsed-time accumulator for the host's first-visible turn summary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TurnTimingState {
    first_token_ms: Option<u64>,
    first_tool_ms: Option<u64>,
    round_one_narration_chars: usize,
}

impl TurnTimingState {
    /// Record text; the first non-whitespace delta marks visible output.
    pub fn text_delta(&mut self, elapsed_ms: u64, delta: &str, round: u32) -> bool {
        let first_visible = self.first_token_ms.is_none() && !delta.trim().is_empty();
        if first_visible {
            self.first_token_ms = Some(elapsed_ms);
        }
        if round <= 1 {
            self.round_one_narration_chars += delta.chars().count();
        }
        first_visible
    }

    /// Record the first tool call time; later calls leave it unchanged.
    pub fn tool_call(&mut self, elapsed_ms: u64) -> bool {
        if self.first_tool_ms.is_some() {
            return false;
        }
        self.first_tool_ms = Some(elapsed_ms);
        true
    }

    /// Freeze the elapsed observations into a copyable turn snapshot.
    pub fn snapshot(self, total_ms: u64) -> TurnTimingSnapshot {
        TurnTimingSnapshot {
            first_token_ms: self.first_token_ms,
            first_tool_ms: self.first_tool_ms,
            total_ms: Some(total_ms),
            round_one_narration_chars: self.round_one_narration_chars,
        }
    }
}

/// Plain timing snapshot retained by the host between bridge and delivery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TurnTimingSnapshot {
    pub first_token_ms: Option<u64>,
    pub first_tool_ms: Option<u64>,
    pub total_ms: Option<u64>,
    pub round_one_narration_chars: usize,
}

impl TurnTimingSnapshot {
    /// Build the serialized summary and compute token rate when defined.
    pub fn into_payload(self, output_tokens: Option<u64>) -> WebTurnTimingPayload {
        WebTurnTimingPayload {
            first_token_ms: self.first_token_ms,
            first_tool_ms: self.first_tool_ms,
            total_ms: self.total_ms,
            tokens_per_second: tokens_per_second(output_tokens, self.total_ms),
        }
    }
}

/// Rate limiter for live turn-cost updates using monotonic elapsed milliseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TurnCostThrottle {
    last_emit_ms: Option<u64>,
}

impl TurnCostThrottle {
    /// Return true for the first update and updates at least 750 ms apart.
    pub fn should_emit(&mut self, elapsed_ms: u64) -> bool {
        let due = self
            .last_emit_ms
            .is_none_or(|last| elapsed_ms.saturating_sub(last) >= 750);
        if due {
            self.last_emit_ms = Some(elapsed_ms);
        }
        due
    }
}
