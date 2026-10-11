//! Host-selected retry policy for module-owned provider listeners.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Reconnection timing supplied by the host; execution remains in the module.
///
/// The initial delay is at least one second. The maximum delay is raised to
/// the initial delay when smaller. Module-owned jitter is added to each delay.
/// The module rejects selected delays greater than one day.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ChannelSupervisionPolicy {
    /// First retry delay in seconds.
    pub initial_backoff_secs: u64,
    /// Maximum exponential retry delay in seconds, before jitter.
    pub max_backoff_secs: u64,
}
impl Default for ChannelSupervisionPolicy {
    fn default() -> Self {
        Self {
            initial_backoff_secs: 2,
            max_backoff_secs: 60,
        }
    }
}

#[cfg(test)]
#[path = "module_supervision_tests.rs"]
mod tests;
