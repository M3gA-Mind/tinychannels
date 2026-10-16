//! The v1 to v2 steps of the `channels` owner.

use tinystoragedrivers_core::schema::Step;

/// Every step, in the order the schema runs them.
pub(crate) fn steps() -> Vec<Step> {
    Vec::new()
}

#[cfg(test)]
#[path = "migrate_tests.rs"]
mod tests;
