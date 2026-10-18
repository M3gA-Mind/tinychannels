//! The account a linked WhatsApp Web session is authenticated as.
//!
//! Configuration names only a session label, and a mutable label is not an
//! account identity. The authenticated identity is the phone-number JID the
//! live session reports once it is linked. It is learned once; if a later
//! connection reports a different account (the device was re-linked to
//! another phone) the identity is poisoned for the life of this instance, so
//! durable media scoped by it can never be read or written across accounts.

use parking_lot::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
enum State {
    #[default]
    Unknown,
    Bound(String),
    Conflict,
}

/// Set-once account identity with conflict detection.
#[derive(Default)]
pub(super) struct LinkedIdentity {
    state: Mutex<State>,
}

impl LinkedIdentity {
    /// Record the account the session currently reports. Returns whether the
    /// identity is still usable.
    pub(super) fn observe(&self, account: Option<&str>) -> bool {
        let Some(account) = account.map(str::trim).filter(|account| !account.is_empty()) else {
            return !matches!(*self.state.lock(), State::Conflict);
        };
        let mut state = self.state.lock();
        match &*state {
            State::Unknown => {
                *state = State::Bound(account.to_owned());
                true
            }
            State::Bound(bound) if bound == account => true,
            State::Bound(_) | State::Conflict => {
                if !matches!(*state, State::Conflict) {
                    tracing::warn!(
                        "[whatsapp_web] the linked account changed mid-session; media identity disabled"
                    );
                }
                *state = State::Conflict;
                false
            }
        }
    }

    /// The bound account, or `None` while unknown or after a conflict.
    pub(super) fn current(&self) -> Option<String> {
        match &*self.state.lock() {
            State::Bound(account) => Some(account.clone()),
            State::Unknown | State::Conflict => None,
        }
    }

    pub(super) fn is_conflicted(&self) -> bool {
        matches!(*self.state.lock(), State::Conflict)
    }
}

#[cfg(test)]
#[path = "identity_tests.rs"]
mod tests;
