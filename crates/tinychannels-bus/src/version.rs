//! Wire-contract versioning for TinyChannels hosts.

/// Current TinyChannels wire-contract version.
pub const CONTRACT_VERSION: u32 = 22;

/// Returns whether a host requiring `required` can bind to this contract.
///
/// Contract revisions support only exact-version bindings.
#[must_use]
pub const fn is_compatible(required: u32) -> bool {
    required == CONTRACT_VERSION
}
