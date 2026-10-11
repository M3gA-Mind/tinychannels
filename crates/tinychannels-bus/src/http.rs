//! Portable HTTP transport settings for hosts that construct providers in modules.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Host-selected proxy inputs for network-backed channel providers.
///
/// Proxy URLs may contain credentials. Callers must deliver this value only
/// through a confidential TinyBus member. The host resolves its own scope and
/// selector policy into `service_selectors` before sending it.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct HttpProxyPolicy {
    /// Explicit HTTP proxy URL.
    pub http_proxy: Option<String>,
    /// Explicit HTTPS proxy URL.
    pub https_proxy: Option<String>,
    /// Explicit proxy URL applied to all schemes.
    pub all_proxy: Option<String>,
    /// Host and address patterns that bypass explicit proxies.
    pub no_proxy: Vec<String>,
    /// Service identifiers to which explicit proxies apply.
    pub service_selectors: Vec<String>,
}
