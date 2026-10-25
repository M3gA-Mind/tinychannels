//! WhatsApp Web provider module exports and feature-off compatibility stub.

mod config;
pub use config::WhatsAppWebConfig;

#[cfg(feature = "whatsapp-web")]
mod channel;
#[cfg(feature = "whatsapp-web")]
pub use channel::WhatsAppWebChannel;
#[cfg(all(test, feature = "whatsapp-web"))]
pub(crate) use channel::{client, fakes, media_build};

// Stub implementation when the feature is not enabled. Keeps the public ctor
// signature compatible so channel startup compiles unchanged.
#[cfg(not(feature = "whatsapp-web"))]
pub struct WhatsAppWebChannel {
    _private: (),
    /// Retained so `startup` can call `.with_lifecycle(...)` unconditionally
    /// regardless of feature flags.
    lifecycle: Option<std::sync::Arc<dyn crate::host::LifecycleRegistry>>,
}

#[cfg(not(feature = "whatsapp-web"))]
impl WhatsAppWebChannel {
    pub fn new(
        _session_path: String,
        _pair_phone: Option<String>,
        _pair_code: Option<String>,
        _allowed_numbers: Vec<String>,
    ) -> Self {
        Self {
            _private: (),
            lifecycle: None,
        }
    }

    pub fn from_config(config: WhatsAppWebConfig) -> Self {
        Self::new(
            config.session_path,
            config.pair_phone,
            config.pair_code,
            config.allowed_numbers,
        )
    }

    /// No-op host lifecycle wiring for the disabled-feature stub. Kept so
    /// callers can invoke `.with_lifecycle(...)` unconditionally.
    pub fn with_lifecycle(
        mut self,
        lifecycle: std::sync::Arc<dyn crate::host::LifecycleRegistry>,
    ) -> Self {
        self.lifecycle = Some(lifecycle);
        self
    }
}
