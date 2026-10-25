//! Configuration owned by the WhatsApp Web adapter.

/// Linked-device settings passed from the channel factory to the provider.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WhatsAppWebConfig {
    /// Durable session label used by host storage.
    pub session_path: String,
    /// Optional E.164 number used for pair-code linking.
    pub pair_phone: Option<String>,
    /// Optional pre-allocated pair code paired with `pair_phone`.
    pub pair_code: Option<String>,
    /// Allowed E.164 numbers, or `*` for any sender. An empty list allows all.
    pub allowed_numbers: Vec<String>,
}
