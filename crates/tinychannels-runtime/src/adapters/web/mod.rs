//! The in-app chat (desktop, web, TUI, relayed SaaS) as a channel adapter.
//!
//! `chat:start` submissions become v2 inbound envelopes with typed
//! attachments and voice notes; canonical `ChannelOutputEvent`s become the
//! deployed `WebChannelEvent` wire, published through a host-owned
//! [`WebEventSink`]. See `docs/spec/web-adapter.md`.

mod adapter;
mod inbound;
mod project;
mod sink;

pub use adapter::{SYSTEM_CLIENT_ID, WebChannelAdapter};
pub use inbound::{WEB_CHANNEL_ID, normalize_chat_start};
pub use project::{WebTurnProjector, context_mut};
pub use sink::{WebEventSink, WebScope};
pub use tinychannels_bus::web::WebChatStart;
