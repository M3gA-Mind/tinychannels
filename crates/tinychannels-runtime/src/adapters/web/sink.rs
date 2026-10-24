//! Host port that carries web events to connected surfaces.

use crate::channel::ChannelSendError;
use async_trait::async_trait;
use tinychannels_bus::web::WebChannelEvent;

/// Host-owned routing stamps for one web publication.
///
/// The scope selects which tenant, profile and agent a subscriber may see. It
/// is metadata for the sink only: it never appears in a [`WebChannelEvent`],
/// so the wire shape stays byte-identical to the deployed web chat.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct WebScope {
    pub tenant_id: Option<String>,
    pub profile_id: Option<String>,
    pub agent_id: Option<String>,
}

impl WebScope {
    /// Scope for a single-user host with no tenant partitioning.
    pub fn local() -> Self {
        Self::default()
    }

    /// Scope pinned to one agent.
    pub fn for_agent(agent_id: impl Into<String>) -> Self {
        Self {
            agent_id: Some(agent_id.into()),
            ..Self::default()
        }
    }
}

/// Port the host implements to deliver web events (Socket.IO room emit, SSE
/// fan-out, relay uplink). Transports stay in the host; this crate only
/// decides which events exist and in what order.
#[async_trait]
pub trait WebEventSink: Send + Sync {
    /// Publish one event to subscribers visible to `scope`.
    async fn publish(
        &self,
        scope: &WebScope,
        event: WebChannelEvent,
    ) -> Result<(), ChannelSendError>;
}
