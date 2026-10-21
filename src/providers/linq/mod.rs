//! Linq (iMessage, RCS and SMS through the Linq Partner V3 API) on the v2
//! adapter contract.
//!
//! Outbound text, attachments and voice clips go through the REST API.
//! Inbound traffic is a signed webhook: the host forwards each raw POST to
//! [`ChannelAdapter::deliver_webhook`](crate::channel::ChannelAdapter::deliver_webhook), which authenticates the exact bytes, downloads
//! media, stores voice notes and publishes on the ordered ingress path.

mod capabilities;
mod config;
mod inbound;
mod outbound;
pub use inbound::verify_linq_signature;


pub use config::LinqChannel;




}

super::native::native_v2_adapter!(LinqChannel);

#[cfg(test)]
#[path = "fixtures_tests.rs"]
mod fixtures;

#[cfg(test)]
#[path = "conformance_tests.rs"]
mod conformance;

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
