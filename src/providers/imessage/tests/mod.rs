use super::*;
use crate::channel::{ChannelAdapter, ChannelInboundSink, ChannelTypedSend};
use tinychannels_bus::{
    ChannelInboundEnvelope, ChannelIngress, ChannelOutboundMessage, ContentPart, ConversationKind,
    ConversationRef, OutboundContentPart,
};
use tokio::time::timeout;

/// Helper to create a temporary test database with Messages schema
fn create_test_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("chat.db");

    let conn = Connection::open(&db_path).unwrap();
    db::tests::create_schema(&conn);
    // The adapter's own fixtures seed a handle; these tests insert their own.
    conn.execute("DELETE FROM handle", []).unwrap();

    (dir, db_path)
}

#[path = "database_tests.rs"]
mod database_tests;
#[path = "runtime_tests.rs"]
mod runtime_tests;
#[path = "validation_tests.rs"]
mod validation_tests;
