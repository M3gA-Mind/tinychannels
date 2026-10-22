//! Transport-free values shared by channel approval surfaces.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Address information needed to reply to an approval prompt in its source conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplyContext {
    pub reply_target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_ts: Option<String>,
}
