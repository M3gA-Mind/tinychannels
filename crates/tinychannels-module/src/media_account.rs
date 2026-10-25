//! Stable, opaque account identities used to scope durable channel media.

use sha2::{Digest, Sha256};
use tinychannels::config::ChannelsConfig;

/// Derive an opaque media authority from only the selected provider's account
/// identity material. Mutable access policy is excluded so policy edits do not
/// orphan stored media. Credentials are hashed and never returned or logged.
pub(crate) fn configured_media_account_scope(
    instance: &str,
    config: &ChannelsConfig,
) -> Option<String> {
    let provider = tinychannels_bus::provider_id(instance);
    let identity: Vec<String> = match provider {
        "telegram" => {
            let c = config.telegram.as_ref()?;
            if c.bot_token.trim().is_empty() {
                return None;
            }
            vec![c.bot_token.clone()]
        }
        "mattermost" => {
            let c = config.mattermost.as_ref()?;
            if c.url.trim().is_empty() || c.bot_token.trim().is_empty() {
                return None;
            }
            vec![c.url.clone(), c.bot_token.clone()]
        }
        "lark" => {
            let c = config.lark.as_ref()?;
            if c.app_id.trim().is_empty() || c.app_secret.trim().is_empty() {
                return None;
            }
            vec![
                c.app_id.clone(),
                c.app_secret.clone(),
                if c.use_feishu { "feishu" } else { "lark" }.into(),
            ]
        }
        "dingtalk" => {
            let c = config.dingtalk.as_ref()?;
            if c.client_id.trim().is_empty() || c.client_secret.trim().is_empty() {
                return None;
            }
            vec![c.client_id.clone(), c.client_secret.clone()]
        }
        "qq" => {
            let c = config.qq.as_ref()?;
            if c.app_id.trim().is_empty() || c.app_secret.trim().is_empty() {
                return None;
            }
            vec![c.app_id.clone(), c.app_secret.clone()]
        }
        _ => return None,
    };
    Some(media_scope_digest(provider, &identity))
}

fn media_scope_digest(provider: &str, identity: &[String]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tinychannels-media-account-v1\0");
    digest.update((provider.len() as u64).to_le_bytes());
    digest.update(provider.as_bytes());
    for part in identity {
        digest.update((part.len() as u64).to_le_bytes());
        digest.update(part.as_bytes());
    }
    format!("{provider}:media-v1:{}", hex::encode(digest.finalize()))
}

#[cfg(test)]
#[path = "media_account_tests.rs"]
mod tests;
