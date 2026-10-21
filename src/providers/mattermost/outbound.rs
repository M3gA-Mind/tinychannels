//! Mattermost typing indicators and outbound actions.

use super::*;

impl MattermostChannel {
    pub(super) async fn start_typing_loop(&self, recipient: &str) -> Result<()> {
        self.stop_typing_loop()?;
        let client = self.http_client();
        let token = self.bot_token.clone();
        let base_url = self.base_url.clone();
        let (channel_id, parent_id) = match recipient.split_once(':') {
            Some((channel, parent)) => (channel.to_string(), Some(parent.to_string())),
            None => (recipient.to_string(), None),
        };
        let handle = tokio::spawn(async move {
            let url = format!("{base_url}/api/v4/users/me/typing");
            loop {
                let mut body = serde_json::json!({ "channel_id": channel_id });
                if let Some(ref pid) = parent_id {
                    body.as_object_mut()
                        .unwrap()
                        .insert("parent_id".to_string(), serde_json::json!(pid));
                }
                if let Ok(response) = client
                    .post(&url)
                    .bearer_auth(&token)
                    .json(&body)
                    .send()
                    .await
                    && !response.status().is_success()
                {
                    tracing::debug!(status = %response.status(), "Mattermost typing indicator failed");
                }
                tokio::time::sleep(std::time::Duration::from_secs(4)).await;
            }
        });
        *self.typing_handle.lock() = Some(handle);
        Ok(())
    }

    pub(super) fn stop_typing_loop(&self) -> Result<()> {
        let mut guard = self.typing_handle.lock();
        if let Some(handle) = guard.take() {
            handle.abort();
        }
        Ok(())
    }
}

