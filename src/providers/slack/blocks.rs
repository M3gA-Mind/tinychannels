//! Block Kit rendering for buttons, approvals and presentation blocks.

use super::api::pre_dispatch;
use serde_json::{Value, json};
use tinychannels_bus::channel::{ButtonAction, ButtonStyle, ChannelSendError};

const MAX_SECTION_CHARS: usize = 3000;
const MAX_ACTIONS_PER_BLOCK: usize = 5;
const MAX_ROWS: usize = 24;
const MAX_LABEL_CHARS: usize = 75;
const MAX_ACTION_ID_BYTES: usize = 255;

/// Rendered presentation: text plus optional button rows.
pub(crate) struct Presentation {
    pub(crate) text: String,
    pub(crate) rows: Vec<Vec<ButtonAction>>,
}

/// Section text followed by `actions` blocks of at most five buttons each.
pub(crate) fn button_blocks(
    mrkdwn: &str,
    rows: &[Vec<ButtonAction>],
) -> Result<Vec<Value>, ChannelSendError> {
    if mrkdwn.trim().is_empty() {
        return Err(pre_dispatch("Slack buttons require message text"));
    }
    if mrkdwn.chars().count() > MAX_SECTION_CHARS {
        return Err(pre_dispatch(
            "Slack button message text exceeds 3000 characters",
        ));
    }
    if rows.len() > MAX_ROWS {
        return Err(pre_dispatch("Slack button rows exceed the block limit"));
    }
    let mut blocks = vec![json!({
        "type": "section",
        "text": {"type": "mrkdwn", "text": mrkdwn},
    })];
    for row in rows {
        for chunk in row.chunks(MAX_ACTIONS_PER_BLOCK) {
            let elements = chunk
                .iter()
                .map(button_element)
                .collect::<Result<Vec<_>, _>>()?;
            blocks.push(json!({"type": "actions", "elements": elements}));
        }
    }
    if blocks.len() == 1 {
        return Err(pre_dispatch("Slack buttons contain no actions"));
    }
    Ok(blocks)
}

fn button_element(button: &ButtonAction) -> Result<Value, ChannelSendError> {
    if button.disabled {
        return Err(pre_dispatch("Slack buttons cannot render disabled actions"));
    }
    if button.id.is_empty()
        || button.id.len() > MAX_ACTION_ID_BYTES
        || button.label.is_empty()
        || button.label.chars().count() > MAX_LABEL_CHARS
    {
        return Err(pre_dispatch(
            "Slack button label or action ID violates Block Kit limits",
        ));
    }
    let mut element = json!({
        "type": "button",
        "text": {"type": "plain_text", "text": button.label},
        "action_id": button.id,
        "value": button.id,
    });
    match button.style {
        Some(ButtonStyle::Primary) => element["style"] = "primary".into(),
        Some(ButtonStyle::Danger) => element["style"] = "danger".into(),
        Some(ButtonStyle::Secondary) | None => {}
    }
    Ok(element)
}

/// Render an approval request or plain text blocks.
pub(crate) fn presentation(value: &Value) -> Result<Presentation, ChannelSendError> {
    if value.get("kind").and_then(Value::as_str) == Some("approval") {
        return approval(value);
    }
    let mut parts = Vec::new();
    collect_text(value, &mut parts)?;
    Ok(Presentation {
        text: parts.join("\n"),
        rows: Vec::new(),
    })
}

fn approval(value: &Value) -> Result<Presentation, ChannelSendError> {
    let object = value.as_object().filter(|object| {
        object.len() == 4
            && object
                .keys()
                .all(|key| ["kind", "id", "prompt", "choices"].contains(&key.as_str()))
    });
    let object = object.ok_or_else(|| pre_dispatch("Slack approval has unsupported fields"))?;
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty());
    let prompt = object
        .get("prompt")
        .and_then(Value::as_str)
        .filter(|prompt| !prompt.trim().is_empty());
    let choices = object
        .get("choices")
        .and_then(Value::as_array)
        .filter(|choices| !choices.is_empty() && choices.len() <= MAX_ROWS);
    let (Some(id), Some(prompt), Some(choices)) = (id, prompt, choices) else {
        return Err(pre_dispatch("Slack approval request is invalid"));
    };
    let mut row = Vec::new();
    for (index, choice) in choices.iter().enumerate() {
        let label = choice
            .as_str()
            .filter(|label| !label.trim().is_empty())
            .ok_or_else(|| pre_dispatch("Slack approval choice label is invalid"))?;
        let action = tinychannels_bus::channel::approval_action_id(id, index)
            .ok_or_else(|| pre_dispatch("Slack approval action ID is too long"))?;
        row.push(ButtonAction {
            id: action,
            label: label.to_owned(),
            style: None,
            disabled: false,
        });
    }
    Ok(Presentation {
        text: prompt.to_owned(),
        rows: vec![row],
    })
}

fn collect_text(value: &Value, out: &mut Vec<String>) -> Result<(), ChannelSendError> {
    match value {
        Value::String(text) => {
            if !text.trim().is_empty() {
                out.push(text.clone());
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_text(item, out)?;
            }
        }
        Value::Object(object) => {
            const KEYS: [&str; 6] = ["type", "text", "title", "heading", "caption", "description"];
            if object.keys().any(|key| !KEYS.contains(&key.as_str())) {
                return Err(pre_dispatch(
                    "Slack presentation block has unsupported fields",
                ));
            }
            for key in ["title", "heading", "text", "caption", "description"] {
                match object.get(key) {
                    None => {}
                    Some(Value::String(text)) if !text.trim().is_empty() => out.push(text.clone()),
                    Some(Value::String(_)) => {}
                    Some(_) => {
                        return Err(pre_dispatch(
                            "Slack presentation text fields must be strings",
                        ));
                    }
                }
            }
        }
        _ => return Err(pre_dispatch("Slack presentation only accepts text blocks")),
    }
    Ok(())
}

#[cfg(test)]
#[path = "blocks_tests.rs"]
mod tests;
