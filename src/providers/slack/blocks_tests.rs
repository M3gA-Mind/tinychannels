use super::*;

fn button(id: &str, label: &str) -> ButtonAction {
    ButtonAction {
        id: id.into(),
        label: label.into(),
        style: None,
        disabled: false,
    }
}

#[test]
fn buttons_render_a_section_and_split_rows_into_five_wide_action_blocks() {
    let mut styled = button("b", "Beta");
    styled.style = Some(ButtonStyle::Danger);
    let mut primary = button("c", "Gamma");
    primary.style = Some(ButtonStyle::Primary);
    let mut secondary = button("d", "Delta");
    secondary.style = Some(ButtonStyle::Secondary);
    let row = vec![
        button("a", "Alpha"),
        styled,
        primary,
        secondary,
        button("e", "E"),
        button("f", "F"),
    ];
    let blocks = button_blocks("*pick*", &[row]).unwrap();
    assert_eq!(blocks.len(), 3);
    assert_eq!(blocks[0]["text"]["text"], "*pick*");
    assert_eq!(blocks[1]["elements"].as_array().unwrap().len(), 5);
    assert_eq!(blocks[2]["elements"].as_array().unwrap().len(), 1);
    assert_eq!(blocks[1]["elements"][0]["action_id"], "a");
    assert_eq!(blocks[1]["elements"][0]["value"], "a");
    assert_eq!(blocks[1]["elements"][1]["style"], "danger");
    assert_eq!(blocks[1]["elements"][2]["style"], "primary");
    assert!(blocks[1]["elements"][3].get("style").is_none());
}

#[test]
fn buttons_reject_inputs_slack_would_refuse() {
    let ok = vec![vec![button("a", "A")]];
    assert!(button_blocks("  ", &ok).is_err());
    assert!(button_blocks(&"x".repeat(3001), &ok).is_err());
    assert!(button_blocks("t", &vec![vec![button("a", "A")]; 25]).is_err());
    assert!(button_blocks("t", &[]).is_err());
    assert!(button_blocks("t", &[vec![]]).is_err());
    let mut disabled = button("a", "A");
    disabled.disabled = true;
    assert!(button_blocks("t", &[vec![disabled]]).is_err());
    for bad in [
        button("", "A"),
        button("a", ""),
        button("a", &"l".repeat(76)),
        button(&"i".repeat(256), "A"),
    ] {
        assert!(button_blocks("t", &[vec![bad]]).is_err());
    }
}

#[test]
fn approvals_become_one_row_of_buttons_with_approval_action_ids() {
    let rendered = presentation(&json!({
        "kind": "approval", "id": "req1", "prompt": "Run it?", "choices": ["Yes", "No"]
    }))
    .unwrap();
    assert_eq!(rendered.text, "Run it?");
    assert_eq!(rendered.rows[0][0].id, "approval-v1:req1:0");
    assert_eq!(rendered.rows[0][1].label, "No");
}

#[test]
fn invalid_approvals_are_rejected_before_dispatch() {
    let base = || json!({"kind": "approval", "id": "r", "prompt": "p", "choices": ["Yes"]});
    let mut extra = base();
    extra["extra"] = json!(1);
    let mut no_id = base();
    no_id["id"] = json!("");
    let mut no_prompt = base();
    no_prompt["prompt"] = json!("  ");
    let mut no_choices = base();
    no_choices["choices"] = json!([]);
    let mut blank_choice = base();
    blank_choice["choices"] = json!([" "]);
    let mut long_id = base();
    long_id["id"] = json!("r".repeat(80));
    for bad in [extra, no_id, no_prompt, no_choices, blank_choice, long_id] {
        assert!(presentation(&bad).is_err(), "{bad}");
    }
}

#[test]
fn plain_presentation_joins_text_fields_and_rejects_foreign_content() {
    let rendered = presentation(&json!([
        "plain",
        " ",
        {"type": "heading", "title": "Head", "text": "Body", "caption": " "}
    ]))
    .unwrap();
    assert_eq!(rendered.text, "plain\nHead\nBody");
    assert!(rendered.rows.is_empty());
    for bad in [json!(5), json!({"image_url": "x"}), json!({"text": 5})] {
        assert!(presentation(&bad).is_err(), "{bad}");
    }
}
