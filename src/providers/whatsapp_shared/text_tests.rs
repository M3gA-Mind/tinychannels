use super::*;

#[test]
fn short_text_is_one_chunk() {
    assert_eq!(split_text("hello", 10), ["hello"]);
}

#[test]
fn text_breaks_at_a_newline_before_a_space() {
    let chunks = split_text("aaaa bbbb\ncccc dddd", 12);
    assert_eq!(chunks, ["aaaa bbbb", "cccc dddd"]);
}

#[test]
fn text_breaks_at_a_space_when_there_is_no_newline() {
    assert_eq!(split_text("aaaa bbbb cccc", 9), ["aaaa bbbb", "cccc"]);
}

#[test]
fn unbroken_text_splits_mid_word_on_character_boundaries() {
    let chunks = split_text("ééééééé", 3);
    assert_eq!(chunks, ["ééé", "ééé", "é"]);
    assert_eq!(chunks.concat(), "ééééééé");
}

#[test]
fn blank_chunks_are_dropped_and_zero_limit_is_clamped() {
    assert!(split_text("   ", 5).is_empty());
    assert_eq!(split_text("ab", 0), ["a", "b"]);
}

#[test]
fn tool_call_markup_is_stripped() {
    assert_eq!(
        strip_tool_call_tags("hi <tool_call>x</tool_call>").trim(),
        "hi"
    );
}
