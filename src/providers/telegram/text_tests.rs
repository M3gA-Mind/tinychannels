use super::*;

#[test]
fn markdown_v2_escapes_literals_and_preserves_balanced_formatting() {
    assert_eq!(markdown_v2_text("*bold* value!"), "*bold* value\\!");
    assert_eq!(markdown_v2_text("plain (text)"), "plain \\(text\\)");
    assert_eq!(markdown_v2_text("unclosed *marker"), "unclosed \\*marker");
    assert_eq!(markdown_v2_text("||hidden||"), "||hidden||");
}
