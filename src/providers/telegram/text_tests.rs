use super::*;

#[test]
fn markdown_v2_escapes_literals_and_preserves_balanced_formatting() {
    assert_eq!(markdown_v2_text("*bold* value!"), "*bold* value\\!");
    assert_eq!(markdown_v2_text("plain (text)"), "plain \\(text\\)");
    assert_eq!(markdown_v2_text("unclosed *marker"), "unclosed \\*marker");
    assert_eq!(markdown_v2_text("||hidden||"), "||hidden||");
    assert_eq!(markdown_v2_text("unclosed ||spoiler"), "unclosed \\|\\|spoiler");
    assert_eq!(markdown_v2_text("`a.b = c`"), "`a.b = c`");
    assert_eq!(markdown_v2_text("```a.b = c```"), "```a.b = c```");
    assert_eq!(markdown_v2_text(r"keep \*literal\*"), r"keep \*literal\*");
    assert_eq!(markdown_v2_text("[site](https://example.test/a-b)"), "[site](https://example.test/a-b)");
    assert_eq!(markdown_v2_text("[broken](url"), "\\[broken\\]\\(url");
}
