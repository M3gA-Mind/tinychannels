use super::*;

#[test]
fn markdown_v2_escapes_literals_and_preserves_balanced_formatting() {
    assert_eq!(markdown_v2_text("*bold* value!"), "*bold* value\\!");
    assert_eq!(markdown_v2_text("plain (text)"), "plain \\(text\\)");
    assert_eq!(markdown_v2_text("unclosed *marker"), "unclosed \\*marker");
    assert_eq!(markdown_v2_text("||hidden||"), "||hidden||");
    assert_eq!(
        markdown_v2_text("unclosed ||spoiler"),
        "unclosed \\|\\|spoiler"
    );
    assert_eq!(markdown_v2_text("`a.b = c`"), "`a.b = c`");
    assert_eq!(markdown_v2_text("```a.b = c```"), "```a.b = c```");
    assert_eq!(markdown_v2_text(r"keep \*literal\*"), r"keep \*literal\*");
    assert_eq!(
        markdown_v2_text("[site](https://example.test/a-b)"),
        "[site](https://example.test/a-b)"
    );
    assert_eq!(markdown_v2_text("[broken](url"), "\\[broken\\]\\(url");
}

#[test]
fn markdown_v2_keeps_link_destinations_and_escapes_literal_label_markup() {
    assert_eq!(
        markdown_v2_text("[foo_bar](https://example.test/a_b)"),
        "[foo\\_bar](https://example.test/a_b)"
    );
}

#[test]
fn markdown_v2_does_not_preserve_crossing_format_delimiters() {
    assert_eq!(markdown_v2_text("*a _b* c_"), "\\*a \\_b\\* c\\_");
}

#[test]
fn markdown_v2_preserves_escapes_in_code_and_is_linear_on_unmatched_controls() {
    assert_eq!(markdown_v2_text(r"`a\\b`"), r"`a\\b`");
    assert_eq!(markdown_v2_text(r"`a\`b`"), r"`a\`b`");
    assert_eq!(
        markdown_v2_text("```let x = `raw`;```"),
        "```let x = \\`raw\\`;```"
    );
    assert_eq!(
        markdown_v2_text("`literal ```ticks`"),
        "`literal \\`\\`\\`ticks`"
    );
    let malformed = format!("{}{}", "\\".repeat(32_000), "[".repeat(32_000));
    assert_eq!(markdown_v2_text(&malformed).len(), malformed.len() + 32_000);
}
