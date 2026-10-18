use super::*;

#[test]
fn markdown_bold_links_and_headings_become_mrkdwn() {
    assert_eq!(
        to_mrkdwn("# Title\nsome **bold** and [docs](https://x.test/a?b=1) & <tag>"),
        "*Title*\nsome *bold* and <https://x.test/a?b=1|docs> &amp; &lt;tag&gt;"
    );
}

#[test]
fn unsafe_links_and_unclosed_markers_stay_literal() {
    assert_eq!(
        to_mrkdwn("[x](javascript:alert) **open [y](https://a|b)"),
        "[x](javascript:alert) **open [y](https://a|b)"
    );
    assert_eq!(to_mrkdwn("[](https://a.test) [a"), "[](https://a.test) [a");
    assert_eq!(to_mrkdwn("####### seven"), "####### seven");
    assert_eq!(to_mrkdwn("#nospace"), "#nospace");
    assert_eq!(to_mrkdwn("****"), "****");
}

#[test]
fn chunks_prefer_line_then_word_boundaries_and_drop_empty_pieces() {
    assert_eq!(chunk_text("short", 10), ["short"]);
    assert_eq!(chunk_text("aaa bbb\nccc ddd", 8), ["aaa bbb", "ccc ddd"]);
    assert_eq!(chunk_text("aaaa bbbb cccc", 9), ["aaaa bbbb", "cccc"]);
    assert_eq!(chunk_text("abcdefghij", 4), ["abcd", "efgh", "ij"]);
    assert_eq!(chunk_text("   ", 4), Vec::<String>::new());
    assert_eq!(chunk_text("\n\n\nabcdefgh", 4), ["abcd", "efgh"]);
}
