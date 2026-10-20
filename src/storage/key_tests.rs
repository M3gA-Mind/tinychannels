use super::*;

#[test]
fn composite_keys_keep_the_v1_escape_and_unicode_bytes() {
    assert_eq!(
        of(["a/b", "100%", "x\0y"]).unwrap().as_str(),
        "a%2Fb/100%25/x%00y"
    );
    assert_eq!(of(["~abc", "~def"]).unwrap().as_str(), "%7Eabc/~def");
    assert_eq!(
        of(["héllo", "日本語", "🦀/🦀"]).unwrap().as_str(),
        "héllo/日本語/🦀%2F🦀"
    );
}

#[test]
fn long_composite_keys_keep_the_v1_sha256_base32_encoding() {
    assert_eq!(
        of(["a".repeat(600)]).unwrap().as_str(),
        "~xi24c4dssql7csm6bcdopyjpzw2kwafnieirblq6rcghm3ko24gq"
    );
    assert_eq!(of(["/".repeat(170)]).unwrap().as_str().len(), 510);
    assert_eq!(of(["/".repeat(171)]).unwrap().as_str().len(), 53);
}
