//! TinyChannels' stable composite-key encoding.
//!
//! The v1 storage API's `Key::of` encoded composite IDs before they were
//! persisted. TSD v2 deliberately exposes only validation for already encoded
//! keys, so this owner-local helper keeps the old byte format for readback.

use sha2::{Digest, Sha256};
use tinystoragedrivers_types::{Key, StorageError};

const MAX_ID_LEN: usize = 512;

/// Encode parts exactly like the previous `Key::of`, including its long-key
/// digest format, then validate the result with the current storage types.
pub(crate) fn of<I, S>(parts: I) -> Result<Key, StorageError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut escaped = String::new();
    let mut count = 0usize;
    for part in parts {
        if count > 0 {
            escaped.push('/');
        }
        escape_into(part.as_ref(), count == 0, &mut escaped);
        count += 1;
    }
    if count == 0 {
        return Err(StorageError::invalid_input("a key needs at least one part"));
    }
    if escaped.len() > MAX_ID_LEN {
        escaped = format!(
            "~{}",
            data_encoding::BASE32_NOPAD
                .encode(&Sha256::digest(escaped.as_bytes()))
                .to_ascii_lowercase()
        );
    }
    Key::new(escaped)
}

fn escape_into(part: &str, first: bool, out: &mut String) {
    for (i, c) in part.chars().enumerate() {
        match c {
            '%' => out.push_str("%25"),
            '/' => out.push_str("%2F"),
            '\0' => out.push_str("%00"),
            '~' if first && i == 0 => out.push_str("%7E"),
            c => out.push(c),
        }
    }
}

#[cfg(test)]
#[path = "key_tests.rs"]
mod tests;
