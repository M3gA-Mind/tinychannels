//! Outbound text preparation.

pub(crate) use crate::providers::telegram::text::strip_tool_call_tags;

/// Split `text` into chunks of at most `limit` characters, breaking at a
/// newline, else a space, else mid-word. Chunks keep the original characters;
/// only the break character itself is consumed.
pub(crate) fn split_text(text: &str, limit: usize) -> Vec<String> {
    let limit = limit.max(1);
    let mut chunks = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        if rest.chars().count() <= limit {
            chunks.push(rest.to_owned());
            break;
        }
        let window_end = rest
            .char_indices()
            .nth(limit)
            .map_or(rest.len(), |(index, _)| index);
        let window = &rest[..window_end];
        if rest[window_end..].starts_with(['\n', ' ']) {
            chunks.push(window.to_owned());
            rest = &rest[window_end + 1..];
            continue;
        }
        let floor = window
            .char_indices()
            .nth(limit / 2)
            .map_or(0, |(index, _)| index);
        let split = window
            .rfind('\n')
            .filter(|index| *index >= floor)
            .or_else(|| window.rfind(' ').filter(|index| *index >= floor));
        match split {
            Some(index) => {
                chunks.push(rest[..index].to_owned());
                let skip = rest[index..].chars().next().map_or(0, char::len_utf8);
                rest = &rest[index + skip..];
            }
            None => {
                chunks.push(window.to_owned());
                rest = &rest[window_end..];
            }
        }
    }
    chunks.retain(|chunk| !chunk.trim().is_empty());
    chunks
}

#[cfg(test)]
#[path = "text_tests.rs"]
mod tests;
