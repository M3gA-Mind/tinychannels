//! Markdown to Slack `mrkdwn` and message chunking.

pub(crate) use crate::providers::telegram::strip_tool_call_tags;

/// Convert common Markdown into Slack `mrkdwn`: escape control characters,
/// `**bold**` to `*bold*`, `[label](url)` to `<url|label>` and headings to bold.
pub(crate) fn to_mrkdwn(markdown: &str) -> String {
    markdown
        .lines()
        .map(convert_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn convert_line(line: &str) -> String {
    let trimmed = line.trim_start_matches('#');
    let hashes = line.len() - trimmed.len();
    if (1..=6).contains(&hashes) && trimmed.starts_with(' ') && !trimmed.trim().is_empty() {
        return format!("*{}*", convert_inline(trimmed.trim()));
    }
    convert_inline(line)
}

fn convert_inline(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(ch) = rest.chars().next() {
        if let Some(after) = rest.strip_prefix("**")
            && let Some(end) = after.find("**")
            && end > 0
        {
            out.push('*');
            out.push_str(&escape(&after[..end]));
            out.push('*');
            rest = &after[end + 2..];
            continue;
        }
        if ch == '['
            && let Some((label, url, consumed)) = markdown_link(rest)
        {
            out.push_str(&format!("<{url}|{}>", escape(label)));
            rest = &rest[consumed..];
            continue;
        }
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            other => out.push(other),
        }
        rest = &rest[ch.len_utf8()..];
    }
    out
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// `[label](url)` at the start of `text` with an http(s) URL.
fn markdown_link(text: &str) -> Option<(&str, &str, usize)> {
    let close = text.find("](")?;
    let label = &text[1..close];
    let after = &text[close + 2..];
    let end = after.find(')')?;
    let url = &after[..end];
    if label.is_empty()
        || label.contains('[')
        || !(url.starts_with("https://") || url.starts_with("http://"))
        || url.contains(['<', '>', '|', ' '])
    {
        return None;
    }
    Some((label, url, close + 2 + end + 1))
}

/// Split `text` into chunks of at most `max_chars`, preferring line then word
/// boundaries. Never returns an empty chunk.
pub(crate) fn chunk_text(text: &str, max_chars: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut remaining = text.trim();
    while !remaining.is_empty() {
        if remaining.chars().count() <= max_chars {
            chunks.push(remaining.to_owned());
            break;
        }
        let hard = remaining
            .char_indices()
            .nth(max_chars)
            .map(|(index, _)| index)
            .unwrap_or(remaining.len());
        let window = &remaining[..hard];
        let cut = if remaining[hard..].starts_with(char::is_whitespace) {
            hard
        } else {
            window
                .rfind('\n')
                .or_else(|| window.rfind(' '))
                .filter(|index| *index > 0)
                .unwrap_or(hard)
        };
        let (head, tail) = remaining.split_at(cut);
        if !head.trim().is_empty() {
            chunks.push(head.trim().to_owned());
        }
        remaining = tail.trim_start();
    }
    chunks
}

#[cfg(test)]
#[path = "text_tests.rs"]
mod tests;
