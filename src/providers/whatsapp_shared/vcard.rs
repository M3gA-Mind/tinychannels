//! vCard 3.0 reading and writing for WhatsApp contact cards.

#![cfg_attr(
    not(feature = "whatsapp-web"),
    allow(
        dead_code,
        reason = "vCard reading and writing serve the WhatsApp Web adapter"
    )
)]

use tinychannels_bus::ContactMethod;

/// A contact as both adapters carry it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ContactCard {
    pub display_name: String,
    pub phones: Vec<ContactMethod>,
    pub emails: Vec<ContactMethod>,
    pub organization: Option<String>,
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace(';', "\\;")
        .replace(',', "\\,")
        .replace('\n', "\\n")
        .replace('\r', "")
}

fn unescape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n' | 'N') => out.push('\n'),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

/// A label may only become a vCard parameter when it is a known type token;
/// anything else could inject parameters or properties.
fn type_token<'a>(label: Option<&'a str>, fallback: &'a str) -> &'a str {
    match label.map(str::trim) {
        Some(label)
            if ["CELL", "HOME", "WORK", "FAX", "PAGER", "VOICE", "INTERNET"]
                .iter()
                .any(|known| known.eq_ignore_ascii_case(label)) =>
        {
            label
        }
        _ => fallback,
    }
}

/// The digits of a phone number, for WhatsApp's `waid` parameter.
pub(crate) fn phone_digits(phone: &str) -> String {
    phone.chars().filter(char::is_ascii_digit).collect()
}

/// Render a card as a vCard 3.0 body. Phones carry `waid` so WhatsApp offers
/// to message the contact.
pub(crate) fn write(card: &ContactCard) -> String {
    let name = escape(&card.display_name);
    let mut out = format!("BEGIN:VCARD\r\nVERSION:3.0\r\nFN:{name}\r\nN:{name};;;;\r\n");
    for phone in &card.phones {
        let digits = phone_digits(&phone.value);
        let kind = type_token(phone.label.as_deref(), "CELL");
        if digits.is_empty() {
            out.push_str(&format!("TEL;type={kind}:{}\r\n", escape(&phone.value)));
        } else {
            out.push_str(&format!(
                "TEL;type={kind};waid={digits}:{}\r\n",
                escape(&phone.value)
            ));
        }
    }
    for email in &card.emails {
        let kind = type_token(email.label.as_deref(), "INTERNET");
        out.push_str(&format!("EMAIL;type={kind}:{}\r\n", escape(&email.value)));
    }
    if let Some(org) = card.organization.as_deref().filter(|org| !org.is_empty()) {
        out.push_str(&format!("ORG:{}\r\n", escape(org)));
    }
    out.push_str("END:VCARD\r\n");
    out
}

/// Read the first card of a vCard body. Unknown properties are ignored.
pub(crate) fn read(body: &str) -> ContactCard {
    // Unfold continuation lines before splitting into properties.
    let unfolded = body.replace("\r\n ", "").replace("\n ", "");
    let mut card = ContactCard::default();
    let mut structured_name = None;
    for line in unfolded.lines() {
        let Some((head, value)) = line.split_once(':') else {
            continue;
        };
        let mut params = head.split(';');
        let property = params.next().unwrap_or_default().to_ascii_uppercase();
        let label = params
            .find_map(|param| {
                let (key, value) = param.split_once('=')?;
                key.eq_ignore_ascii_case("type").then(|| value.to_owned())
            })
            .filter(|value| !value.is_empty());
        let value = unescape(value.trim());
        match property.as_str() {
            "FN" if card.display_name.is_empty() => card.display_name = value,
            "N" if structured_name.is_none() => {
                let parts: Vec<&str> = value.split(';').collect();
                let given = parts.get(1).copied().unwrap_or_default().trim();
                let family = parts.first().copied().unwrap_or_default().trim();
                let joined = [given, family]
                    .into_iter()
                    .filter(|part| !part.is_empty())
                    .collect::<Vec<_>>()
                    .join(" ");
                structured_name = Some(joined);
            }
            "TEL" if !value.is_empty() => card.phones.push(ContactMethod { value, label }),
            "EMAIL" if !value.is_empty() => card.emails.push(ContactMethod { value, label }),
            "ORG" if card.organization.is_none() && !value.is_empty() => {
                card.organization = Some(value.replace(';', " ").trim().to_owned());
            }
            _ => {}
        }
    }
    if card.display_name.is_empty() {
        card.display_name = structured_name.unwrap_or_default();
    }
    card
}

#[cfg(test)]
#[path = "vcard_tests.rs"]
mod tests;
