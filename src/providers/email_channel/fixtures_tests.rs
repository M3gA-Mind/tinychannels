//! A capturing SMTP server and recording host ports for the email tests.

use super::*;
use crate::providers::native::fixtures::*;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

pub(super) const BOT: &str = "bot@example.com";
pub(super) const PEER: &str = "ada@example.org";

/// An SMTP server that accepts any number of sessions and keeps the DATA of
/// every message it accepted, headers first.
pub(super) struct Smtp {
    pub(super) port: u16,
    pub(super) messages: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Smtp {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) async fn smtp(refuse_rcpt: bool) -> Smtp {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let messages = Arc::new(Mutex::new(Vec::new()));
    let captured = messages.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            let captured = captured.clone();
            tokio::spawn(async move {
                let (read, mut write) = sock.into_split();
                let mut lines = BufReader::new(read).lines();
                let _ = write.write_all(b"220 fake ESMTP\r\n").await;
                let mut data: Option<Vec<String>> = None;
                while let Ok(Some(line)) = lines.next_line().await {
                    let reply: &[u8] = if let Some(collected) = data.as_mut() {
                        if line == "." {
                            captured
                                .lock()
                                .unwrap()
                                .push(data.take().unwrap_or_default().join("\n"));
                            b"250 queued\r\n"
                        } else {
                            collected.push(line);
                            continue;
                        }
                    } else {
                        let upper = line.to_ascii_uppercase();
                        if upper.starts_with("EHLO") {
                            b"250-fake\r\n250 AUTH PLAIN LOGIN\r\n"
                        } else if upper.starts_with("AUTH") {
                            b"235 ok\r\n"
                        } else if upper.starts_with("RCPT") && refuse_rcpt {
                            b"550 no such user\r\n"
                        } else if upper.starts_with("DATA") {
                            data = Some(Vec::new());
                            b"354 go ahead\r\n"
                        } else if upper.starts_with("QUIT") {
                            let _ = write.write_all(b"221 bye\r\n").await;
                            break;
                        } else {
                            b"250 ok\r\n"
                        }
                    };
                    if write.write_all(reply).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    Smtp {
        port,
        messages,
        task,
    }
}

pub(super) fn config_for(smtp: &Smtp) -> EmailConfig {
    EmailConfig {
        imap_host: "imap.example.com".into(),
        imap_port: 993,
        smtp_host: "127.0.0.1".into(),
        smtp_port: smtp.port,
        smtp_tls: false,
        username: String::new(),
        password: "pw".into(),
        from_address: BOT.into(),
        allowed_senders: vec!["*".into()],
        ..Default::default()
    }
}

pub(super) struct Rig {
    pub(super) channel: EmailChannel,
    pub(super) media: Arc<StubMedia>,
    pub(super) voice: Arc<StubVoice>,
    pub(super) events: Arc<RecordingEvents>,
}

pub(super) fn rig(smtp: &Smtp) -> Rig {
    let media = Arc::new(StubMedia::default());
    let voice = Arc::new(StubVoice::default());
    *voice.audio_base64.lock().unwrap() = Some(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"voice",
    ));
    let events = Arc::new(RecordingEvents::delivering());
    let channel = EmailChannel::new(config_for(smtp))
        .with_media_repository(media.clone())
        .with_voice_media_store(voice.clone())
        .with_transcriber(Arc::new(StubTranscriber::default()))
        .with_events(events.clone());
    Rig {
        channel,
        media,
        voice,
        events,
    }
}

/// The value of header `name` in a captured message (unfolded), if present.
pub(super) fn header(message: &str, name: &str) -> Option<String> {
    let prefix = format!("{}:", name.to_ascii_lowercase());
    let mut found: Option<String> = None;
    for line in message.lines() {
        if line.is_empty() {
            break;
        }
        if let Some(current) = found.as_mut() {
            if line.starts_with(' ') || line.starts_with('\t') {
                current.push(' ');
                current.push_str(line.trim());
                continue;
            }
            break;
        }
        if line.to_ascii_lowercase().starts_with(&prefix) {
            found = Some(line[prefix.len()..].trim().to_owned());
        }
    }
    found
}

/// A raw RFC 822 message from `PEER`.
pub(super) fn raw_mail(headers: &str, body: &str) -> Vec<u8> {
    format!(
        "From: Ada <{PEER}>\r\nTo: {BOT}\r\nDate: Mon, 15 Jan 2025 12:00:00 +0000\r\n{headers}\r\n{body}"
    )
    .into_bytes()
}

/// A multipart message with a text body and one base64 attachment.
pub(super) fn raw_with_attachment(
    headers: &str,
    body: &str,
    mime: &str,
    name: &str,
    bytes: &[u8],
) -> Vec<u8> {
    let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
    let mut mail = format!(
        "From: Ada <{PEER}>\r\nTo: {BOT}\r\nDate: Mon, 15 Jan 2025 12:00:00 +0000\r\n{headers}MIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: text/plain\r\n\r\n{body}\r\n--b\r\nContent-Type: {mime}; name=\"{name}\"\r\nContent-Disposition: attachment; filename=\"{name}\"\r\nContent-Transfer-Encoding: base64\r\n\r\n{encoded}\r\n--b--\r\n"
    )
    .into_bytes();
    mail.shrink_to_fit();
    mail
}

/// Dispatch `raw` the way the IMAP loop would after the allowlist and dedupe.
pub(super) async fn dispatch(
    channel: &EmailChannel,
    msg_id: &str,
    raw: Vec<u8>,
) -> (bool, Vec<ChannelMessage>) {
    let (tx, mut rx) = mpsc::channel(8);
    let parsed = MessageParser::default().parse(&raw).unwrap();
    let email = ParsedEmail {
        _uid: 1,
        msg_id: msg_id.to_owned(),
        sender: EmailChannel::extract_sender(&parsed),
        content: format!(
            "Subject: {}\n\n{}",
            parsed.subject().unwrap_or("(no subject)"),
            EmailChannel::extract_text(&parsed)
        ),
        timestamp: 1_736_942_400,
        raw,
    };
    let alive = channel.dispatch_email(email, &tx).await;
    drop(tx);
    let mut legacy = Vec::new();
    while let Some(message) = rx.recv().await {
        legacy.push(message);
    }
    (alive, legacy)
}
