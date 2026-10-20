//! A scratch Messages folder (database, attachments, a recording AppleScript
//! runner) and recording host ports for the iMessage tests.

use super::*;
use crate::providers::native::fixtures::*;
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub(super) const PEER: &str = "+15555550123";
pub(super) const GROUP: &str = "iMessage;+;chat123456";

pub(super) struct Rig {
    pub(super) dir: tempfile::TempDir,
    pub(super) db_path: PathBuf,
    /// The scripts the runner was asked to run, each followed by `\n---\n`.
    pub(super) out: PathBuf,
    pub(super) channel: IMessageChannel,
    pub(super) media: Arc<StubMedia>,
    pub(super) voice: Arc<StubVoice>,
    pub(super) events: Arc<RecordingEvents>,
}

/// A runner that appends the `-e` script to `out`.
fn recording_runner(out: &Path) -> Vec<String> {
    vec![
        "sh".into(),
        "-c".into(),
        format!("printf '%s\\n---\\n' \"$2\" >> '{}'", out.display()),
        "fake-osascript".into(),
    ]
}

/// A converter that copies its input (the fifth argument) to its output (the
/// sixth), matching the shape of `afconvert -f m4af -d aac <input> <output>`.
fn copy_converter(dir: &Path) -> Vec<String> {
    if cfg!(windows) {
        // The iMessage conversion fixture runs on Windows CI too. Keep the
        // fake conversion on the Windows command interpreter there so paths
        // such as `D:\a\...` are passed to the native `copy` command intact.
        let script = dir.join("fake-afconvert.cmd");
        std::fs::write(
            &script,
            "@echo off\r\ncopy /B /Y \"%~5\" \"%~6\" >NUL\r\nexit /b %ERRORLEVEL%\r\n",
        )
        .unwrap();
        vec![
            "cmd.exe".into(),
            "/D".into(),
            "/C".into(),
            "call".into(),
            script.to_string_lossy().into_owned(),
        ]
    } else {
        vec![
            "sh".into(),
            "-c".into(),
            "cp \"$5\" \"$6\"".into(),
            "fake-afconvert".into(),
        ]
    }
}

pub(super) fn rig() -> Rig {
    let dir = tempfile::Builder::new()
        .prefix("tiny channels-")
        .tempdir()
        .unwrap();
    let db_path = dir.path().join("chat.db");
    let conn = Connection::open(&db_path).unwrap();
    db::tests::create_schema(&conn);
    conn.execute("UPDATE handle SET id = ?1 WHERE ROWID = 1", [PEER])
        .unwrap();
    std::fs::create_dir_all(dir.path().join("Attachments")).unwrap();
    let out = dir.path().join("scripts.txt");
    let media = Arc::new(StubMedia::default());
    let voice = Arc::new(StubVoice::default());
    *voice.audio_base64.lock().unwrap() = Some(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"voice",
    ));
    let events = Arc::new(RecordingEvents::delivering());
    let mut channel = IMessageChannel::new(vec!["*".into()])
        .with_media_repository(media.clone())
        .with_voice_media_store(voice.clone())
        .with_transcriber(Arc::new(StubTranscriber::default()))
        .with_events(events.clone());
    channel.db_path = Some(db_path.clone());
    channel.poll_interval = Duration::from_millis(5);
    channel.script_command = recording_runner(&out);
    channel.converter_command = copy_converter(dir.path());
    channel.resolve_timeout = Duration::from_secs(5);
    Rig {
        dir,
        db_path,
        out,
        channel,
        media,
        voice,
        events,
    }
}

impl Rig {
    pub(super) fn conn(&self) -> Connection {
        Connection::open(&self.db_path).unwrap()
    }

    /// The scripts the runner has seen so far.
    pub(super) fn scripts(&self) -> Vec<String> {
        std::fs::read_to_string(&self.out)
            .unwrap_or_default()
            .split("\n---\n")
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect()
    }

    /// Stand in for Messages: record a sent row for every script the runner
    /// has run, as `SENT1`, `SENT2`, ... with ROWIDs from 100.
    pub(super) fn spawn_messages(&self) -> tokio::task::JoinHandle<()> {
        let (db_path, out) = (self.db_path.clone(), self.out.clone());
        tokio::spawn(async move {
            let mut recorded = 0usize;
            loop {
                let text = std::fs::read_to_string(&out).unwrap_or_default();
                let count = text.matches("\n---\n").count();
                if count > recorded {
                    let conn = Connection::open(&db_path).unwrap();
                    for n in recorded + 1..=count {
                        conn.execute(
                            "INSERT INTO message (ROWID, guid, handle_id, text, is_from_me, date) \
                             VALUES (?1, ?2, 1, 'sent', 1, 700000000000000000)",
                            rusqlite::params![100 + n as i64, format!("SENT{n}")],
                        )
                        .unwrap();
                    }
                    recorded = count;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    }

    /// Write `bytes` under the attachments folder and return its path.
    pub(super) fn attachment(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.dir.path().join("Attachments").join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    /// Insert an inbound row and its attachments, returning the row.
    pub(super) fn inbound(
        &self,
        rowid: i64,
        text: Option<&str>,
        audio: bool,
        attachments: &[(&Path, &str)],
    ) -> db::InboundRow {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO message (ROWID, guid, handle_id, text, is_from_me, date, is_audio_message) \
             VALUES (?1, ?2, 1, ?3, 0, 700000000000000000, ?4)",
            rusqlite::params![rowid, format!("IN{rowid}"), text, audio],
        )
        .unwrap();
        for (n, (path, mime)) in attachments.iter().enumerate() {
            let id = rowid * 10 + n as i64;
            conn.execute(
                "INSERT INTO attachment (ROWID, filename, mime_type, transfer_name, total_bytes) \
                 VALUES (?1, ?2, ?3, ?4, 5)",
                rusqlite::params![
                    id,
                    path.to_string_lossy(),
                    mime,
                    path.file_name().unwrap().to_string_lossy()
                ],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO message_attachment_join VALUES (?1, ?2)",
                [rowid, id],
            )
            .unwrap();
        }
        db::inbound_since(&conn, rowid - 1)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }
}
