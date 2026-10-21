//! Cancellation-safe typed polling for the configured Messages database.

use super::{IMessageChannel, ListenerCursor, ListenerRegistration};
use crate::channel::ChannelInboundSink;
use rusqlite::Connection;
use std::sync::atomic::Ordering;
use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};
use tokio::sync::{oneshot, watch};

impl IMessageChannel {
    pub(super) async fn listen_typed(
        &self,
        sink: &(dyn ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        let (cancel, mut cancellation) = watch::channel(false);
        let generation = self
            .listener_generation
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        {
            let mut active = self
                .listener_cancel
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if active.is_some() {
                return Err(ChannelSendError {
                    kind: SendErrorKind::Transient,
                    message: "iMessage listener is already running".into(),
                    retryable: true,
                    ..Default::default()
                });
            }
            *active = Some((generation, cancel));
        }
        let _registration = ListenerRegistration {
            slot: self.listener_cancel.clone(),
            generation,
        };

        let result = self.listen_typed_with(sink, &mut cancellation, None).await;
        if *cancellation.borrow() {
            Ok(())
        } else {
            result
        }
    }

    pub(super) async fn stop_typed_listener(&self) -> Result<(), ChannelSendError> {
        if let Some((_, cancel)) = self
            .listener_cancel
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            let _ = cancel.send(true);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn listen_typed_with_ready(
        &self,
        sink: &(dyn ChannelInboundSink + Send + Sync),
        mut cancellation: watch::Receiver<bool>,
        ready: oneshot::Sender<()>,
    ) -> Result<(), ChannelSendError> {
        self.listen_typed_with(sink, &mut cancellation, Some(ready))
            .await
    }

    async fn listen_typed_with(
        &self,
        sink: &(dyn ChannelInboundSink + Send + Sync),
        cancellation: &mut watch::Receiver<bool>,
        ready: Option<oneshot::Sender<()>>,
    ) -> Result<(), ChannelSendError> {
        let db_path = self
            .messages_db_path()
            .ok_or_else(|| listener_error("Cannot find Messages database path"))?;
        if !db_path.exists() {
            return Err(listener_error("Messages database does not exist"));
        }

        let path = db_path.clone();
        let conn = tokio::task::spawn_blocking(move || super::db::open_read_only(&path))
            .await
            .map_err(|_| listener_error("iMessage database worker failed"))?
            .map_err(|_| listener_error("iMessage database could not be opened"))?;
        let (mut conn, mut cursor) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let rowid = super::db::max_inbound_rowid(&conn)?;
            let (read_date, read_rowid) = super::db::max_read_cursor(&conn)?;
            Ok::<(Connection, ListenerCursor), anyhow::Error>((
                conn,
                ListenerCursor {
                    inbound_rowid: rowid,
                    read_date,
                    read_rowid,
                },
            ))
        })
        .await
        .map_err(|_| listener_error("iMessage cursor worker failed"))?
        .map_err(|_| listener_error("iMessage startup cursor could not be read"))?;

        {
            let mut saved = self
                .listener_cursor
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(previous) = *saved {
                cursor = previous;
            } else {
                *saved = Some(cursor);
            }
        }
        if let Some(ready) = ready {
            let _ = ready.send(());
        }

        loop {
            let slept = tokio::select! {
                biased;
                _ = cancellation.wait_for(|stopped| *stopped) => return Ok(()),
                _ = tokio::time::sleep(self.poll_interval) => true,
            };
            if !slept {
                return Ok(());
            }

            let since_rowid = cursor.inbound_rowid;
            let since_read = cursor.read_date;
            let since_read_rowid = cursor.read_rowid;
            let (returned_conn, rows, reads) = tokio::task::spawn_blocking(move || {
                let rows = super::db::inbound_since(&conn, since_rowid);
                let reads = super::db::read_since_cursor(&conn, since_read, since_read_rowid);
                (conn, rows, reads)
            })
            .await
            .map_err(|_| listener_error("iMessage poll worker failed"))?;
            conn = returned_conn;

            if let Ok(reads) = reads {
                for read in reads {
                    let (date_read, rowid) = (read.date_read, read.rowid);
                    self.dispatch_read_typed(read, sink, cancellation).await?;
                    cursor.read_date = date_read;
                    cursor.read_rowid = rowid;
                    self.save_listener_cursor(cursor);
                }
            }

            if let Ok(rows) = rows {
                for row in rows {
                    let rowid = row.rowid;
                    self.dispatch_row_typed(row, sink, cancellation).await?;
                    cursor.inbound_rowid = cursor.inbound_rowid.max(rowid);
                    self.save_listener_cursor(cursor);
                }
            }
        }
    }

    fn save_listener_cursor(&self, cursor: ListenerCursor) {
        *self
            .listener_cursor
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(cursor);
    }
}

fn listener_error(message: &str) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::Transient,
        message: message.into(),
        retryable: true,
        ..Default::default()
    }
}
