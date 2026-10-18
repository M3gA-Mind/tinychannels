//! Module-owned state and methods for projecting canonical web outputs.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use tinybus::{Error as BusError, Result as BusResult};
use tinychannels_bus::module_web_output::{
    BeginWebTurn, EndWebTurn, ProjectWebOutput, WebOutputEvents, WebTurnHandle,
};
use tinychannels_bus::web::WebChannelEvent;
use tinychannels_runtime::adapters::web::WebTurnProjector;

const MAX_ACTIVE_WEB_TURNS: usize = 256;
const MAX_WEB_TURN_CONTEXT_FIELD_BYTES: usize = 1024;
const MAX_WEB_TURN_SCOPE_BYTES: usize = 2048;
const MAX_WEB_TURN_HANDLE_BYTES: usize = 64;
const MAX_WEB_TURN_RETAINED_BYTES: usize = 1024 * 1024;

struct WebTurn {
    // The host-derived isolation key remains associated with the opaque handle;
    // it is never used to stamp client-facing event routing fields.
    _scope_key: String,
    retained_bytes: usize,
    projector: WebTurnProjector,
}

#[derive(Default)]
pub(crate) struct WebTurnRegistry {
    turns: HashMap<String, WebTurn>,
    retained_bytes: usize,
}

impl WebTurnRegistry {
    pub(crate) fn begin(&mut self, request: BeginWebTurn) -> BusResult<WebTurnHandle> {
        let context = request.context;
        if request.scope_key.is_empty()
            || request.scope_key.len() > MAX_WEB_TURN_SCOPE_BYTES
            || context.client_id.is_empty()
            || context.thread_id.is_empty()
            || context.request_id.is_empty()
            || context.client_id.len() > MAX_WEB_TURN_CONTEXT_FIELD_BYTES
            || context.thread_id.len() > MAX_WEB_TURN_CONTEXT_FIELD_BYTES
            || context.request_id.len() > MAX_WEB_TURN_CONTEXT_FIELD_BYTES
            || context
                .turn_request_id
                .as_ref()
                .is_some_and(|id| id.len() > MAX_WEB_TURN_CONTEXT_FIELD_BYTES)
        {
            return Err(BusError::failed("web turn context exceeds module bounds"));
        }
        if context.seq.is_some() || context.ts.is_some() {
            return Err(BusError::failed(
                "web turn seq and timestamp are module-owned",
            ));
        }
        if self.turns.len() >= MAX_ACTIVE_WEB_TURNS {
            return Err(BusError::failed("active web turn capacity reached"));
        }

        // Count retained user-controlled strings. The turn-count cap also
        // bounds per-entry map/projector overhead.
        let retained_bytes = request.scope_key.len()
            + context.client_id.len()
            + context.thread_id.len()
            + context.request_id.len()
            + context.turn_request_id.as_ref().map_or(0, String::len);
        if self.retained_bytes.saturating_add(retained_bytes) > MAX_WEB_TURN_RETAINED_BYTES {
            return Err(BusError::failed("web turn context capacity reached"));
        }

        let handle = loop {
            let candidate = uuid::Uuid::new_v4().to_string();
            if !self.turns.contains_key(&candidate) {
                break candidate;
            }
        };
        let mut projector =
            WebTurnProjector::new(&context.client_id, &context.thread_id, &context.request_id);
        if let Some(turn_request_id) = context.turn_request_id {
            projector = projector.with_turn_request_id(turn_request_id);
        }
        self.retained_bytes += retained_bytes;
        self.turns.insert(
            handle.clone(),
            WebTurn {
                _scope_key: request.scope_key,
                retained_bytes,
                projector,
            },
        );
        Ok(WebTurnHandle { handle })
    }

    pub(crate) fn project(&mut self, request: ProjectWebOutput) -> BusResult<WebOutputEvents> {
        validate_handle(&request.handle)?;
        let turn = self
            .turns
            .get_mut(&request.handle)
            .ok_or_else(|| BusError::failed("web turn handle is not active"))?;
        let events = turn.projector.project(request.event, unix_ms());
        if events.iter().any(is_terminal) {
            self.remove(&request.handle);
        }
        Ok(WebOutputEvents { events })
    }

    pub(crate) fn end(&mut self, request: EndWebTurn) -> BusResult<()> {
        validate_handle(&request.handle)?;
        // Terminal projection may already have removed the entry; explicit
        // host cleanup is idempotent, while Project still fails closed.
        self.remove(&request.handle);
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        self.turns.clear();
        self.retained_bytes = 0;
    }

    fn remove(&mut self, handle: &str) -> Option<WebTurn> {
        let turn = self.turns.remove(handle)?;
        self.retained_bytes = self.retained_bytes.saturating_sub(turn.retained_bytes);
        Some(turn)
    }
}

fn validate_handle(handle: &str) -> BusResult<()> {
    if handle.is_empty() || handle.len() > MAX_WEB_TURN_HANDLE_BYTES {
        return Err(BusError::failed("web turn handle is invalid"));
    }
    Ok(())
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

fn is_terminal(event: &WebChannelEvent) -> bool {
    matches!(
        event,
        WebChannelEvent::ChatDone { .. }
            | WebChannelEvent::ChatError { .. }
            | WebChannelEvent::ChatCancelled { .. }
    )
}

#[cfg(test)]
#[path = "web_output_tests.rs"]
mod tests;
