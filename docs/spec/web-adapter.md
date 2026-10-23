# Web adapter

The in-app chat (desktop, web, TUI, relayed SaaS) is a channel. The adapter
lives in `tinychannels-runtime` (`adapters::web`) and speaks only the v2
contract: `ChannelInboundEnvelope` in, `ChannelOutputEvent` out, and the
`WebChannelEvent` wire (`tinychannels-bus::web`) toward browsers.

## Inbound

`WebChatStart` is the transport-free request contract in `tinychannels-bus`;
the host builds it from a Socket.IO `chat:start` or the `channel.web_chat` RPC.
Attachments are typed `ContentPart`s (image, video,
audio, file, voice note), replacing the `[IMAGE:..]` / `[FILE:..]` text
markers. `normalize_chat_start` produces the envelope:

- channel `web` (+ account), conversation = the thread (`dm`), message id =
  the request id, sender = host-authenticated user or the socket `client_id`;
- the host sets `sender_id` after authenticating its session; client JSON cannot
  supply it. The host also sets `command_authorized`; anonymous submissions are
  never command-authorized. Every request requires a nonempty `client_id`,
  `thread_id` and `request_id`. For anonymous requests, `client_id` is also the
  sender identity;
- `parts` are ordered (typed text first, then attachments); `media` is the
  compatibility projection;
- a voice note with no typed text contributes its **completed** transcript as
  `text`;
- the queue disposition, `speak_reply` and `source` ride in `raw`;
- addressing gaps, an empty submission, or a non-media attachment fail with
  `BadFormat`.

`WebChannelAdapter::submit` rejects input until `start` has been called.

## Outbound

`WebTurnProjector` maps `ChannelOutputEvent` to `WebChannelEvent`, stamping
`client_id`/`thread_id`/`request_id` and a strictly increasing per-turn `seq`
plus `ts`. Events with no web form (`Native`, unknown artifact operations or
lifecycle stages, subagent usage records) project to nothing. Subagent
`state` values: `spawned`, `completed`, `failed`, `awaiting_user`,
`iteration_start`, `tool_call`, `tool_result`, `text_delta`, `thinking_delta`.
Subagent output and arguments are capped at `MAX_WEB_TOOL_PAYLOAD_BYTES`.

`WebChannelAdapter::publish_output` projects and publishes through the host's
`WebEventSink`, keyed by a `WebScope` (tenant, profile, agent) that never
reaches the wire. A terminal event (`chat_done`, `chat_error`,
`chat_cancelled`) ends the turn's `seq` counter. Host-only events (queue,
goals, todos, run mode) go through `publish_event` unchanged.

`ChannelAdapter::send` publishes a `proactive_message` (client `system`),
which is how scheduled and relayed deliveries reach an open web surface.

Socket.IO and SSE transports stay in `openhuman-rpc`; they implement
`WebEventSink`.
