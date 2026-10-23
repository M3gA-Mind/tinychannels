# Providers

`src/providers/` contains portable provider modules. The public exports are
centralized from `src/lib.rs` and `src/providers/mod.rs`.

## Exported Providers

| Provider | Export |
|----------|--------|
| DingTalk | `DingTalkChannel` |
| Discord | `DiscordChannel` |
| Email | `EmailChannel` |
| iMessage | `IMessageChannel` |
| IRC | `IrcChannel`, `IrcChannelConfig` |
| Lark | `LarkChannel` |
| Linq | `LinqChannel`, `verify_linq_signature` |
| Mattermost | `MattermostChannel` |
| QQ | `QQChannel` |
| Signal | `SignalChannel` |
| Slack | `SlackChannel` |
| Telegram | `TelegramChannel` |
| WhatsApp | `WhatsAppChannel` |
| WhatsApp Web | `WhatsAppWebChannel` |
| Yuanbao | `YuanbaoChannel` |

## Provider and host boundary

Configured providers run inside the TinyChannels module as adapter-backed
`BuiltChannel` instances. OpenHuman sends typed bus operations to the configured
module instead of constructing provider objects. Provider-specific events are
normalized into `ChannelInboundEnvelope` with ordered typed content; outbound
sends preserve typed parts and return a structured receipt. The host supplies
scoped capabilities such as media, speech, event publication, and storage
without taking ownership of provider transport or delivery policy. Legacy
`Channel` and `ChannelBackend` APIs remain compatibility surfaces and are not
the module's production factory path.

## Conformance matrix

The shared `tinychannels-conformance` crate defines 24 canonical cases.
Provider test suites run the applicable cases against mock transports and
check declared support against observed requests and receipts. This table is a
test-coverage index, not a claim that each listed suite runs every case or that
all providers support every feature.

| Provider | Conformance suite |
| --- | --- |
| Discord | `providers/discord/conformance_tests.rs` |
| Email | `providers/email_channel/conformance_tests.rs` |
| iMessage | `providers/imessage/conformance_tests.rs` |
| IRC | `providers/irc_listen_tests.rs::shared_conformance_checks_irc_text_against_a_real_peer_without_fabricated_receipts` |
| Linq | `providers/linq/conformance_tests.rs` |
| Mattermost, QQ, DingTalk | `providers/long_tail_conformance_tests.rs` |
| Signal | `providers/signal/conformance_tests.rs` |
| Slack | `providers/slack/adapter_tests.rs` |
| Telegram | `providers/telegram/channel_ops_tests.rs::configured_telegram_adapter_passes_mock_wire_conformance` |
| Lark | `providers/lark/legacy_conformance_tests.rs` |
| WhatsApp Cloud and Web | `providers/whatsapp*/conformance_tests.rs` |
| Yuanbao | `providers/yuanbao/conformance_tests.rs` |

The matrix names the operations being tested. The configured provider
descriptor and mock-wire assertion in each suite are authoritative for its
supported subset. Some providers have broad conformance coverage; others have
focused suites or exercise only the cases their transport supports. Controller-
mediated operations are checked separately from the shared cases.

## Declared capability overview

These are descriptor declarations, not a promise that every configured account
has every capability. Provider constructors derive the descriptor from the
services and protocol options actually wired for that account. `conditional`
means the capability depends on host services, provider options, or protocol
support; consult the provider's capability implementation for accepted MIME
types, limits, and exact events. The runtime descriptor is authoritative.

| Provider | Text | Typed media | Voice | Addressing and interaction highlights |
| --- | --- | --- | --- | --- |
| DingTalk | In/out | Image and file when media storage is wired | Audio/voice when voice services are wired | Reply and provider events |
| Discord | In/out | Conditional image, video, audio, and file | Conditional | Reply/thread, edit/delete, reactions, streaming drafts |
| Email | In/out | Conditional image, video, audio, and file | Conditional | Reply/thread and send/reply actions |
| iMessage | In/out | Conditional image, video, audio, and file | Conditional | Reply addressing, reactions, read receipts |
| IRC | In/out | Not declared | Not declared | Text send only |
| Lark | In/out | Provider and media-service dependent | Conditional | Provider-specific reply/thread behavior |
| Linq | In/out | Conditional image, video, audio, and file | Conditional | Send and typing actions |
| Mattermost | In/out | Conditional outbound image, video, audio, and file | Not declared | Reply/thread, typing, reactions, edit/delete |
| QQ | In/out | Conditional image, video, and file; inbound support depends on ingress services | Voice output conditional | Provider-specific events and reply behavior |
| Signal | In/out | Conditional image, video, audio, and file | Conditional | Reply addressing, reactions, read receipts |
| Slack | In/out | Conditional image, video, audio, and file | Conditional | Reply/thread, edit/delete, reactions, streaming drafts |
| Telegram | In/out | Conditional image, video, audio, file, and sticker | Conditional | Reply/thread, reactions, edit, streaming drafts; location/contact/buttons/polls are also declared |
| WhatsApp Cloud | In/out | Conditional image, video, audio, file, and sticker | Conditional | Reply, reactions, read/delivery receipts; edit/delete are not declared |
| WhatsApp Web | In/out | Conditional image, video, audio, file, and sticker | Conditional | Reply, reactions, read/delivery receipts; edit/delete are declared by this protocol adapter |
| Yuanbao | In/out | Image and file when media storage is wired | Voice input conditional | Provider-specific text actions |

The shared long-tail text descriptor starts with bidirectional text and send.
Each long-tail provider extends it only for behavior its adapter implements.
The WhatsApp capability builder separately advertises typed media, voice,
events, and edit/delete from its wired ports and configuration. These rules
explain the conditional cells; they do not replace the runtime descriptor.

## Controller Registry

The setup UI registry currently exposes definitions for:

- Telegram
- Discord
- Web
- iMessage
- Lark
- DingTalk
- Email
- Yuanbao

That registry is intentionally smaller than the config/provider set. Some
config-backed providers remain hidden until their setup flows are promoted into
`ChannelDefinition` metadata.

## Provider Implementation Checklist

When adding or porting a provider:

1. Add provider configuration to `src/config.rs` if needed.
2. Add or update a provider module under `src/providers/`.
3. Export the provider from `src/providers/mod.rs` and `src/lib.rs` only if it
   is part of the public API.
4. Normalize inbound platform events into `ChannelInboundEnvelope`.
5. Build outbound sends with typed bus operations and preserve idempotency keys.
6. Return `MessageReceipt` where the provider can identify platform message ids.
7. Advertise honest capabilities.
8. Add setup metadata to `src/controllers/definitions.rs` only when the UI flow
   is ready.
9. Add focused tests for serialization, routing, delivery, and provider edge
   cases.

## Rich Provider Rule

If a provider needs harness turns, speech, approvals, history, events, shutdown,
or telemetry, use `ProviderContext::host`. Do not import OpenHuman application
modules into TinyChannels provider code.
