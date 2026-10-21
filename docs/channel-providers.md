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
| WhatsApp | `WhatsAppChannel` |
| Yuanbao | `YuanbaoChannel` |

## Provider and host boundary

Providers run inside the TinyChannels module. OpenHuman sends typed bus
operations to the configured module instead of constructing provider objects.
Provider-specific events are normalized into `ChannelInboundEnvelope` with
ordered typed content; outbound sends preserve typed parts and return a
structured receipt. The host supplies scoped capabilities such as media reads,
speech, and event publication without taking ownership of provider transport.

## Conformance matrix

The shared `tinychannels-conformance` crate defines the 24 canonical cases.
Provider test suites run the matrix against mock transports and check declared
support against observed requests and receipts. This table identifies the
current full-matrix suites; it is a test-coverage index, not a claim that all
providers support every feature.

| Provider | Conformance suite |
| --- | --- |
| Discord | `providers/discord/conformance_tests.rs` |
| Email | `providers/email_channel/conformance_tests.rs` |
| iMessage | `providers/imessage/conformance_tests.rs` |
| Linq | `providers/linq/conformance_tests.rs` |
| Mattermost, QQ, DingTalk | `providers/long_tail_conformance_tests.rs` |
| Signal | `providers/signal/conformance_tests.rs` |
| Slack | `providers/slack/adapter_tests.rs` |
| Lark | `providers/lark_conformance_tests.rs` |
| WhatsApp Cloud and Web | `providers/whatsapp*/conformance_tests.rs` |
| Yuanbao | `providers/yuanbao/conformance_tests.rs` |

The matrix names the operations being tested. The provider descriptor and
mock-wire assertion in each suite are authoritative for its supported subset.

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
