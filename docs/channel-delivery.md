# Delivery

`src/delivery/` owns durable outbound delivery state and retry policy.

## Goals

Durable delivery exists to make outbound sends safer around crashes, platform
timeouts, reconnects, and unknown post-send outcomes. It does not promise every
platform has the same guarantees; instead it negotiates requested durability
against adapter-advertised capabilities.

## Durability Negotiation

`DeliveryDurability` has three modes:

- `Required`
- `BestEffort`
- `Disabled`

`required_durable_final_capabilities` derives required capability flags from a
`ChannelOutboundIntent`. For example:

- text payloads require text support
- media, voice, and files require media support
- polls require poll support
- native or presentation payloads require payload support
- replies require reply support
- threads require thread support

`negotiate_delivery_durability` downgrades to best effort when required
capabilities are missing.

## Queue State Machine

The queue module provides operations for:

- enqueueing pending delivery entries before platform I/O
- marking platform send start
- acknowledging successful delivery
- recording failed attempts
- marking unknown outcome after a platform send may have completed
- moving exhausted entries to failed storage
- loading and draining pending deliveries
- recovering pending deliveries in enqueue order

`DeliveryQueueStore` owns persistence. `DeliveryQueueHandler` owns the actual
send and unknown-send reconciliation behavior.

## Retry Policy

Backoff follows an OpenClaw-compatible sequence:

```text
0 ms -> 5 s -> 25 s -> 120 s -> 600 s
```

Permanent delivery errors are detected from known platform failure patterns such
as not-found, blocked, kicked, empty chat id, invalid recipient, ambiguous
recipient, or missing outbound configuration.

Entries stop retrying after `MAX_RETRIES`.

## Unknown Send Reconciliation

If the process crashes or loses transport state after platform I/O starts, the
delivery entry can be marked as:

- `SendAttemptStarted`
- `UnknownAfterSend`

Recovery asks the handler to reconcile the unknown send. A handler can report:

- sent - ack and remove the pending entry
- not sent - retry when safe
- unresolved - keep or fail according to retry state

This is the key difference between pre-send failure and post-send uncertainty:
post-send uncertainty must avoid accidental duplicate delivery when the platform
may already have accepted the message.
