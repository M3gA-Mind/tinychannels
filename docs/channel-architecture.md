# Architecture

TinyChannels owns provider I/O and normalization; OpenHuman owns application
policy and execution. Hosts exchange typed requests, events, and results with
the loaded module through `tinychannels-bus`, rather than linking provider
implementations.

## Layered Model

```text
                         +----------------------------+
                         | OpenHuman application      |
                         | config, auth, harness, UI  |
                         +--------------+-------------+
                                        |
                                        v
                         +----------------------------+
                         | Host policy and execution  |
                         | auth, sessions, agent turns|
                         +--------------+-------------+
                                        |
                                        v
+----------------+       +----------------------------+       +----------------+
| Controller UI  |------>| tinychannels-bus           |------>| Channel defs   |
| setup/status   |       | typed request/result DTOs   |       | auth fields    |
+----------------+       +--------------+-------------+       +----------------+
                                        |
                                        v
                         +----------------------------+
                         | TinyChannels module        |
                         | provider runtime + relay   |
                         +--------------+-------------+
                                        |
                    +-------------------+-------------------+
                    |                                       |
                    v                                       v
        +------------------------+              +------------------------+
        | Provider transports    |              | Relay transport        |
        | normalize/send/listen  |              | frames + reconnect     |
        +-----------+------------+              +-----------+------------+
                    |                                       |
                    v                                       v
        +------------------------+              +------------------------+
        | Channel contracts      |              | Relay action contract  |
        | envelope, intent,      |              | send, passthrough,     |
        | receipt, capabilities  |              | inbound ack            |
        +-----------+------------+              +-----------+------------+
                    |                                       |
                    +-------------------+-------------------+
                                        |
                                        v
                         +----------------------------+
                         | Delivery queue             |
                         | durability + retry policy  |
                         +----------------------------+
```

## Boundary Rules

TinyChannels owns provider behavior and portable contracts:

- what a channel message looks like before and after normalization
- what a provider can advertise as capabilities
- what a UI setup definition needs to render auth forms
- what a relay connector sends over the socket
- provider normalization, capability declarations, and send behavior

OpenHuman owns host-specific behavior:

- user accounts and credential storage
- REST/JWT/session authorization
- model and harness execution
- durable conversation history, sessions, and run-ledger storage
- event-bus fanout and process lifecycle management

The host depends on the bus contract and loaded module. Module operations call
only the host capabilities exposed through that boundary; OpenHuman does not
provide a substitute implementation when an operation is missing.

## Main Data Flow

Inbound:

```text
platform event
  -> TinyChannels module normalizes ChannelInboundEnvelope
  -> typed inbound preparation over TinyBus
  -> OpenHuman applies identity, authorization, and session policy
  -> agent harness turn
  -> canonical output event stream
  -> TinyChannels web-output projection when a web surface is configured
```

Outbound:

```text
agent output or controller send
  -> typed outbound request over TinyBus
  -> module/provider send or relay action
  -> typed receipt and delivery result
  -> host-owned durable queue where required
```

Setup and control:

```text
UI action
  -> OpenHuman controller and policy validation
  -> typed channel controller operation over TinyBus
  -> configured provider/module operation
  -> typed connection/status/test result
```

## Compatibility Strategy

Compatibility DTOs remain at selected migration boundaries. New host
integration should use bus DTOs and operations, preserve typed content and
address fields, and keep authenticated relay provenance outside serialized
message data. Provider normalization prepares identity inputs; OpenHuman selects
and persists the durable conversation session.
