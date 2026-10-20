# Roadmap

> **Superseded:** the channel contract and migration direction are defined in
> [docs/spec/contract-v2.md](docs/spec/contract-v2.md). This initial scaffold
> roadmap is historical and is not the current implementation roadmap.

TinyChannels starts as a blank scaffold. The roadmap favors small, well-tested
modules that build toward a production-grade channel communication layer for
OpenHuman.

## Foundation

- define transport-neutral channel message envelopes
- define harness-facing communication contracts
- define channel lifecycle and status events
- add adapter boundaries for OpenHuman channel surfaces
- add observability hooks for message flow and failures

## Stability

The public API is pre-1.0. Breaking changes are expected while the OpenHuman
integration boundary is being shaped.
