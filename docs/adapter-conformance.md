# Channel adapter conformance

The adapter contract is being adopted in stages. The module's
`ConfiguredAdapterFactory` currently returns an explicitly `LegacyBacked`
handle around the already-running provider actor. This preserves listener
ordering and routes both durable and best-effort typed sends through the same
adapter dispatch seam. The handle binds the configured account identity,
validates supplied channel and conversation addresses, and preserves the full
typed send outcome. Rich content is sent only when the configured provider's
capability facts advertise that content kind.

This bridge is not a provider conformance claim. Its lifecycle is owned by the
module runtime, and it does not expose optional typing, reaction, edit, delete,
or streaming-draft traits. Capability facts continue to come from the
configured provider and are marked `adapterMode=legacy-backed` on the
descriptor.

## Current automated coverage

The `configured_adapter_*` tests exercise account mismatch rejection before
dispatch and verify that a partial send retains its primary receipt and cannot
be retried. `scripts/test-adapter-conformance.sh` runs these tests in the
feature-test CI lane. Telegram's provider tests separately exercise its
configured Bot API implementation against mocked HTTP responses; they do not
yet instantiate a native `ChannelAdapter`.

| Behavior | Foundation status | Final provider coverage |
| --- | --- | --- |
| Text | Existing typed provider tests; bridge scope and receipt behavior tested | Pending provider ports |
| Image, video, audio, files, stickers, locations, contacts, buttons, polls | Capability-gated at adapter dispatch | Pending per-provider wire assertions |
| Voice in and out | Existing shared voice contract tests | Pending native adapter fixtures |
| Reply and thread addressing | Full refs preserved and mismatches rejected | Pending per-provider wire assertions |
| Typing, reactions, edit, delete | Not exposed by the bridge | Pending native adapter traits |
| Read receipts | Not exposed by the bridge | Pending provider ports |
| Streaming draft lifecycle | Not exposed by the bridge | Pending native adapter traits |
| Capability honesty | Unsupported rich content is rejected before dispatch | Pending observed wire behavior per declared feature |

Provider leaves #106–#110 must add mock-platform fixtures that assert the
actual request and receipt for each declared operation. The final matrix and
legacy trait removal remain open until those fixtures pass for every provider.
