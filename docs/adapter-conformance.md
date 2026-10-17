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

The legacy `start_channel` service currently selects providers by their bare
provider name and uses that name as the configured instance identity; it does
not create separate account aliases for multiple accounts of one provider.
At the bridge boundary, a missing `ChannelRef.account_id` is filled from the
running instance, while an explicit account must match that instance. Tests
that construct a `Running` actor with an explicit instance exercise the
adapter contract but do not imply that the legacy service can start aliased
accounts.

## Current automated coverage

The reusable `tinychannels-conformance` crate defines a 24-case feature matrix
and checks declared capability behavior against wire observations. Its own
tests reject authentication failures mislabeled as unsupported, missing or
mis-scoped receipts, wrong payloads, mismatched response IDs, partial effects
reported as unsupported, and voice ingress without both audio and transcript
evidence. Non-message operations can be observed without manufacturing a
message receipt; streaming drafts must report create, update, finalize, and
cancel-cleanup operations.

The module tests exercise account mismatch rejection before dispatch, partial
receipt retry safety, raw voice-synthesis capability gating, and a real
configured Running adapter that sends through Telegram's Bot API against a
wiremock server. The conformance kit observes the actual `sendMessage` request
and the fixture's configured response ID, then verifies the configured
account and conversation scope. The same configured Telegram adapter rejects
undeclared image content before any platform request. CI runs these tests and
the conformance-kit self-tests through `scripts/test-adapter-conformance.sh`.

The shared media foundation also tests a connected Telegram inbound photo
flow: a wiremock `getFile` response and authenticated file download pass through
MIME/body validation into the host-scoped blob store, then appear as an ordered
typed image with scope-bound readback. This proves the shared inbound path; it
does not claim Telegram native outbound image delivery or conformance for other
providers. Native upload request/receipt evidence remains required from each
provider port before final issue acceptance.

| Behavior | Foundation status | Final provider coverage |
| --- | --- | --- |
| Text | Running factory and Telegram Bot API mock-wire request/receipt tested | Pending full provider port coverage |
| Image, video, audio, files, stickers, locations, contacts, buttons, polls | Capability-gated at adapter dispatch | Pending per-provider wire assertions |
| Voice in and out | Raw synthesis is rejected unless configured synthesis is declared | Pending native adapter fixtures and ingress lifecycle coverage |
| Reply and thread addressing | Full refs preserved and mismatches rejected | Pending per-provider wire assertions |
| Typing, reactions, edit, delete | Not exposed by the bridge | Pending native adapter traits |
| Read receipts | Not exposed by the bridge | Pending provider ports |
| Streaming draft lifecycle | Not exposed by the bridge | Pending native adapter traits |
| Capability honesty | Kit validates unsupported outcomes and real configured Telegram text/image cases | Pending observed wire behavior for every declared feature |

Provider leaves #106–#111 must add mock-platform fixtures that assert the
actual request and receipt for each declared operation. The final matrix and
legacy trait removal remain open until those fixtures pass for every provider.
