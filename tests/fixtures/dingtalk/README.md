# DingTalk AMR test fixture

`amrnb_mode0.amr` is the 331-byte, 500 ms mono AMR-NB mode-0 storage fixture
from TinyVoice commit
[`321014b01a5d45fc75703e0a800d9c7f3354cc89`](https://github.com/tinyhumansai/tinyvoice/tree/321014b01a5d45fc75703e0a800d9c7f3354cc89/crates/tinyvoice/tests/fixtures/amr).
TinyVoice attributes the fixture to `rvoip-codec-core` source commit
[`f4d528eaabbac935bce9a6404d3ce4fdec35ed0f`](https://github.com/eisenzopf/rvoip/tree/f4d528eaabbac935bce9a6404d3ce4fdec35ed0f)
and includes the 3GPP TS 26.073 reference decoder PCM output for it. The
fixture is used here only to verify exact upload-byte forwarding. Its MIT
notice is included in [`AMR-FIXTURE-LICENSE-MIT.txt`](AMR-FIXTURE-LICENSE-MIT.txt).
