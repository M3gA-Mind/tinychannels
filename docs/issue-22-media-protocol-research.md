# Issue 22 media protocol research

This note records protocol evidence for the native media work in issue 22.
It distinguishes a provider's native voice-message type from sending an audio
file as an ordinary attachment. Capability claims should follow the type the
wire protocol actually sends.

## Yuanbao / Tencent IM

The Yuanbao adapter uses the BizMsg protobuf protocol, not Tencent IM's
documented REST message API. Its local codec defines `TIMSoundElem` as
`MsgContent.sound` (protobuf field 7; see `src/providers/yuanbao/types.rs`,
`proto_constants.rs`, and `proto.rs`). Inbound decoding already recognizes
that element as `MessageKind::Voice` (`src/providers/yuanbao/inbound.rs`), but
the current text projection drops sound-only messages before dispatch.

Tencent's [Message Formats reference](https://intl.cloud.tencent.com/pt/document/product/647/74355)
documents `TIMSoundElem` fields `Url`, `UUID`, `Size`, `Second`, and
`Download_Flag`. It says the downloadable URL is available in SDK v4+, while
SDK v2/v3 only provide UUID, size, and duration. The Yuanbao custom BizMsg
schema currently has a `sound` string, and generic URL/file-size fields, but
does not model `Download_Flag` or duration. Therefore a sound element can be
downloaded only when its actual payload contains a validated URL; UUID-only
payloads cannot be turned into bytes using the modeled protocol. This is an
inbound transport constraint, not evidence that Yuanbao has no voice element.

For an independent implementation reference, hermes-agent commit
[`548091836760a67e331b8f5643be8e5c08fae1e6`](https://github.com/NousResearch/hermes-agent/commit/548091836760a67e331b8f5643be8e5c08fae1e6)
has `gateway/platforms/yuanbao.py` and `gateway/platforms/yuanbao_proto.py`.
The protocol schema in the latter models `MsgContent.sound` (field 7), `url`
(field 10), `file_size` (field 11), and `file_name` (field 12). Its adapter
classifies sound as voice, while its media-reference resolver handles image
and file kinds and does not resolve voice. This corroborates the distinction
between recognition and retrievable audio bytes.

No verified native outbound `TIMSoundElem` encoder is present in this
repository or the cited schema: the current encoder lacks verified
`Download_Flag`/duration semantics for a sound payload. Keep native voice
message capability disabled until a captured or documented BizMsg encoding
establishes the required fields. The current generic sender already encodes
every non-image file as `TIMFileElem` (see
`src/providers/yuanbao/outbound.rs::prepare_media_body_with_caption`), so an
audio MIME could use ordinary file transport. It still needs dedicated
planner/capability and wire-receipt tests before advertising audio attachment
support, and must not be described as a native voice bubble.

## Lark / Feishu

Lark/Feishu explicitly supports native audio and other media message types:

- [Message content description](https://open.feishu.cn/document/server-docs/im-v1/message-content-description/create_json)
  defines `msg_type=image` with `image_key`, `msg_type=audio` with `file_key`,
  `msg_type=media` with `file_key` and optional `image_key`, and
  `msg_type=file` with `file_key`.
- [Upload image](https://open.feishu.cn/document/server-docs/im-v1/image/create)
  creates image keys; [upload file](https://open.feishu.cn/document/server-docs/im-v1/file/create)
  creates file keys, including audio/video/documents.
- [Download message resources](https://open.feishu.cn/document/server-docs/im-v1/message/get)
  provides the message-resource retrieval surface for received media. The
  resource URL is `/im/v1/messages/:message_id/resources/:file_key`.

The same API surface is used for Feishu and Lark with the configured regional
API base (`open.feishu.cn` or `open.larksuite.com`). The current adapter only
parses text/post and sends `msg_type=text` (`src/providers/lark.rs`), so its
missing media support is an implementation gap, not a platform limitation.
Implementation should use the image upload endpoint for image parts, file
upload for file and audio parts (including the required `file_type` and
duration metadata where the API requires it), then send the corresponding
message `msg_type` and returned key. Inbound message type/content parsing and
resource download should preserve the provider message id and chat/thread
scope, enforce the existing user allowlist, and ingest bytes through the
shared host media/voice ports with size and MIME bounds.

Audio sent as `msg_type=audio` is a native audio message on this API. Whether
Lark's event payload marks a received audio message as a voice note versus a
generic audio attachment must be determined from the concrete event resource
metadata; do not infer voice-note semantics from MIME alone.
