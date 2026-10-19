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
- The documented message-resource API route is
  `GET /open-apis/im/v1/messages/:message_id/resources/:file_key` (with the
  tenant token). The docs site currently serves “This document is not found”
  for the guessed `message-resource/get` article path, so this note records
  the route without linking that nonexistent page. The provider must verify
  the event's `message_id` and resource key before download.

The official upload docs specify a 10 MiB image limit and 30 MiB file limit.
The file upload's required `file_type` accepts `opus` for OPUS audio and says
other audio must be converted to OPUS; `duration` is optional and is measured
in milliseconds. Unknown document types can use `stream`. The message-content
reference gives `msg_type=audio` content as `{"file_key":"..."}`. This means
the adapter can truthfully send native audio when it has OPUS bytes; other
voice codecs require the shared host codec port before they can use this
native type. Audio sent as a generic file remains a file message.

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

## QQ Official Bot

The QQ Bot platform documents a native rich-media path rather than a text URL
fallback:

- [Rich-media overview](https://bot.q.qq.com/wiki/develop/api-v2/server-inter/message/rich-media.html)
  says image, video, voice, and file messages use msg_type=7 and a media.file_info
  returned by an upload request. It lists image formats, MP4 video, voice formats
  (Silk/MP3/WAV/OGG), and arbitrary files.
- [Direct-message file upload](https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_users_user_openid_files.post.html)
  documents file_type 1=image, 2=video, 3=voice, 4=file; file_data is base64
  binary data and direct upload is limited to 5 MiB. The published soft limits are
  20 MiB images, 30 MiB videos, 20 MiB voice and 200 MiB files; larger local
  payloads require the documented pre-upload/chunk/finish sequence.
- [Direct-message send](https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_users_user_openid_messages.post.html)
  accepts msg_type=7 with media.file_info. The matching group upload/send
  endpoints are /v2/groups/{group_openid}/files and /v2/groups/{group_openid}/messages;
  uploaded media is isolated between DM and group scopes.

This adapter can safely support bounded native uploads without implementing
chunk transfer: it will enforce a 5 MiB maximum before upload, classify the
actual native file type, upload with srv_send_msg=false, then send the returned
opaque file_info in the separate message request. The declared capability
limit must remain 5 MiB until the full chunk-upload protocol is implemented.

Tencent's own [Node.js bot SDK](https://github.com/tencent-connect/qqbot-nodejs/blob/main/USAGE.md)
documents inbound `attachments` with `content_type`, `url`, and optional
filename on C2C/group event objects, and the official
[botpy C2C file example](https://github.com/tencent-connect/botpy/blob/master/examples/demo_c2c_reply_file.py)
shows native upload followed by a msg_type=7 response. This confirms that the
existing QQ listener's text-only projection drops supported inbound
attachments; it is an implementation gap, not a protocol limitation.

The message timestamp is RFC3339 text, not Unix milliseconds. Tencent's
[official Go SDK message model](https://github.com/tencent-connect/botgo/blob/fe31c0dfe469001e0f783d2f07e7de7bd08b403f/dto/message.go#L15-L20)
types `timestamp` as its `Timestamp` string, and its
[Timestamp parser](https://github.com/tencent-connect/botgo/blob/fe31c0dfe469001e0f783d2f07e7de7bd08b403f/dto/timestamp.go)
uses Go's `time.RFC3339` layout. The typed ingress should parse that source
format into Unix milliseconds instead of silently dropping the timestamp.

## DingTalk App Bot

DingTalk's official [bot overview](https://opensource.dingtalk.com/developerpedia/docs/learn/bot/overview/)
distinguishes App Bots from custom webhook bots: App Bots can call OpenAPI,
including media upload, while custom webhook bots cannot. The official
[App Bot reply guide](https://opensource.dingtalk.com/developerpedia/docs/learn/bot/appbot/reply/)
says session webhooks support text and Markdown, while OpenAPI supports media
uploads and richer message forms. The current adapter is an App Bot Stream Mode
client (`client_id` / `client_secret`) but retains only each incoming message's
session webhook, so it does not yet have enough target state for the native
OpenAPI media-send path.

The official [received-message type reference](https://opensource.dingtalk.com/developerpedia/docs/learn/bot/message/)
lists image, video, voice, file, and rich-text events. The official
[Stream FAQ](https://opensource.dingtalk.com/developerpedia/docs/learn/stream/faq/)
confirms rich image callbacks can carry `pictureDownloadCode` and `downloadCode`;
file and video event examples carry download codes too. This establishes that
typed inbound media is a supported platform capability, not something to
infer from the current text-only listener.

The DingTalk API reference mirrors document the robot media templates and
upload operation: [message templates](https://dingtalk.apifox.cn/doc-3550110)
lists `sampleFile`, `sampleVideo`, and `sampleAudio`, and
[media upload](https://dingtalk.apifox.cn/api-9093351) describes
`POST https://oapi.dingtalk.com/media/upload` returning a reusable `media_id`.
The newer robot send API is
`POST https://api.dingtalk.com/v1.0/robot/groupMessages/send` with
`robotCode`, `openConversationId`, `msgKey`, and JSON-string `msgParam`; the
private-chat endpoint is documented separately. These endpoints require an
App Bot's robot code and an access token. The current adapter receives
`robotCode` in callbacks but does not retain it, so adding this path requires
account-scoped retention and tests for both group and direct-message targets.

There is a material platform/API compatibility caveat: Hermes Agent
[issue #30144](https://github.com/NousResearch/hermes-agent/issues/30144)
reports `sampleFile` returning `robot not found` for internal App Bots on both
new send endpoints. Treat this as a reported limitation, not a confirmed
universal restriction; verify the send shape against the supported bot type
before declaring file delivery. The docs also distinguish native image sends
from attachments: `sampleImageMsg` takes a `photoURL`, while uploaded media IDs
are consumed by file/video/audio templates. Do not turn an uploaded private
image into a Markdown URL or advertise it as inline image delivery.
