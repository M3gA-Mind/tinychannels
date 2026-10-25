# Issue 22 media protocol research

This note records protocol evidence gathered during the original issue 22
research. Provider implementation statements below are a historical baseline,
not a description of current support. Native media was implemented afterward;
the status table links to the current code and tests. Keep native voice
messages distinct from ordinary audio-file attachments, and base capability
claims on the type the wire protocol actually sends.

## Current implementation status

| Provider | Current native media path | Implementation evidence |
| --- | --- | --- |
| Lark / Feishu | Typed inbound files and voice notes; outbound images, files, video, audio, and voice, subject to provider limits, configured stores, and codecs. | [`lark/native.rs`](../src/providers/lark/native.rs), [`lark/api_tests.rs`](../src/providers/lark/api_tests.rs) |
| QQ Official Bot | Typed image, video, and file ingress and bounded native media sends; native voice output when a host voice store is configured. Direct uploads are limited to 5 MiB; larger uploads need the chunk protocol. | [`qq/native.rs`](../src/providers/qq/native.rs), [`qq/native_tests.rs`](../src/providers/qq/native_tests.rs) |
| DingTalk App Bot | Typed media and voice ingress plus native file and voice sends, subject to configured stores/codecs and provider limits. | [`dingtalk/native.rs`](../src/providers/dingtalk/native.rs), [`dingtalk/native_tests.rs`](../src/providers/dingtalk/native_tests.rs), [`dingtalk/dingtalk_tests.rs`](../src/providers/dingtalk/dingtalk_tests.rs) |
| Yuanbao | URL-backed inbound `TIMSoundElem` becomes a typed voice note when the host voice store is configured. Native voice output remains unsupported. | [`yuanbao/inbound.rs`](../src/providers/yuanbao/inbound.rs), [`yuanbao/channel_tests.rs`](../src/providers/yuanbao/channel_tests.rs), [`yuanbao/native_tests.rs`](../src/providers/yuanbao/native_tests.rs) |

These links describe the implementation at the TinyChannels revision recorded
by the OpenHuman epic. They do not promise support for every media type; consult
the provider's declared capabilities and limits.

## Yuanbao / Tencent IM

The Yuanbao adapter uses the BizMsg protobuf protocol, not Tencent IM's
documented REST message API. Its local codec defines `TIMSoundElem` as
`MsgContent.sound` (protobuf field 7; see `src/providers/yuanbao/types.rs`,
`proto_constants.rs`, and `proto.rs`). Inbound decoding recognizes that
element as `MessageKind::Voice` (`src/providers/yuanbao/inbound.rs`). At the
time of this research, the text projection dropped sound-only messages before
dispatch; current code can ingest a URL-backed sound as a typed voice note
when the host voice store is configured (see the current implementation links
above).

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
repository or the cited schema: the encoder lacks verified
`Download_Flag`/duration semantics for a sound payload, so native voice output
remains disabled. The generic sender encodes non-image files as `TIMFileElem`
(see `src/providers/yuanbao/outbound.rs::prepare_media_body_with_caption`).
That file path is distinct from a native voice message; consult current
declared capabilities before claiming audio-file delivery.

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
API base (`open.feishu.cn` or `open.larksuite.com`). The text-only adapter
described in the original research was the pre-media baseline. The current
provider implements typed image, file, audio, video, and voice paths, including
resource download, scoped host storage, and configured voice codec handling;
the provider tests linked above cover requests and typed envelopes. Platform
constraints still apply: native audio requires compatible OPUS input, and
captions or thread/reply addressing are unsupported on some media paths.

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

The current adapter supports bounded native uploads without implementing
chunk transfer: it enforces a 5 MiB maximum before upload, classifies the
native file type, uploads with `srv_send_msg=false`, then sends the returned
opaque `file_info` in the separate message request. The declared capability
limit remains 5 MiB until the full chunk-upload protocol is implemented.

Tencent's own [Node.js bot SDK](https://github.com/tencent-connect/qqbot-nodejs/blob/main/USAGE.md)
documents inbound `attachments` with `content_type`, `url`, and optional
filename on C2C/group event objects, and the official
[botpy C2C file example](https://github.com/tencent-connect/botpy/blob/master/examples/demo_c2c_reply_file.py)
shows native upload followed by a msg_type=7 response. The original research
identified that the QQ listener dropped supported inbound attachments. The
current native path ingests typed image, video, and file attachments; tests
linked above cover bounded uploads and inbound storage.

The message timestamp is RFC3339 text, not Unix milliseconds. Tencent's
[official Go SDK message model](https://github.com/tencent-connect/botgo/blob/fe31c0dfe469001e0f783d2f07e7de7bd08b403f/dto/message.go#L15-L20)
types `timestamp` as its `Timestamp` string, and its
[Timestamp parser](https://github.com/tencent-connect/botgo/blob/fe31c0dfe469001e0f783d2f07e7de7bd08b403f/dto/timestamp.go)
uses Go's `time.RFC3339` layout. The current typed ingress parses that source
format into Unix milliseconds; `providers/qq/native_tests.rs` covers RFC3339,
integer, and invalid timestamp values.

## DingTalk App Bot

DingTalk's official [bot overview](https://opensource.dingtalk.com/developerpedia/docs/learn/bot/overview/)
distinguishes App Bots from custom webhook bots: App Bots can call OpenAPI,
including media upload, while custom webhook bots cannot. The official
[App Bot reply guide](https://opensource.dingtalk.com/developerpedia/docs/learn/bot/appbot/reply/)
says session webhooks support text and Markdown, while OpenAPI supports media
uploads and richer message forms. The original research found that the App Bot
Stream Mode adapter retained only each incoming message's session webhook.
The current native provider path implements typed inbound media and voice
handling and App Bot file/voice sends; see the current implementation and test
links above for its supported scope.

The official [received-message type reference](https://opensource.dingtalk.com/developerpedia/docs/learn/bot/message/)
lists image, video, voice, file, and rich-text events. The official
[Stream FAQ](https://opensource.dingtalk.com/developerpedia/docs/learn/stream/faq/)
confirms rich image callbacks can carry `pictureDownloadCode` and `downloadCode`;
file and video event examples carry download codes too. This establishes that
typed inbound media is a supported platform capability, not something to
infer from the current text-only listener.

The official [robot message type reference](https://open.dingtalk.com/document/orgapp/types-of-messages-sent-by-robots)
defines `sampleAudio` with a `mediaId` and `duration`; it explicitly supports
OGG and AMR, and specifies milliseconds for audio duration. The official
[upload media API](https://open.dingtalk.com/document/orgapp/upload-media-files)
returns the reusable `media_id`. Its voice-upload contract uses
`POST https://oapi.dingtalk.com/media/upload` with `type=voice`, accepts AMR,
MP3, and WAV, and limits uploaded voice to 2 MiB. This upload limit does not
establish an inbound-download limit; callback downloads retain the adapter's
existing 10 MiB bound and codec support. The newer robot send API is
`POST https://api.dingtalk.com/v1.0/robot/groupMessages/send` with
`robotCode`, `openConversationId`, `msgKey`, and JSON-string `msgParam`; the
private-chat endpoint is documented separately. These endpoints require an
App Bot's robot code and an access token. Current account-scoped send behavior
is covered by the native provider tests linked above. Keep the
platform-specific caveats below: support depends on bot type, configured
callback scope, and provider response.

There is a material platform/API compatibility caveat: Hermes Agent
[issue #30144](https://github.com/NousResearch/hermes-agent/issues/30144)
reports `sampleFile` returning `robot not found` for internal App Bots on both
new send endpoints. Treat this as a reported limitation, not a confirmed
universal restriction; verify the send shape against the supported bot type
before declaring file delivery. The docs also distinguish native image sends
from attachments: `sampleImageMsg` takes a `photoURL`, while uploaded media IDs
are consumed by file/video/audio templates. Do not turn an uploaded private
image into a Markdown URL or advertise it as inline image delivery.
