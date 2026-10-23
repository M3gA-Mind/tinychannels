#![allow(deprecated)]

use super::*;
use crate::channel::ChannelAdapter;
use tinychannels_bus::SendMessage;

fn make_channel() -> WhatsAppChannel {
    WhatsAppChannel::new(
        "test-token".into(),
        "123456789".into(),
        "verify-me".into(),
        vec!["+1234567890".into()],
    )
}

#[test]
fn whatsapp_channel_name() {
    let ch = make_channel();
    assert_eq!(ch.descriptor().id, "whatsapp");
}

#[test]
fn whatsapp_verify_token() {
    let ch = make_channel();
    assert_eq!(ch.verify_token(), "verify-me");
}

#[test]
fn whatsapp_number_allowed_exact() {
    let ch = make_channel();
    assert!(ch.is_number_allowed("+1234567890"));
    assert!(!ch.is_number_allowed("+9876543210"));
}

#[test]
fn whatsapp_number_allowed_wildcard() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    assert!(ch.is_number_allowed("+1234567890"));
    assert!(ch.is_number_allowed("+9999999999"));
}

#[test]
fn whatsapp_number_denied_empty() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec![]);
    assert!(!ch.is_number_allowed("+1234567890"));
}

#[test]
fn whatsapp_parse_empty_payload() {
    let ch = make_channel();
    let payload = serde_json::json!({});
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty());
}

#[test]
fn whatsapp_parse_valid_text_message() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "object": "whatsapp_business_account",
        "entry": [{
            "id": "123",
            "changes": [{
                "value": {
                    "messaging_product": "whatsapp",
                    "metadata": {
                        "display_phone_number": "15551234567",
                        "phone_number_id": "123456789"
                    },
                    "messages": [{
                        "from": "1234567890",
                        "id": "wamid.xxx",
                        "timestamp": "1699999999",
                        "type": "text",
                        "text": {
                            "body": "Hello TinyChannels!"
                        }
                    }]
                },
                "field": "messages"
            }]
        }]
    });

    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].sender, "+1234567890");
    assert_eq!(msgs[0].content, "Hello TinyChannels!");
    assert_eq!(msgs[0].channel, "whatsapp");
    assert_eq!(msgs[0].timestamp, 1_699_999_999);
}

#[test]
fn whatsapp_parse_unauthorized_number() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "object": "whatsapp_business_account",
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "9999999999",
                        "timestamp": "1699999999",
                        "type": "text",
                        "text": { "body": "Spam" }
                    }]
                }
            }]
        }]
    });

    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty(), "Unauthorized numbers should be filtered");
}

#[test]
fn whatsapp_parse_non_text_message_skipped() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "1234567890",
                        "timestamp": "1699999999",
                        "type": "image",
                        "image": { "id": "img123" }
                    }]
                }
            }]
        }]
    });

    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty(), "Non-text messages should be skipped");
}

#[test]
fn whatsapp_parse_multiple_messages() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [
                        { "from": "111", "timestamp": "1", "type": "text", "text": { "body": "First" } },
                        { "from": "222", "timestamp": "2", "type": "text", "text": { "body": "Second" } }
                    ]
                }
            }]
        }]
    });

    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].content, "First");
    assert_eq!(msgs[1].content, "Second");
}

#[test]
fn whatsapp_parse_normalizes_phone_with_plus() {
    let ch = WhatsAppChannel::new(
        "tok".into(),
        "123".into(),
        "ver".into(),
        vec!["+1234567890".into()],
    );
    // API sends without +, but we normalize to +
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "1234567890",
                        "timestamp": "1",
                        "type": "text",
                        "text": { "body": "Hi" }
                    }]
                }
            }]
        }]
    });

    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].sender, "+1234567890");
}

#[test]
fn whatsapp_empty_text_skipped() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "111",
                        "timestamp": "1",
                        "type": "text",
                        "text": { "body": "" }
                    }]
                }
            }]
        }]
    });

    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty());
}

// ══════════════════════════════════════════════════════════
// Edge cases: comprehensive coverage.
// ══════════════════════════════════════════════════════════

#[test]
fn whatsapp_parse_missing_entry_array() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "object": "whatsapp_business_account"
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty());
}

#[test]
fn whatsapp_parse_entry_not_array() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "entry": "not_an_array"
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty());
}

#[test]
fn whatsapp_parse_missing_changes_array() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "entry": [{ "id": "123" }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty());
}

#[test]
fn whatsapp_parse_changes_not_array() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "entry": [{
            "changes": "not_an_array"
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty());
}

#[test]
fn whatsapp_parse_missing_value() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{ "field": "messages" }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty());
}

#[test]
fn whatsapp_parse_missing_messages_array() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "metadata": {}
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty());
}

#[test]
fn whatsapp_parse_messages_not_array() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": "not_an_array"
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty());
}

#[test]
fn whatsapp_parse_missing_from_field() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "timestamp": "1",
                        "type": "text",
                        "text": { "body": "No sender" }
                    }]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty(), "Messages without 'from' should be skipped");
}

#[test]
fn whatsapp_parse_missing_text_body() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "111",
                        "timestamp": "1",
                        "type": "text",
                        "text": {}
                    }]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(
        msgs.is_empty(),
        "Messages with empty text object should be skipped"
    );
}

#[test]
fn whatsapp_parse_null_text_body() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "111",
                        "timestamp": "1",
                        "type": "text",
                        "text": { "body": null }
                    }]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty(), "Messages with null body should be skipped");
}

#[test]
fn whatsapp_parse_invalid_timestamp_uses_current() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "111",
                        "timestamp": "not_a_number",
                        "type": "text",
                        "text": { "body": "Hello" }
                    }]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 1);
    // Timestamp should be current time (non-zero)
    assert!(msgs[0].timestamp > 0);
}

#[test]
fn whatsapp_parse_missing_timestamp_uses_current() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "111",
                        "type": "text",
                        "text": { "body": "Hello" }
                    }]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 1);
    assert!(msgs[0].timestamp > 0);
}

#[test]
fn whatsapp_parse_multiple_entries() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [
            {
                "changes": [{
                    "value": {
                        "messages": [{
                            "from": "111",
                            "timestamp": "1",
                            "type": "text",
                            "text": { "body": "Entry 1" }
                        }]
                    }
                }]
            },
            {
                "changes": [{
                    "value": {
                        "messages": [{
                            "from": "222",
                            "timestamp": "2",
                            "type": "text",
                            "text": { "body": "Entry 2" }
                        }]
                    }
                }]
            }
        ]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].content, "Entry 1");
    assert_eq!(msgs[1].content, "Entry 2");
}

#[test]
fn whatsapp_parse_multiple_changes() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [
                {
                    "value": {
                        "messages": [{
                            "from": "111",
                            "timestamp": "1",
                            "type": "text",
                            "text": { "body": "Change 1" }
                        }]
                    }
                },
                {
                    "value": {
                        "messages": [{
                            "from": "222",
                            "timestamp": "2",
                            "type": "text",
                            "text": { "body": "Change 2" }
                        }]
                    }
                }
            ]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].content, "Change 1");
    assert_eq!(msgs[1].content, "Change 2");
}

#[test]
fn whatsapp_parse_status_update_ignored() {
    // Status updates have "statuses" instead of "messages"
    let ch = make_channel();
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "statuses": [{
                        "id": "wamid.xxx",
                        "status": "delivered",
                        "timestamp": "1699999999"
                    }]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty(), "Status updates should be ignored");
}

#[test]
fn whatsapp_parse_non_text_media_message_types_are_skipped() {
    // Every non-text message type hits the same `type != "text" -> continue`
    // branch in parse_webhook_payload. Table-driven, one representative case
    // per media type (collapsed from 7 byte-identical tests, plan.md §2.1).
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let cases = [
        (
            "audio",
            serde_json::json!({ "id": "audio123", "mime_type": "audio/ogg" }),
        ),
        ("video", serde_json::json!({ "id": "video123" })),
        (
            "document",
            serde_json::json!({ "id": "doc123", "filename": "file.pdf" }),
        ),
        ("sticker", serde_json::json!({ "id": "sticker123" })),
        (
            "location",
            serde_json::json!({ "latitude": 40.7128, "longitude": -74.0060 }),
        ),
        (
            "contacts",
            serde_json::json!([{ "name": { "formatted_name": "John" } }]),
        ),
        (
            "reaction",
            serde_json::json!({ "message_id": "wamid.xxx", "emoji": "\u{1F44D}" }),
        ),
    ];
    for (kind, sub) in cases {
        let mut message = serde_json::json!({
            "from": "111",
            "timestamp": "1",
            "type": kind,
        });
        message[kind] = sub;
        let payload = serde_json::json!({
            "entry": [{ "changes": [{ "value": { "messages": [message] } }] }]
        });
        let msgs = ch.parse_webhook_payload(&payload);
        assert!(msgs.is_empty(), "{kind} message must be skipped");
    }
}

#[test]
fn whatsapp_parse_mixed_authorized_unauthorized() {
    let ch = WhatsAppChannel::new(
        "tok".into(),
        "123".into(),
        "ver".into(),
        vec!["+1111111111".into()],
    );
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [
                        { "from": "1111111111", "timestamp": "1", "type": "text", "text": { "body": "Allowed" } },
                        { "from": "9999999999", "timestamp": "2", "type": "text", "text": { "body": "Blocked" } },
                        { "from": "1111111111", "timestamp": "3", "type": "text", "text": { "body": "Also allowed" } }
                    ]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].content, "Allowed");
    assert_eq!(msgs[1].content, "Also allowed");
}

#[test]
fn whatsapp_parse_unicode_message() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "111",
                        "timestamp": "1",
                        "type": "text",
                        "text": { "body": "Hello 👋 世界 🌍 مرحبا" }
                    }]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].content, "Hello 👋 世界 🌍 مرحبا");
}

#[test]
fn whatsapp_parse_very_long_message() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let long_text = "A".repeat(10_000);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "111",
                        "timestamp": "1",
                        "type": "text",
                        "text": { "body": long_text }
                    }]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].content.len(), 10_000);
}

#[test]
fn whatsapp_parse_whitespace_only_message_skipped() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "111",
                        "timestamp": "1",
                        "type": "text",
                        "text": { "body": "   " }
                    }]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    // Whitespace-only is NOT empty, so it passes through
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].content, "   ");
}

#[test]
fn whatsapp_number_allowed_multiple_numbers() {
    let ch = WhatsAppChannel::new(
        "tok".into(),
        "123".into(),
        "ver".into(),
        vec![
            "+1111111111".into(),
            "+2222222222".into(),
            "+3333333333".into(),
        ],
    );
    assert!(ch.is_number_allowed("+1111111111"));
    assert!(ch.is_number_allowed("+2222222222"));
    assert!(ch.is_number_allowed("+3333333333"));
    assert!(!ch.is_number_allowed("+4444444444"));
}

#[test]
fn whatsapp_number_allowed_case_sensitive() {
    // Phone numbers should be exact match
    let ch = WhatsAppChannel::new(
        "tok".into(),
        "123".into(),
        "ver".into(),
        vec!["+1234567890".into()],
    );
    assert!(ch.is_number_allowed("+1234567890"));
    // Different number should not match
    assert!(!ch.is_number_allowed("+1234567891"));
}

#[test]
fn whatsapp_parse_phone_already_has_plus() {
    let ch = WhatsAppChannel::new(
        "tok".into(),
        "123".into(),
        "ver".into(),
        vec!["+1234567890".into()],
    );
    // If API sends with +, we should still handle it
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "+1234567890",
                        "timestamp": "1",
                        "type": "text",
                        "text": { "body": "Hi" }
                    }]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].sender, "+1234567890");
}

#[test]
fn whatsapp_channel_fields_stored_correctly() {
    let ch = WhatsAppChannel::new(
        "my-access-token".into(),
        "phone-id-123".into(),
        "my-verify-token".into(),
        vec!["+111".into(), "+222".into()],
    );
    assert_eq!(ch.verify_token(), "my-verify-token");
    assert!(ch.is_number_allowed("+111"));
    assert!(ch.is_number_allowed("+222"));
    assert!(!ch.is_number_allowed("+333"));
}

#[test]
fn whatsapp_parse_empty_messages_array() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": []
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty());
}

#[test]
fn whatsapp_parse_empty_entry_array() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "entry": []
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty());
}

#[test]
fn whatsapp_parse_empty_changes_array() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "entry": [{
            "changes": []
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert!(msgs.is_empty());
}

#[test]
fn whatsapp_parse_newlines_preserved() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "111",
                        "timestamp": "1",
                        "type": "text",
                        "text": { "body": "Line 1\nLine 2\nLine 3" }
                    }]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].content, "Line 1\nLine 2\nLine 3");
}

#[test]
fn whatsapp_parse_special_characters() {
    let ch = WhatsAppChannel::new("tok".into(), "123".into(), "ver".into(), vec!["*".into()]);
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "messages": [{
                        "from": "111",
                        "timestamp": "1",
                        "type": "text",
                        "text": { "body": "<script>alert('xss')</script> & \"quotes\" 'apostrophe'" }
                    }]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 1);
    assert_eq!(
        msgs[0].content,
        "<script>alert('xss')</script> & \"quotes\" 'apostrophe'"
    );
}

#[test]
fn whatsapp_parse_takes_the_senders_profile_name() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "entry": [{
            "changes": [{
                "value": {
                    "contacts": [
                        { "wa_id": "9999999999", "profile": { "name": "Someone Else" } },
                        { "wa_id": "1234567890", "profile": { "name": " Alice " } }
                    ],
                    "messages": [{
                        "from": "1234567890",
                        "timestamp": "1700000000",
                        "text": { "body": "hi" }
                    }]
                }
            }]
        }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].sender, "+1234567890");
    assert_eq!(msgs[0].sender_name.as_deref(), Some("Alice"));
}

#[test]
fn whatsapp_parse_without_contacts_has_no_name() {
    let ch = make_channel();
    let payload = serde_json::json!({
        "entry": [{ "changes": [{ "value": { "messages": [{
            "from": "1234567890", "timestamp": "1700000000", "text": { "body": "hi" }
        }] } }] }]
    });
    let msgs = ch.parse_webhook_payload(&payload);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].sender_name, None);
}

#[test]
fn sensitive_cloud_requests_require_https() {
    assert!(ensure_https("https://graph.facebook.com/v18.0/123").is_ok());
    for url in ["http://graph.facebook.com", "file:///token", ""] {
        assert!(
            ensure_https(url)
                .unwrap_err()
                .to_string()
                .contains("non-HTTPS")
        );
    }
}

#[tokio::test]
async fn injected_cloud_client_reports_connection_failure_and_webhook_start_is_immediate() {
    // An invalid proxy fails locally, before contacting Meta.
    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::all("http://127.0.0.1:0").unwrap())
        .timeout(std::time::Duration::from_secs(1))
        .build()
        .unwrap();
    let channel = WhatsAppChannel::with_http_client(
        "token".into(),
        "123".into(),
        "verify".into(),
        vec![],
        client,
    );
    assert_eq!(channel.verify_token(), "verify");
    assert!(
        crate::provider_test_support::send_v1(&channel, &SendMessage::new("hello", "+15551234567"))
            .await
            .is_err()
    );
    assert!(!crate::provider_test_support::is_healthy(&channel).await);
    let sink = crate::provider_test_support::RecordingInboundSink::default();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        crate::channel::ChannelAdapter::start(&channel, &sink),
    )
    .await
    .expect("webhook adapters do not own a polling listener")
    .unwrap();
    assert!(sink.events().is_empty());
}

// ---------------------------------------------------------------------------
// v2 channel behaviour (Cloud API over a mock Graph server)
// ---------------------------------------------------------------------------

mod v2 {
    use super::*;
    use crate::channel::{ChannelDelete as _, ChannelEdit as _, ChannelStreamingDraft as _};
    use crate::providers::whatsapp::api::GRAPH_VERSION;
    use crate::providers::whatsapp_shared::fakes::{CollectingSink, MemoryMedia, MemoryVoices};
    use hmac::{Hmac, KeyInit, Mac};
    use serde_json::json;
    use sha2::Sha256;
    use tinychannels_bus::channel::{ChannelRef, ConversationRef};
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn channel(server: &MockServer) -> WhatsAppChannel {
        WhatsAppChannel::new(
            "tok".into(),
            "123".into(),
            "verify-me".into(),
            vec!["*".into()],
        )
        .with_api_base_for_test(&server.uri())
        .with_app_secret(Some("secret".into()))
    }

    async fn accept_messages(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path(format!("/{GRAPH_VERSION}/123/messages")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"messages": [{"id": "wamid.ok"}]})),
            )
            .mount(server)
            .await;
    }

    fn conversation() -> ConversationRef {
        ConversationRef {
            id: "+15550001111".into(),
            ..Default::default()
        }
    }

    #[test]
    fn capabilities_follow_the_wired_ports() {
        let bare = WhatsAppChannel::new("t".into(), "1".into(), "v".into(), vec![]);
        let facts = bare.declared_capabilities();
        assert!(facts.reactions && facts.addressing.reply_to_id && !facts.addressing.thread_id);
        assert!(!facts.content.image.outbound && !facts.voice_out.supported);
        assert!(crate::provider_test_support::supports_reactions(&bare));
        assert!(crate::provider_test_support::supports_reply_to_id(&bare));
        assert!(crate::provider_test_support::supports_remote_control(&bare));
        assert!(crate::provider_test_support::supports_chat_approvals(&bare));
        assert!(crate::provider_test_support::history_key_ignores_thread(
            &bare
        ));
        assert!(!crate::provider_test_support::supports_edit(&bare));
        assert!(!crate::provider_test_support::supports_draft_updates(&bare));

        let wired = WhatsAppChannel::new("t".into(), "1".into(), "v".into(), vec![])
            .with_media_repository(Arc::new(MemoryMedia::default()))
            .with_voice_media_store(Arc::new(MemoryVoices::default()))
            .with_voice_synthesis(true);
        let facts = wired.declared_capabilities();
        assert!(facts.content.image.outbound && facts.content.sticker.outbound);
        assert!(facts.voice_in.supported && facts.voice_out.synthesis);
        assert!(
            !facts.voice_out.native_waveform,
            "the Cloud API has no waveform field"
        );
        // Synthesis needs the voice store; without it the flag alone is not enough.
        let synth_only = WhatsAppChannel::new("t".into(), "1".into(), "v".into(), vec![])
            .with_voice_synthesis(true);
        assert!(!synth_only.declared_capabilities().voice_out.synthesis);
        assert_eq!(wired.declared_capabilities(), wired.declared_capabilities());
    }

    #[test]
    fn the_adapter_describes_itself_as_the_native_cloud_backend() {
        let channel = WhatsAppChannel::new("t".into(), "1".into(), "v".into(), vec![]);
        let descriptor = channel.descriptor();
        assert_eq!(descriptor.id, "whatsapp");
        assert_eq!(
            descriptor.metadata.get("adapterMode").map(String::as_str),
            Some("native-v2")
        );
        assert_eq!(
            descriptor.metadata.get("backend").map(String::as_str),
            Some("cloud")
        );
    }

    #[tokio::test]
    async fn the_legacy_text_send_is_a_projection_of_the_typed_send() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                json!({"to": "15551234567", "type": "text", "text": {"body": "hi"}}),
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"messages": [{"id": "wamid.1"}]})),
            )
            .expect(1)
            .mount(&server)
            .await;
        crate::provider_test_support::send_v1(
            &channel(&server),
            &SendMessage::new("hi", "+15551234567"),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn health_follows_the_graph_api_answer() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/{GRAPH_VERSION}/123")))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let healthy = channel(&server);
        assert!(crate::provider_test_support::is_healthy(&healthy).await);
        assert_eq!(
            tinychannels_runtime::channel::ChannelAdapter::status(&healthy)
                .await
                .unwrap()
                .health_state
                .as_deref(),
            Some("healthy"),
            "adapter status must report the actual Graph API result"
        );
        server.reset().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let unhealthy = channel(&server);
        assert!(!crate::provider_test_support::is_healthy(&unhealthy).await);
        assert_eq!(
            tinychannels_runtime::channel::ChannelAdapter::status(&unhealthy)
                .await
                .unwrap()
                .health_state
                .as_deref(),
            Some("unhealthy")
        );
        let insecure = WhatsAppChannel::new("t".into(), "1".into(), "v".into(), vec![])
            .with_api_base_for_test("http://graph.example.invalid");
        assert!(!crate::provider_test_support::is_healthy(&insecure).await);
    }

    #[tokio::test]
    async fn typing_is_anchored_to_the_latest_inbound_message() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({
                "status": "read",
                "message_id": "wamid.anchor",
                "typing_indicator": {"type": "text"}
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"success": true})))
            .expect(1)
            .mount(&server)
            .await;
        let channel = channel(&server);
        // No inbound message yet: nothing to anchor the indicator to.
        let error = crate::channel::ChannelTyping::start_typing(&channel, "+15550001111")
            .await
            .unwrap_err();
        assert_eq!(error.kind, crate::channel::SendErrorKind::Forbidden);
        assert!(matches!(
            crate::channel::ChannelTyping::start_typing(&channel, "+15550001111").await,
            Err(error) if !error.unsupported && !error.sent_before_error
        ));
        channel
            .anchors
            .lock()
            .insert("+15550001111", "wamid.anchor");
        crate::channel::ChannelTyping::start_typing(&channel, "+15550001111")
            .await
            .unwrap();
        crate::channel::ChannelTyping::stop_typing(&channel, "+15550001111")
            .await
            .unwrap();
        assert!(
            crate::channel::ChannelTyping::start_typing(&channel, "no digits")
                .await
                .is_err()
        );
        crate::channel::ChannelTyping::stop_typing(&channel, "+15550001111")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn reactions_send_the_emoji_and_removal_sends_an_empty_one() {
        let server = MockServer::start().await;
        accept_messages(&server).await;
        let channel = channel(&server);
        let thumbs = tinychannels_bus::ChannelReaction {
            value: "👍".into(),
            custom_emoji_id: None,
        };
        crate::channel::LegacyChannelReaction::send_reaction(
            &channel,
            &ChannelRef {
                id: "whatsapp".into(),
                account_id: None,
            },
            &conversation(),
            "wamid.t",
            &thumbs,
            false,
        )
        .await
        .unwrap();
        crate::channel::LegacyChannelReaction::send_reaction(
            &channel,
            &ChannelRef {
                id: "whatsapp".into(),
                account_id: None,
            },
            &conversation(),
            "wamid.t",
            &thumbs,
            true,
        )
        .await
        .unwrap();
        let bodies: Vec<serde_json::Value> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|request| serde_json::from_slice(&request.body).unwrap())
            .collect();
        assert_eq!(bodies[0]["reaction"]["emoji"], "👍");
        assert_eq!(bodies[1]["reaction"]["emoji"], "");
        assert_eq!(bodies[1]["reaction"]["message_id"], "wamid.t");

        let custom = tinychannels_bus::ChannelReaction {
            value: "x".into(),
            custom_emoji_id: Some("c".into()),
        };
        let blank = tinychannels_bus::ChannelReaction::default();
        for (reaction, unsupported) in [(&custom, true), (&blank, false)] {
            let error = crate::channel::LegacyChannelReaction::send_reaction(
                &channel,
                &ChannelRef {
                    id: "whatsapp".into(),
                    account_id: None,
                },
                &conversation(),
                "wamid.t",
                reaction,
                false,
            )
            .await
            .unwrap_err();
            assert_eq!(error.unsupported, unsupported);
        }
        assert!(
            crate::channel::LegacyChannelReaction::send_reaction(
                &channel,
                &ChannelRef {
                    id: "whatsapp".into(),
                    account_id: None
                },
                &ConversationRef {
                    id: "name".into(),
                    ..Default::default()
                },
                "wamid.t",
                &thumbs,
                false
            )
            .await
            .is_err()
        );
        // The adapter port checks the channel identity first.
        let other = ChannelRef {
            id: "telegram".into(),
            account_id: None,
        };
        let mine = ChannelRef {
            id: "whatsapp".into(),
            account_id: None,
        };
        assert!(
            crate::channel::LegacyChannelReaction::send_reaction(
                &channel,
                &other,
                &conversation(),
                "w",
                &thumbs,
                false
            )
            .await
            .is_err()
        );
        crate::channel::LegacyChannelReaction::send_reaction(
            &channel,
            &mine,
            &conversation(),
            "w",
            &thumbs,
            false,
        )
        .await
        .unwrap();
        let bad = crate::channel::LegacyChannelReaction::send_reaction(
            &channel,
            &mine,
            &conversation(),
            "w",
            &custom,
            false,
        )
        .await
        .unwrap_err();
        assert!(bad.unsupported);
    }

    #[tokio::test]
    async fn mark_read_posts_a_read_status_without_a_typing_indicator() {
        let server = MockServer::start().await;
        accept_messages(&server).await;
        let channel = channel(&server);
        crate::channel::ChannelAdapter::mark_read(&channel, &conversation(), "wamid.in")
            .await
            .unwrap();
        let body: serde_json::Value =
            serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
        assert_eq!(body["status"], "read");
        assert!(body.get("typing_indicator").is_none());
        assert!(
            crate::channel::ChannelAdapter::mark_read(&channel, &conversation(), " ")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn edit_delete_and_streaming_drafts_are_explicitly_unsupported() {
        let channel = WhatsAppChannel::new("t".into(), "1".into(), "v".into(), vec![]);
        let receipt = tinychannels_bus::channel::MessageReceipt::default();
        let intent = tinychannels_bus::ChannelOutboundIntent::default();
        assert!(
            channel
                .edit(&receipt, intent.clone())
                .await
                .unwrap_err()
                .unsupported
        );
        assert!(channel.delete(&receipt).await.unwrap_err().unsupported);
        assert!(
            channel
                .upsert_draft(None, intent)
                .await
                .unwrap_err()
                .unsupported
        );
        assert!(channel.start(&NoSink).await.is_ok());
        assert!(channel.stop().await.is_ok());
        let status = channel.status().await.unwrap();
        assert_eq!(status.channel_id, "whatsapp");
    }

    struct NoSink;

    #[async_trait::async_trait]
    impl crate::channel::ChannelInboundSink for NoSink {
        async fn push(
            &self,
            envelope: tinychannels_bus::ChannelInboundEnvelope,
        ) -> Result<(), crate::channel::ChannelSendError> {
            self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                .await
        }

        async fn push_ingress(
            &self,
            _ingress: tinychannels_bus::ChannelIngress,
        ) -> Result<(), crate::channel::ChannelSendError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn typed_sends_return_receipts_and_failures_are_structured() {
        let server = MockServer::start().await;
        accept_messages(&server).await;
        let channel = channel(&server);
        let intent = tinychannels_bus::ChannelOutboundIntent {
            idempotency_key: "k".into(),
            channel_id: "whatsapp".into(),
            conversation_id: "+15550001111".into(),
            payload: tinychannels_bus::OutboundPayload::Text { text: "hi".into() },
            ..Default::default()
        };
        let outcome = crate::channel::ChannelAdapter::send_outcome(&channel, intent.clone())
            .await
            .unwrap();
        assert_eq!(outcome.receipt.platform_message_ids, ["wamid.ok"]);
        let message = tinychannels_bus::ChannelOutboundMessage::from_intent(&intent);
        let typed = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
            .await
            .unwrap();
        assert_eq!(typed.receipt.platform_message_ids, ["wamid.ok"]);
        crate::channel::ChannelTypedSend::send_typed(&channel, &message)
            .await
            .unwrap();
        let receipt = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
            .await
            .unwrap();
        assert!(!receipt.receipt.platform_message_ids.is_empty());

        let mut threaded = intent;
        threaded.thread_id = Some("t".into());
        let error = crate::channel::ChannelAdapter::send_outcome(&channel, threaded.clone())
            .await
            .unwrap_err();
        assert!(error.unsupported);
        let typed_error = crate::channel::ChannelTypedSend::send_typed(
            &channel,
            &tinychannels_bus::ChannelOutboundMessage::from_intent(&threaded),
        )
        .await
        .unwrap_err();
        assert!(typed_error.unsupported);
        assert!(
            crate::channel::ChannelTypedSend::send_typed(
                &channel,
                &tinychannels_bus::ChannelOutboundMessage::from_intent(&threaded)
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn webhooks_are_authenticated_and_refusals_are_marked_as_client_errors() {
        let sink = Arc::new(CollectingSink::new());
        let channel = WhatsAppChannel::new(
            "t".into(),
            "123".into(),
            "verify-me".into(),
            vec!["*".into()],
        )
        .with_app_secret(Some("secret".into()))
        .with_events(sink.clone());
        let body = serde_json::to_vec(&json!({
            "object": "whatsapp_business_account",
            "entry": [{"changes": [{"field": "messages", "value": {
                "metadata": {"phone_number_id": "123"},
                "messages": [{"from": "15550001111", "id": "wamid.1", "timestamp": "1", "type": "text", "text": {"body": "hi"}}]
            }}]}]
        }))
        .unwrap();
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(&body);
        let signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
        let outcome =
            crate::channel::ChannelAdapter::deliver_webhook(&channel, Some(&signature), &body)
                .await
                .unwrap();
        assert_eq!(outcome.messages, 1);
        let refused =
            crate::channel::ChannelAdapter::deliver_webhook(&channel, Some("sha256=00"), &body)
                .await
                .unwrap_err();
        assert!(
            refused
                .downcast_ref::<tinychannels_runtime::WebhookRejected>()
                .is_some()
        );
        // A host failure after authentication is not a client error.
        let failing = WhatsAppChannel::new("t".into(), "123".into(), "v".into(), vec!["*".into()])
            .with_app_secret(Some("secret".into()))
            .with_events(Arc::new(CollectingSink {
                fail: true,
                ..CollectingSink::new()
            }));
        let error =
            crate::channel::ChannelAdapter::deliver_webhook(&failing, Some(&signature), &body)
                .await
                .unwrap_err();
        assert!(
            error
                .downcast_ref::<tinychannels_runtime::WebhookRejected>()
                .is_none()
        );
        assert_eq!(
            crate::channel::ChannelAdapter::verify_webhook(
                &channel,
                "subscribe",
                "verify-me",
                "c1"
            )
            .as_deref(),
            Some("c1")
        );
        assert_eq!(
            crate::channel::ChannelAdapter::verify_webhook(&channel, "subscribe", "nope", "c1"),
            None
        );
    }

    #[test]
    fn failures_that_are_not_ours_are_uncertain() {
        let foreign = anyhow::anyhow!("socket reset");
        assert!(matches!(failure_of(&foreign), WaFailure::Uncertain(_)));
        let ours = anyhow::Error::new(WaFailure::rejected("no"));
        assert!(matches!(failure_of(&ours), WaFailure::Rejected(_)));
    }

    #[tokio::test]
    async fn webhook_adapter_start_does_not_claim_a_stream_listener() {
        let channel = WhatsAppChannel::new("t".into(), "1".into(), "v".into(), vec![]);
        let sink = crate::provider_test_support::RecordingInboundSink::default();
        tokio::time::timeout(std::time::Duration::from_secs(1), channel.start(&sink))
            .await
            .expect("webhook adapter start is nonblocking")
            .unwrap();
        channel.stop().await.unwrap();
        assert!(sink.events().is_empty());
    }
}
