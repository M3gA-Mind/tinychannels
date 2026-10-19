use super::*;

#[test]
fn file_mime_types_map_to_lark_upload_types() {
    for (mime, file_type) in [
        ("application/pdf", "pdf"),
        ("application/msword", "doc"),
        (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "doc",
        ),
        ("application/vnd.ms-excel", "xls"),
        (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "xls",
        ),
        ("application/vnd.ms-powerpoint", "ppt"),
        (
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "ppt",
        ),
        ("application/octet-stream", "stream"),
    ] {
        assert_eq!(file_type_for_mime(mime), file_type, "{mime}");
    }
}

#[test]
fn ogg_opus_detection_rejects_invalid_headers_and_truncated_segments() {
    let valid = include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg");
    assert!(is_ogg_opus(valid));
    assert!(!is_ogg_opus(b""));
    assert!(!is_ogg_opus(b"OggS"));

    let mut wrong_version = valid.to_vec();
    wrong_version[4] = 1;
    assert!(!is_ogg_opus(&wrong_version));

    let mut truncated_lacing = valid[..27].to_vec();
    truncated_lacing[26] = 1;
    assert!(!is_ogg_opus(&truncated_lacing));

    let mut wrong_packet = valid.to_vec();
    wrong_packet[28..36].copy_from_slice(b"vorbis\0\0");
    assert!(!is_ogg_opus(&wrong_packet));
}

#[test]
fn lark_send_errors_preserve_delivery_certainty() {
    let unsent = known_unsent("upload rejected");
    assert_eq!(
        unsent.kind,
        tinychannels_bus::channel::SendErrorKind::Transient
    );
    assert!(unsent.retryable);
    assert!(!unsent.sent_before_error);

    let uncertain = uncertain_send("message response lost");
    assert_eq!(
        uncertain.kind,
        tinychannels_bus::channel::SendErrorKind::Unknown
    );
    assert!(uncertain.sent_before_error);
    assert!(!uncertain.retryable);
}

#[test]
fn lark_rejects_threaded_outbound_targets_before_delivery() {
    let channel = super::super::LarkChannel::new(
        "app-id".into(),
        "app-secret".into(),
        "verify-token".into(),
        None,
        vec!["*".into()],
    );
    let target = Target {
        channel: ChannelRef {
            id: "lark".into(),
            account_id: None,
        },
        conversation: ConversationRef {
            id: "chat-1".into(),
            ..Default::default()
        },
        reply_to: None,
        thread: Some("thread-1".into()),
    };

    let error = NativeProvider::validate_target(&channel, &target).unwrap_err();

    assert!(error.unsupported);
    assert!(!error.sent_before_error);
}
