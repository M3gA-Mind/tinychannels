use super::*;

#[test]
fn request_overrides_preserve_the_assembled_turn() {
    let turn = ChannelTurn {
        id: "turn".into(),
        ..Default::default()
    };
    let request = DispatchRequest::new(turn.clone());
    assert_eq!(request.turn, turn);
    assert!(request.options.stream_partial);
    assert!(request.options.allow_tools);
    let options = DispatchOptions {
        stream_partial: false,
        allow_tools: false,
        model_override: Some("provider:model".into()),
        locale: Some("en".into()),
        timeout_secs: Some(15),
    };
    let request = request.with_options(options.clone());
    assert_eq!(request.turn, turn);
    assert_eq!(request.options, options);
}

#[tokio::test]
async fn handle_streams_events_in_order_and_closes_after_completion() {
    let (sender, receiver) = tokio::sync::mpsc::channel(3);
    let first = ChannelOutputEvent::TextDelta {
        text: "partial".into(),
        iteration: None,
    };
    let final_event = ChannelOutputEvent::FinalMessage {
        text: "final".into(),
        parts: vec![],
        receipt: None,
    };
    sender.send(first.clone()).await.unwrap();
    sender.send(final_event.clone()).await.unwrap();
    drop(sender);
    let mut handle = TurnHandle::new("run", receiver, Arc::new(|| {}));
    assert_eq!(handle.run_id, "run");
    assert_eq!(handle.next().await, Some(first));
    assert_eq!(handle.collect().await, vec![final_event]);
}

#[tokio::test]
async fn cancelling_a_handle_invokes_the_host_callback() {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let handle = TurnHandle::new(
        "run",
        receiver,
        Arc::new(move || {
            let _ = sender.try_send(ChannelOutputEvent::Cancellation {
                reason: Some("cancelled".into()),
            });
        }),
    );
    handle.cancel();
    let mut handle = handle;
    assert_eq!(
        handle.next().await,
        Some(ChannelOutputEvent::Cancellation {
            reason: Some("cancelled".into())
        })
    );
}
