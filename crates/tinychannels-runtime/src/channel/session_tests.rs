use super::*;

#[test]
fn inbound_thread_key_preserves_provider_compatibility_and_escapes_collisions() {
    assert_eq!(
        derive_inbound_thread_id("telegram", Some("u1"), Some("c1"), Some("topic")),
        "channel:telegram/u1/c1"
    );
    assert_eq!(
        derive_inbound_thread_id("slack", Some("u1"), Some("c1"), Some("thread/1")),
        "channel:slack/u1/c1#thread:thread%2F1"
    );
    assert_eq!(
        derive_inbound_thread_id("discord", Some("  "), Some("#room"), None),
        "channel:discord/%23room"
    );
    assert_ne!(
        derive_inbound_thread_id("a/b", Some("c"), None, None),
        derive_inbound_thread_id("a", Some("b/c"), None, None)
    );
}

fn envelope() -> ChannelInboundEnvelope {
    ChannelInboundEnvelope {
        channel: ChannelRef {
            id: "telegram".into(),
            account_id: Some("telegram:bot-a".into()),
        },
        conversation: ConversationRef {
            kind: ConversationKind::Group,
            id: "-100".into(),
            ..Default::default()
        },
        sender: SenderRef {
            id: "42".into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn key(namespace: &str, envelope: &ChannelInboundEnvelope, policy: SessionKeyPolicy) -> String {
    build_session_key_for_inbound_envelope(namespace, envelope, policy)
}

#[test]
fn default_group_policy_keeps_topic_distinct_from_same_sender_id() {
    let unthreaded = envelope();
    let mut topic_named_for_sender = unthreaded.clone();
    topic_named_for_sender.conversation.topic_id = Some("42".into());

    let policy = SessionKeyPolicy::default();
    assert_ne!(
        key("main", &unthreaded, policy),
        key("main", &topic_named_for_sender, policy),
        "a topic id must not collide with the group participant suffix"
    );
}

#[test]
fn canonical_v22_key_uses_tagged_fields_and_utf8_byte_lengths() {
    let mut envelope = envelope();
    envelope.channel.account_id = Some("telegram:bot-a".into());
    envelope.conversation.topic_id = Some("42".into());

    assert_eq!(
        key("租户", &envelope, SessionKeyPolicy::default()),
        "v22|namespace=p6:租户|channel=p8:telegram|account=p14:telegram:bot-a|kind=p5:group|scope=a|conversation=p4:-100|parent=a|topic=p2:42|thread=a|participant=a"
    );

    let mut absent_account = envelope.clone();
    absent_account.channel.account_id = None;
    let mut empty_account = envelope;
    empty_account.channel.account_id = Some(String::new());
    assert_ne!(
        key("main", &absent_account, SessionKeyPolicy::default()),
        key("main", &empty_account, SessionKeyPolicy::default()),
        "an absent account must differ from a present empty account"
    );
}

#[test]
fn identity_preserves_parent_topic_thread_scope_and_presence() {
    let policy = SessionKeyPolicy {
        group_sessions_per_user: false,
        thread_sessions_per_user: false,
    };
    let base = envelope();
    let base_key = key("main", &base, policy);

    let mut parent = base.clone();
    parent.conversation.parent_id = Some("parent-1".into());
    assert_ne!(
        base_key,
        key("main", &parent, policy),
        "parent id is identity"
    );

    let mut topic = base.clone();
    topic.conversation.topic_id = Some("topic-1".into());
    assert_ne!(
        base_key,
        key("main", &topic, policy),
        "topic id is identity"
    );

    let mut thread = base.clone();
    thread.conversation.thread_id = Some("thread-1".into());
    assert_ne!(
        base_key,
        key("main", &thread, policy),
        "thread id is identity"
    );

    let mut both = topic.clone();
    both.conversation.thread_id = Some("thread-1".into());
    let mut other_thread = both.clone();
    other_thread.conversation.thread_id = Some("thread-2".into());
    assert_ne!(
        key("main", &both, policy),
        key("main", &other_thread, policy),
        "topic and thread must remain separate when both are present"
    );

    let mut empty_topic = base.clone();
    empty_topic.conversation.topic_id = Some(String::new());
    assert_ne!(
        base_key,
        key("main", &empty_topic, policy),
        "present-empty topic must differ from absent topic"
    );

    let mut empty_parent = base.clone();
    empty_parent.conversation.parent_id = Some(String::new());
    assert_ne!(
        base_key,
        key("main", &empty_parent, policy),
        "present-empty parent must differ from absent parent"
    );

    let mut empty_scope = base.clone();
    empty_scope.conversation.scope_id = Some(String::new());
    assert_ne!(
        base_key,
        key("main", &empty_scope, policy),
        "present-empty scope must differ from absent scope"
    );

    let mut empty_thread = base.clone();
    empty_thread.conversation.thread_id = Some(String::new());
    assert_ne!(
        base_key,
        key("main", &empty_thread, policy),
        "present-empty thread must differ from absent thread"
    );

    let mut scoped = base.clone();
    scoped.conversation.scope_id = Some("scope-1".into());
    let mut scope_spelled_in_conversation = base.clone();
    scope_spelled_in_conversation.conversation.id = "scope-1:-100".into();
    assert_ne!(
        key("main", &scoped, policy),
        key("main", &scope_spelled_in_conversation, policy),
        "scope and conversation components must stay structurally distinct"
    );

    let mut scope_and_conversation = base.clone();
    scope_and_conversation.conversation.scope_id = Some("scope-a".into());
    scope_and_conversation.conversation.id = "conversation-b".into();
    let mut conversation_and_topic = base.clone();
    conversation_and_topic.conversation.id = "scope-a".into();
    conversation_and_topic.conversation.topic_id = Some("conversation-b".into());
    assert_ne!(
        key("main", &scope_and_conversation, policy),
        key("main", &conversation_and_topic, policy),
        "scope/conversation and conversation/topic placement must not collide"
    );
}

#[test]
fn identity_components_are_isolated_escaped_and_deterministic() {
    let policy = SessionKeyPolicy {
        group_sessions_per_user: false,
        thread_sessions_per_user: false,
    };
    let base = envelope();
    let base_key = key("tenant:one", &base, policy);
    assert_eq!(base_key, key("tenant:one", &base, policy));
    assert_eq!(key("default", &base, policy), key("main", &base, policy));
    assert_ne!(base_key, key("tenant:two", &base, policy));

    let mut account = base.clone();
    account.channel.account_id = Some("telegram:bot-b".into());
    assert_ne!(base_key, key("tenant:one", &account, policy));

    let mut provider = base.clone();
    provider.channel.id = "discord".into();
    assert_ne!(base_key, key("tenant:one", &provider, policy));

    let mut conversation = base.clone();
    conversation.conversation.id = "-101".into();
    assert_ne!(base_key, key("tenant:one", &conversation, policy));

    let mut sender = base.clone();
    sender.sender.id = "43".into();
    assert_eq!(base_key, key("tenant:one", &sender, policy));

    let mut provider_colon = base.clone();
    provider_colon.channel.id = "telegram:bot".into();
    provider_colon.channel.account_id = Some("a".into());
    let mut account_colon = base.clone();
    account_colon.channel.account_id = Some("bot:a".into());
    assert_ne!(
        key("tenant:one", &provider_colon, policy),
        key("tenant:one", &account_colon, policy),
        "escaped separators must not make adjacent components ambiguous"
    );

    let mut percent_escape = base.clone();
    percent_escape.conversation.id = "room%3Aone".into();
    let mut separator = base.clone();
    separator.conversation.id = "room:one".into();
    assert_ne!(
        key("tenant:one", &percent_escape, policy),
        key("tenant:one", &separator, policy),
        "literal percent escapes must differ from the escaped value"
    );

    let mut unicode = base.clone();
    unicode.conversation.id = "会話-42".into();
    let mut ascii = base.clone();
    ascii.conversation.id = "conversation-42".into();
    assert_ne!(
        key("tenant:one", &unicode, policy),
        key("tenant:one", &ascii, policy)
    );
}

#[test]
fn session_key_policy_controls_group_and_thread_participant_sharing() {
    let first = envelope();
    let mut second = first.clone();
    second.sender.id = "43".into();

    let default = SessionKeyPolicy::default();
    assert_ne!(
        key("main", &first, default),
        key("main", &second, default),
        "default group policy isolates participants"
    );

    let shared = SessionKeyPolicy {
        group_sessions_per_user: false,
        thread_sessions_per_user: false,
    };
    assert_eq!(
        key("main", &first, shared),
        key("main", &second, shared),
        "explicit group-sharing policy shares a conversation"
    );

    let mut topic_first = first.clone();
    topic_first.conversation.topic_id = Some("topic-9".into());
    let mut topic_second = topic_first.clone();
    topic_second.sender.id = "43".into();
    assert_eq!(
        key("main", &topic_first, default),
        key("main", &topic_second, default),
        "default thread policy shares a topic across participants"
    );

    let isolated = SessionKeyPolicy {
        group_sessions_per_user: true,
        thread_sessions_per_user: true,
    };
    assert_ne!(
        key("main", &topic_first, isolated),
        key("main", &topic_second, isolated),
        "explicit thread-per-user policy isolates participants"
    );
}
