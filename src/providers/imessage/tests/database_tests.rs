use super::*;

#[tokio::test]
async fn get_max_rowid_empty_database() {
    let (_dir, db_path) = create_test_db();
    let result = get_max_rowid(&db_path).await;
    assert!(result.is_ok());
    // Empty table returns 0 (NULL coalesced)
    assert_eq!(result.unwrap(), 0);
}

#[tokio::test]
async fn get_max_rowid_with_messages() {
    let (_dir, db_path) = create_test_db();

    // Insert test data
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (100, 1, 'Hello', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (200, 1, 'World', 0)",
            [],
        )
        .unwrap();
        // This one is from_me=1, should be ignored
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (300, 1, 'Sent', 1)",
            [],
        )
        .unwrap();
    }

    let result = get_max_rowid(&db_path).await.unwrap();
    // Should return 200, not 300 (ignores is_from_me=1)
    assert_eq!(result, 200);
}

#[tokio::test]
async fn get_max_rowid_nonexistent_database() {
    let path = std::path::Path::new("/nonexistent/path/chat.db");
    let result = get_max_rowid(path).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn fetch_new_messages_empty_database() {
    let (_dir, db_path) = create_test_db();
    let result = fetch_new_messages(&db_path, 0).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());
}

#[tokio::test]
async fn fetch_new_messages_returns_correct_data() {
    let (_dir, db_path) = create_test_db();

    // Insert test data
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (2, 'user@example.com')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'First message', 0)",
            []
        ).unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (20, 2, 'Second message', 0)",
            []
        ).unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(
        result[0],
        (10, "+1234567890".to_string(), "First message".to_string())
    );
    assert_eq!(
        result[1],
        (
            20,
            "user@example.com".to_string(),
            "Second message".to_string()
        )
    );
}

#[tokio::test]
async fn fetch_new_messages_filters_by_rowid() {
    let (_dir, db_path) = create_test_db();

    // Insert test data
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Old message', 0)",
            []
        ).unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (20, 1, 'New message', 0)",
            []
        ).unwrap();
    }

    // Fetch only messages after ROWID 15
    let result = fetch_new_messages(&db_path, 15).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, 20);
    assert_eq!(result[0].2, "New message");
}

#[tokio::test]
async fn fetch_new_messages_excludes_sent_messages() {
    let (_dir, db_path) = create_test_db();

    // Insert test data
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Received', 0)",
            []
        ).unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (20, 1, 'Sent by me', 1)",
            []
        ).unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].2, "Received");
}

#[tokio::test]
async fn fetch_new_messages_excludes_null_text() {
    let (_dir, db_path) = create_test_db();

    // Insert test data
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Has text', 0)",
            []
        ).unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (20, 1, NULL, 0)",
            [],
        )
        .unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].2, "Has text");
}

#[tokio::test]
async fn fetch_new_messages_respects_limit() {
    let (_dir, db_path) = create_test_db();

    // Insert 25 messages (limit is 20)
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        for i in 1..=25 {
            conn.execute(
                &format!("INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES ({i}, 1, 'Message {i}', 0)"),
                []
            ).unwrap();
        }
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 20); // Limited to 20
    assert_eq!(result[0].0, 1); // First message
    assert_eq!(result[19].0, 20); // 20th message
}

#[tokio::test]
async fn fetch_new_messages_ordered_by_rowid_asc() {
    let (_dir, db_path) = create_test_db();

    // Insert messages out of order
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (30, 1, 'Third', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'First', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (20, 1, 'Second', 0)",
            [],
        )
        .unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].0, 10);
    assert_eq!(result[1].0, 20);
    assert_eq!(result[2].0, 30);
}

#[tokio::test]
async fn fetch_new_messages_nonexistent_database() {
    let path = std::path::Path::new("/nonexistent/path/chat.db");
    let result = fetch_new_messages(path, 0).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn fetch_new_messages_handles_special_characters() {
    let (_dir, db_path) = create_test_db();

    // Insert message with special characters (potential SQL injection patterns)
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Hello \"world'' OR 1=1; DROP TABLE message;--', 0)",
            []
        ).unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 1);
    // The special characters should be preserved, not interpreted as SQL
    assert!(result[0].2.contains("DROP TABLE"));
}

#[tokio::test]
async fn fetch_new_messages_handles_unicode() {
    let (_dir, db_path) = create_test_db();

    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Hello 🦀 世界 مرحبا', 0)",
            []
        ).unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].2, "Hello 🦀 世界 مرحبا");
}

#[tokio::test]
async fn fetch_new_messages_handles_empty_text() {
    let (_dir, db_path) = create_test_db();

    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, '', 0)",
            [],
        )
        .unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    // Empty string is NOT NULL, so it's included
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].2, "");
}

#[tokio::test]
async fn fetch_new_messages_negative_rowid_edge_case() {
    let (_dir, db_path) = create_test_db();

    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Test', 0)",
            [],
        )
        .unwrap();
    }

    // Negative rowid should still work (fetch all messages with ROWID > -1)
    let result = fetch_new_messages(&db_path, -1).await.unwrap();
    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn fetch_new_messages_large_rowid_edge_case() {
    let (_dir, db_path) = create_test_db();

    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Test', 0)",
            [],
        )
        .unwrap();
    }

    // Very large rowid should return empty (no messages after this)
    let result = fetch_new_messages(&db_path, i64::MAX - 1).await.unwrap();
    assert!(result.is_empty());
}
