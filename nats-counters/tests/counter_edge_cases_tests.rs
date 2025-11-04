// Copyright 2025 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod common;

use futures_util::StreamExt;
use nats_counters::CounterExt;
use num_bigint::BigInt;

/// Test that adding to an empty subject string results in an error
#[tokio::test]
async fn test_counter_add_empty_subject() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_EMPTY_ADD", vec!["test.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_EMPTY_ADD")
        .await
        .expect("Failed to get counter");

    // Try to add to empty subject
    let result = counter.add("", 10).await;

    // Should fail - empty subject is not valid
    assert!(
        result.is_err(),
        "Adding to empty subject should return an error"
    );
}

/// Test that loading from an empty subject string results in an error
#[tokio::test]
async fn test_counter_load_empty_subject() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_EMPTY_LOAD", vec!["test.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_EMPTY_LOAD")
        .await
        .expect("Failed to get counter");

    // Try to load empty subject
    let result = counter.load("").await;

    // Should fail - empty subject is not valid
    assert!(
        result.is_err(),
        "Loading empty subject should return an error"
    );
}

/// Test that getting an empty subject string results in an error
#[tokio::test]
async fn test_counter_get_empty_subject() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_EMPTY_GET", vec!["test.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_EMPTY_GET")
        .await
        .expect("Failed to get counter");

    // Try to get empty subject entry
    let result = counter.get("").await;

    // Should fail - empty subject is not valid
    assert!(
        result.is_err(),
        "Getting empty subject should return an error"
    );
}

/// Test that get_multiple with an empty string in the subjects list handles it correctly
#[tokio::test]
async fn test_counter_get_multiple_with_empty_subject() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_EMPTY_MULTIPLE", vec!["batch.*".to_string()])
            .await;

    let counter = js
        .get_counter("TEST_EMPTY_MULTIPLE")
        .await
        .expect("Failed to get counter");

    // Add a valid counter
    counter.add("batch.valid", 10).await.unwrap();

    // Try to get multiple including an empty subject
    let subjects = vec!["batch.valid".to_string(), "".to_string()];

    let result = counter.get_multiple(subjects).await;

    // Should either fail or the stream should produce an error
    match result {
        Err(_) => {
            // Expected: get_multiple fails immediately
        }
        Ok(mut stream) => {
            // Or: get_multiple succeeds but stream produces error
            let mut has_error = false;
            while let Some(entry_result) = stream.next().await {
                if entry_result.is_err() {
                    has_error = true;
                    break;
                }
            }
            assert!(
                has_error,
                "Stream should produce an error for empty subject"
            );
        }
    }
}

/// Test that get_multiple with an empty subjects array handles it correctly
#[tokio::test]
async fn test_counter_get_multiple_with_empty_array() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_EMPTY_ARRAY", vec!["batch.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_EMPTY_ARRAY")
        .await
        .expect("Failed to get counter");

    // Try to get multiple with empty subjects array
    let subjects: Vec<String> = vec![];

    let result = counter.get_multiple(subjects).await;

    // Should either fail or return empty stream
    match result {
        Err(_) => {
            // Expected: get_multiple fails for empty array
        }
        Ok(mut stream) => {
            // Or: succeeds but stream is empty
            let first = stream.next().await;
            assert!(
                first.is_none(),
                "Stream should be empty for empty subjects array"
            );
        }
    }
}

/// Test that adding zero maintains the current value
#[tokio::test]
async fn test_counter_add_zero_value() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream = common::create_counter_stream(&js, "TEST_ZERO", vec!["zero.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_ZERO")
        .await
        .expect("Failed to get counter");

    // Initialize counter with a value
    let initial = counter.add("zero.value", 42).await.unwrap();
    assert_eq!(initial, BigInt::from(42));

    // Add zero - should not change the value
    let after_zero = counter.add("zero.value", 0).await.unwrap();
    assert_eq!(after_zero, BigInt::from(42));

    // Verify with load
    let loaded = counter.load("zero.value").await.unwrap();
    assert_eq!(loaded, BigInt::from(42));
}

/// Test get_multiple with duplicate subjects in the list
#[tokio::test]
async fn test_counter_get_multiple_with_duplicates() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_DUPLICATES", vec!["dup.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_DUPLICATES")
        .await
        .expect("Failed to get counter");

    // Add counter value
    counter.add("dup.value", 100).await.unwrap();

    // Request the same subject multiple times
    let subjects = vec![
        "dup.value".to_string(),
        "dup.value".to_string(),
        "dup.value".to_string(),
    ];

    let mut stream = counter
        .get_multiple(subjects)
        .await
        .expect("Failed to get multiple");

    let mut count = 0;
    while let Some(entry) = stream.next().await {
        let entry = entry.expect("Failed to get entry");
        assert_eq!(entry.subject, "dup.value");
        assert_eq!(entry.value, BigInt::from(100));
        count += 1;
    }

    // Behavior may vary: could return 1 or 3 entries
    // At minimum should return 1, document actual behavior
    assert!(
        count >= 1,
        "Should return at least one entry for duplicate subjects"
    );
    println!("Duplicate subjects returned {} entries", count);
}

/// Test get_multiple with a mix of existing and non-existing subjects
#[tokio::test]
async fn test_counter_get_multiple_partial_success() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_PARTIAL", vec!["partial.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_PARTIAL")
        .await
        .expect("Failed to get counter");

    // Add only some counters
    counter.add("partial.exists1", 10).await.unwrap();
    counter.add("partial.exists2", 20).await.unwrap();

    // Request mix of existing and non-existing
    let subjects = vec![
        "partial.exists1".to_string(),
        "partial.missing".to_string(),
        "partial.exists2".to_string(),
        "partial.also_missing".to_string(),
    ];

    let mut stream = counter
        .get_multiple(subjects.clone())
        .await
        .expect("Failed to get multiple");

    let mut found = vec![];
    while let Some(entry_result) = stream.next().await {
        // Non-existent subjects may be silently skipped or produce errors
        match entry_result {
            Ok(entry) => {
                found.push(entry.subject.clone());
            }
            Err(_) => {
                // Some implementations may error on missing subjects
            }
        }
    }

    // Should find at least the existing subjects
    assert!(
        found.contains(&"partial.exists1".to_string()),
        "Should find exists1"
    );
    assert!(
        found.contains(&"partial.exists2".to_string()),
        "Should find exists2"
    );

    // Non-existent subjects should not appear
    assert!(
        !found.contains(&"partial.missing".to_string()),
        "Should not find missing"
    );
    assert!(
        !found.contains(&"partial.also_missing".to_string()),
        "Should not find also_missing"
    );
}

/// Test very large negative values
#[tokio::test]
async fn test_counter_large_negative_values() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream = common::create_counter_stream(&js, "TEST_NEG", vec!["neg.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_NEG")
        .await
        .expect("Failed to get counter");

    // Test with very large negative value
    let huge_negative = BigInt::parse_bytes(b"-999999999999999999999999999999", 10).unwrap();
    let result = counter
        .add("neg.huge", huge_negative.clone())
        .await
        .expect("Failed to add huge negative value");

    assert_eq!(result, huge_negative);

    // Add a positive value to the negative counter
    let result = counter
        .add("neg.huge", BigInt::from(1000000000000000000i64))
        .await
        .expect("Failed to add to negative counter");

    // Should still be very negative
    assert!(result < BigInt::from(0));
}
