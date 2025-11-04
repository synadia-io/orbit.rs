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
use nats_counters::{CounterExt, errors::CounterErrorKind};
use num_bigint::BigInt;

#[tokio::test]
async fn test_counter_basic_operations() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    // Create a counter-enabled stream
    let _stream =
        common::create_counter_stream(&js, "TEST_COUNTER", vec!["foo.*".to_string()]).await;

    // Get the counter
    let counter = js
        .get_counter("TEST_COUNTER")
        .await
        .expect("Failed to get counter");

    // Test Add operation
    let value = counter
        .add("foo.bar", 10)
        .await
        .expect("Failed to add to counter");
    assert_eq!(value, BigInt::from(10));

    // Test Load operation
    let loaded_value = counter
        .load("foo.bar")
        .await
        .expect("Failed to load counter");
    assert_eq!(loaded_value, BigInt::from(10));

    // Test adding more to the same counter
    let value = counter
        .add("foo.bar", 5)
        .await
        .expect("Failed to add to counter");
    assert_eq!(value, BigInt::from(15));

    // Test negative values (decrement)
    let value = counter
        .add("foo.bar", BigInt::from(-3))
        .await
        .expect("Failed to add negative value");
    assert_eq!(value, BigInt::from(12));

    // Verify final value with Load
    let final_value = counter
        .load("foo.bar")
        .await
        .expect("Failed to load final counter value");
    assert_eq!(final_value, BigInt::from(12));

    // Test adding 0
    let value = counter
        .add("foo.bar", BigInt::from(0))
        .await
        .expect("Failed to add 0 to counter");
    assert_eq!(value, BigInt::from(12));
}

#[tokio::test]
async fn test_counter_get_with_incr() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_COUNTER2", vec!["events.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_COUNTER2")
        .await
        .expect("Failed to get counter");

    // Add some values
    counter.add("events.clicks", 5).await.unwrap();
    counter.add("events.clicks", 3).await.unwrap();

    // Get full entry with increment info
    let entry = counter
        .get("events.clicks")
        .await
        .expect("Failed to get counter entry");

    assert_eq!(entry.subject, "events.clicks");
    assert_eq!(entry.value, BigInt::from(8));
    // The last increment should be 3
    assert_eq!(entry.increment, Some(BigInt::from(3)));
}

#[tokio::test]
async fn test_counter_multiple_subjects() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_COUNTER3", vec!["metrics.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_COUNTER3")
        .await
        .expect("Failed to get counter");

    // Add to different subjects
    counter.add("metrics.requests", 100).await.unwrap();
    counter.add("metrics.errors", 5).await.unwrap();
    counter.add("metrics.success", 95).await.unwrap();

    // Verify each counter
    assert_eq!(
        counter.load("metrics.requests").await.unwrap(),
        BigInt::from(100)
    );
    assert_eq!(
        counter.load("metrics.errors").await.unwrap(),
        BigInt::from(5)
    );
    assert_eq!(
        counter.load("metrics.success").await.unwrap(),
        BigInt::from(95)
    );
}

#[tokio::test]
async fn test_counter_not_enabled_error() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    // Create a stream WITHOUT counter support
    let config = async_nats::jetstream::stream::Config {
        name: "NON_COUNTER_STREAM".to_string(),
        subjects: vec!["test.*".to_string()],
        allow_message_counter: false, // Counter NOT enabled
        allow_direct: true,
        ..Default::default()
    };

    js.create_stream(config)
        .await
        .expect("Failed to create non-counter stream");

    // Try to get counter from non-counter stream
    let result = js.get_counter("NON_COUNTER_STREAM").await;

    match result {
        Err(e) if e.kind() == CounterErrorKind::CounterNotEnabled => {
            // Expected error
        }
        _ => panic!("Expected CounterNotEnabled error"),
    }
}

#[tokio::test]
async fn test_counter_direct_access_required() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    // Create a stream WITHOUT direct access
    let config = async_nats::jetstream::stream::Config {
        name: "NO_DIRECT_STREAM".to_string(),
        subjects: vec!["test.*".to_string()],
        allow_message_counter: true,
        allow_direct: false, // Direct access NOT enabled
        ..Default::default()
    };

    js.create_stream(config)
        .await
        .expect("Failed to create stream without direct access");

    // Try to get counter from stream without direct access
    let result = js.get_counter("NO_DIRECT_STREAM").await;

    match result {
        Err(e) if e.kind() == CounterErrorKind::DirectAccessRequired => {
            // Expected error
        }
        _ => panic!("Expected DirectAccessRequired error"),
    }
}

#[tokio::test]
async fn test_counter_not_found() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    // Try to get a counter that doesn't exist
    let result = js.get_counter("NON_EXISTENT_STREAM").await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_counter_no_counter_for_subject() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_COUNTER4", vec!["data.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_COUNTER4")
        .await
        .expect("Failed to get counter");

    // Try to load a counter that hasn't been initialized
    let result = counter.load("data.nonexistent").await;

    match result {
        Err(e) if e.kind() == CounterErrorKind::NoCounterForSubject => {
            // Expected error
        }
        _ => panic!("Expected NoCounterForSubject error"),
    }
}

#[tokio::test]
async fn test_counter_large_values() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_COUNTER5", vec!["huge.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_COUNTER5")
        .await
        .expect("Failed to get counter");

    // Test with very large values
    let huge_value = BigInt::parse_bytes(b"999999999999999999999999999999", 10).unwrap();
    let result = counter
        .add("huge.number", huge_value.clone())
        .await
        .expect("Failed to add huge value");

    assert_eq!(result, huge_value);

    // Add another huge value
    let result = counter
        .add("huge.number", huge_value.clone())
        .await
        .expect("Failed to add another huge value");

    assert_eq!(result, &huge_value + &huge_value);
}

#[tokio::test]
async fn test_counter_get_multiple() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_COUNTER6", vec!["batch.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_COUNTER6")
        .await
        .expect("Failed to get counter");

    // Add values to multiple subjects
    counter.add("batch.one", 10).await.unwrap();
    counter.add("batch.two", 20).await.unwrap();
    counter.add("batch.three", 30).await.unwrap();
    counter.add("batch.one", 5).await.unwrap(); // Add more to batch.one

    // Get multiple counters
    let subjects = vec![
        "batch.one".to_string(),
        "batch.two".to_string(),
        "batch.three".to_string(),
    ];

    let mut stream = counter
        .get_multiple(subjects.clone())
        .await
        .expect("Failed to get multiple counters");

    let mut results = std::collections::HashMap::new();
    while let Some(entry) = stream.next().await {
        let entry = entry.expect("Failed to get entry");
        results.insert(entry.subject.clone(), entry.value.clone());
    }

    // Verify we got all subjects
    assert_eq!(results.len(), 3);
    assert_eq!(results.get("batch.one"), Some(&BigInt::from(15)));
    assert_eq!(results.get("batch.two"), Some(&BigInt::from(20)));
    assert_eq!(results.get("batch.three"), Some(&BigInt::from(30)));
}

#[tokio::test]
async fn test_counter_get_multiple_with_missing() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_COUNTER7", vec!["missing.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_COUNTER7")
        .await
        .expect("Failed to get counter");

    // Add value to only one subject
    counter.add("missing.exists", 42).await.unwrap();

    // Try to get multiple including non-existent subjects
    let subjects = vec!["missing.exists".to_string(), "missing.nothere".to_string()];

    let mut stream = counter
        .get_multiple(subjects.clone())
        .await
        .expect("Failed to get multiple counters");

    let mut results = Vec::new();
    while let Some(entry) = stream.next().await {
        // Non-existent subjects won't be in the stream
        let entry = entry.expect("Failed to get entry");
        results.push(entry.subject.clone());
    }

    // We should only get the existing subject
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], "missing.exists");
}

#[tokio::test]
async fn test_counter_increment() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_INCREMENT", vec!["inc.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_INCREMENT")
        .await
        .expect("Failed to get counter");

    // Test increment from zero (first increment initializes to 1)
    let value = counter
        .increment("inc.counter")
        .await
        .expect("Failed to increment");
    assert_eq!(value, BigInt::from(1));

    // Increment again
    let value = counter
        .increment("inc.counter")
        .await
        .expect("Failed to increment");
    assert_eq!(value, BigInt::from(2));

    // Multiple increments
    for i in 3..=10 {
        let value = counter
            .increment("inc.counter")
            .await
            .expect("Failed to increment");
        assert_eq!(value, BigInt::from(i));
    }

    // Verify final value
    let final_value = counter.load("inc.counter").await.unwrap();
    assert_eq!(final_value, BigInt::from(10));
}

#[tokio::test]
async fn test_counter_decrement() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_DECREMENT", vec!["dec.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_DECREMENT")
        .await
        .expect("Failed to get counter");

    // Initialize counter with a positive value
    counter.add("dec.counter", 10).await.unwrap();

    // Test decrement
    let value = counter
        .decrement("dec.counter")
        .await
        .expect("Failed to decrement");
    assert_eq!(value, BigInt::from(9));

    // Decrement again
    let value = counter
        .decrement("dec.counter")
        .await
        .expect("Failed to decrement");
    assert_eq!(value, BigInt::from(8));

    // Verify with load
    let loaded = counter.load("dec.counter").await.unwrap();
    assert_eq!(loaded, BigInt::from(8));
}

#[tokio::test]
async fn test_counter_decrement_to_negative() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_DEC_NEG", vec!["neg.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_DEC_NEG")
        .await
        .expect("Failed to get counter");

    // Start with 3
    counter.add("neg.counter", 3).await.unwrap();

    // Decrement to 2, 1, 0, -1, -2
    for expected in [2, 1, 0, -1, -2] {
        let value = counter
            .decrement("neg.counter")
            .await
            .expect("Failed to decrement");
        assert_eq!(value, BigInt::from(expected));
    }

    // Verify final negative value
    let final_value = counter.load("neg.counter").await.unwrap();
    assert_eq!(final_value, BigInt::from(-2));
}

#[tokio::test]
async fn test_counter_increment_multiple_times() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_INC_MULT", vec!["mult.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_INC_MULT")
        .await
        .expect("Failed to get counter");

    // Rapid increments - test that each one is applied
    let count = 100;
    for i in 1..=count {
        let value = counter
            .increment("mult.rapid")
            .await
            .expect("Failed to increment");
        assert_eq!(value, BigInt::from(i));
    }

    // Final value should be exactly count
    let final_value = counter.load("mult.rapid").await.unwrap();
    assert_eq!(final_value, BigInt::from(count));
}

#[tokio::test]
async fn test_counter_increment_and_decrement_combined() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    let _stream =
        common::create_counter_stream(&js, "TEST_COMBINED", vec!["comb.*".to_string()]).await;

    let counter = js
        .get_counter("TEST_COMBINED")
        .await
        .expect("Failed to get counter");

    // Start with increment
    let value = counter.increment("comb.value").await.unwrap();
    assert_eq!(value, BigInt::from(1));

    // Add some more
    counter.add("comb.value", 9).await.unwrap(); // Now at 10

    // Decrement
    let value = counter.decrement("comb.value").await.unwrap();
    assert_eq!(value, BigInt::from(9));

    // Increment back
    let value = counter.increment("comb.value").await.unwrap();
    assert_eq!(value, BigInt::from(10));

    // Mix of operations
    counter.increment("comb.value").await.unwrap(); // 11
    counter.increment("comb.value").await.unwrap(); // 12
    counter.decrement("comb.value").await.unwrap(); // 11
    let final_value = counter.increment("comb.value").await.unwrap(); // 12

    assert_eq!(final_value, BigInt::from(12));
}
