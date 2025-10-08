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

use async_nats::jetstream::{self, stream};
use futures::StreamExt;
use jetstream_extra::batch_fetch::{BatchFetchErrorKind, BatchFetchExt};
use time::OffsetDateTime;

#[tokio::test]
async fn test_batch_fetch_basic() -> Result<(), Box<dyn std::error::Error>> {
    // Start an embedded NATS server
    let server = nats_server::run_server("tests/configs/jetstream.conf");

    // Connect to the server
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    // Create a stream with some test data
    let stream_name = "TEST_STREAM";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["test.*".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await?;

    // Publish some test messages
    for i in 0..10 {
        context
            .publish(format!("test.{}", i), format!("message {}", i).into())
            .await?
            .await?;
    }

    // Test batch fetch
    let mut messages = context.get_batch(stream_name, 5).send().await?;

    let mut count = 0;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        println!("Fetched message seq {}: {:?}", msg.sequence, msg.subject);
        count += 1;
    }

    assert_eq!(count, 5, "Should have fetched exactly 5 messages");

    Ok(())
}

#[tokio::test]
async fn test_batch_fetch_with_subject_filter() -> Result<(), Box<dyn std::error::Error>> {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    let stream_name = "TEST_STREAM_FILTER";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["events.*".to_string()],
            allow_direct: true, // Required for DIRECT.GET API
            ..Default::default()
        })
        .await?;

    // Publish messages to different subjects
    for i in 0..5 {
        context
            .publish("events.user", format!("user message {}", i).into())
            .await?
            .await?;
        context
            .publish("events.system", format!("system message {}", i).into())
            .await?
            .await?;
    }

    // Fetch only user messages
    let mut messages = context
        .get_batch(stream_name, 10)
        .subject("events.user")
        .send()
        .await?;

    let mut count = 0;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        assert_eq!(msg.subject, "events.user".into());
        count += 1;
    }

    assert_eq!(count, 5, "Should have fetched only user messages");

    Ok(())
}

#[tokio::test]
async fn test_get_last_msgs_for() -> Result<(), Box<dyn std::error::Error>> {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    let stream_name = "TEST_STREAM_LAST";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["data.*".to_string()],
            allow_direct: true, // Required for DIRECT.GET API
            ..Default::default()
        })
        .await?;

    // Publish multiple messages to different subjects
    for i in 0..3 {
        context
            .publish("data.sensor1", format!("sensor1 reading {}", i).into())
            .await?
            .await?;
        context
            .publish("data.sensor2", format!("sensor2 reading {}", i).into())
            .await?
            .await?;
        context
            .publish("data.sensor3", format!("sensor3 reading {}", i).into())
            .await?
            .await?;
    }

    // Get the last message for each sensor
    let subjects = vec![
        "data.sensor1".to_string(),
        "data.sensor2".to_string(),
        "data.sensor3".to_string(),
    ];

    let mut messages = context
        .get_last_messages_for(stream_name)
        .subjects(subjects)
        .send()
        .await?;

    let mut count = 0;
    let mut subjects_found = std::collections::HashSet::new();

    while let Some(msg) = messages.next().await {
        let msg = msg?;
        subjects_found.insert(msg.subject.to_string());
        count += 1;

        // The last message for each sensor should be reading 2
        let payload = String::from_utf8(msg.payload.to_vec())?;
        assert!(
            payload.ends_with("reading 2"),
            "Should get the last message"
        );
    }

    assert_eq!(
        count, 3,
        "Should have fetched last message for each subject"
    );
    assert_eq!(subjects_found.len(), 3, "Should have all three subjects");

    Ok(())
}

#[tokio::test]
async fn test_batch_fetch_seq_and_subject() -> Result<(), Box<dyn std::error::Error>> {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    let stream_name = "TEST_STREAM_SEQ_SUBJECT";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["test.*".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await?;

    // Publish mixed messages
    // Sequence: 1=A, 2=B, 3=A, 4=B, 5=A, 6=B, 7=A, 8=B, 9=A, 10=B
    for i in 0..10 {
        let subject = if i % 2 == 0 { "test.A" } else { "test.B" };
        context
            .publish(subject, format!("message {}", i).into())
            .await?
            .await?;
    }

    // Test: Get 5 messages from sequence 5, subject test.B only
    // Should get B messages at sequences 6, 8, 10 (only 3 messages)
    let mut messages = context
        .get_batch(stream_name, 5)
        .sequence(5)
        .subject("test.B")
        .send()
        .await?;

    let mut sequences = Vec::new();
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        assert_eq!(msg.subject, "test.B".into());
        sequences.push(msg.sequence);
    }

    assert_eq!(
        sequences,
        vec![6, 8, 10],
        "Should get only B messages from seq 5+"
    );

    Ok(())
}

#[tokio::test]
async fn test_batch_fetch_sequence_range() -> Result<(), Box<dyn std::error::Error>> {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    let stream_name = "TEST_STREAM_SEQ";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["seq.*".to_string()],
            allow_direct: true, // Required for DIRECT.GET API
            ..Default::default()
        })
        .await?;

    // Publish 10 messages
    for i in 0..10 {
        context
            .publish("seq.msg", format!("message {}", i).into())
            .await?
            .await?;
    }

    // Fetch messages starting from sequence 5
    let mut messages = context.get_batch(stream_name, 3).sequence(5).send().await?;

    let mut sequences = Vec::new();
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        sequences.push(msg.sequence);
    }

    assert_eq!(
        sequences,
        vec![5, 6, 7],
        "Should fetch correct sequence range"
    );

    Ok(())
}

#[tokio::test]
async fn test_batch_size_limit() -> Result<(), Box<dyn std::error::Error>> {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    let stream_name = "TEST_STREAM_SIZE_LIMIT";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["test.*".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await?;

    // Test batch size validation - should fail with > 1000
    let result = context.get_batch(stream_name, 1001).send().await;

    match result {
        Err(e) if e.kind() == BatchFetchErrorKind::BatchSizeTooLarge => {
            // Expected error
            Ok(())
        }
        Err(e) => {
            panic!("Expected BatchSizeTooLarge error, got: {:?}", e);
        }
        Ok(_) => {
            panic!("Expected error for batch size > 1000");
        }
    }
}

#[tokio::test]
async fn test_time_based_fetching() -> Result<(), Box<dyn std::error::Error>> {
    use std::time::{Duration, SystemTime};

    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    let stream_name = "TEST_STREAM_TIME";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["time.*".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await?;

    // Publish messages with timestamps
    let _before = SystemTime::now();
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..5 {
        context
            .publish("time.msg", format!("early message {}", i).into())
            .await?
            .await?;
    }

    let middle = OffsetDateTime::now_utc();
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..5 {
        context
            .publish("time.msg", format!("late message {}", i).into())
            .await?
            .await?;
    }

    // Fetch messages after middle timestamp - should get only late messages
    let mut messages = context
        .get_batch(stream_name, 10)
        .start_time(middle)
        .send()
        .await?;

    let mut count = 0;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        let payload = String::from_utf8(msg.payload.to_vec())?;
        assert!(
            payload.starts_with("late message"),
            "Should only get late messages"
        );
        count += 1;
    }

    assert_eq!(count, 5, "Should have fetched only late messages");
    Ok(())
}

#[tokio::test]
async fn test_subject_count_limit() -> Result<(), Box<dyn std::error::Error>> {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    let stream_name = "TEST_STREAM_SUBJECT_LIMIT";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["test.*".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await?;

    // Create more than 1024 subjects
    let mut subjects = Vec::new();
    for i in 0..1025 {
        subjects.push(format!("test.subject{}", i));
    }

    // Test subject count validation - should fail with > 1024
    let result = context
        .get_last_messages_for(stream_name)
        .subjects(subjects)
        .send()
        .await;

    match result {
        Err(e) if e.kind() == BatchFetchErrorKind::TooManySubjects => {
            // Expected error
            Ok(())
        }
        Err(e) => {
            panic!("Expected TooManySubjects error, got: {:?}", e);
        }
        Ok(_) => {
            panic!("Expected error for subject count > 1024");
        }
    }
}

#[tokio::test]
async fn test_invalid_parameters() -> Result<(), Box<dyn std::error::Error>> {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    let stream_name = "TEST_STREAM_INVALID";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["test.*".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await?;

    // Test: seq = 0 should fail
    let result = context.get_batch(stream_name, 5).sequence(0).send().await;

    match result {
        Err(e) if e.kind() == BatchFetchErrorKind::InvalidOption => {
            // Expected
        }
        _ => panic!("Expected InvalidOption error for seq=0"),
    }

    // Test: max_bytes = 0 should fail
    let result = context.get_batch(stream_name, 5).max_bytes(0).send().await;

    match result {
        Err(e) if e.kind() == BatchFetchErrorKind::InvalidOption => {
            // Expected
        }
        _ => panic!("Expected InvalidOption error for max_bytes=0"),
    }

    // Test: batch = 0 in get_last should fail
    let result = context
        .get_last_messages_for(stream_name)
        .subjects(vec!["test.A".to_string()])
        .batch(0)
        .send()
        .await;

    match result {
        Err(e) if e.kind() == BatchFetchErrorKind::InvalidOption => {
            // Expected
        }
        _ => panic!("Expected InvalidOption error for batch=0 in get_last"),
    }

    Ok(())
}

#[tokio::test]
async fn test_invalid_stream_name() -> Result<(), Box<dyn std::error::Error>> {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    // Test empty stream name for get_batch
    let result = context.get_batch("", 5).send().await;

    match result {
        Err(e) if e.kind() == BatchFetchErrorKind::InvalidStreamName => {
            // Expected error
        }
        Err(e) => {
            panic!(
                "Expected InvalidStreamName error for get_batch, got: {:?}",
                e
            );
        }
        Ok(_) => {
            panic!("Expected error for empty stream name in get_batch");
        }
    }

    // Test empty stream name for get_last_messages_for
    let result = context
        .get_last_messages_for("")
        .subjects(vec!["test.subject".to_string()])
        .send()
        .await;

    match result {
        Err(e) if e.kind() == BatchFetchErrorKind::InvalidStreamName => {
            // Expected error
            Ok(())
        }
        Err(e) => {
            panic!(
                "Expected InvalidStreamName error for get_last_messages_for, got: {:?}",
                e
            );
        }
        Ok(_) => {
            panic!("Expected error for empty stream name in get_last_messages_for");
        }
    }
}

#[tokio::test]
async fn test_more_than_available() -> Result<(), Box<dyn std::error::Error>> {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    let stream_name = "TEST_STREAM_PARTIAL";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["test.*".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await?;

    // Publish only 5 messages
    for i in 0..5 {
        context
            .publish("test.msg", format!("message {}", i).into())
            .await?
            .await?;
    }

    // Request 10 messages but only 5 available
    let mut messages = context
        .get_batch(stream_name, 10)
        .sequence(1)
        .send()
        .await?;

    let mut count = 0;
    while let Some(msg) = messages.next().await {
        let _msg = msg?;
        count += 1;
    }

    assert_eq!(
        count, 5,
        "Should get only 5 messages even though 10 requested"
    );
    Ok(())
}

#[tokio::test]
async fn test_seq_higher_than_available() -> Result<(), Box<dyn std::error::Error>> {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    let stream_name = "TEST_STREAM_HIGH_SEQ";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["test.*".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await?;

    // Publish 5 messages (seq 1-5)
    for i in 0..5 {
        context
            .publish("test.msg", format!("message {}", i).into())
            .await?
            .await?;
    }

    // Request starting from seq 10 (doesn't exist)
    let mut messages = context
        .get_batch(stream_name, 5)
        .sequence(10)
        .send()
        .await?;

    // Should get no messages
    let mut count = 0;
    while let Some(msg) = messages.next().await {
        match msg {
            Ok(_) => count += 1,
            Err(e) if e.kind() == BatchFetchErrorKind::NoMessages => break,
            Err(e) => return Err(e.into()),
        }
    }

    assert_eq!(
        count, 0,
        "Should get no messages when seq is higher than available"
    );
    Ok(())
}

#[tokio::test]
async fn test_get_last_with_up_to_seq() -> Result<(), Box<dyn std::error::Error>> {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    let stream_name = "TEST_STREAM_UP_TO_SEQ";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["data.*".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await?;

    // Publish messages
    // Sequences: 1,2,3 = data.A; 4,5,6 = data.B; 7,8,9 = data.A
    for i in 0..3 {
        context
            .publish("data.A", format!("A{}", i).into())
            .await?
            .await?;
    }
    for i in 0..3 {
        context
            .publish("data.B", format!("B{}", i).into())
            .await?
            .await?;
    }
    for i in 3..6 {
        context
            .publish("data.A", format!("A{}", i).into())
            .await?
            .await?;
    }

    // Get last messages up to sequence 6
    // Should get: data.A at seq 3, data.B at seq 6
    let mut messages = context
        .get_last_messages_for(stream_name)
        .subjects(vec!["data.A".to_string(), "data.B".to_string()])
        .up_to_seq(6)
        .send()
        .await?;

    let mut results = std::collections::HashMap::new();
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        results.insert(msg.subject.to_string(), msg.sequence);
    }

    assert_eq!(
        results.get("data.A"),
        Some(&3),
        "Last data.A up to seq 6 should be seq 3"
    );
    assert_eq!(
        results.get("data.B"),
        Some(&6),
        "Last data.B up to seq 6 should be seq 6"
    );

    Ok(())
}

#[tokio::test]
async fn test_no_messages_match_filter() -> Result<(), Box<dyn std::error::Error>> {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    let stream_name = "TEST_STREAM_NO_MATCH";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["data.*".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await?;

    // Publish messages only to data.A
    for i in 0..5 {
        context
            .publish("data.A", format!("msg{}", i).into())
            .await?
            .await?;
    }

    // Try to get last messages for data.B and data.C (don't exist)
    let mut messages = context
        .get_last_messages_for(stream_name)
        .subjects(vec!["data.B".to_string(), "data.C".to_string()])
        .send()
        .await?;

    let mut count = 0;
    while let Some(msg) = messages.next().await {
        match msg {
            Ok(_) => count += 1,
            Err(e) if e.kind() == BatchFetchErrorKind::NoMessages => break,
            Err(e) => return Err(e.into()),
        }
    }

    assert_eq!(count, 0, "Should get no messages when subjects don't match");
    Ok(())
}

#[tokio::test]
async fn test_batch_fetch_unsupported_server() -> Result<(), Box<dyn std::error::Error>> {
    // Start an embedded NATS server (current versions don't support batch get)
    let server = nats_server::run_server("tests/configs/jetstream.conf");

    // Connect to the server
    let client = async_nats::connect(server.client_url()).await?;
    let context = jetstream::new(client);

    // Create a stream
    let stream_name = "TEST_STREAM_UNSUPPORTED";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["test.*".to_string()],
            ..Default::default()
        })
        .await?;

    // Attempt batch fetch - the error will come when trying to consume the stream
    let mut messages = context.get_batch(stream_name, 5).send().await?;

    // Try to get the first message - should fail with UnsupportedByServer
    match messages.next().await {
        Some(Err(e)) if e.kind() == BatchFetchErrorKind::UnsupportedByServer => {
            // Expected error
            Ok(())
        }
        Some(Err(e)) => {
            panic!("Expected UnsupportedByServer error, got: {:?}", e);
        }
        Some(Ok(_)) => {
            panic!("Expected error but got a message");
        }
        None => {
            panic!("Expected error but stream ended without messages");
        }
    }
}

#[tokio::test]
async fn test_timeout() -> Result<(), Box<dyn std::error::Error>> {
    // Start an embedded NATS server (current versions don't support batch get)
    let server = nats_server::run_server("tests/configs/jetstream.conf");

    // Connect to the server
    let client = async_nats::connect(server.client_url()).await?;
    let mut context = jetstream::new(client);

    // Create a stream
    let stream_name = "TIMEOUT";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["test.*".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await?;

    for i in 0..1000 {
        context
            .publish("test.msg", format!("message {}", i).into())
            .await?
            .await?;
    }

    context.set_timeout(std::time::Duration::from_nanos(1));
    let messages = context.get_batch(stream_name, 1000).send().await?;

    let results = messages.collect::<Vec<_>>().await;
    assert!(
        results
            .iter()
            .any(|r| { matches!(r, Err(e) if e.kind() == BatchFetchErrorKind::TimedOut) }),
        "Should have a timeout error"
    );

    Ok(())
}
