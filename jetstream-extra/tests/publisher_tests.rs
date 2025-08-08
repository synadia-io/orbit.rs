// Copyright 2024 Synadia Communications Inc.
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

use async_nats::jetstream;
use jetstream_extra::publisher::{PublishMode, Publisher, PublisherErrorKind};

async fn setup_jetstream() -> (nats_server::Server, async_nats::Client, jetstream::Context) {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await.unwrap();
    let jetstream = jetstream::new(client.clone());

    // Create a test stream
    jetstream
        .create_stream(jetstream::stream::Config {
            name: "TEST".to_string(),
            subjects: vec!["test.*".to_string()],
            ..Default::default()
        })
        .await
        .unwrap();

    (server, client, jetstream)
}

#[tokio::test]
async fn test_publisher_wait_mode() {
    let (_server, _client, context) = setup_jetstream().await;

    let publisher = Publisher::builder(context)
        .max_in_flight(2)
        .mode(PublishMode::Backpressure)
        .build();

    // First two publishes should succeed immediately
    let ack1 = publisher.publish("test.1", "msg1".into()).await.unwrap();
    let ack2 = publisher.publish("test.2", "msg2".into()).await.unwrap();

    assert_eq!(publisher.available_capacity(), 0);

    // Third publish should wait for a permit
    let publish_handle = tokio::spawn({
        let publisher = publisher.clone();
        async move { publisher.publish("test.3", "msg3".into()).await }
    });

    // Give some time to ensure the third publish is waiting
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Complete one of the acks to release a permit
    let _ = ack1.await.unwrap();

    // Now the third publish should complete
    let ack3 = publish_handle.await.unwrap().unwrap();
    let _ = ack2.await.unwrap();
    let _ = ack3.await.unwrap();
}

#[tokio::test]
async fn test_publisher_error_mode() {
    let (_server, _client, context) = setup_jetstream().await;

    let publisher = Publisher::builder(context)
        .max_in_flight(1)
        .mode(PublishMode::Fail)
        .build();

    // First publish should succeed
    let _ack1 = publisher.publish("test.1", "msg1".into()).await.unwrap();

    // Second publish should error immediately
    let result = publisher.publish("test.2", "msg2".into()).await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().kind(),
        PublisherErrorKind::MaxInflightReached
    );
}

#[tokio::test]
async fn test_permit_release_on_ack_error() {
    let (_server, _client, context) = setup_jetstream().await;

    let publisher = Publisher::builder(context)
        .max_in_flight(1)
        .mode(PublishMode::Fail)
        .build();

    // Publish to a subject not covered by the stream
    let ack = publisher
        .publish("wrong.subject", "msg".into())
        .await
        .unwrap();

    // This should fail with stream not found
    let result = ack.await;
    assert!(result.is_err());

    // Permit should be released, so this should succeed
    let _ack2 = publisher.publish("test.1", "msg".into()).await.unwrap();
}

#[tokio::test]
async fn test_concurrent_publishes() {
    let (_server, _client, context) = setup_jetstream().await;

    let publisher = Publisher::builder(context)
        .max_in_flight(10)
        .mode(PublishMode::Backpressure)
        .build();

    let mut futures = Vec::new();

    // Spawn 20 concurrent publishes with a limit of 10
    for i in 0..20 {
        let publisher = publisher.clone();
        let future = tokio::spawn(async move {
            let ack = publisher
                .publish(format!("test.{}", i), format!("msg{}", i).into())
                .await
                .unwrap();
            ack.await.unwrap()
        });
        futures.push(future);
    }

    // All should complete successfully
    for future in futures {
        let ack = future.await.unwrap();
        assert!(!ack.duplicate);
    }
}

