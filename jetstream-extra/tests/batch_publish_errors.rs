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

#[cfg(test)]
mod batch_publish_error_tests {
    use std::time::Duration;

    use async_nats::jetstream::{self, message::OutboundMessage, stream};
    use futures_util::StreamExt;
    use jetstream_extra::batch_publish::{BatchPublishErrorKind, BatchPublishExt};

    async fn setup_test_stream(
        jetstream: &async_nats::jetstream::Context,
        stream_name: &str,
        allow_atomic: bool,
    ) -> stream::Stream {
        let stream_config = stream::Config {
            name: stream_name.to_string(),
            subjects: vec![format!("{}.*", stream_name)],
            allow_atomic_publish: allow_atomic,
            allow_message_ttl: true,
            ..Default::default()
        };

        // Try to delete existing stream first (ignore errors)
        let _ = jetstream.delete_stream(stream_name).await;

        // Create the stream
        jetstream.create_stream(stream_config).await.unwrap()
    }

    #[tokio::test]
    async fn test_batch_size_limit() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let _ = setup_test_stream(&jetstream, "test_limit", true).await;

        let mut batch = jetstream.batch_publish().ack_first(false).build();

        for i in 0..999 {
            batch
                .add(format!("test_limit.{}", i), format!("msg{}", i).into())
                .await
                .unwrap();
        }

        // Can commit with 999 messages (total will be 1000 with commit)
        let ack = batch
            .commit("test_limit.final", "final".into())
            .await
            .unwrap();
        assert_eq!(ack.batch_size, 1000); // 999 + 1 commit message

        // Try another batch - add exactly 1000 messages
        let mut batch2 = jetstream.batch_publish().ack_first(false).build();

        // Add 1000 messages (0-999)
        for i in 0..1000 {
            batch2
                .add(format!("test_limit.b2.{}", i), format!("msg{}", i).into())
                .await
                .unwrap();
        }

        // The 1001st add should fail
        let err = batch2
            .add("test_limit.b2.1000", "too many".into())
            .await
            .unwrap_err();
        assert_eq!(err.kind(), BatchPublishErrorKind::MaxMessagesExceeded);

        // Commit should also fail since we're already at 1000
        let err = batch2
            .commit("test_limit.b2.final", "commit".into())
            .await
            .unwrap_err();
        assert_eq!(err.kind(), BatchPublishErrorKind::MaxMessagesExceeded);
    }

    #[tokio::test]
    async fn test_batch_publish_not_enabled() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        // Create stream WITHOUT atomic publish enabled
        let _ = setup_test_stream(&jetstream, "test_disabled", false).await;

        let mut batch = jetstream.batch_publish().build();

        // First add should fail with BatchPublishNotEnabled
        let err = batch
            .add("test_disabled.1", "message".into())
            .await
            .unwrap_err();

        assert_eq!(err.kind(), BatchPublishErrorKind::BatchPublishNotEnabled);
    }

    #[tokio::test]
    async fn test_unsupported_headers() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let _ = setup_test_stream(&jetstream, "test_headers", true).await;

        let mut batch = jetstream.batch_publish().build();

        // Test Nats-Msg-Id header
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", "test-msg-id");

        let message = OutboundMessage {
            subject: "test_headers.1".into(),
            payload: "data".into(),
            headers: Some(headers),
        };

        let err = batch.add_message(message).await.unwrap_err();
        assert_eq!(
            err.kind(),
            BatchPublishErrorKind::BatchPublishUnsupportedHeader
        );

        // Test Nats-Expected-Last-Msg-Id header
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Expected-Last-Msg-Id", "last-msg-id");

        let message = OutboundMessage {
            subject: "test_headers.2".into(),
            payload: "data".into(),
            headers: Some(headers),
        };

        let err = batch.add_message(message).await.unwrap_err();
        assert_eq!(
            err.kind(),
            BatchPublishErrorKind::BatchPublishUnsupportedHeader
        );

        // Test that commit also rejects unsupported headers
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", "commit-msg-id");

        let message = OutboundMessage {
            subject: "test_headers.3".into(),
            payload: "commit".into(),
            headers: Some(headers),
        };

        let err = batch.commit_message(message).await.unwrap_err();
        assert_eq!(
            err.kind(),
            BatchPublishErrorKind::BatchPublishUnsupportedHeader
        );
    }

    #[tokio::test]
    async fn test_ttl_with_batch() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test_ttl", true).await;

        let mut batch = jetstream.batch_publish().build();

        // Add a normal message
        batch.add("test_ttl.1", "permanent".into()).await.unwrap();

        // Add a message with TTL using PublishMessage builder
        let ttl_message = jetstream::message::PublishMessage::build()
            .ttl(Duration::from_secs(2))
            .outbound_message("test_ttl.2");

        batch.add_message(ttl_message).await.unwrap();

        // Add another normal message
        batch
            .add("test_ttl.3", "also permanent".into())
            .await
            .unwrap();

        let ack = batch.commit("test_ttl.4", "final".into()).await.unwrap();
        assert_eq!(ack.batch_size, 4);

        // Verify all messages are there initially
        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 4);

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Check that TTL message is gone but others remain
        let info = stream.info().await.unwrap();
        assert_eq!(
            info.state.messages, 3,
            "Should have 3 messages after TTL expiry"
        );
    }

    #[tokio::test]
    async fn test_incomplete_batch_error() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let _ = setup_test_stream(&jetstream, "test_incomplete", true).await;

        // Create many incomplete batches to trigger the server limit
        for _ in 0..50 {
            let mut batch = jetstream.batch_publish().build();
            batch.add("test_incomplete.1", "data".into()).await.unwrap();
            // Intentionally not committing, just dropping the batch
        }

        // Wait a moment for server to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The 51st batch should eventually fail
        let mut batch = jetstream.batch_publish().ack_first(true).build();
        let err = batch.add("test_incomplete.1", "data".into()).await;

        // This might fail either immediately or on commit
        if err.is_err() {
            assert_eq!(
                err.unwrap_err().kind(),
                BatchPublishErrorKind::BatchPublishIncomplete
            );
        } else {
            // If add succeeded, commit should fail
            let err = batch
                .commit("test_incomplete.2", "final".into())
                .await
                .unwrap_err();
            assert_eq!(err.kind(), BatchPublishErrorKind::BatchPublishIncomplete);
        }
    }

    #[tokio::test]
    async fn test_custom_headers_preserved() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let stream = setup_test_stream(&jetstream, "test_custom_headers", true).await;

        let mut batch = jetstream.batch_publish().build();

        // Add message with custom headers (not the unsupported ones)
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("X-Custom-Header", "custom-value");
        headers.insert("X-Another", "another-value");

        let message = OutboundMessage {
            subject: "test_custom_headers.1".into(),
            payload: "data".into(),
            headers: Some(headers.clone()),
        };

        batch.add_message(message).await.unwrap();

        // Commit with custom headers too
        let commit_message = OutboundMessage {
            subject: "test_custom_headers.2".into(),
            payload: "final".into(),
            headers: Some(headers),
        };

        let ack = batch.commit_message(commit_message).await.unwrap();
        assert_eq!(ack.batch_size, 2);

        // Verify custom headers were preserved
        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some("test_consumer".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut messages = consumer.messages().await.unwrap();

        let msg1 = messages.next().await.unwrap().unwrap();
        let headers1 = msg1.headers.as_ref().unwrap();
        assert_eq!(
            headers1.get("X-Custom-Header").unwrap().as_str(),
            "custom-value"
        );
        assert_eq!(headers1.get("X-Another").unwrap().as_str(), "another-value");
        // Should also have batch headers
        assert!(headers1.get("Nats-Batch-Id").is_some());
        assert_eq!(headers1.get("Nats-Batch-Sequence").unwrap().as_str(), "1");

        let msg2 = messages.next().await.unwrap().unwrap();
        let headers2 = msg2.headers.as_ref().unwrap();
        assert_eq!(
            headers2.get("X-Custom-Header").unwrap().as_str(),
            "custom-value"
        );
        assert_eq!(headers2.get("Nats-Batch-Commit").unwrap().as_str(), "1");
    }

    #[tokio::test]
    async fn test_flow_control_with_large_batch() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test_flow", true).await;

        let mut batch = jetstream
            .batch_publish()
            .ack_every(100) // Ack every 100 messages for flow control
            .timeout(Duration::from_secs(5))
            .build();

        // Add 500 messages
        for i in 0..500 {
            batch
                .add(format!("test_flow.{}", i), format!("msg{}", i).into())
                .await
                .unwrap();
        }

        let ack = batch
            .commit("test_flow.final", "done".into())
            .await
            .unwrap();
        assert_eq!(ack.batch_size, 501);

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 501);
    }
}
