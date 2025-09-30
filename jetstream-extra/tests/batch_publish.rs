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
mod batch_publish_tests {
    use std::time::Duration;

    use async_nats::jetstream::{self, stream};
    use jetstream_extra::batch_publish::BatchPublishExt;

    async fn setup_test_stream(
        jetstream: &async_nats::jetstream::Context,
        stream_name: &str,
    ) -> stream::Stream {
        // Create a test stream with atomic publishing enabled
        let stream_config = stream::Config {
            name: stream_name.to_string(),
            subjects: vec![format!("{}.*", stream_name)],
            allow_atomic_publish: true,
            allow_message_ttl: true,
            // Enable atomic publishing for batch support
            ..Default::default()
        };

        // Try to delete existing stream first (ignore errors)
        let _ = jetstream.delete_stream(stream_name).await;

        // Create the stream
        jetstream.create_stream(stream_config).await.unwrap()
    }

    #[tokio::test]
    async fn test_basic_batch_publish() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test").await;

        let mut batch = jetstream.batch_publish().build();

        batch.add("test.data", "Hello".into()).await.unwrap();
        batch.add("test.subject", "World".into()).await.unwrap();
        let ack = batch.commit("test.subject", "World".into()).await.unwrap();

        assert_eq!(ack.batch_size, 3);
        assert_eq!(ack.stream, "test");

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 3);
    }

    #[tokio::test]
    async fn test_batch_publish_options() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test").await;

        let mut batch = jetstream.batch_publish().build();

        let message = jetstream::message::PublishMessage::build()
            .ttl(Duration::from_secs(2))
            .outbound_message("test.ttl");

        batch.add("test.normal", "data".into()).await.unwrap();
        batch.add_message(message).await.unwrap();

        let ack = batch.commit("test.commit", "data".into()).await.unwrap();
        assert_eq!(ack.batch_size, 3);

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 3);

        tokio::time::sleep(Duration::from_secs(3)).await;

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 2);
    }

    #[tokio::test]
    async fn test_error() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test").await;

        let mut batch = jetstream.batch_publish().ack_every(1).build();

        let message = jetstream::message::PublishMessage::build()
            .expected_last_sequence(20)
            .outbound_message("test.sequence");

        batch.add_message(message).await.unwrap();
        batch.add("test.normal", "data".into()).await.unwrap();

        batch
            .commit("test.commit", "data".into())
            .await
            .unwrap_err();

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 0);
    }

    #[tokio::test]
    async fn test_batch_discard() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test_discard").await;

        let mut batch = jetstream.batch_publish().build();

        // Add some messages to the batch
        batch
            .add("test_discard.1", "message1".into())
            .await
            .unwrap();
        batch
            .add("test_discard.2", "message2".into())
            .await
            .unwrap();
        batch
            .add("test_discard.3", "message3".into())
            .await
            .unwrap();

        // Get the batch size before discarding
        let batch_size = batch.size();
        assert_eq!(batch_size, 3);

        // Discard the batch instead of committing
        batch.discard();

        // Messages are kept by server for 10 seconds before being discarded
        // Check immediately - messages should be pending but not committed
        let info = stream.info().await.unwrap();
        assert_eq!(
            info.state.messages, 0,
            "Stream should have no committed messages immediately after discard"
        );

        // Wait for server to discard the pending messages
        tokio::time::sleep(std::time::Duration::from_secs(11)).await;

        // Verify messages are still not in the stream (they were never committed)
        let info = stream.info().await.unwrap();
        assert_eq!(
            info.state.messages, 0,
            "Stream should have no messages after server timeout"
        );
    }
}
