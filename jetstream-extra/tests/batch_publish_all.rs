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
mod batch_publish_all_tests {
    use std::time::Duration;

    use async_nats::jetstream::{self, message::OutboundMessage, stream};
    use futures_util::{StreamExt, stream as futures_stream};
    use jetstream_extra::batch_publish::{BatchPublishErrorKind, BatchPublishExt};
    use tokio_stream::wrappers::ReceiverStream;

    async fn setup_test_stream(
        jetstream: &async_nats::jetstream::Context,
        stream_name: &str,
    ) -> stream::Stream {
        let stream_config = stream::Config {
            name: stream_name.to_string(),
            subjects: vec![format!("{}.*", stream_name)],
            allow_atomic_publish: true,
            allow_message_ttl: true,
            ..Default::default()
        };

        let _ = jetstream.delete_stream(stream_name).await;
        jetstream.create_stream(stream_config).await.unwrap()
    }

    #[tokio::test]
    async fn test_vec_publish() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test_vec").await;

        let messages = vec![
            OutboundMessage {
                subject: "test_vec.1".into(),
                payload: "Hello".into(),
                headers: None,
            },
            OutboundMessage {
                subject: "test_vec.2".into(),
                payload: "World".into(),
                headers: None,
            },
            OutboundMessage {
                subject: "test_vec.3".into(),
                payload: "!".into(),
                headers: None,
            },
        ];

        let ack = jetstream
            .batch_publish_all()
            .publish(futures_stream::iter(messages))
            .await
            .unwrap();

        assert_eq!(ack.batch_size, 3);
        assert_eq!(ack.stream, "test_vec");
        assert!(!ack.batch_id.is_empty(), "Batch ID should not be empty");
        assert!(ack.sequence > 0, "Sequence should be greater than 0");

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 3);
    }

    #[tokio::test]
    async fn test_channel_publish() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test_channel").await;

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            for i in 0..5 {
                let msg = OutboundMessage {
                    subject: format!("test_channel.{}", i).into(),
                    payload: format!("Message {}", i).into(),
                    headers: None,
                };
                tx.send(msg).await.unwrap();
            }
        });

        let ack = jetstream
            .batch_publish_all()
            .publish(ReceiverStream::new(rx))
            .await
            .unwrap();

        assert_eq!(ack.batch_size, 5);
        assert_eq!(ack.stream, "test_channel");

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 5);
    }

    #[tokio::test]
    async fn test_stream_transformation() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test_transform").await;

        let data = vec!["apple", "banana", "cherry"];
        let message_stream =
            futures_stream::iter(data)
                .enumerate()
                .map(|(i, fruit)| OutboundMessage {
                    subject: format!("test_transform.{}", i).into(),
                    payload: fruit.into(),
                    headers: None,
                });

        let ack = jetstream
            .batch_publish_all()
            .publish(message_stream)
            .await
            .unwrap();

        assert_eq!(ack.batch_size, 3);
        assert_eq!(ack.stream, "test_transform");

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 3);
    }

    #[tokio::test]
    async fn test_merged_sources() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test_merge").await;

        let source1 = futures_stream::iter(vec![
            OutboundMessage {
                subject: "test_merge.1".into(),
                payload: "First".into(),
                headers: None,
            },
            OutboundMessage {
                subject: "test_merge.2".into(),
                payload: "Second".into(),
                headers: None,
            },
        ]);

        let source2 = futures_stream::iter(vec![
            OutboundMessage {
                subject: "test_merge.3".into(),
                payload: "Third".into(),
                headers: None,
            },
            OutboundMessage {
                subject: "test_merge.4".into(),
                payload: "Fourth".into(),
                headers: None,
            },
        ]);

        let merged = source1.chain(source2);

        let ack = jetstream.batch_publish_all().publish(merged).await.unwrap();

        assert_eq!(ack.batch_size, 4);
        assert_eq!(ack.stream, "test_merge");

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 4);
    }

    #[tokio::test]
    async fn test_error_handling_stream() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test_errors").await;

        // Simulate a stream that might have errors
        let fallible_stream = futures_stream::iter(vec![
            Ok(OutboundMessage {
                subject: "test_errors.1".into(),
                payload: "Good1".into(),
                headers: None,
            }),
            Err("simulated error"),
            Ok(OutboundMessage {
                subject: "test_errors.2".into(),
                payload: "Good2".into(),
                headers: None,
            }),
        ]);

        let message_stream = futures_stream::iter(
            fallible_stream
                .filter_map(|result| async move {
                    result.ok() // Skip errors
                })
                .collect::<Vec<_>>()
                .await,
        );

        let ack = jetstream
            .batch_publish_all()
            .publish(message_stream)
            .await
            .unwrap();

        assert_eq!(ack.batch_size, 2); // Only good messages
        assert_eq!(ack.stream, "test_errors");

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 2);
    }

    #[tokio::test]
    async fn test_array_publish() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test_array").await;

        let ack = jetstream
            .batch_publish_all()
            .publish(futures_stream::iter([
                OutboundMessage {
                    subject: "test_array.1".into(),
                    payload: "First".into(),
                    headers: None,
                },
                OutboundMessage {
                    subject: "test_array.2".into(),
                    payload: "Second".into(),
                    headers: None,
                },
            ]))
            .await
            .unwrap();

        assert_eq!(ack.batch_size, 2);
        assert_eq!(ack.stream, "test_array");

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 2);
    }

    #[tokio::test]
    async fn test_empty_stream_error() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let _ = setup_test_stream(&jetstream, "test_empty").await;

        let empty_stream = futures_stream::empty();

        let result = jetstream.batch_publish_all().publish(empty_stream).await;

        assert!(result.is_err(), "Empty stream should return error");
        let err = result.unwrap_err();
        assert_eq!(err.kind(), BatchPublishErrorKind::EmptyBatch);
        assert!(
            err.to_string().contains("empty"),
            "Error message should mention empty batch"
        );
    }

    #[tokio::test]
    async fn test_ack_every_configuration() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test_ack_every").await;

        let messages: Vec<_> = (0..10)
            .map(|i| OutboundMessage {
                subject: format!("test_ack_every.{}", i).into(),
                payload: format!("Message {}", i).into(),
                headers: None,
            })
            .collect();

        let ack = jetstream
            .batch_publish_all()
            .ack_every(3) // Ack every 3 messages
            .publish(futures_stream::iter(messages))
            .await
            .unwrap();

        assert_eq!(ack.batch_size, 10);
        assert_eq!(ack.stream, "test_ack_every");

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 10);
    }

    #[tokio::test]
    async fn test_timeout_configuration() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test_timeout").await;

        let messages = vec![
            OutboundMessage {
                subject: "test_timeout.1".into(),
                payload: "Message 1".into(),
                headers: None,
            },
            OutboundMessage {
                subject: "test_timeout.2".into(),
                payload: "Message 2".into(),
                headers: None,
            },
        ];

        let ack = jetstream
            .batch_publish_all()
            .timeout(Duration::from_secs(10))
            .publish(futures_stream::iter(messages))
            .await
            .unwrap();

        assert_eq!(ack.batch_size, 2);
        assert_eq!(ack.stream, "test_timeout");

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 2);
    }

    #[tokio::test]
    async fn test_batch_headers_correctly_set() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let stream = setup_test_stream(&jetstream, "test_headers").await;

        let messages = vec![
            OutboundMessage {
                subject: "test_headers.1".into(),
                payload: "msg1".into(),
                headers: None,
            },
            OutboundMessage {
                subject: "test_headers.2".into(),
                payload: "msg2".into(),
                headers: None,
            },
            OutboundMessage {
                subject: "test_headers.3".into(),
                payload: "msg3".into(),
                headers: None,
            },
        ];

        let ack = jetstream
            .batch_publish_all()
            .publish(futures_stream::iter(messages))
            .await
            .unwrap();

        // Fetch messages from the stream and verify headers
        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some("test_consumer".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut batch_messages = consumer.messages().await.unwrap();

        // Check first message - should have batch headers but no commit
        let msg1 = batch_messages.next().await.unwrap().unwrap();
        let headers1 = msg1.headers.as_ref().unwrap();
        assert_eq!(
            headers1.get("Nats-Batch-Id").unwrap().as_str(),
            &ack.batch_id
        );
        assert_eq!(headers1.get("Nats-Batch-Sequence").unwrap().as_str(), "1");
        assert!(
            headers1.get("Nats-Batch-Commit").is_none(),
            "First message should not have commit header"
        );

        // Check second message - should have batch headers but no commit
        let msg2 = batch_messages.next().await.unwrap().unwrap();
        let headers2 = msg2.headers.as_ref().unwrap();
        assert_eq!(
            headers2.get("Nats-Batch-Id").unwrap().as_str(),
            &ack.batch_id
        );
        assert_eq!(headers2.get("Nats-Batch-Sequence").unwrap().as_str(), "2");
        assert!(
            headers2.get("Nats-Batch-Commit").is_none(),
            "Middle message should not have commit header"
        );

        // Check third message - should have batch headers AND commit
        let msg3 = batch_messages.next().await.unwrap().unwrap();
        let headers3 = msg3.headers.as_ref().unwrap();
        assert_eq!(
            headers3.get("Nats-Batch-Id").unwrap().as_str(),
            &ack.batch_id
        );
        assert_eq!(headers3.get("Nats-Batch-Sequence").unwrap().as_str(), "3");
        assert_eq!(
            headers3.get("Nats-Batch-Commit").unwrap().as_str(),
            "1",
            "Last message must have commit header"
        );
    }

    #[tokio::test]
    async fn test_single_message_batch() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let mut stream = setup_test_stream(&jetstream, "test_single").await;

        let messages = vec![OutboundMessage {
            subject: "test_single.1".into(),
            payload: "Only message".into(),
            headers: None,
        }];

        let ack = jetstream
            .batch_publish_all()
            .publish(futures_stream::iter(messages))
            .await
            .unwrap();

        assert_eq!(ack.batch_size, 1, "Single message batch should have size 1");
        assert_eq!(ack.stream, "test_single");
        assert!(
            !ack.batch_id.is_empty(),
            "Batch ID should be generated even for single message"
        );

        let info = stream.info().await.unwrap();
        assert_eq!(info.state.messages, 1);

        // Verify the single message acts as both first and last (commit) message
        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some("test_consumer".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut batch_messages = consumer.messages().await.unwrap();
        let msg = batch_messages.next().await.unwrap().unwrap();

        let headers = msg.headers.as_ref().unwrap();
        assert_eq!(
            headers.get("Nats-Batch-Id").unwrap().as_str(),
            &ack.batch_id
        );
        assert_eq!(headers.get("Nats-Batch-Sequence").unwrap().as_str(), "1");
        assert_eq!(
            headers.get("Nats-Batch-Commit").unwrap().as_str(),
            "1",
            "Single message must have commit header"
        );
    }

    #[tokio::test]
    async fn test_batch_ids_are_unique() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let _ = setup_test_stream(&jetstream, "test_unique_ids").await;

        // Create multiple batches
        let ack1 = jetstream
            .batch_publish_all()
            .publish(futures_stream::iter(vec![OutboundMessage {
                subject: "test_unique_ids.1".into(),
                payload: "batch1".into(),
                headers: None,
            }]))
            .await
            .unwrap();

        let ack2 = jetstream
            .batch_publish_all()
            .publish(futures_stream::iter(vec![OutboundMessage {
                subject: "test_unique_ids.2".into(),
                payload: "batch2".into(),
                headers: None,
            }]))
            .await
            .unwrap();

        let ack3 = jetstream
            .batch_publish_all()
            .publish(futures_stream::iter(vec![OutboundMessage {
                subject: "test_unique_ids.3".into(),
                payload: "batch3".into(),
                headers: None,
            }]))
            .await
            .unwrap();

        // All batch IDs should be different
        assert_ne!(ack1.batch_id, ack2.batch_id, "Batch IDs must be unique");
        assert_ne!(ack2.batch_id, ack3.batch_id, "Batch IDs must be unique");
        assert_ne!(ack1.batch_id, ack3.batch_id, "Batch IDs must be unique");

        // All should be non-empty
        assert!(!ack1.batch_id.is_empty());
        assert!(!ack2.batch_id.is_empty());
        assert!(!ack3.batch_id.is_empty());
    }

    #[tokio::test]
    async fn test_preserves_custom_headers() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let stream = setup_test_stream(&jetstream, "test_custom").await;

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("X-Custom", "custom-value");
        headers.insert("X-Another", "another-value");

        let messages = vec![
            OutboundMessage {
                subject: "test_custom.1".into(),
                payload: "with headers".into(),
                headers: Some(headers.clone()),
            },
            OutboundMessage {
                subject: "test_custom.2".into(),
                payload: "also with headers".into(),
                headers: Some(headers),
            },
        ];

        let ack = jetstream
            .batch_publish_all()
            .publish(futures_stream::iter(messages))
            .await
            .unwrap();

        // Verify custom headers are preserved along with batch headers
        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some("test_consumer".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut batch_messages = consumer.messages().await.unwrap();

        // Check first message
        let msg1 = batch_messages.next().await.unwrap().unwrap();
        let headers1 = msg1.headers.as_ref().unwrap();
        assert_eq!(headers1.get("X-Custom").unwrap().as_str(), "custom-value");
        assert_eq!(headers1.get("X-Another").unwrap().as_str(), "another-value");
        assert_eq!(
            headers1.get("Nats-Batch-Id").unwrap().as_str(),
            &ack.batch_id
        );
        assert!(headers1.get("Nats-Batch-Commit").is_none());

        // Check second message (commit message)
        let msg2 = batch_messages.next().await.unwrap().unwrap();
        let headers2 = msg2.headers.as_ref().unwrap();
        assert_eq!(headers2.get("X-Custom").unwrap().as_str(), "custom-value");
        assert_eq!(headers2.get("X-Another").unwrap().as_str(), "another-value");
        assert_eq!(
            headers2.get("Nats-Batch-Id").unwrap().as_str(),
            &ack.batch_id
        );
        assert_eq!(headers2.get("Nats-Batch-Commit").unwrap().as_str(), "1");
    }
}
