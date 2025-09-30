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

//! Batch publishing support for NATS JetStream.
//!
//! This module provides functionality for publishing multiple messages as an atomic batch
//! to a JetStream stream. Batch publishing ensures that either all messages in a batch
//! are stored or none are.
//!
//! # Overview
//!
//! Batch publishing works by:
//! 1. Assigning internally a unique batch ID to all messages in a batch
//! 2. Publishing messages with special headers indicating batch membership
//! 3. Committing the batch with a final message containing a commit marker
//!
//! # Thread Safety
//!
//! The batch publisher types in this module are designed to be used from a single task/thread.
//! They do not implement `Send` or `Sync` as they maintain internal mutable state during
//! the batch publishing process.
//!
//! If you need to share a batch publisher across threads, you should:
//! - Use separate `BatchPublish` instances per thread
//! - Clone the underlying JetStream context (which is `Send + Sync + Clone`)
//! - Coordinate batch IDs externally if needed
//!
//! The underlying NATS client connection is thread-safe and can be shared across threads.
//!
//! # Usage Patterns
//!
//! ## Standard API
//!
//! Use [BatchPublish] when you need control over individual message publishing:
//!
//! ```no_run
//! # use jetstream_extra::batch_publish::BatchPublishExt;
//! # async fn example(client: impl BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
//! let mut batch = client.batch_publish().build();
//! batch.add("events.1", "data1".into()).await?;
//! batch.add("events.2", "data2".into()).await?;
//! let ack = batch.commit("events.3", "final".into()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Convenience API - Bulk Publishing
//!
//! Use [BatchPublishAllBuilder] when you have all messages ready:
//!
//! ```no_run
//! # use jetstream_extra::batch_publish::BatchPublishExt;
//! # use futures_util::stream;
//! # use async_nats::jetstream::message::OutboundMessage;
//! # async fn example(client: impl BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
//! let messages = vec![/* ... */];
//! let ack = client.batch_publish_all()
//!     .publish(stream::iter(messages))
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Flow Control
//!
//! Both APIs support flow control through acknowledgments:
//!
//! - `ack_first()` - Wait for server acknowledgment after the first message
//! - `ack_every(n)` - Wait for acknowledgment every N messages
//! - `timeout(duration)` - Set timeout for acknowledgment requests
//!
//! # Error Handling
//!
//! All operations return [BatchPublishError] with specific error kinds:
//!
//! - `BatchPublishNotEnabled` - Stream doesn't have `allow_atomic_publish` enabled
//! - `BatchPublishIncomplete` - Too many outstanding batches (server limit: 50)
//! - `BatchPublishUnsupportedHeader` - Message contains `Nats-Msg-Id` or `Nats-Expected-Last-Msg-Id`
//! - `MaxMessagesExceeded` - Batch exceeds 1000 message limit
//! - `EmptyBatch` - Attempting to commit an empty batch
//!
//! Server errors are automatically mapped to the appropriate error kind based on the error code.
//! Errors during `add` with flow control may indicate transient issues or configuration problems.

use futures_util::{Stream, StreamExt};
use std::{
    fmt::{Debug, Display},
    time::Duration,
};

use async_nats::{
    Request, client,
    jetstream::{self, message::OutboundMessage, response::Response},
    subject::ToSubject,
};
use serde::{Deserialize, de::DeserializeOwned};

/// Maximum number of messages allowed in a single batch (server limit)
const MAX_BATCH_SIZE: u64 = 1000;

pub trait BatchPublishExt:
    client::traits::Requester
    + client::traits::Publisher
    + jetstream::context::traits::TimeoutProvider
    + Clone
{
    fn batch_publish(&self) -> BatchPublishBuilder<Self>;
    fn batch_publish_all(&self) -> BatchPublishAllBuilder<Self>;
}

impl<C> BatchPublishExt for C
where
    C: client::traits::Requester
        + client::traits::Publisher
        + jetstream::context::traits::TimeoutProvider
        + Clone,
{
    fn batch_publish(&self) -> BatchPublishBuilder<Self> {
        BatchPublishBuilder::new(self.clone())
    }

    fn batch_publish_all(&self) -> BatchPublishAllBuilder<Self> {
        BatchPublishAllBuilder::new(self.clone())
    }
}

pub struct BatchPublishBuilder<C> {
    client: C,
    timeout: Duration,
    ack_first: bool,
    ack_every: Option<u64>,
}

impl<C> BatchPublishBuilder<C>
where
    C: client::traits::Requester
        + client::traits::Publisher
        + jetstream::context::traits::TimeoutProvider
        + Clone,
{
    pub fn new(context: C) -> Self {
        Self {
            client: context.clone(),
            ack_first: true,
            timeout: context.timeout(),
            ack_every: None,
        }
    }

    /// Configures acknowledgment for every N messages.
    /// That acknowledgement is used for flow-control purposes.
    /// It does not provide any actual acknowledgment data back.
    /// Those are only provided on the final commit message ack.
    pub fn ack_every(mut self, count: u64) -> Self {
        self.ack_every = Some(count);
        self
    }

    /// Enables acknowledgment for the first message in the batch.
    /// That acknowledgement is used for flow-control purposes.
    /// It does not provide any actual acknowledgment data back.
    /// Those are only provided on the final commit message ack.
    pub fn ack_first(mut self, ack_first: bool) -> Self {
        self.ack_first = ack_first;
        self
    }

    /// Sets the timeout for acknowledgment requests.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.timeout = duration;
        self
    }

    pub fn build(self) -> BatchPublish<C> {
        BatchPublish {
            context: self.client,
            sequence: 0,
            batch_id: nuid::next().to_string(),
            ack_every: self.ack_every,
            ack_first: self.ack_first,
            timeout: self.timeout,
        }
    }
}

pub struct BatchPublish<C> {
    pub context: C,
    pub sequence: u64,
    pub batch_id: String,
    ack_every: Option<u64>,
    ack_first: bool,
    timeout: Duration,
}

impl<C> BatchPublish<C>
where
    C: client::traits::Requester
        + client::traits::Publisher
        + jetstream::context::traits::TimeoutProvider
        + Clone,
{
    pub fn new(context: C, sequence: u64, batch_id: String) -> Self {
        Self {
            sequence,
            batch_id,
            timeout: context.timeout(),
            context,
            ack_first: true,
            ack_every: None,
        }
    }

    /// Get the current number of messages in the batch.
    ///
    /// This includes messages that have been added but excludes the final commit message
    /// until it is sent.
    pub fn size(&self) -> u64 {
        self.sequence
    }

    /// Add a message to the batch with the specified subject and payload.
    ///
    /// The message is sent immediately with batch headers. If flow control is configured
    /// (via `ack_first` or `ack_every`), this method may wait for acknowledgment from
    /// the server.
    ///
    /// # Errors
    ///
    /// Returns `MaxMessagesExceeded` if adding this message would exceed the server's
    /// batch size limit of 1000 messages.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use jetstream_extra::batch_publish::BatchPublishExt;
    /// # async fn example(client: impl BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut batch = client.batch_publish().build();
    /// batch.add("events.user.created", r#"{"id":123}"#.into()).await?;
    /// batch.add("events.user.updated", r#"{"id":123,"name":"Alice"}"#.into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add<S: ToSubject>(
        &mut self,
        subject: S,
        payload: bytes::Bytes,
    ) -> Result<(), BatchPublishError> {
        self.add_message(OutboundMessage {
            subject: subject.to_subject(),
            payload,
            headers: None,
        })
        .await
    }

    /// Add a pre-constructed message to the batch.
    ///
    /// This is useful when you need to include headers or have already constructed
    /// the [OutboundMessage].
    ///
    /// # Errors
    ///
    /// Returns `MaxMessagesExceeded` if adding this message would exceed the server's
    /// batch size limit of 1000 messages.
    ///
    /// Returns `BatchPublishUnsupportedHeader` if the message contains unsupported
    /// headers like `Nats-Msg-Id` or `Nats-Expected-Last-Msg-Id`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use jetstream_extra::batch_publish::BatchPublishExt;
    /// # use async_nats::jetstream::message::OutboundMessage;
    /// # async fn example(client: impl BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut batch = client.batch_publish().build();
    ///
    /// let message = OutboundMessage {
    ///     subject: "events.important".into(),
    ///     payload: "critical data".into(),
    ///     headers: None,
    /// };
    ///
    /// batch.add_message(message).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add_message(
        &mut self,
        mut message: jetstream::message::OutboundMessage,
    ) -> Result<(), BatchPublishError> {
        // Check for unsupported headers
        if let Some(headers) = &message.headers
            && (headers.get("Nats-Msg-Id").is_some()
                || headers.get("Nats-Expected-Last-Msg-Id").is_some())
        {
            return Err(BatchPublishError::new(
                BatchPublishErrorKind::BatchPublishUnsupportedHeader,
            ));
        }

        self.sequence += 1;

        if self.sequence > MAX_BATCH_SIZE {
            return Err(BatchPublishError::new(
                BatchPublishErrorKind::MaxMessagesExceeded,
            ));
        }
        self.add_header(&mut message);

        if let Some(ack_every) = self.ack_every
            && self.sequence.is_multiple_of(ack_every)
        {
            self.add_request(message).await?;
        } else if self.ack_first && self.sequence == 1 {
            self.add_request(message).await?;
        } else {
            self.context
                .publish_message(message.into())
                .await
                .map_err(|e| BatchPublishError::with_source(BatchPublishErrorKind::Publish, e))?;
        }
        Ok(())
    }

    /// Commit the batch with a final message.
    ///
    /// This sends the final message with batch headers and a commit marker,
    /// completing the batch operation. The batch cannot be used after committing.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use jetstream_extra::batch_publish::BatchPublishExt;
    /// # async fn example(client: impl BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut batch = client.batch_publish().build();
    ///
    /// batch.add("events.1", "data1".into()).await?;
    /// batch.add("events.2", "data2".into()).await?;
    ///
    /// // Commit with final message
    /// let ack = batch.commit("events.3", "data3".into()).await?;
    ///
    /// println!("Batch {} committed with {} messages", ack.batch_id, ack.batch_size);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn commit<S: ToSubject>(
        self,
        subject: S,
        payload: bytes::Bytes,
    ) -> Result<BatchPubAck, BatchPublishError> {
        self.commit_message(OutboundMessage {
            subject: subject.to_subject(),
            payload,
            headers: None,
        })
        .await
    }

    /// Commit the batch with a pre-constructed final message.
    ///
    /// Like [commit](Self::commit), but accepts a pre-constructed [OutboundMessage].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use jetstream_extra::batch_publish::BatchPublishExt;
    /// # use async_nats::jetstream::message::OutboundMessage;
    /// # async fn example(client: impl BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut batch = client.batch_publish().build();
    ///
    /// let final_message = OutboundMessage {
    ///     subject: "events.complete".into(),
    ///     payload: "batch done".into(),
    ///     headers: None,
    /// };
    ///
    /// let ack = batch.commit_message(final_message).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn commit_message(
        mut self,
        mut message: jetstream::message::OutboundMessage,
    ) -> Result<BatchPubAck, BatchPublishError> {
        // Check for unsupported headers
        if let Some(headers) = &message.headers
            && (headers.get("Nats-Msg-Id").is_some()
                || headers.get("Nats-Expected-Last-Msg-Id").is_some())
        {
            return Err(BatchPublishError::new(
                BatchPublishErrorKind::BatchPublishUnsupportedHeader,
            ));
        }

        self.sequence += 1;

        if self.sequence > MAX_BATCH_SIZE {
            return Err(BatchPublishError::new(
                BatchPublishErrorKind::MaxMessagesExceeded,
            ));
        }

        self.add_header(&mut message);
        // Headers are guaranteed to exist after add_header
        let headers = message
            .headers
            .get_or_insert_with(async_nats::HeaderMap::new);
        headers.insert("Nats-Batch-Commit", "1");

        self.commit_request(message).await
    }

    /// Discard the batch without committing.
    ///
    /// This consumes the batch without sending a commit message. The server will
    /// eventually abandon the batch after a timeout.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use jetstream_extra::batch_publish::BatchPublishExt;
    /// # async fn example(client: impl BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut batch = client.batch_publish().build();
    ///
    /// batch.add("events.1", "data".into()).await?;
    ///
    /// // Decide to abandon the batch
    /// batch.discard();
    /// # Ok(())
    /// # }
    /// ```
    pub fn discard(self) {
        // Dropping the batch without committing
    }

    fn add_header(&self, message: &mut jetstream::message::OutboundMessage) {
        let headers = message
            .headers
            .get_or_insert_with(async_nats::HeaderMap::new);
        headers.insert("Nats-Batch-Id", self.batch_id.clone());
        headers.insert("Nats-Batch-Sequence", self.sequence.to_string());
    }

    async fn add_request(&self, message: OutboundMessage) -> Result<(), BatchPublishError> {
        let request = Request {
            payload: Some(message.payload),
            headers: message.headers,
            timeout: Some(Some(self.timeout)),
            inbox: None,
        };
        let response = self
            .context
            .send_request(message.subject, request)
            .await
            .map_err(|e| BatchPublishError::with_source(BatchPublishErrorKind::Request, e))?;

        if response.payload.is_empty() {
            return Ok(());
        }

        let resp: Response<()> = serde_json::from_slice(response.payload.as_ref())
            .map_err(|e| BatchPublishError::with_source(BatchPublishErrorKind::Serialization, e))?;

        match resp {
            Response::Err { error } => {
                let kind = BatchPublishErrorKind::from_api_error(&error);
                Err(BatchPublishError::with_source(kind, error))
            }
            Response::Ok(()) => Ok(()),
        }
    }

    async fn commit_request<T: DeserializeOwned + Debug>(
        &self,
        message: OutboundMessage,
    ) -> Result<T, BatchPublishError> {
        let request = Request {
            payload: Some(message.payload),
            headers: message.headers,
            timeout: Some(Some(self.timeout)),
            inbox: None,
        };
        let response = self
            .context
            .send_request(message.subject, request)
            .await
            .map_err(|e| BatchPublishError::with_source(BatchPublishErrorKind::Request, e))?;

        let resp: Response<T> = serde_json::from_slice(response.payload.as_ref())
            .map_err(|e| BatchPublishError::with_source(BatchPublishErrorKind::Serialization, e))?;

        match resp {
            Response::Err { error } => {
                let kind = BatchPublishErrorKind::from_api_error(&error);
                Err(BatchPublishError::with_source(kind, error))
            }
            Response::Ok(ack) => Ok(ack),
        }
    }
}

/// Acknowledgment returned after successfully committing a batch.
///
/// Contains information about the committed batch including the stream it was
/// published to, the sequence number, and batch metadata.
#[derive(Debug, Deserialize)]
pub struct BatchPubAck {
    /// The stream the batch was published to.
    pub stream: String,
    /// The stream sequence number of the last message in the batch.
    #[serde(rename = "seq")]
    pub sequence: u64,
    /// The domain the stream belongs to, if any.
    #[serde(default)]
    pub domain: Option<String>,
    /// The unique identifier for the batch.
    #[serde(rename = "batch")]
    pub batch_id: String,
    /// The number of messages in the committed batch.
    #[serde(rename = "count")]
    pub batch_size: u64,
}

/// Builder for bulk publishing multiple messages at once
pub struct BatchPublishAllBuilder<C> {
    client: C,
    timeout: Duration,
    ack_first: bool,
    ack_every: Option<u64>,
}

impl<C> BatchPublishAllBuilder<C>
where
    C: client::traits::Requester
        + client::traits::Publisher
        + jetstream::context::traits::TimeoutProvider
        + Clone,
{
    pub fn new(client: C) -> Self {
        Self {
            client: client.clone(),
            ack_first: true,
            timeout: client.timeout(),
            ack_every: None,
        }
    }

    /// Configure acknowledgment for every N messages.
    ///
    /// See [BatchPublishBuilder::ack_every] for details.
    pub fn ack_every(mut self, count: u64) -> Self {
        self.ack_every = Some(count);
        self
    }

    /// Enable acknowledgment for the first message in the batch.
    ///
    /// See [BatchPublishBuilder::ack_first] for details.
    pub fn ack_first(mut self, ack_first: bool) -> Self {
        self.ack_first = ack_first;
        self
    }

    /// Set the timeout for acknowledgment requests.
    ///
    /// See [BatchPublishBuilder::timeout] for details.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.timeout = duration;
        self
    }

    /// Publish all messages from a stream and commit with the last message.
    ///
    /// This is a convenience method that internally uses `BatchPublish::add_message`
    /// for all messages except the last, and `BatchPublish::commit_message` for
    /// the last message.
    ///
    /// # Examples
    ///
    /// ## From a Vec
    /// ```no_run
    /// # use futures_util::stream;
    /// # use async_nats::jetstream::message::OutboundMessage;
    /// # async fn example(client: impl jetstream_extra::batch_publish::BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
    /// let messages = vec![
    ///     OutboundMessage {
    ///         subject: "test.1".into(),
    ///         payload: "msg1".into(),
    ///         headers: None,
    ///     },
    ///     OutboundMessage {
    ///         subject: "test.2".into(),
    ///         payload: "msg2".into(),
    ///         headers: None,
    ///     },
    /// ];
    /// let ack = client.batch_publish_all()
    ///     .ack_first(true)
    ///     .publish(stream::iter(messages))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## From an async channel
    /// ```no_run
    /// # use tokio_stream::wrappers::ReceiverStream;
    /// # use async_nats::jetstream::message::OutboundMessage;
    /// # use jetstream_extra::batch_publish::BatchPublishExt;
    /// # async fn example(client: impl BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
    /// let (tx, rx) = tokio::sync::mpsc::channel(100);
    ///
    /// tokio::spawn(async move {
    ///     for i in 0..10 {
    ///         let msg = OutboundMessage {
    ///             subject: format!("test.{}", i).into(),
    ///             payload: format!("Message {}", i).into(),
    ///             headers: None,
    ///         };
    ///         tx.send(msg).await.unwrap();
    ///     }
    /// });
    ///
    /// let ack = client.batch_publish_all()
    ///     .publish(ReceiverStream::new(rx))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## With stream transformations
    /// ```no_run
    /// # use futures_util::{stream, StreamExt};
    /// # use async_nats::jetstream::message::OutboundMessage;
    /// # async fn example(client: impl jetstream_extra::batch_publish::BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
    /// let data = vec!["apple", "banana", "cherry"];
    /// let message_stream = stream::iter(data)
    ///     .enumerate()
    ///     .map(|(i, fruit)| OutboundMessage {
    ///         subject: format!("fruits.{}", i).into(),
    ///         payload: fruit.into(),
    ///         headers: None,
    ///     });
    ///
    /// let ack = client.batch_publish_all()
    ///     .publish(message_stream)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## From async file reading
    /// ```no_run
    /// # use tokio::io::{AsyncBufReadExt, BufReader};
    /// # use futures_util::{stream, StreamExt};
    /// # use async_nats::jetstream::message::OutboundMessage;
    /// # use jetstream_extra::batch_publish::BatchPublishExt;
    /// # use std::time::Duration;
    /// # async fn example(client: impl BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
    /// // Read file lines into a vector first
    /// let file = tokio::fs::File::open("data.txt").await?;
    /// let reader = BufReader::new(file);
    /// let mut lines = reader.lines();
    /// let mut messages = Vec::new();
    ///
    /// while let Some(line) = lines.next_line().await? {
    ///     messages.push(OutboundMessage {
    ///         subject: "file.line".into(),
    ///         payload: line.into(),
    ///         headers: None,
    ///     });
    /// }
    ///
    /// let ack = client.batch_publish_all()
    ///     .timeout(Duration::from_secs(30))
    ///     .publish(stream::iter(messages))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Rate-limited publishing
    /// ```no_run
    /// # use futures_util::stream;
    /// # use async_nats::jetstream::message::OutboundMessage;
    /// # use jetstream_extra::batch_publish::BatchPublishExt;
    /// # use std::time::Duration;
    /// # async fn example(client: impl BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
    /// let messages = vec![
    ///     OutboundMessage {
    ///         subject: "test.1".into(),
    ///         payload: "msg1".into(),
    ///         headers: None,
    ///     },
    ///     // ... more messages
    /// ];
    ///
    /// // Note: For actual rate limiting, you would use tokio_stream::StreamExt::throttle
    /// // For now, showing the pattern with regular stream
    /// let message_stream = stream::iter(messages);
    ///
    /// let ack = client.batch_publish_all()
    ///     .timeout(Duration::from_secs(10))
    ///     .publish(message_stream)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## From multiple sources merged
    /// ```no_run
    /// # use futures_util::{stream, StreamExt};
    /// # use async_nats::jetstream::message::OutboundMessage;
    /// # async fn example(client: impl jetstream_extra::batch_publish::BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
    /// let source1 = stream::iter(vec![
    ///     OutboundMessage {
    ///         subject: "test.1".into(),
    ///         payload: "msg1".into(),
    ///         headers: None,
    ///     },
    /// ]);
    /// let source2 = stream::iter(vec![
    ///     OutboundMessage {
    ///         subject: "test.2".into(),
    ///         payload: "msg2".into(),
    ///         headers: None,
    ///     },
    /// ]);
    ///
    /// let merged = source1.chain(source2);
    ///
    /// let ack = client.batch_publish_all()
    ///     .publish(merged)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## With error handling in stream
    /// ```no_run
    /// # use futures_util::{stream, StreamExt};
    /// # use async_nats::jetstream::message::OutboundMessage;
    /// # async fn example(client: impl jetstream_extra::batch_publish::BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
    /// // First filter errors, then create stream from good messages
    /// let results: Vec<Result<OutboundMessage, &str>> = vec![
    ///     Ok(OutboundMessage {
    ///         subject: "test.1".into(),
    ///         payload: "msg1".into(),
    ///         headers: None,
    ///     }),
    ///     Err("simulated error"),
    ///     Ok(OutboundMessage {
    ///         subject: "test.2".into(),
    ///         payload: "msg2".into(),
    ///         headers: None,
    ///     }),
    /// ];
    ///
    /// // Filter out errors synchronously
    /// let good_messages: Vec<_> = results
    ///     .into_iter()
    ///     .filter_map(|result| match result {
    ///         Ok(msg) => Some(msg),
    ///         Err(e) => {
    ///             eprintln!("Skipping message: {}", e);
    ///             None
    ///         }
    ///     })
    ///     .collect();
    ///
    /// let ack = client.batch_publish_all()
    ///     .publish(stream::iter(good_messages))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## From arrays
    /// ```no_run
    /// # use futures_util::stream;
    /// # use async_nats::jetstream::message::OutboundMessage;
    /// # async fn example(client: impl jetstream_extra::batch_publish::BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
    /// let ack = client.batch_publish_all()
    ///     .publish(stream::iter([
    ///         OutboundMessage {
    ///             subject: "test.1".into(),
    ///             payload: "First".into(),
    ///             headers: None,
    ///         },
    ///         OutboundMessage {
    ///             subject: "test.2".into(),
    ///             payload: "Second".into(),
    ///             headers: None,
    ///         },
    ///     ]))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish<S>(self, messages: S) -> Result<BatchPubAck, BatchPublishError>
    where
        S: Stream<Item = OutboundMessage> + Unpin,
    {
        // Build regular batch with same configuration
        let mut batch = BatchPublish {
            context: self.client,
            sequence: 0,
            batch_id: nuid::next().to_string(),
            ack_every: self.ack_every,
            ack_first: self.ack_first,
            timeout: self.timeout,
        };

        // Buffer one message to identify the last
        let mut last_msg = None;
        futures_util::pin_mut!(messages);

        while let Some(msg) = messages.next().await {
            if let Some(prev) = last_msg.replace(msg) {
                batch.add_message(prev).await?;
            }
        }

        // Commit with the last message
        match last_msg {
            Some(msg) => batch.commit_message(msg).await,
            None => Err(BatchPublishError::new(BatchPublishErrorKind::EmptyBatch)),
        }
    }
}

/// Error type for batch publish operations
pub type BatchPublishError = async_nats::error::Error<BatchPublishErrorKind>;

/// Kinds of errors that can occur during batch publish operations
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BatchPublishErrorKind {
    /// Failed to send request to the server
    Request,
    /// Failed to publish message
    Publish,
    /// Failed to serialize or deserialize data
    Serialization,
    /// Batch is in an invalid state for the operation
    BatchFull,
    /// Exceeded maximum allowed messages in batch (server limit: 1000)
    MaxMessagesExceeded,
    /// Empty batch cannot be committed
    EmptyBatch,
    /// Batch publishing is not enabled on the stream (allow_atomic_publish must be true)
    BatchPublishNotEnabled,
    /// Batch publish is incomplete and was abandoned
    BatchPublishIncomplete,
    /// Batch uses unsupported headers (Nats-Msg-Id or Nats-Expected-Last-Msg-Id)
    BatchPublishUnsupportedHeader,
    /// Other unspecified error
    Other,
}

impl BatchPublishErrorKind {
    /// Map a JetStream API error to the appropriate batch publish error kind
    fn from_api_error(error: &async_nats::jetstream::Error) -> Self {
        use async_nats::jetstream::ErrorCode;
        match error.error_code() {
            ErrorCode::ATOMIC_PUBLISH_DISABLED => Self::BatchPublishNotEnabled,
            ErrorCode::ATOMIC_PUBLISH_INCOMPLETE_BATCH => Self::BatchPublishIncomplete,
            ErrorCode::ATOMIC_PUBLISH_UNSUPPORTED_HEADER => Self::BatchPublishUnsupportedHeader,
            ErrorCode::ATOMIC_PUBLISH_TOO_LARGE_BATCH => Self::MaxMessagesExceeded,
            _ => Self::Other,
        }
    }
}

impl Display for BatchPublishErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request => write!(f, "request failed"),
            Self::Publish => write!(f, "publish failed"),
            Self::Serialization => write!(f, "serialization/deserialization error"),
            Self::BatchFull => write!(f, "batch is full"),
            Self::MaxMessagesExceeded => write!(f, "batch exceeds server limit (1000 messages)"),
            Self::EmptyBatch => write!(f, "empty batch cannot be committed"),
            Self::BatchPublishNotEnabled => write!(f, "batch publishing not enabled on stream"),
            Self::BatchPublishIncomplete => {
                write!(f, "batch publish is incomplete and was abandoned")
            }
            Self::BatchPublishUnsupportedHeader => write!(
                f,
                "batch uses unsupported headers (Nats-Msg-Id or Nats-Expected-Last-Msg-Id)"
            ),
            Self::Other => write!(f, "other error"),
        }
    }
}
