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

//! Batch fetch operations for JetStream streams.
//!
//! This module provides efficient batch fetching of messages from JetStream streams
//! using the DIRECT.GET API as specified in ADR-31.
//!
//! # Examples
//!
//! ## Fetch a batch of messages
//!
//! ```no_run
//! # use jetstream_extra::batch_fetch::BatchFetchExt;
//! # use futures::StreamExt;
//! # async fn example(context: async_nats::jetstream::Context) -> Result<(), Box<dyn std::error::Error>> {
//! use jetstream_extra::batch_fetch::BatchFetchExt;
//!
//! // Fetch 100 messages starting from sequence 1
//! let mut messages = context
//!     .get_batch("my_stream", 100)
//!     .send()
//!     .await?;
//!
//! while let Some(msg) = messages.next().await {
//!     let msg = msg?;
//!     println!("Message at seq {}: {:?}", msg.sequence, msg.payload);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Get last messages for multiple subjects
//!
//! ```no_run
//! # use jetstream_extra::batch_fetch::BatchFetchExt;
//! # use futures::StreamExt;
//! # async fn example(context: async_nats::jetstream::Context) -> Result<(), Box<dyn std::error::Error>> {
//! use jetstream_extra::batch_fetch::BatchFetchExt;
//!
//! // Get the last message for each subject
//! let subjects = vec!["events.user.1".to_string(), "events.user.2".to_string()];
//! let mut messages = context
//!     .get_last_messages_for("my_stream")
//!     .subjects(subjects)
//!     .send()
//!     .await?;
//!
//! while let Some(msg) = messages.next().await {
//!     let msg = msg?;
//!     println!("Last message for {}: {:?}", msg.subject, msg.payload);
//! }
//! # Ok(())
//! # }
//! ```

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::StreamMessage;
use async_nats::jetstream::context::traits::{ClientProvider, RequestSender, TimeoutProvider};
use async_nats::{Message, Subject, Subscriber};
use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt};
use serde::Serialize;
use time::OffsetDateTime;
use time::serde::rfc3339;
use tracing::debug;

// State types for compile-time mutual exclusivity
pub struct NoSeq;
pub struct WithSeq;
pub struct NoTime;
pub struct WithTime;

/// Builder for batch fetching messages from a stream.
pub struct GetBatchBuilder<T, SEQ = NoSeq, TIME = NoTime>
where
    T: ClientProvider + TimeoutProvider + RequestSender,
{
    context: T,
    stream: String,
    batch: usize,
    seq: Option<u64>,
    subject: Option<String>,
    max_bytes: Option<usize>,
    start_time: Option<OffsetDateTime>,
    _phantom: PhantomData<(SEQ, TIME)>,
}

/// Builder for fetching last messages for multiple subjects.
pub struct GetLastBuilder<T, SEQ = NoSeq, TIME = NoTime>
where
    T: ClientProvider + TimeoutProvider + RequestSender,
{
    context: T,
    stream: String,
    subjects: Option<Vec<String>>,
    up_to_seq: Option<u64>,
    up_to_time: Option<OffsetDateTime>,
    batch: Option<usize>,
    _phantom: PhantomData<(SEQ, TIME)>,
}

/// Extension trait for batch fetching messages from JetStream streams.
pub trait BatchFetchExt: ClientProvider + TimeoutProvider + RequestSender + Clone {
    /// Create a builder for fetching a batch of messages from a stream.
    ///
    /// Uses the DIRECT.GET API to efficiently retrieve multiple messages
    /// in a single request. The server sends messages without flow control
    /// up to the specified batch size or max_bytes limit.
    fn get_batch(&self, stream: &str, batch: usize) -> GetBatchBuilder<Self, NoSeq, NoTime>;

    /// Create a builder for fetching the last messages for multiple subjects.
    ///
    /// Retrieves the most recent message for each of the specified subjects
    /// from the stream. Supports consistent point-in-time reads across
    /// multiple subjects using `up_to_seq` or `up_to_time` options.
    fn get_last_messages_for(&self, stream: &str) -> GetLastBuilder<Self, NoSeq, NoTime>;
}

/// Error type for batch fetch operations.
pub type BatchFetchError = async_nats::error::Error<BatchFetchErrorKind>;

/// Kinds of errors that can occur during batch fetch operations.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BatchFetchErrorKind {
    /// The server does not support batch get operations.
    UnsupportedByServer,
    NoMessages,
    InvalidResponse,
    Serialization,
    Subscription,
    Publish,
    MissingHeader,
    InvalidHeader,
    InvalidRequest,
    TooManySubjects,
    BatchSizeTooLarge,
    BatchSizeRequired,
    SubjectsRequired,
    InvalidStreamName,
    InvalidOption,
    TimedOut,
    Other,
}

impl std::fmt::Display for BatchFetchErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedByServer => write!(f, "batch get not supported by server"),
            Self::NoMessages => write!(f, "no messages found"),
            Self::InvalidResponse => write!(f, "invalid response from server"),
            Self::Serialization => write!(f, "serialization error"),
            Self::Subscription => write!(f, "subscription error"),
            Self::Publish => write!(f, "publish error"),
            Self::MissingHeader => write!(f, "missing required header"),
            Self::InvalidHeader => write!(f, "invalid header value"),
            Self::InvalidRequest => write!(f, "invalid request parameters"),
            Self::TooManySubjects => write!(f, "too many subjects (max 1024)"),
            Self::BatchSizeTooLarge => write!(f, "batch size too large (max 1000)"),
            Self::BatchSizeRequired => write!(f, "batch size is required"),
            Self::SubjectsRequired => write!(f, "subjects are required for multi_last"),
            Self::InvalidStreamName => write!(f, "invalid stream name"),
            Self::InvalidOption => write!(f, "invalid option"),
            Self::TimedOut => write!(f, "batch fetch operation timed out"),
            Self::Other => write!(f, "batch fetch error"),
        }
    }
}

/// Request for batch get operations
#[derive(Debug, Serialize)]
struct GetBatchRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_by_subj: Option<String>,
    batch: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none", with = "rfc3339::option")]
    start_time: Option<time::OffsetDateTime>,
}

/// Request for multi-last get operations
#[derive(Debug, Serialize)]
struct GetLastRequest {
    multi_last: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    up_to_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", with = "rfc3339::option")]
    up_to_time: Option<time::OffsetDateTime>,
}

/// Stream of messages from batch fetch operations.
pub struct BatchStream {
    subscriber: Subscriber,
    timeout: std::time::Duration,
    timeout_at: Option<Pin<Box<tokio::time::Sleep>>>,
    terminated: bool,
}

impl Stream for BatchStream {
    type Item = Result<StreamMessage, BatchFetchError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        let timeout = self.timeout;
        match self
            .timeout_at
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)))
            .poll_unpin(cx)
        {
            Poll::Ready(_) => {
                debug!("Batch fetch operation timed out after {:?}", timeout);
                self.terminated = true;
                return Poll::Ready(Some(Err(BatchFetchError::new(
                    BatchFetchErrorKind::TimedOut,
                ))));
            }
            Poll::Pending => {}
        }

        match self.subscriber.next().poll_unpin(cx) {
            Poll::Ready(Some(msg)) => {
                // Check for End-Of-Batch marker
                // EOB can be detected in two ways:
                // 1. ADR-31 spec: Empty payload with Status: 204, Description: EOB
                // 2. Current server impl: Empty payload with missing essential headers
                if msg.payload.is_empty()
                    && let Some(headers) = &msg.headers
                {
                    let status = headers.get("Status").map(|v| v.as_str());
                    let desc = headers.get("Description").map(|v| v.as_str());

                    // Termination by EOB.
                    if status == Some("204") && desc == Some("EOB") {
                        self.terminated = true;
                        return Poll::Ready(None);
                    }

                    // Termination by empty message with headers missing.
                    if headers.is_empty() {
                        self.terminated = true;
                        return Poll::Ready(None);
                    }

                    // Termination by end of batch.
                    // TODO(jrm): we should consider hinting those to the user.
                    if headers.get(async_nats::header::NATS_SEQUENCE).is_some()
                        || headers.get("Nats-Num-Pending").is_some()
                        || headers.get("Nats-UpTo-Sequence").is_some()
                    {
                        self.terminated = true;
                        return Poll::Ready(None);
                    }
                }

                match convert_to_stream_message(msg) {
                    Ok(raw_msg) => Poll::Ready(Some(Ok(raw_msg))),
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            Poll::Ready(None) => {
                self.terminated = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> BatchFetchExt for T
where
    T: ClientProvider + TimeoutProvider + RequestSender + Clone + Send + Sync,
{
    fn get_batch(&self, stream: &str, batch: usize) -> GetBatchBuilder<Self, NoSeq, NoTime> {
        GetBatchBuilder {
            context: self.clone(),
            stream: stream.to_string(),
            batch,
            seq: None,
            subject: None,
            max_bytes: None,
            start_time: None,
            _phantom: PhantomData,
        }
    }

    fn get_last_messages_for(&self, stream: &str) -> GetLastBuilder<Self, NoSeq, NoTime> {
        GetLastBuilder {
            context: self.clone(),
            stream: stream.to_string(),
            subjects: None,
            up_to_seq: None,
            up_to_time: None,
            batch: None,
            _phantom: PhantomData,
        }
    }
}

// Implementation for all states - common methods
impl<T, SEQ, TIME> GetBatchBuilder<T, SEQ, TIME>
where
    T: ClientProvider + TimeoutProvider + RequestSender + Clone + Send + Sync,
{
    /// Set the subject filter (may include wildcards).
    pub fn subject<S: Into<String>>(mut self, subject: S) -> Self {
        self.subject = Some(subject.into());
        self
    }

    /// Set the maximum bytes to return.
    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }
}

// Methods only available when no time has been set
impl<T> GetBatchBuilder<T, NoSeq, NoTime>
where
    T: ClientProvider + TimeoutProvider + RequestSender + Clone + Send + Sync,
{
    /// Set the starting sequence number.
    /// This is mutually exclusive with `start_time`.
    pub fn sequence(mut self, seq: u64) -> GetBatchBuilder<T, WithSeq, NoTime> {
        self.seq = Some(seq);
        GetBatchBuilder {
            context: self.context,
            stream: self.stream,
            seq: self.seq,
            batch: self.batch,
            subject: self.subject,
            max_bytes: self.max_bytes,
            start_time: self.start_time,
            _phantom: PhantomData,
        }
    }

    /// Set the start time for time-based fetching.
    /// This is mutually exclusive with `seq`.
    pub fn start_time(mut self, start_time: OffsetDateTime) -> GetBatchBuilder<T, NoSeq, WithTime> {
        self.start_time = Some(start_time);
        GetBatchBuilder {
            context: self.context,
            stream: self.stream,
            batch: self.batch,
            seq: self.seq,
            subject: self.subject,
            max_bytes: self.max_bytes,
            start_time: self.start_time,
            _phantom: PhantomData,
        }
    }
}

// Additional methods for WithSeq state
impl<T> GetBatchBuilder<T, WithSeq, NoTime>
where
    T: ClientProvider + TimeoutProvider + RequestSender + Clone + Send + Sync,
{
    // seq() and start_time() are not available in this state
}

// Additional methods for WithTime state
impl<T> GetBatchBuilder<T, NoSeq, WithTime>
where
    T: ClientProvider + TimeoutProvider + RequestSender + Clone + Send + Sync,
{
    // seq() and start_time() are not available in this state
}

// Send method available for all states
impl<T, SEQ, TIME> GetBatchBuilder<T, SEQ, TIME>
where
    T: ClientProvider + TimeoutProvider + RequestSender + Clone + Send + Sync,
{
    /// Send the batch fetch request and return a stream of messages.
    pub async fn send(self) -> Result<BatchStream, BatchFetchError> {
        // Validate stream name
        if self.stream.is_empty() {
            return Err(BatchFetchError::new(BatchFetchErrorKind::InvalidStreamName));
        }

        // Validate batch size against server limit
        if self.batch > 1000 {
            return Err(BatchFetchError::new(BatchFetchErrorKind::BatchSizeTooLarge));
        }

        // Validate seq if specified (must be > 0)
        if let Some(seq) = self.seq
            && seq == 0
        {
            return Err(BatchFetchError::new(BatchFetchErrorKind::InvalidOption));
        }

        // Validate max_bytes if specified (must be > 0)
        if let Some(max_bytes) = self.max_bytes
            && max_bytes == 0
        {
            return Err(BatchFetchError::new(BatchFetchErrorKind::InvalidOption));
        }

        // Build the batch request per ADR-31
        let request = GetBatchRequest {
            seq: if self.seq.is_some() {
                self.seq
            } else if self.start_time.is_none() {
                Some(1) // Default to sequence 1 if neither seq nor start_time is specified
            } else {
                None
            },
            next_by_subj: self.subject,
            batch: self.batch,
            max_bytes: self.max_bytes,
            start_time: self.start_time,
        };

        let payload = serde_json::to_vec(&request)
            .map_err(|e| BatchFetchError::with_source(BatchFetchErrorKind::Serialization, e))?
            .into();
        // RequestSender will add the proper prefix ($JS.API. or custom)
        let subject = format!("DIRECT.GET.{}", self.stream);

        send_batch_request(&self.context, subject, payload).await
    }
}

// Implementation for all states - common methods
impl<T, SEQ, TIME> GetLastBuilder<T, SEQ, TIME>
where
    T: ClientProvider + TimeoutProvider + RequestSender + Clone + Send + Sync,
{
    /// Set the subjects to fetch last messages for.
    ///
    /// # Limits
    /// - Maximum: 1024 subjects per request (server limit)
    /// - Returns `BatchFetchErrorKind::TooManySubjects` if exceeded
    pub fn subjects(mut self, subjects: Vec<String>) -> Self {
        self.subjects = Some(subjects);
        self
    }

    /// Set the optional batch size.
    ///
    /// # Limits
    /// - Maximum: 1000 messages per request (server limit)
    /// - Returns `BatchFetchErrorKind::BatchSizeTooLarge` if exceeded
    pub fn batch(mut self, batch: usize) -> Self {
        self.batch = Some(batch);
        self
    }
}

// Methods only available when no up_to_seq has been set
impl<T> GetLastBuilder<T, NoSeq, NoTime>
where
    T: ClientProvider + TimeoutProvider + RequestSender + Clone + Send + Sync,
{
    /// Set the sequence number to fetch up to (inclusive).
    /// This is mutually exclusive with `up_to_time`.
    pub fn up_to_seq(mut self, seq: u64) -> GetLastBuilder<T, WithSeq, NoTime> {
        self.up_to_seq = Some(seq);
        GetLastBuilder {
            context: self.context,
            stream: self.stream,
            subjects: self.subjects,
            up_to_seq: self.up_to_seq,
            up_to_time: self.up_to_time,
            batch: self.batch,
            _phantom: PhantomData,
        }
    }

    /// Set the time to fetch up to.
    /// This is mutually exclusive with `up_to_seq`.
    pub fn up_to_time(mut self, time: OffsetDateTime) -> GetLastBuilder<T, NoSeq, WithTime> {
        self.up_to_time = Some(time);
        GetLastBuilder {
            context: self.context,
            stream: self.stream,
            subjects: self.subjects,
            up_to_seq: self.up_to_seq,
            up_to_time: self.up_to_time,
            batch: self.batch,
            _phantom: PhantomData,
        }
    }
}

// Additional methods for WithSeq state
impl<T> GetLastBuilder<T, WithSeq, NoTime>
where
    T: ClientProvider + TimeoutProvider + RequestSender + Clone + Send + Sync,
{
    // up_to_seq() and up_to_time() are not available in this state
}

// Additional methods for WithTime state
impl<T> GetLastBuilder<T, NoSeq, WithTime>
where
    T: ClientProvider + TimeoutProvider + RequestSender + Clone + Send + Sync,
{
    // up_to_seq() and up_to_time() are not available in this state
}

// Send method available for all states
impl<T, SEQ, TIME> GetLastBuilder<T, SEQ, TIME>
where
    T: ClientProvider + TimeoutProvider + RequestSender + Clone + Send + Sync,
{
    /// Send the request to get last messages and return a stream.
    pub async fn send(self) -> Result<BatchStream, BatchFetchError> {
        if self.stream.is_empty() {
            return Err(BatchFetchError::new(BatchFetchErrorKind::InvalidStreamName));
        }

        let subjects = self
            .subjects
            .ok_or_else(|| BatchFetchError::new(BatchFetchErrorKind::SubjectsRequired))?;

        // Validate subject count against server limit
        if subjects.len() > 1024 {
            return Err(BatchFetchError::new(BatchFetchErrorKind::TooManySubjects));
        }

        // Validate batch size if specified
        if let Some(batch) = self.batch {
            if batch == 0 {
                return Err(BatchFetchError::new(BatchFetchErrorKind::InvalidOption));
            }
            if batch > 1000 {
                return Err(BatchFetchError::new(BatchFetchErrorKind::BatchSizeTooLarge));
            }
        }

        if subjects.is_empty() {
            return Err(BatchFetchError::new(BatchFetchErrorKind::SubjectsRequired));
        }

        let request = GetLastRequest {
            multi_last: subjects,
            batch: self.batch,
            up_to_seq: self.up_to_seq,
            up_to_time: self.up_to_time,
        };

        let payload = serde_json::to_vec(&request)
            .map_err(|e| BatchFetchError::with_source(BatchFetchErrorKind::Serialization, e))?
            .into();
        let subject = format!("DIRECT.GET.{}", self.stream);

        send_batch_request(&self.context, subject, payload).await
    }
}

async fn send_batch_request<T>(
    context: &T,
    subject: String,
    payload: Bytes,
) -> Result<BatchStream, BatchFetchError>
where
    T: ClientProvider + TimeoutProvider + RequestSender,
{
    // Create inbox and subscribe to it for responses
    let client = context.client();
    let inbox = client.new_inbox();
    let subscriber = client
        .subscribe(inbox.clone())
        .await
        .map_err(|e| BatchFetchError::with_source(BatchFetchErrorKind::Subscription, e))?;

    let request = async_nats::Request {
        inbox: Some(inbox),
        payload: Some(payload),
        headers: None,
        timeout: None,
    };
    context
        .send_request(subject, request)
        .await
        .map_err(|e| BatchFetchError::with_source(BatchFetchErrorKind::Publish, e))?;

    Ok(BatchStream {
        subscriber,
        terminated: false,
        timeout: context.timeout(),
        timeout_at: None,
    })
}

fn convert_to_stream_message(msg: Message) -> Result<StreamMessage, BatchFetchError> {
    if msg.payload.is_empty()
        && let Some(headers) = &msg.headers
    {
        let status = headers.get("Status").map(|v| v.as_str());
        match status {
            Some("404") => return Err(BatchFetchError::new(BatchFetchErrorKind::NoMessages)),
            Some("408") => return Err(BatchFetchError::new(BatchFetchErrorKind::InvalidRequest)),
            Some("413") => return Err(BatchFetchError::new(BatchFetchErrorKind::TooManySubjects)),
            _ => {}
        }
    }

    let headers = msg
        .headers
        .ok_or_else(|| BatchFetchError::new(BatchFetchErrorKind::InvalidResponse))?;

    // Check if server supports batch get by looking for Nats-Num-Pending header
    // Servers that don't support batch get won't include this header
    if headers.get("Nats-Num-Pending").is_none() {
        return Err(BatchFetchError::new(
            BatchFetchErrorKind::UnsupportedByServer,
        ));
    }

    let subject = headers
        .get(async_nats::header::NATS_SUBJECT)
        .ok_or_else(|| BatchFetchError::new(BatchFetchErrorKind::MissingHeader))?
        .to_string();

    let sequence = headers
        .get(async_nats::header::NATS_SEQUENCE)
        .ok_or_else(|| BatchFetchError::new(BatchFetchErrorKind::MissingHeader))?
        .as_str()
        .parse::<u64>()
        .map_err(|e| BatchFetchError::with_source(BatchFetchErrorKind::InvalidHeader, e))?;

    let time_str = headers
        .get(async_nats::header::NATS_TIME_STAMP)
        .ok_or_else(|| BatchFetchError::new(BatchFetchErrorKind::MissingHeader))?
        .as_str();

    // Parse RFC3339 timestamp
    let time =
        time::OffsetDateTime::parse(time_str, &time::format_description::well_known::Rfc3339)
            .map_err(|e| BatchFetchError::with_source(BatchFetchErrorKind::InvalidHeader, e))?;

    Ok(StreamMessage {
        subject: Subject::from(subject),
        sequence,
        payload: msg.payload,
        headers,
        time,
    })
}
