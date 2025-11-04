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

//! JetStream counter implementation.

use crate::{
    COUNTER_INCREMENT_HEADER, Entry, Result,
    errors::{CounterError, CounterErrorKind},
    parser::{
        parse_counter_value, parse_counter_value_from_string, parse_increment, parse_sources,
    },
};
use async_nats::{
    HeaderMap,
    jetstream::{self, stream::Stream},
    subject::ToSubject,
};
use futures_util::{Stream as FutureStream, TryStreamExt};
use jetstream_extra::batch_fetch::BatchFetchExt;
use num_bigint::BigInt;
use std::pin::Pin;

/// Implementation of distributed counters using a JetStream stream.
pub struct Counter {
    stream: Stream,
    context: jetstream::Context,
}

impl Counter {
    /// Creates a counter from an existing stream.
    pub async fn from_stream(context: jetstream::Context, mut stream: Stream) -> Result<Self> {
        let info = stream
            .info()
            .await
            .map_err(|e| CounterError::with_source(CounterErrorKind::Request, e))?;

        if !info.config.allow_message_counter {
            return Err(CounterError::new(CounterErrorKind::CounterNotEnabled));
        }

        if !info.config.allow_direct {
            return Err(CounterError::new(CounterErrorKind::DirectAccessRequired));
        }

        Ok(Self { stream, context })
    }

    /// Adds a value to the counter for the given subject.
    pub async fn add<S, V>(&self, subject: S, value: V) -> Result<BigInt>
    where
        S: ToSubject,
        V: Into<BigInt> + Send,
    {
        let value = value.into();

        let mut headers = HeaderMap::new();
        headers.insert(COUNTER_INCREMENT_HEADER, value.to_string());

        let ack = self
            .context
            .publish_with_headers(subject, headers, vec![].into())
            .await
            .map_err(|e| CounterError::with_source(CounterErrorKind::Publish, e))?
            .await
            .map_err(|e| CounterError::with_source(CounterErrorKind::Publish, e))?;

        parse_counter_value_from_string(ack.value)
    }

    /// Gets the counter entry for a subject, including sources and last increment.
    ///
    /// Returns complete information about a counter including its current value,
    /// source breakdown (if using stream sourcing), and the last increment value.
    pub async fn get<S: ToSubject>(&self, subject: S) -> Result<Entry> {
        let subject_str = subject.to_subject();

        let msg = self
            .stream
            .get_last_raw_message_by_subject(&subject_str)
            .await
            .map_err(|e| match e.kind() {
                jetstream::stream::LastRawMessageErrorKind::NoMessageFound => {
                    CounterError::new(CounterErrorKind::NoCounterForSubject)
                }
                _ => CounterError::with_source(CounterErrorKind::Stream, e),
            })?;

        let value = parse_counter_value(&msg.payload)?;
        let sources = parse_sources(&msg.headers)?;
        let increment = parse_increment(&msg.headers)?;

        Ok(Entry {
            subject: subject_str.to_string(),
            value,
            sources,
            increment,
        })
    }

    /// Loads just the counter value for a subject.
    ///
    /// This is a convenience method that returns only the value,
    /// without sources or increment information.
    pub async fn load<S: ToSubject>(&self, subject: S) -> Result<BigInt> {
        Ok(self.get(subject).await?.value)
    }

    /// Increments the counter by 1 for the given subject.
    /// This is a convenience method for the common case of incrementing by 1.
    pub async fn increment<S>(&self, subject: S) -> Result<BigInt>
    where
        S: ToSubject,
    {
        self.add(subject, 1).await
    }

    /// Decrements the counter by 1 for the given subject.
    /// This is a convenience method for the common case of decrementing by 1.
    pub async fn decrement<S>(&self, subject: S) -> Result<BigInt>
    where
        S: ToSubject,
    {
        self.add(subject, -1).await
    }

    /// Gets multiple counter entries matching the given subjects.
    ///
    /// Efficiently fetches multiple counters in a batch operation.
    /// Returns a stream of entries.
    ///
    /// Note: Subjects should be exact subjects, not patterns. Non-existent
    /// counters are silently skipped in the results.
    pub async fn get_multiple(
        &self,
        subjects: Vec<String>,
    ) -> Result<Pin<Box<dyn FutureStream<Item = Result<Entry>> + Send + '_>>> {
        // Validate subjects
        if subjects.is_empty() {
            return Err(CounterError::new(CounterErrorKind::InvalidCounterValue));
        }

        for subject in &subjects {
            if subject.is_empty() {
                return Err(CounterError::new(CounterErrorKind::InvalidCounterValue));
            }
        }

        let stream_name = self.stream.cached_info().config.name.clone();

        let messages = self
            .context
            .get_last_messages_for(&stream_name)
            .subjects(subjects)
            .send()
            .await
            .map_err(|e| CounterError::with_source(CounterErrorKind::Stream, e))?;

        let entries = messages
            .map_err(|e| CounterError::with_source(CounterErrorKind::Stream, e))
            .and_then(|msg| async move {
                let value = parse_counter_value(&msg.payload)?;
                let sources = parse_sources(&msg.headers)?;
                let increment = parse_increment(&msg.headers)?;

                Ok(Entry {
                    subject: msg.subject.to_string(),
                    value,
                    sources,
                    increment,
                })
            });

        Ok(Box::pin(entries)
            as Pin<
                Box<dyn FutureStream<Item = Result<Entry>> + Send + '_>,
            >)
    }
}
