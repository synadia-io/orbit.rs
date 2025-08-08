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

//! JetStream Publisher with controlled outstanding acknowledgments.
//!
//! This module provides a [`Publisher`] that can limit the number of
//! outstanding publish acknowledgments, helping to control memory usage
//! and prevent overwhelming the server.

use async_nats::jetstream::{
    self,
    context::{PublishAckFuture, PublishError},
};
use async_nats::{subject::ToSubject, HeaderMap};
use bytes::Bytes;
use std::fmt::{self, Display};
use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Semaphore, TryAcquireError};

/// Controls behavior when maximum in-flight publishes is reached.
#[derive(Debug, Clone, Copy)]
pub enum PublishMode {
    /// If number of outstanding acks reaches the limit, wait.
    Backpressure,
    /// Return an error immediately if number of outstanding acks is reached.
    Fail,
}

/// Error kinds for the publisher.
#[derive(Clone, Debug, PartialEq)]
pub enum PublisherErrorKind {
    /// To many in-flight publishes.
    MaxInflightReached,
    /// Error from underlying publish operation.
    Publish,
}

impl Display for PublisherErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaxInflightReached => write!(f, "maximum in-flight publishes reached"),
            Self::Publish => write!(f, "publish error"),
        }
    }
}

/// Errors that can occur when publishing with the rate-limited publisher.
pub type PublisherError = async_nats::error::Error<PublisherErrorKind>;

/// A JetStream publisher that controls the number of outstanding acknowledgments.
#[derive(Clone)]
pub struct Publisher {
    context: jetstream::Context,
    semaphore: Arc<Semaphore>,
    mode: PublishMode,
}

impl Publisher {
    /// Create a new publisher builder.
    pub fn builder(context: jetstream::Context) -> PublisherBuilder {
        PublisherBuilder::new(context)
    }

    /// Publish a message to a subject.
    ///
    /// Returns a future that resolves to the publish acknowledgment.
    /// The future will automatically release the semaphore permit when
    /// the acknowledgment is received or an error occurs.
    pub async fn publish<S: ToSubject>(
        &self,
        subject: S,
        payload: Bytes,
    ) -> Result<PermitGuardedAckFuture, PublisherError> {
        self.publish_with_headers(subject, HeaderMap::new(), payload)
            .await
    }

    /// Publish a message with headers to a subject.
    pub async fn publish_with_headers<S: ToSubject>(
        &self,
        subject: S,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Result<PermitGuardedAckFuture, PublisherError> {
        let permit = match self.mode {
            PublishMode::Backpressure => self.semaphore.clone().acquire_owned().await.unwrap(),
            PublishMode::Fail => {
                self.semaphore
                    .clone()
                    .try_acquire_owned()
                    .map_err(|e| match e {
                        TryAcquireError::NoPermits => {
                            PublisherError::new(PublisherErrorKind::MaxInflightReached)
                        }
                        TryAcquireError::Closed => unreachable!("semaphore should not be closed"),
                    })?
            }
        };

        let ack_future = self
            .context
            .publish_with_headers(subject, headers, payload)
            .await
            .map_err(|err| PublisherError::with_source(PublisherErrorKind::Publish, err))?;

        Ok(PermitGuardedAckFuture {
            inner: ack_future,
            _permit: permit,
        })
    }

    /// Get the number of unused publish slots.
    pub fn available_capacity(&self) -> usize {
        self.semaphore.available_permits()
    }
}

/// A future that holds a semaphore permit until the ack is received.
#[derive(Debug)]
pub struct PermitGuardedAckFuture {
    inner: PublishAckFuture,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl IntoFuture for PermitGuardedAckFuture {
    type Output = Result<async_nats::jetstream::publish::PublishAck, PublishError>;
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.inner.await })
    }
}

/// Builder for creating a [`Publisher`] with custom configuration.
pub struct PublisherBuilder {
    context: jetstream::Context,
    max_in_flight: usize,
    mode: PublishMode,
}

impl PublisherBuilder {
    /// Create a new builder with the given JetStream context.
    pub fn new(context: jetstream::Context) -> Self {
        Self {
            context,
            max_in_flight: 1000,
            mode: PublishMode::Backpressure,
        }
    }

    /// Set the maximum number of in-flight publishes.
    ///
    /// This controls how many publish operations can be outstanding
    /// (waiting for acknowledgment) at any given time.
    pub fn max_in_flight(mut self, max: usize) -> Self {
        self.max_in_flight = max;
        self
    }

    /// Set the behavior when the maximum in-flight limit is reached.
    pub fn mode(mut self, mode: PublishMode) -> Self {
        self.mode = mode;
        self
    }

    /// Build the [`Publisher`].
    pub fn build(self) -> Publisher {
        Publisher {
            context: self.context,
            semaphore: Arc::new(Semaphore::new(self.max_in_flight)),
            mode: self.mode,
        }
    }
}

