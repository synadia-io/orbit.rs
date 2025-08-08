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

use async_nats::jetstream::{self, context::{PublishAckFuture, PublishError}};
use async_nats::{HeaderMap, subject::ToSubject};
use bytes::Bytes;
use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Semaphore, TryAcquireError};

/// Controls behavior when maximum in-flight publishes is reached.
#[derive(Debug, Clone, Copy)]
pub enum PublishMode {
    /// Wait for a permit to become available.
    WaitForPermit,
    /// Return an error immediately if no permits are available.
    ErrorOnFull,
}

/// Errors that can occur when publishing with the rate-limited publisher.
#[derive(Debug, thiserror::Error)]
pub enum PublisherError {
    /// No permits available and mode is set to ErrorOnFull.
    #[error("maximum in-flight publishes reached")]
    NoPermitsAvailable,
    /// Error from underlying publish operation.
    #[error(transparent)]
    PublishError(#[from] PublishError),
}

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
            PublishMode::WaitForPermit => self.semaphore.clone().acquire_owned().await.unwrap(),
            PublishMode::ErrorOnFull => self
                .semaphore
                .clone()
                .try_acquire_owned()
                .map_err(|e| match e {
                    TryAcquireError::NoPermits => PublisherError::NoPermitsAvailable,
                    TryAcquireError::Closed => unreachable!("semaphore should not be closed"),
                })?,
        };

        let ack_future = self
            .context
            .publish_with_headers(subject, headers, payload)
            .await?;

        Ok(PermitGuardedAckFuture {
            inner: ack_future,
            _permit: permit,
        })
    }

    /// Get the number of available permits (unused publish slots).
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

/// A future that holds a semaphore permit until the ack is received.
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
            max_in_flight: 100,
            mode: PublishMode::WaitForPermit,
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_nats::jetstream;

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
            .mode(PublishMode::WaitForPermit)
            .build();

        // First two publishes should succeed immediately
        let ack1 = publisher.publish("test.1", "msg1".into()).await.unwrap();
        let ack2 = publisher.publish("test.2", "msg2".into()).await.unwrap();

        assert_eq!(publisher.available_permits(), 0);

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
            .mode(PublishMode::ErrorOnFull)
            .build();

        // First publish should succeed
        let _ack1 = publisher.publish("test.1", "msg1".into()).await.unwrap();

        // Second publish should error immediately
        let result = publisher.publish("test.2", "msg2".into()).await;
        assert!(matches!(result, Err(PublisherError::NoPermitsAvailable)));
    }

    #[tokio::test]
    async fn test_permit_release_on_ack_error() {
        let (_server, _client, context) = setup_jetstream().await;

        let publisher = Publisher::builder(context)
            .max_in_flight(1)
            .mode(PublishMode::ErrorOnFull)
            .build();

        // Publish to a subject not covered by the stream
        let ack = publisher.publish("wrong.subject", "msg".into()).await.unwrap();

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
            .mode(PublishMode::WaitForPermit)
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
}