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

//! JetStream extensions for async-nats.
//!
//! This crate provides additional functionality for NATS JetStream that extends
//! the capabilities of the async-nats client.
//!
//! # Features
//!
//! ## Batch Publishing
//!
//! The [batch_publish] module provides atomic batch publishing capabilities:
//!
//! ```no_run
//! # use jetstream_extra::batch_publish::BatchPublishExt;
//! # async fn example(client: impl BatchPublishExt) -> Result<(), Box<dyn std::error::Error>> {
//! // Publish multiple messages as an atomic batch
//! let mut batch = client.batch_publish().build();
//! batch.add("events.1", "data1".into()).await?;
//! batch.add("events.2", "data2".into()).await?;
//! let ack = batch.commit("events.3", "final".into()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Batch Fetching
//!
//! The [batch_fetch] module provides efficient batch fetching of messages from streams
//! using the DIRECT.GET API:
//!
//! ```no_run
//! # use jetstream_extra::batch_fetch::BatchFetchExt;
//! # use futures::StreamExt;
//! # async fn example(context: async_nats::jetstream::Context) -> Result<(), Box<dyn std::error::Error>> {
//! // Fetch 100 messages from a stream
//! let mut messages = context
//!     .get_batch("my_stream")
//!     .batch(100)
//!     .send()
//!     .await?;
//!
//! while let Some(msg) = messages.next().await {
//!     let msg = msg?;
//!     println!("Message: {:?}", msg);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! See the [batch_publish] and [batch_fetch] modules for detailed documentation.

pub mod batch_fetch;
pub mod batch_publish;
