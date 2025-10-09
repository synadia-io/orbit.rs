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

//! NATS JetStream distributed counters.
//!
//! This crate provides support for distributed counters using NATS JetStream streams
//! configured with the `AllowMsgCounter` option.
//!
//! # Overview
//!
//! The counters module wraps JetStream streams configured with `AllowMsgCounter` to provide
//! distributed counters. Each subject in the stream represents a separate counter.
//!
//! Counters are tracked across multiple sources, allowing for aggregation and source history.
//! The module supports operations like incrementing/decrementing counters, loading current values,
//! and retrieving source contributions.
//!
//! ## Key Features
//!
//! - **Arbitrary Precision**: Uses `BigInt` for unlimited integer size
//! - **Source Tracking**: Track counter contributions from multiple streams
//! - **Batch Operations**: Efficiently fetch multiple counter values
//! - **Atomic Operations**: Server-side atomic increment/decrement operations
//!
//! ## Stream Requirements
//!
//! Streams must have the following configuration:
//! - `allow_message_counter: true` - Enables counter functionality
//! - `allow_direct: true` - Required for efficient counter reads
//!
//! # Quick Start
//!
//! ```no_run
//! use nats_counters::CounterExt;
//! use async_nats::jetstream::stream::Config;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Connect to NATS and create JetStream context
//! let client = async_nats::connect("localhost:4222").await?;
//! let js = async_nats::jetstream::new(client);
//!
//! // Create a counter-enabled stream
//! let config = Config {
//!     name: "COUNTERS".to_string(),
//!     subjects: vec!["counters.>".to_string()],
//!     allow_message_counter: true,
//!     allow_direct: true,
//!     ..Default::default()
//! };
//! js.create_stream(config).await?;
//!
//! // Get the counter
//! let counter = js.get_counter("COUNTERS").await?;
//!
//! // Increment a counter
//! let value = counter.add("counters.visits", 1).await?;
//! println!("Visits: {}", value);
//!
//! // Read current value
//! let current = counter.load("counters.visits").await?;
//! println!("Current visits: {}", current);
//! # Ok(())
//! # }
//! ```
//!
//! # Source Tracking
//!
//! When using stream sourcing, counters track contributions from each source stream:
//!
//! ```no_run
//! # async fn example(counter: &nats_counters::Counter) -> Result<(), Box<dyn std::error::Error>> {
//! // Get counter with source information
//! let entry = counter.get("counters.total").await?;
//! println!("Total: {}", entry.value);
//!
//! // Show breakdown by source
//! for (stream, subjects) in &entry.sources {
//!     for (subject, value) in subjects {
//!         println!("  {} contributed {} from {}", stream, value, subject);
//!     }
//! }
//! # Ok(())
//! # }

pub mod errors;
pub mod parser;

use num_bigint::BigInt;
use std::collections::HashMap;

pub use errors::{CounterError, CounterErrorKind, Result};

/// Header key used to store source contributions in counter messages.
pub const COUNTER_SOURCES_HEADER: &str = "Nats-Counter-Sources";

/// Header key used to indicate counter increment values.
pub const COUNTER_INCREMENT_HEADER: &str = "Nats-Incr";

/// Map of source streams to their subject-value contributions.
pub type CounterSources = HashMap<String, HashMap<String, BigInt>>;

/// Represents a counter's current state with full source history.
#[derive(Debug, Clone)]
pub struct Entry {
    /// The counter's subject name.
    pub subject: String,

    /// The current counter value.
    pub value: BigInt,

    /// Maps source identifiers to their subject-value contributions.
    pub sources: CounterSources,

    /// Most recent increment value for this entry.
    /// Useful for recounting and auditing purposes.
    pub increment: Option<BigInt>,
}

// Module for the extension trait
pub mod counter_ext;

// Module for the counter implementation
mod counter;

// Re-export main types
pub use counter::Counter;
pub use counter_ext::CounterExt;
