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

//! Basic counter operations example.
//!
//! This example demonstrates how to use NATS JetStream counters for
//! distributed counting operations.
//!
//! Run this example with:
//! ```sh
//! cargo run --example basic_counter
//! ```

use async_nats::jetstream;
use nats_counters::CounterExt;
use num_bigint::BigInt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to NATS server
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // Create or get a counter-enabled stream
    let stream_config = jetstream::stream::Config {
        name: "EVENTS_COUNTER".to_string(),
        subjects: vec!["events.*".to_string()],
        allow_message_counter: true,
        allow_direct: true,
        ..Default::default()
    };

    // Try to create the stream (ignore error if it already exists)
    let _ = js.create_stream(stream_config).await;

    // Get the counter
    let counter = js.get_counter("EVENTS_COUNTER").await?;

    println!("=== NATS JetStream Counter Example ===\n");

    // Increment counters for different event types
    println!("Recording events...");

    let clicks = counter.add("events.clicks", 5).await?;
    println!("  Clicks: +5 = {}", clicks);

    let views = counter.add("events.views", 100).await?;
    println!("  Views: +100 = {}", views);

    let errors = counter.add("events.errors", 2).await?;
    println!("  Errors: +2 = {}", errors);

    // Add more events
    println!("\nRecording more events...");

    let clicks = counter.add("events.clicks", 3).await?;
    println!("  Clicks: +3 = {}", clicks);

    let views = counter.add("events.views", 50).await?;
    println!("  Views: +50 = {}", views);

    // Decrement errors (correction)
    let errors = counter.add("events.errors", BigInt::from(-1)).await?;
    println!("  Errors: -1 = {}", errors);

    // Load current values
    println!("\nCurrent totals:");
    println!("  Total clicks: {}", counter.load("events.clicks").await?);
    println!("  Total views: {}", counter.load("events.views").await?);
    println!("  Total errors: {}", counter.load("events.errors").await?);

    // Get full entry with metadata
    println!("\nDetailed entry for clicks:");
    let entry = counter.get("events.clicks").await?;
    println!("  Subject: {}", entry.subject);
    println!("  Value: {}", entry.value);
    if let Some(increment) = entry.increment {
        println!("  Last increment: {}", increment);
    }
    if !entry.sources.is_empty() {
        println!("  Sources: {:?}", entry.sources);
    }

    println!("\n=== Counter operations completed ===");

    Ok(())
}
