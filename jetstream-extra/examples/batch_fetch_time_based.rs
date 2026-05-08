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

use async_nats::jetstream::{self, stream};
use futures::StreamExt;
use jetstream_extra::batch_fetch::BatchFetchExt;
use std::time::{Duration, SystemTime};
use time::OffsetDateTime;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to NATS server
    let client = async_nats::connect("localhost:4222").await?;
    let context = jetstream::new(client);

    // Create a stream
    let stream_name = "TIME_BASED_STREAM";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["sensor.*".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await?;

    // Publish messages at different times
    println!("Publishing messages with time gaps:");

    // Publish first batch
    let _start_time = SystemTime::now();
    for i in 0..5 {
        context
            .publish("sensor.temperature", format!("temp reading {}", i).into())
            .await?
            .await?;
        println!("  Published: temp reading {} at start", i);
    }

    // Wait 2 seconds
    println!("\n  Waiting 2 seconds...\n");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mid_time = OffsetDateTime::now_utc();

    // Publish second batch
    for i in 5..10 {
        context
            .publish("sensor.temperature", format!("temp reading {}", i).into())
            .await?
            .await?;
        println!("  Published: temp reading {} at +2s", i);
    }

    // Wait another 2 seconds
    println!("\n  Waiting 2 seconds...\n");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let late_time = OffsetDateTime::now_utc();

    // Publish third batch
    for i in 10..15 {
        context
            .publish("sensor.temperature", format!("temp reading {}", i).into())
            .await?
            .await?;
        println!("  Published: temp reading {} at +4s", i);
    }

    // Example 1: Fetch all messages
    println!("\n=== Example 1: Fetch all messages ===");
    let mut messages = context.get_batch(stream_name, 20).send().await?;

    let mut all_count = 0;
    while let Some(msg) = messages.next().await {
        let _msg = msg?;
        all_count += 1;
    }
    println!("Total messages in stream: {}", all_count);

    // Example 2: Fetch messages after mid_time (should get last 10)
    println!("\n=== Example 2: Fetch messages after +2s mark ===");
    let mut messages = context
        .get_batch(stream_name, 20)
        .start_time(mid_time)
        .send()
        .await?;

    let mut mid_count = 0;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        let payload = String::from_utf8(msg.payload.to_vec())?;
        println!("  {}: {}", msg.sequence, payload);
        mid_count += 1;
    }
    println!("Messages after +2s: {} (expected 10)", mid_count);

    // Example 3: Fetch messages after late_time (should get last 5)
    println!("\n=== Example 3: Fetch messages after +4s mark ===");
    let mut messages = context
        .get_batch(stream_name, 20)
        .start_time(late_time)
        .send()
        .await?;

    let mut late_count = 0;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        let payload = String::from_utf8(msg.payload.to_vec())?;
        println!("  {}: {}", msg.sequence, payload);
        late_count += 1;
    }
    println!("Messages after +4s: {} (expected 5)", late_count);

    // Example 4: Use sequence starting point instead of time
    println!("\n=== Example 4: Starting from sequence 8 ===");
    let mut messages = context
        .get_batch(stream_name, 20)
        .sequence(8)
        .send()
        .await?;

    let mut combo_count = 0;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        let payload = String::from_utf8(msg.payload.to_vec())?;
        println!("  Seq {}: {}", msg.sequence, payload);
        combo_count += 1;
    }
    println!("Messages from seq 8: {}", combo_count);

    // Example 5: Get last messages up to a specific time
    println!("\n=== Example 5: Get last messages for subjects up to +2s mark ===");
    let mut messages = context
        .get_last_messages_for(stream_name)
        .subjects(vec!["sensor.temperature".to_string()])
        .up_to_time(mid_time)
        .send()
        .await?;

    while let Some(msg) = messages.next().await {
        let msg = msg?;
        let payload = String::from_utf8(msg.payload.to_vec())?;
        println!(
            "  Last message up to +2s: {} (seq {})",
            payload, msg.sequence
        );
    }

    // Example 6: Demonstrate nanosecond precision preservation
    println!("\n=== Example 6: Time precision check ===");
    let _precise_time = SystemTime::now();

    // Publish a message with precise timestamp
    context
        .publish("sensor.precision", "precision test".into())
        .await?
        .await?;

    // Fetch it back
    let mut messages = context
        .get_batch(stream_name, 1)
        .subject("sensor.precision")
        .send()
        .await?;

    if let Some(msg) = messages.next().await {
        let msg = msg?;
        println!("  Message timestamp: {:?}", msg.time);
        println!(
            "  Nanosecond precision preserved: {}",
            msg.time.nanosecond()
        );
    }

    Ok(())
}
