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
use jetstream_extra::batch_fetch::{BatchFetchErrorKind, BatchFetchExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to NATS server
    let client = async_nats::connect("localhost:4222").await?;
    let context = jetstream::new(client);

    // Create a stream with direct access enabled
    let stream_name = "EXAMPLE_STREAM";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["events.*".to_string()],
            allow_direct: true, // Required for batch fetch
            ..Default::default()
        })
        .await?;

    // Publish some test messages
    for i in 0..10 {
        context
            .publish(
                format!("events.type{}", i % 3),
                format!("message {}", i).into(),
            )
            .await?
            .await?;
    }

    // Example 1: Handle batch size limit errors
    println!("Example 1: Batch size limit validation");
    match context.get_batch(stream_name, 200).send().await {
        Ok(_) => println!("Unexpected success"),
        Err(e) if e.kind() == BatchFetchErrorKind::BatchSizeTooLarge => {
            println!("✓ Correctly rejected batch size > 1000: {}", e);
        }
        Err(e) => println!("Unexpected error: {}", e),
    }

    // Example 2: Handle empty stream name
    println!("\nExample 3: Empty stream name validation");
    match context.get_batch("", 10).send().await {
        Ok(_) => println!("Unexpected success"),
        Err(e) if e.kind() == BatchFetchErrorKind::InvalidStreamName => {
            println!("✓ Correctly rejected empty stream name: {}", e);
        }
        Err(e) => println!("Unexpected error: {}", e),
    }

    // Example 4: Successful batch fetch with error handling
    println!("\nExample 4: Successful batch fetch with error handling");
    let mut messages = context
        .get_batch(stream_name, 5)
        .subject("events.type0")
        .send()
        .await?;

    let mut count = 0;
    while let Some(result) = messages.next().await {
        match result {
            Ok(msg) => {
                println!(
                    "  Message seq {}: subject={}, payload_size={}",
                    msg.sequence,
                    msg.subject,
                    msg.payload.len()
                );
                count += 1;
            }
            Err(e) => {
                // Handle individual message errors
                match e.kind() {
                    BatchFetchErrorKind::NoMessages => {
                        println!("  No more messages available");
                        break;
                    }
                    BatchFetchErrorKind::UnsupportedByServer => {
                        println!("  Server doesn't support batch fetch");
                        break;
                    }
                    _ => {
                        println!("  Error fetching message: {}", e);
                    }
                }
            }
        }
    }
    println!("  Successfully fetched {} messages", count);

    // Example 5: Handle too many subjects in multi_last
    println!("\nExample 5: Too many subjects validation");
    let many_subjects: Vec<String> = (0..1025).map(|i| format!("events.{}", i)).collect();
    match context
        .get_last_messages_for(stream_name)
        .subjects(many_subjects)
        .send()
        .await
    {
        Ok(_) => println!("Unexpected success"),
        Err(e) if e.kind() == BatchFetchErrorKind::TooManySubjects => {
            println!("✓ Correctly rejected > 1024 subjects: {}", e);
        }
        Err(e) => println!("Unexpected error: {}", e),
    }

    Ok(())
}
