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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to NATS server
    let client = async_nats::connect("localhost:4222").await?;
    let context = jetstream::new(client);

    // Create a stream
    let stream_name = "BYTES_LIMIT_STREAM";
    context
        .create_stream(stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["data.*".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await?;

    // Publish messages of varying sizes
    println!("Publishing messages of different sizes:");
    for i in 0..20 {
        let size = (i + 1) * 100; // 100, 200, 300... bytes
        let payload = "x".repeat(size);
        context
            .publish(format!("data.msg{}", i), payload.into())
            .await?
            .await?;
        println!("  Published message {} with {} bytes", i, size);
    }

    // Example 1: Fetch messages with max_bytes limit
    println!("\nFetching messages with max_bytes=1000:");
    let mut messages = context
        .get_batch(stream_name, 20)
        .max_bytes(1000) // But limit total bytes to 1000
        .send()
        .await?;

    let mut total_bytes = 0;
    let mut count = 0;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        let payload_size =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &msg.payload)?.len();
        total_bytes += payload_size;
        count += 1;
        println!(
            "  Message {}: {} bytes (total: {} bytes)",
            msg.sequence, payload_size, total_bytes
        );
    }

    println!(
        "\nFetched {} messages totaling {} bytes (limit was 1000)",
        count, total_bytes
    );

    // Example 2: Compare with unlimited fetch
    println!("\nFetching same batch without byte limit:");
    let mut messages = context.get_batch(stream_name, 20).send().await?;

    let mut total_bytes_unlimited = 0;
    let mut count_unlimited = 0;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        let payload_size =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &msg.payload)?.len();
        total_bytes_unlimited += payload_size;
        count_unlimited += 1;
    }

    println!(
        "Without limit: {} messages, {} bytes",
        count_unlimited, total_bytes_unlimited
    );

    // Example 3: Use max_bytes with subject filter
    println!("\nCombining max_bytes with subject filter:");
    let mut messages = context
        .get_batch(stream_name, 20)
        .subject("data.msg1*") // Only msg10-19
        .max_bytes(2000)
        .send()
        .await?;

    let mut filtered_bytes = 0;
    let mut filtered_count = 0;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        let payload_size =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &msg.payload)?.len();
        filtered_bytes += payload_size;
        filtered_count += 1;
        println!(
            "  {} (seq {}): {} bytes",
            msg.subject, msg.sequence, payload_size
        );
    }

    println!(
        "\nFiltered fetch: {} messages, {} bytes",
        filtered_count, filtered_bytes
    );

    Ok(())
}
