// Copyright 2026 Synadia Communications Inc.
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

//! Example: high-throughput fast-ingest batch publishing.
//!
//! Requires nats-server 2.14 or later running on `nats://127.0.0.1:4222`.
//!
//! ```bash
//! # One-time setup — start nats-server with JetStream
//! nats-server -js
//!
//! # Run the example
//! cargo run -p jetstream-extra --example fast_publisher
//! ```
//!
//! The example:
//! 1. Connects to NATS and creates a stream with `allow_batched: true`
//!    (via a raw `$JS.API.STREAM.CREATE` request because
//!    `async-nats 0.45.0`'s `StreamConfig` does not yet expose the field).
//! 2. Builds a `FastPublisher` with a low flow ceiling (to exercise the
//!    stall gate + auto-ping on a small run).
//! 3. Publishes 500 messages, printing progress every 50.
//! 4. Closes the batch via end-of-batch commit (the EOB message itself is
//!    not persisted; only the 500 real messages are).

use std::time::Duration;

use jetstream_extra::batch_publish_fast::{FastPublishExt, GapMode};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect("nats://127.0.0.1:4222").await?;
    let js = async_nats::jetstream::new(client);

    // Create the stream via raw JSON (async-nats 0.45.0 doesn't know about
    // allow_batched yet).
    let _ = js.delete_stream("METRICS").await;
    let body = json!({
        "name": "METRICS",
        "subjects": ["metrics.>"],
        "retention": "limits",
        "storage": "file",
        "allow_batched": true,
    });
    let resp: serde_json::Value = js.request("STREAM.CREATE.METRICS", &body).await?;
    if resp.get("error").is_some() {
        return Err(format!("STREAM.CREATE failed: {resp}").into());
    }
    println!("created stream METRICS with allow_batched=true");

    // Build the publisher. Low flow + max=2 means acks arrive every 50
    // messages and up to 100 are in flight at once — exercises the stall
    // gate and auto-ping path on a realistic load.
    let mut batch = js
        .fast_publish()
        .flow(50)
        .max_outstanding_acks(2)
        .gap_mode(GapMode::Fail)
        .ack_timeout(Duration::from_secs(10))
        .on_error(|err| eprintln!("fast publish event: {err}"))
        .build()?;

    println!("publishing 500 messages ...");
    for i in 0..500 {
        let ack = batch
            .add("metrics.cpu", format!("sample {i}").into())
            .await?;
        if i % 50 == 49 {
            println!(
                "  published seq={} (server acked up to {})",
                ack.batch_sequence, ack.ack_sequence
            );
        }
    }

    // End-of-batch commit: the commit message itself is NOT stored, so the
    // final stream state has exactly 500 messages.
    let pub_ack = batch.close().await?;
    println!(
        "batch committed: stream={}, batch_size={}, batch_id={}",
        pub_ack.stream, pub_ack.batch_size, pub_ack.batch_id
    );

    // Verify what landed on the stream.
    let stream = js.get_stream("METRICS").await?;
    let info = stream.get_info().await?;
    println!("stream state: {} messages", info.state.messages);
    assert_eq!(info.state.messages, 500);

    Ok(())
}
