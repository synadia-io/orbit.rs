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

//! Atomic batch publish: store every line item of an order, or none of them.
//!
//! The ORDERS stream is created with `allow_atomic_publish` enabled. The three
//! line items are added under one batch id and committed together, so a reader
//! never sees a partially written order. The commit ack reports the batch id
//! and how many messages were stored.
//!
//! ```bash
//! nats-server -js
//! cargo run -p jetstream-extra --example docs_atomic_batch
//! ```

use async_nats::jetstream::stream::Config;
use jetstream_extra::batch_publish::BatchPublishExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = async_nats::jetstream::new(client);

    // Atomic batches require a stream with allow_atomic_publish set.
    js.create_stream(Config {
        name: "ORDERS".into(),
        subjects: vec!["orders.>".into()],
        allow_atomic_publish: true,
        ..Default::default()
    })
    .await?;

    // NATS-DOC-START
    // Open a batch, add the first two line items, then commit with the third.
    // Every message carries the same batch id; the order is stored only if the
    // commit succeeds.
    let mut batch = js.batch_publish().build();
    batch
        .add("orders.created", r#"{"sku":"NATS-TEE","qty":2}"#.into())
        .await?;
    batch
        .add("orders.created", r#"{"sku":"NATS-MUG","qty":1}"#.into())
        .await?;
    let ack = batch
        .commit("orders.created", r#"{"sku":"NATS-CAP","qty":3}"#.into())
        .await?;

    println!(
        "batch {} committed {} line items",
        ack.batch_id, ack.batch_size
    );
    // NATS-DOC-END

    Ok(())
}
