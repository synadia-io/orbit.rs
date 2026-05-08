# jetstream-extra

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Crates.io](https://img.shields.io/crates/v/jetstream-extra.svg)](https://crates.io/crates/jetstream-extra)
[![Documentation](https://docs.rs/jetstream-extra/badge.svg)](https://docs.rs/jetstream-extra/)
[![Build Status](https://github.com/synadia-io/orbit.rs/actions/workflows/jetstream-extra.yml/badge.svg?branch=main)](https://github.com/synadia-io/orbit.rs/actions/workflows/jetstream-extra.yml)

Set of utilities and extensions for the JetStream NATS of the [async-nats](https://crates.io/crates/async-nats) crate.

## Features

- **Batch Publishing** - Atomic batch publishing ensuring all-or-nothing message storage
- **Fast Ingest Batch Publishing** - High-throughput, non-atomic batch publishing with server-driven flow control (requires nats-server 2.14+)
- **Batch Fetching** - Efficient multi-message retrieval using DIRECT.GET API

## Batch Publishing

Atomic batch publishing implementation for JetStream streams, ensuring that either all messages in a batch are stored or none are.

### Complete example

Connect to NATS server with JetStream, and extend the jetstream context with the batch publishing capabilities.

```rust
use async_nats::jetstream;
// Extend the JetStream context with batch publishing.
use jetstream_extra::batch_publish::BatchPublishExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect("demo.nats.io").await?;
    let jetstream = jetstream::new(client);

    // Create or get a stream with atomic publishing enabled
    let _stream = jetstream.get_or_create_stream(jetstream::stream::Config {
        name: "events".to_string(),
        subjects: vec!["events.*".to_string()],
        allow_atomic_publish: true,
        ..Default::default()
    }).await?;

    // Build and use a batch publisher
    let mut batch = jetstream.batch_publish().build();

    // Add messages to the batch
    batch.add("events.order", "order-123".into()).await?;
    batch.add("events.payment", "payment-456".into()).await?;
    batch.add("events.inventory", "item-789".into()).await?;

    // Commit the batch atomically
    let ack = batch.commit("events.notification", "notify-complete".into()).await?;

    println!("Batch published with sequence: {}", ack.sequence);

    Ok(())
}
```

## Fast Ingest Batch Publishing

High-throughput, non-atomic batch publishing using JetStream's fast-ingest feature (ADR-50, requires nats-server 2.14 or later). Unlike atomic batch publishing, messages are persisted as they arrive and the server uses a flow-control channel to coordinate throughput across concurrent publishers.

Use fast ingest when:
- You need to ship millions of messages per batch and don't need all-or-nothing semantics.
- Throughput matters more than atomicity.
- You want the server to dynamically tune ack frequency based on load.

The stream must have `allow_batched: true`. The publisher owns a dedicated inbox subscription for the duration of the batch and drives ack handling inline — no background task, no locks.

### Complete example

```rust
use async_nats::jetstream;
use jetstream_extra::batch_publish_fast::{FastPublishExt, GapMode};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect("nats://127.0.0.1:4222").await?;
    let jetstream = jetstream::new(client);

    // Stream must have allow_batched: true (not yet exposed in async-nats
    // 0.45.0 StreamConfig — create via raw JetStream API until upstream
    // adds the field).

    let mut batch = jetstream
        .fast_publish()
        .flow(100)                                 // ack every 100 messages (ceiling)
        .max_outstanding_acks(2)                   // up to 200 messages in flight
        .gap_mode(GapMode::Fail)                   // abort on any gap (default)
        .ack_timeout(Duration::from_secs(10))
        .on_error(|e| eprintln!("fast publish event: {e}"))
        .build()?;

    // Stream 10,000 messages. The stall gate transparently waits for flow
    // acks and sends pings to recover from any lost acks.
    for i in 0..10_000 {
        batch.add("metrics.cpu", format!("sample {i}").into()).await?;
    }

    // End-of-batch commit — the commit message itself is NOT stored.
    // Use `commit(...)` instead if you want a final message persisted.
    let ack = batch.close().await?;
    println!("committed {} messages as batch {}", ack.batch_size, ack.batch_id);

    Ok(())
}
```

See `examples/fast_publisher.rs` for a runnable example.

## Batch Fetching

Efficient batch fetching of messages from JetStream streams using the DIRECT.GET API, supporting:
- Fetching multiple messages in a single request
- Subject filtering with wildcards
- Sequence and time-based ranges
- Multi-subject last message queries

### Fetch a batch of messages

```rust
use async_nats::jetstream;
use jetstream_extra::batch_fetch::BatchFetchExt;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect("demo.nats.io").await?;
    let context = jetstream::new(client);

    // Fetch 100 messages starting from sequence 1
    let mut messages = context
        .get_batch("my_stream", 100)
        .send()
        .await?;

    while let Some(msg) = messages.next().await {
        let msg = msg?;
        println!("Message at seq {}: {:?}", msg.sequence, msg.subject);
    }

    Ok(())
}
```

### Get last messages for multiple subjects

```rust
use async_nats::jetstream;
use jetstream_extra::batch_fetch::BatchFetchExt;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect("demo.nats.io").await?;
    let context = jetstream::new(client);

    // Get the last message for each sensor
    let subjects = vec![
        "sensors.temp".to_string(),
        "sensors.humidity".to_string(),
        "sensors.pressure".to_string(),
    ];

    let mut messages = context
        .get_last_messages_for("sensor_stream")
        .subjects(subjects)
        .send()
        .await?;

    while let Some(msg) = messages.next().await {
        let msg = msg?;
        println!("Last value for {}: {:?}", msg.subject, msg.payload);
    }

    Ok(())
}
```
