# jetstream-extra

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Crates.io](https://img.shields.io/crates/v/jetstream-extra.svg)](https://crates.io/crates/jetstream-extra)
[![Documentation](https://docs.rs/jetstream-extra/badge.svg)](https://docs.rs/jetstream-extra/)
[![Build Status](https://github.com/synadia-io/orbit.rs/actions/workflows/jetstream-extra.yml/badge.svg?branch=main)](https://github.com/synadia-io/orbit.rs/actions/workflows/jetstream-extra.yml)

Set of utilities and extensions for the JetStream NATS of the [async-nats](https://crates.io/crates/async-nats) crate.

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
    let stream = jetstream.get_or_create_stream(jetstream::stream::Config {
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