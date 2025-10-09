# NATS Counters

Distributed counters for NATS JetStream, providing high-performance counter operations with arbitrary precision integers.


## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
nats-counters = "0.1"
async-nats = "0.44"
```

## Quick Start

```rust
use nats_counters::{Counter, CounterExt};
use async_nats::jetstream::stream::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to NATS
    let client = async_nats::connect("localhost:4222").await?;
    let js = async_nats::jetstream::new(client);

    // Create a counter-enabled stream
    let config = Config {
        name: "COUNTERS".to_string(),
        subjects: vec!["counters.>".to_string()],
        allow_message_counter: true,  // Required!
        allow_direct: true,           // Required!
        ..Default::default()
    };
    js.create_stream(config).await?;

    // Get the counter
    let counter = js.get_counter("COUNTERS").await?;

    // Increment
    let value = counter.add("counters.visits", 1).await?;
    println!("Visits: {}", value);

    // Decrement
    let value = counter.add("counters.visits", -1).await?;
    println!("Visits after decrement: {}", value);

    // Read current value
    let current = counter.load("counters.visits").await?;
    println!("Current visits: {}", current);

    Ok(())
}
```

## Stream Configuration

Counters require specific stream configuration:

```rust
Config {
    allow_message_counter: true,  // Enables counter functionality
    allow_direct: true,           // Required for efficient reads
    // ... other config
}
```

## Examples

### Basic Operations

```rust
// Increment by 10
let new_value = counter.add("metrics.requests", 10).await?;

// Decrement by 3
let new_value = counter.add("metrics.errors", -3).await?;

// Get current value only
let value = counter.load("metrics.requests").await?;

// Get full entry with sources
let entry = counter.get("metrics.requests").await?;
println!("Value: {}, Last increment: {:?}", entry.value, entry.increment);
```

### Batch Operations

```rust
use futures_util::StreamExt;

// Fetch multiple counters efficiently
let subjects = vec![
    "metrics.requests".to_string(),
    "metrics.errors".to_string(),
    "metrics.latency".to_string(),
];

let mut entries = counter.get_multiple(subjects).await?;
while let Some(entry) = entries.next().await {
    let entry = entry?;
    println!("{}: {}", entry.subject, entry.value);
}
```

### Large Numbers

```rust
use num_bigint::BigInt;

// Work with numbers beyond i64 range
let huge = BigInt::parse_bytes(b"999999999999999999999999", 10).unwrap();
let result = counter.add("metrics.huge", huge).await?;
```

### Distributed Counters with Source Tracking

```rust
// Create regional streams that aggregate into a global view
let eu_config = Config {
    name: "METRICS_EU".to_string(),
    subjects: vec!["metrics.eu.>".to_string()],
    allow_message_counter: true,
    allow_direct: true,
    ..Default::default()
};

let global_config = Config {
    name: "METRICS_GLOBAL".to_string(),
    subjects: vec!["metrics.global.>".to_string()],
    allow_message_counter: true,
    allow_direct: true,
    sources: Some(vec![
        Source {
            name: "METRICS_EU".to_string(),
            subject_transforms: vec![SubjectTransform {
                source: "metrics.eu.>".to_string(),
                destination: "metrics.global.>".to_string(),
            }],
            ..Default::default()
        },
        // Add more regions...
    ]),
    ..Default::default()
};

// Get aggregated counter with source breakdown
let entry = global_counter.get("metrics.global.requests").await?;
for (stream, subjects) in &entry.sources {
    for (subject, value) in subjects {
        println!("{} contributed {} from {}", stream, value, subject);
    }
}
```

