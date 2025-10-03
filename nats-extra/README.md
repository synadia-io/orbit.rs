# nats-extra

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Crates.io](https://img.shields.io/crates/v/nats-extra.svg)](https://crates.io/crates/nats-extra)
[![Documentation](https://docs.rs/nats-extra/badge.svg)](https://docs.rs/nats-extra/)
[![Build Status](https://github.com/synadia-io/orbit.rs/actions/workflows/nats-extra.yml/badge.svg?branch=main)](https://github.com/synadia-io/orbit.rs/actions/workflows/nats-extra.yml)

Set of utilities and extensions for the Core NATS of the [async-nats](https://crates.io/crates/async-nats) crate.

## Request Many

Request many pattern implementation useful for streaming responses
and scatter-gather pattern.

### Complete example

Connect to NATS server, and extend the [async-nats::Client] with the request_many capabilities.

```rust
use async_nats::Client;
// Extend the client with request_many.
use nats_extra::request_many::RequestManyExt;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("demo.nats.io").await?;

    let mut requests = client.subscribe("requests").await?;

    let mut responses = client
        .request_many()
        .send("requests", "payload".into())
        .await?;

    let request = requests.next().await.unwrap();
    for _ in 0..100 {
        client.publish(request.reply.clone().unwrap(), "data".into()).await?;
    }

    while let Some(message) = responses.next().await {
        println!("Received: {:?}", message);
    }
    Ok(())
}
```
