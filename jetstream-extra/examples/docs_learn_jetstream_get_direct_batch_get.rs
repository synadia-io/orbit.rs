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

//! Batch Direct Get: read several stored messages in one request.
//!
//! The ORDERS stream has `allow_direct` enabled, so reads are served from any
//! replica without going through a consumer. `get_batch` asks for up to N
//! messages starting at a sequence and returns them as a stream, in order.
//!
//! ```bash
//! nats-server -js
//! cargo run -p jetstream-extra --example docs_learn_jetstream_get_direct_batch_get
//! ```

use async_nats::jetstream::stream::Config;
use futures::StreamExt;
use jetstream_extra::batch_fetch::BatchFetchExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into());
    let client = async_nats::connect(url).await?;
    let js = async_nats::jetstream::new(client);

    // Direct Get reads are served straight from the stream's storage, so the
    // stream must be created with allow_direct.
    js.create_stream(Config {
        name: "ORDERS".into(),
        subjects: vec!["orders.>".into()],
        allow_direct: true,
        ..Default::default()
    })
    .await?;

    // NATS-DOC-START
    // Fetch up to 3 messages starting at stream sequence 1, all in one request.
    let mut messages = js.get_batch("ORDERS", 3).sequence(1).send().await?;

    // The batch arrives as a stream of messages in sequence order.
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        println!("seq {} on subject {}", msg.sequence, msg.subject);
    }
    // NATS-DOC-END

    Ok(())
}
