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

//! Common test utilities for kv-codec tests.

use async_nats::jetstream;
use async_nats::jetstream::kv::Store;
use nats_server::Server;

/// Starts an embedded NATS server with JetStream enabled.
pub fn start_jetstream_server() -> Server {
    nats_server::run_server("tests/configs/jetstream.conf")
}

/// Creates a JetStream context from a client.
pub async fn create_jetstream_context(url: &str) -> jetstream::Context {
    let client = async_nats::connect(url)
        .await
        .expect("Failed to connect to NATS server");
    jetstream::new(client)
}

/// Creates a KV bucket and returns the raw store.
pub async fn create_bucket(js: &jetstream::Context, name: &str) -> Store {
    js.create_key_value(jetstream::kv::Config {
        bucket: name.to_string(),
        history: 10,
        ..Default::default()
    })
    .await
    .expect("Failed to create KV bucket")
}
