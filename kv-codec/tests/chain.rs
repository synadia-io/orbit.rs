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

mod common;

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use kv_codec::{Base64Codec, CodecStore, NoOpCodec, PathCodec};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn chain_basic_roundtrip() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let raw = common::create_bucket(&js, "chain-basic").await;
    let store = CodecStore::new(
        raw.clone(),
        (PathCodec, Base64Codec),
        (Base64Codec, NoOpCodec),
    );

    store
        .put(
            "/config/app/database/host",
            Bytes::from_static(b"localhost"),
        )
        .await
        .unwrap();

    let entry = store
        .entry("/config/app/database/host")
        .await
        .unwrap()
        .expect("entry must exist");
    assert_eq!(entry.key, "/config/app/database/host");
    assert_eq!(entry.value, Bytes::from_static(b"localhost"));

    // Stored form: path-translated, then base64 per token; value base64.
    let raw_entry = raw
        .entry("X3Jvb3Rf.Y29uZmln.YXBw.ZGF0YWJhc2U.aG9zdA")
        .await
        .unwrap()
        .expect("encoded key must exist in raw store");
    assert_eq!(raw_entry.value, Bytes::from_static(b"bG9jYWxob3N0"));
}

#[tokio::test]
async fn chain_watch_with_wildcard() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let raw = common::create_bucket(&js, "chain-watch").await;
    let store = CodecStore::new(raw, (PathCodec, Base64Codec), Base64Codec);

    let mut watch = store.watch("/config/*/database").await.unwrap();

    let keys = [
        "/config/app/database",
        "/config/api/database",
        "/config/worker/database",
    ];
    for key in keys {
        store.put(key, Bytes::from_static(b"value")).await.unwrap();
    }

    let mut received = HashSet::new();
    for _ in 0..keys.len() {
        let entry = timeout(TIMEOUT, watch.next())
            .await
            .expect("watch timed out")
            .expect("watch ended")
            .expect("watch errored");
        assert_eq!(entry.value, Bytes::from_static(b"value"));
        received.insert(entry.key);
    }
    assert_eq!(received, keys.map(String::from).into_iter().collect());
}
