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

//! Verifies the exact stored (encoded) form by reading through the raw,
//! unwrapped store.

mod common;

use bytes::Bytes;
use futures_util::StreamExt;
use kv_codec::{Base64Codec, CodecStoreExt, NoOpCodec, PathCodec};

#[tokio::test]
async fn path_codec_stored_form() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let raw = common::create_bucket(&js, "raw-path").await;
    let store = raw.clone().with_key_codec(PathCodec);

    store
        .put(
            "/config/app/database",
            Bytes::from_static(b"postgres://localhost"),
        )
        .await
        .unwrap();

    // Raw view: encoded key, untouched value.
    let raw_entry = raw
        .entry("_root_.config.app.database")
        .await
        .unwrap()
        .expect("encoded key must exist in raw store");
    assert_eq!(raw_entry.value, Bytes::from_static(b"postgres://localhost"));

    // Wrapped view: original key.
    let entry = store
        .entry("/config/app/database")
        .await
        .unwrap()
        .expect("decoded key must exist");
    assert_eq!(entry.key, "/config/app/database");
    assert_eq!(entry.value, Bytes::from_static(b"postgres://localhost"));
}

#[tokio::test]
async fn base64_stored_form_keys_and_values() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let raw = common::create_bucket(&js, "raw-base64").await;
    let store = raw.clone().with_codecs(Base64Codec, Base64Codec);

    // This key is invalid as a raw KV key (spaces) — the codec enables it.
    let key = "Acme Inc.contact info";
    let value = Bytes::from_static(b"email: info@acme.com, phone: +1-555-123");
    store.put(key, value.clone()).await.unwrap();

    let raw_keys = raw
        .keys()
        .await
        .unwrap()
        .map(|key| key.unwrap())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(raw_keys, vec!["QWNtZSBJbmM.Y29udGFjdCBpbmZv"]);

    let raw_entry = raw
        .entry("QWNtZSBJbmM.Y29udGFjdCBpbmZv")
        .await
        .unwrap()
        .expect("encoded key must exist in raw store");
    assert_ne!(raw_entry.value, value);

    let entry = store.entry(key).await.unwrap().expect("entry must exist");
    assert_eq!(entry.key, key);
    assert_eq!(entry.value, value);
}

#[tokio::test]
async fn base64_keys_only_value_passthrough() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let raw = common::create_bucket(&js, "raw-base64-keys").await;
    let store = raw.clone().with_key_codec(Base64Codec);

    let key = "Special Key with Spaces";
    let value = Bytes::from_static(b"plain value");
    store.put(key, value.clone()).await.unwrap();

    let raw_keys = raw
        .keys()
        .await
        .unwrap()
        .map(|key| key.unwrap())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(raw_keys.len(), 1);
    assert_ne!(raw_keys[0], key);

    // Value stored untouched.
    let raw_value = raw.get(raw_keys[0].clone()).await.unwrap().unwrap();
    assert_eq!(raw_value, value);

    assert_eq!(store.get(key).await.unwrap().unwrap(), value);
}

#[tokio::test]
async fn noop_stored_identical() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let raw = common::create_bucket(&js, "raw-noop").await;
    let store = raw.clone().with_codecs(NoOpCodec, NoOpCodec);

    store
        .put("plain.key", Bytes::from_static(b"plain"))
        .await
        .unwrap();

    let raw_entry = raw.entry("plain.key").await.unwrap().unwrap();
    let entry = store.entry("plain.key").await.unwrap().unwrap();
    assert_eq!(raw_entry, entry);

    let raw_keys = raw
        .keys()
        .await
        .unwrap()
        .map(|key| key.unwrap())
        .collect::<Vec<_>>()
        .await;
    let keys = store
        .keys()
        .await
        .unwrap()
        .map(|key| key.unwrap())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(raw_keys, keys);
}
