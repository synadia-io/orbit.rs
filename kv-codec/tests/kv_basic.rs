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

use std::time::Duration;

use async_nats::jetstream::kv::{CreateError, CreateErrorKind, Operation};
use bytes::Bytes;
use futures_util::StreamExt;
use kv_codec::{
    Base64Codec, CodecStore, KeyCodec, KvCodecErrorKind, NoOpCodec, PathCodec, ValueCodec,
};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);

/// Exercises the full lifecycle through a codec-enabled store.
///
/// `key` and `watch_prefix` are given in the codec's decoded notation.
async fn exercise_basic_ops<K, V>(store: CodecStore<K, V>, key: &str, watch_key: &str)
where
    K: KeyCodec + Clone + Unpin,
    V: ValueCodec + Clone + Unpin,
{
    // Put and read back.
    let revision = store
        .put(key, Bytes::from_static(b"test value"))
        .await
        .unwrap();
    assert_eq!(revision, 1);

    let entry = store.entry(key).await.unwrap().expect("entry must exist");
    assert_eq!(entry.key, key);
    assert_eq!(entry.value, Bytes::from_static(b"test value"));
    assert_eq!(entry.operation, Operation::Put);

    // Update with revision.
    let revision = store
        .update(key, Bytes::from_static(b"updated"), revision)
        .await
        .unwrap();
    assert_eq!(revision, 2);
    assert_eq!(
        store.get(key).await.unwrap().unwrap(),
        Bytes::from_static(b"updated")
    );

    // Watch sees a decoded update.
    let mut watch = store.watch(watch_key).await.unwrap();
    store
        .put(key, Bytes::from_static(b"watched"))
        .await
        .unwrap();
    let entry = timeout(TIMEOUT, watch.next())
        .await
        .expect("watch timed out")
        .expect("watch ended")
        .expect("watch errored");
    assert_eq!(entry.key, key);
    assert_eq!(entry.value, Bytes::from_static(b"watched"));

    // Delete leaves a decoded marker.
    store.delete(key).await.unwrap();
    assert_eq!(store.get(key).await.unwrap(), None);
    let entry = store.entry(key).await.unwrap().expect("delete marker");
    assert_eq!(entry.operation, Operation::Delete);
    assert_eq!(entry.key, key);
}

#[tokio::test]
async fn basic_ops_noop() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "basic-noop").await;
    exercise_basic_ops(
        CodecStore::new(store, NoOpCodec, NoOpCodec),
        "test.key",
        "test.key",
    )
    .await;
}

#[tokio::test]
async fn basic_ops_base64() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "basic-base64").await;
    exercise_basic_ops(
        CodecStore::new(store, Base64Codec, Base64Codec),
        "test.key",
        "test.key",
    )
    .await;
}

#[tokio::test]
async fn basic_ops_path() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "basic-path").await;
    exercise_basic_ops(
        CodecStore::for_key(store, PathCodec),
        "/test/key",
        "/test/key",
    )
    .await;
}

#[tokio::test]
async fn keys_listing_decodes() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "keys-listing").await;
    let store = CodecStore::for_key(store, PathCodec);

    for key in ["list/a", "list/b", "list/c"] {
        store.put(key, Bytes::from_static(b"v")).await.unwrap();
    }

    let mut keys = store
        .keys()
        .await
        .unwrap()
        .map(|key| key.unwrap())
        .collect::<Vec<_>>()
        .await;
    keys.sort();
    assert_eq!(keys, vec!["list/a", "list/b", "list/c"]);
}

#[tokio::test]
async fn create_revision_history_purge() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "history").await;
    let store = CodecStore::new(store, Base64Codec, Base64Codec);

    // Create fails on existing keys; the async-nats error is recoverable
    // from the source via downcast.
    store
        .create("create.key", Bytes::from_static(b"first"))
        .await
        .unwrap();
    let err = store
        .create("create.key", Bytes::from_static(b"second"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), KvCodecErrorKind::Store);
    let already_exists = std::error::Error::source(&err)
        .and_then(|source| source.downcast_ref::<CreateError>())
        .is_some_and(|create| create.kind() == CreateErrorKind::AlreadyExists);
    assert!(already_exists, "source must downcast to CreateError");

    // Two revisions of one key.
    let rev1 = store
        .put("string.key", Bytes::from_static(b"one"))
        .await
        .unwrap();
    let rev2 = store
        .put("string.key", Bytes::from_static(b"two"))
        .await
        .unwrap();
    assert!(rev2 > rev1);

    let entry = store
        .entry_for_revision("string.key", rev1)
        .await
        .unwrap()
        .expect("revision must exist");
    assert_eq!(entry.key, "string.key");
    assert_eq!(entry.value, Bytes::from_static(b"one"));

    let history = store
        .history("string.key")
        .await
        .unwrap()
        .map(|entry| entry.unwrap())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].value, Bytes::from_static(b"one"));
    assert_eq!(history[1].value, Bytes::from_static(b"two"));
    assert!(history.iter().all(|entry| entry.key == "string.key"));

    // Purge collapses history to a single marker.
    store.purge("string.key").await.unwrap();
    assert_eq!(store.get("string.key").await.unwrap(), None);
    let history = store
        .history("string.key")
        .await
        .unwrap()
        .map(|entry| entry.unwrap())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].operation, Operation::Purge);
}
