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

//! Covers the remaining `CodecStore` surface: value-only codecs, the watch
//! family, revision- and TTL-guarded operations, and codecs behind `Arc`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::kv::Operation;
use bytes::Bytes;
use futures_util::StreamExt;
use kv_codec::{Base64Codec, CodecStore, CodecStoreExt};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);

/// Go `NewForValue` parity: keys stored as-is, values encoded.
#[tokio::test]
async fn value_only_codec() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let raw = common::create_bucket(&js, "value-only").await;
    let store = raw.clone().with_value_codec(Base64Codec);

    store
        .put("plain.key", Bytes::from_static(b"secret"))
        .await
        .unwrap();

    // Key readable raw under its plain form; value is encoded.
    let raw_entry = raw.entry("plain.key").await.unwrap().unwrap();
    assert_eq!(raw_entry.value, Bytes::from_static(b"c2VjcmV0"));
    assert_eq!(
        store.get("plain.key").await.unwrap().unwrap(),
        Bytes::from_static(b"secret")
    );

    // CodecStore::for_value is the same construction.
    let store = CodecStore::for_value(raw, Base64Codec);
    assert_eq!(
        store.get("plain.key").await.unwrap().unwrap(),
        Bytes::from_static(b"secret")
    );
}

/// Go's `Watch` default delivers current values first; here that is
/// `watch_with_history`. Initial entries must decode like updates.
#[tokio::test]
async fn watch_with_history_delivers_initial_values() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "watch-history").await;
    let store = CodecStore::new(store, Base64Codec, Base64Codec);

    store
        .put("user.1", Bytes::from_static(b"existing"))
        .await
        .unwrap();

    let mut watch = store.watch_with_history("user.*").await.unwrap();
    let entry = timeout(TIMEOUT, watch.next())
        .await
        .expect("watch timed out")
        .expect("watch ended")
        .expect("watch errored");
    assert_eq!(entry.key, "user.1");
    assert_eq!(entry.value, Bytes::from_static(b"existing"));

    store
        .put("user.2", Bytes::from_static(b"new"))
        .await
        .unwrap();
    let entry = timeout(TIMEOUT, watch.next())
        .await
        .expect("watch timed out")
        .expect("watch ended")
        .expect("watch errored");
    assert_eq!(entry.key, "user.2");
    assert_eq!(entry.value, Bytes::from_static(b"new"));
}

/// watch_all must not run filters through the codec — encoded keys of any
/// shape are observed and decoded.
#[tokio::test]
async fn watch_all_decodes_all_keys() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "watch-all").await;
    let store = CodecStore::new(store, Base64Codec, Base64Codec);

    let mut watch = store.watch_all().await.unwrap();

    // Keys the raw bucket could never hold.
    let keys = ["Acme Inc.alpha", "Beta Corp.beta"];
    for key in keys {
        store.put(key, Bytes::from_static(b"v")).await.unwrap();
    }

    for expected in keys {
        let entry = timeout(TIMEOUT, watch.next())
            .await
            .expect("watch timed out")
            .expect("watch ended")
            .expect("watch errored");
        assert_eq!(entry.key, expected);
        assert_eq!(entry.value, Bytes::from_static(b"v"));
    }
}

/// Revision targeting passes through the codec layer unchanged.
#[tokio::test]
async fn watch_from_revision_variants() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "watch-revision").await;
    let store = CodecStore::new(store, Base64Codec, Base64Codec);

    store
        .put("rev.a", Bytes::from_static(b"one"))
        .await
        .unwrap();
    let rev2 = store
        .put("rev.a", Bytes::from_static(b"two"))
        .await
        .unwrap();

    let mut watch = store.watch_from_revision("rev.a", rev2).await.unwrap();
    let entry = timeout(TIMEOUT, watch.next())
        .await
        .expect("watch timed out")
        .expect("watch ended")
        .expect("watch errored");
    assert_eq!(entry.revision, rev2);
    assert_eq!(entry.value, Bytes::from_static(b"two"));

    let mut watch = store.watch_all_from_revision(rev2).await.unwrap();
    let entry = timeout(TIMEOUT, watch.next())
        .await
        .expect("watch timed out")
        .expect("watch ended")
        .expect("watch errored");
    assert_eq!(entry.key, "rev.a");
    assert_eq!(entry.revision, rev2);
}

#[tokio::test]
async fn watch_many_with_history_encodes_filters() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "watch-many-history").await;
    let store = CodecStore::new(store, Base64Codec, Base64Codec);

    store.put("user.1", Bytes::from_static(b"a")).await.unwrap();
    store
        .put("other.1", Bytes::from_static(b"x"))
        .await
        .unwrap();
    store
        .put("admin.1", Bytes::from_static(b"b"))
        .await
        .unwrap();

    let mut watch = store
        .watch_many_with_history(["user.*", "admin.1"])
        .await
        .unwrap();
    let mut received = Vec::new();
    for _ in 0..2 {
        let entry = timeout(TIMEOUT, watch.next())
            .await
            .expect("watch timed out")
            .expect("watch ended")
            .expect("watch errored");
        received.push(entry.key);
    }
    received.sort();
    assert_eq!(received, vec!["admin.1", "user.1"]);
}

/// Wrong expected revision must fail and the right one succeed — proving
/// the guarded operations target the encoded key.
#[tokio::test]
async fn revision_guarded_delete_and_purge() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "guarded").await;
    let store = CodecStore::new(store, Base64Codec, Base64Codec);

    let rev = store
        .put("del.key", Bytes::from_static(b"v"))
        .await
        .unwrap();
    assert!(
        store
            .delete_expect_revision("del.key", Some(rev + 100))
            .await
            .is_err()
    );
    store
        .delete_expect_revision("del.key", Some(rev))
        .await
        .unwrap();
    assert_eq!(store.get("del.key").await.unwrap(), None);

    let rev = store
        .put("purge.key", Bytes::from_static(b"v"))
        .await
        .unwrap();
    assert!(
        store
            .purge_expect_revision("purge.key", Some(rev + 100))
            .await
            .is_err()
    );
    store
        .purge_expect_revision("purge.key", Some(rev))
        .await
        .unwrap();
    let history = store
        .history("purge.key")
        .await
        .unwrap()
        .map(|entry| entry.unwrap())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].operation, Operation::Purge);
}

/// TTL variants against a bucket with markers enabled (nats-server 2.11+).
#[tokio::test]
async fn ttl_variants() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let raw = js
        .create_key_value(jetstream::kv::Config {
            bucket: "ttl".to_string(),
            history: 10,
            limit_markers: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .await
        .expect("Failed to create KV bucket with markers");
    let store = raw.clone().with_codecs(Base64Codec, Base64Codec);

    // create_with_ttl stores under the encoded key.
    store
        .create_with_ttl("ttl.key", Bytes::from_static(b"v"), Duration::from_secs(60))
        .await
        .unwrap();
    assert!(raw.entry("dHRs.a2V5").await.unwrap().is_some());
    assert_eq!(
        store.get("ttl.key").await.unwrap().unwrap(),
        Bytes::from_static(b"v")
    );

    store
        .purge_with_ttl("ttl.key", Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(store.get("ttl.key").await.unwrap(), None);

    let rev = store
        .put("ttl.other", Bytes::from_static(b"v"))
        .await
        .unwrap();
    store
        .purge_expect_revision_with_ttl("ttl.other", rev, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(store.get("ttl.other").await.unwrap(), None);
}

/// A codec behind `Arc` (the documented pattern for stateful, non-Clone
/// codecs) must keep wildcard filtering working end to end.
#[tokio::test]
async fn arc_wrapped_codec_end_to_end() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "arc-codec").await;
    let store = CodecStore::new(store, Arc::new(Base64Codec), Arc::new(Base64Codec));

    let mut watch = store.watch("user.*").await.unwrap();
    store
        .put("user.1", Bytes::from_static(b"alice"))
        .await
        .unwrap();

    let entry = timeout(TIMEOUT, watch.next())
        .await
        .expect("watch timed out")
        .expect("watch ended")
        .expect("watch errored");
    assert_eq!(entry.key, "user.1");
    assert_eq!(entry.value, Bytes::from_static(b"alice"));
}
