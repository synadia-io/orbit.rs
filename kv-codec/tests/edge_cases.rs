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

use async_nats::jetstream::kv::Operation;
use bytes::Bytes;
use futures_util::StreamExt;
use kv_codec::{Base64Codec, CodecStore, CodecStoreExt, KvCodecErrorKind, NoOpCodec};

#[tokio::test]
async fn empty_key_rejected() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "empty-key").await;
    let store = CodecStore::new(store, NoOpCodec, NoOpCodec);

    let err = store.put("", Bytes::from_static(b"v")).await.unwrap_err();
    assert_eq!(err.kind(), KvCodecErrorKind::Store);
}

#[tokio::test]
async fn literal_wildcard_in_put_key() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let raw = common::create_bucket(&js, "literal-wildcard").await;
    let store = raw.clone().with_codecs(Base64Codec, NoOpCodec);

    // Base64 turns `*` into a valid token, so it works as key data.
    store.put("user.*", Bytes::from_static(b"v")).await.unwrap();
    assert_eq!(
        store.get("user.*").await.unwrap().unwrap(),
        Bytes::from_static(b"v")
    );

    // Without a codec the server library rejects wildcard characters.
    let noop = raw.with_codecs(NoOpCodec, NoOpCodec);
    assert!(noop.put("user.*", Bytes::from_static(b"v")).await.is_err());
}

#[tokio::test]
async fn unicode_keys_via_base64() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let raw = common::create_bucket(&js, "unicode").await;
    let store = raw.clone().with_key_codec(Base64Codec);

    store
        .put("użytkownik.imię", Bytes::from_static(b"Tomasz"))
        .await
        .unwrap();
    assert_eq!(
        store.get("użytkownik.imię").await.unwrap().unwrap(),
        Bytes::from_static(b"Tomasz")
    );

    // The same key without a codec is rejected.
    let noop = raw.with_codecs(NoOpCodec, NoOpCodec);
    assert!(
        noop.put("użytkownik.imię", Bytes::from_static(b"v"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn empty_value_and_tombstones() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "tombstones").await;
    let store = CodecStore::new(store, Base64Codec, Base64Codec);

    // Empty value round-trips and is distinct from a deleted key.
    store.put("empty.value", Bytes::new()).await.unwrap();
    assert_eq!(store.get("empty.value").await.unwrap(), Some(Bytes::new()));

    // Delete markers must not go through the value codec.
    store
        .put("doomed.key", Bytes::from_static(b"v"))
        .await
        .unwrap();
    store
        .put("doomed.key", Bytes::from_static(b"w"))
        .await
        .unwrap();
    store.delete("doomed.key").await.unwrap();

    let history = store
        .history("doomed.key")
        .await
        .unwrap()
        .map(|entry| entry.unwrap())
        .collect::<Vec<_>>()
        .await;
    let operations = history
        .iter()
        .map(|entry| entry.operation)
        .collect::<Vec<_>>();
    assert_eq!(
        operations,
        vec![Operation::Put, Operation::Put, Operation::Delete]
    );
    assert!(history.iter().all(|entry| entry.key == "doomed.key"));
}

#[tokio::test]
async fn corrupted_stored_value_is_an_error() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let raw = common::create_bucket(&js, "corrupted-value").await;
    let store = raw.clone().with_codecs(Base64Codec, Base64Codec);

    // Write a non-base64 value under a properly encoded key via raw store.
    let encoded_key = "Y29ycnVwdA"; // base64("corrupt")
    raw.put(encoded_key, Bytes::from_static(b"!!!not-base64!!!"))
        .await
        .unwrap();

    let err = store.get("corrupt").await.unwrap_err();
    assert_eq!(err.kind(), KvCodecErrorKind::ValueDecode);
    let err = store.entry("corrupt").await.unwrap_err();
    assert_eq!(err.kind(), KvCodecErrorKind::ValueDecode);
}

#[tokio::test]
async fn undecodable_key_surfaces_as_stream_error() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let raw = common::create_bucket(&js, "corrupted-key").await;
    let store = raw.clone().with_key_codec(Base64Codec);

    store
        .put("good.key", Bytes::from_static(b"v"))
        .await
        .unwrap();
    // "z" is subject-legal but has an invalid base64 length.
    raw.put("z", Bytes::from_static(b"v")).await.unwrap();

    let results = store.keys().await.unwrap().collect::<Vec<_>>().await;
    assert_eq!(results.len(), 2);
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let err = results
        .into_iter()
        .find_map(|result| result.err())
        .expect("one key must fail to decode");
    assert_eq!(err.kind(), KvCodecErrorKind::KeyDecode);
}

#[tokio::test]
async fn history_restores_caller_key_verbatim() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "history-key").await;
    let store = CodecStore::for_key(store, kv_codec::PathCodec);

    // Trailing slash is lossy in PathCodec; history must still echo the
    // caller's key, like entry() does (Go originalKey parity).
    store
        .put("hist/key/", Bytes::from_static(b"v"))
        .await
        .unwrap();
    let history = store
        .history("hist/key/")
        .await
        .unwrap()
        .map(|entry| entry.unwrap())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].key, "hist/key/");
}

#[tokio::test]
async fn revisions_agree_with_raw_store() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let raw = common::create_bucket(&js, "revisions").await;
    let store = raw.clone().with_codecs(Base64Codec, Base64Codec);

    let revision = store
        .put("rev.key", Bytes::from_static(b"v"))
        .await
        .unwrap();
    let raw_entry = raw
        .entry("cmV2.a2V5")
        .await
        .unwrap()
        .expect("encoded key must exist");
    assert_eq!(raw_entry.revision, revision);

    let entry = store.entry("rev.key").await.unwrap().unwrap();
    assert_eq!(entry.revision, raw_entry.revision);
    assert_eq!(entry.created, raw_entry.created);
    assert_eq!(entry.delta, raw_entry.delta);
}
