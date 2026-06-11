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
use kv_codec::{Base64Codec, CodecError, CodecStore, CodecStoreExt, KeyCodec, KvCodecErrorKind};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);

/// Prefix codec relying on the default, wildcard-rejecting `encode_filter`.
#[derive(Clone)]
struct NonFilterableCodec;

impl KeyCodec for NonFilterableCodec {
    fn encode_key(&self, key: &str) -> Result<String, CodecError> {
        Ok(format!("encoded_{key}"))
    }

    fn decode_key(&self, key: &str) -> Result<String, CodecError> {
        key.strip_prefix("encoded_")
            .map(str::to_string)
            .ok_or_else(|| "missing prefix".into())
    }
}

#[tokio::test]
async fn filterable_codec_accepts_wildcards() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "filterable").await;
    let store = CodecStore::new(store, Base64Codec, Base64Codec);

    let mut watch = store.watch("user.*").await.unwrap();

    store
        .put("user.1", Bytes::from_static(b"alice"))
        .await
        .unwrap();
    store
        .put("admin.1", Bytes::from_static(b"root"))
        .await
        .unwrap();
    store
        .put("user.2", Bytes::from_static(b"bob"))
        .await
        .unwrap();

    let mut received = Vec::new();
    for _ in 0..2 {
        let entry = timeout(TIMEOUT, watch.next())
            .await
            .expect("watch timed out")
            .expect("watch ended")
            .expect("watch errored");
        received.push((entry.key, entry.value));
    }
    assert_eq!(
        received,
        vec![
            ("user.1".to_string(), Bytes::from_static(b"alice")),
            ("user.2".to_string(), Bytes::from_static(b"bob")),
        ]
    );
}

#[tokio::test]
async fn non_filterable_codec_rejects_wildcards() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "non-filterable").await;
    let store = store.with_key_codec(NonFilterableCodec);

    // Literal keys still watchable.
    assert!(store.watch("user.123").await.is_ok());

    for pattern in ["user.*", "user.>"] {
        let err = store.watch(pattern).await.unwrap_err();
        assert_eq!(err.kind(), KvCodecErrorKind::WildcardNotSupported);
        assert!(err.to_string().contains("wildcard"));
    }
}

#[tokio::test]
async fn watch_many_mixed_patterns() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;
    let store = common::create_bucket(&js, "watch-many").await;
    let store = CodecStore::new(store, Base64Codec, Base64Codec);

    let mut watch = store.watch_many(["user.*", "admin.1"]).await.unwrap();

    store
        .put("user.1", Bytes::from_static(b"alice"))
        .await
        .unwrap();
    store
        .put("other.1", Bytes::from_static(b"nobody"))
        .await
        .unwrap();
    store
        .put("admin.1", Bytes::from_static(b"root"))
        .await
        .unwrap();

    let mut received = HashSet::new();
    for _ in 0..2 {
        let entry = timeout(TIMEOUT, watch.next())
            .await
            .expect("watch timed out")
            .expect("watch ended")
            .expect("watch errored");
        received.insert(entry.key);
    }
    assert_eq!(
        received,
        HashSet::from(["user.1".to_string(), "admin.1".to_string()])
    );
}
