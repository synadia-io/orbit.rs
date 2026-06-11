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

use std::pin::Pin;
use std::task::{Context, Poll};

use async_nats::jetstream::kv::{Entry, History, Keys, Operation, Watch};
use futures_util::{Stream, StreamExt};

use crate::codec::{KeyCodec, ValueCodec};
use crate::errors::{KvCodecError, KvCodecErrorKind};

fn decode_entry<K: KeyCodec, V: ValueCodec>(
    key_codec: &K,
    value_codec: &V,
    mut entry: Entry,
) -> Result<Entry, KvCodecError> {
    entry.key = key_codec
        .decode_key(&entry.key)
        .map_err(|err| KvCodecError::with_source(KvCodecErrorKind::KeyDecode, err))?;
    // Delete and purge markers carry empty values; don't run them through
    // the value codec.
    if entry.operation == Operation::Put {
        entry.value = value_codec
            .decode_value(entry.value)
            .map_err(|err| KvCodecError::with_source(KvCodecErrorKind::ValueDecode, err))?;
    }
    Ok(entry)
}

/// A stream of decoded entries from a watch operation.
///
/// Yields an error item when the underlying watcher fails or an entry
/// cannot be decoded.
///
/// # Examples
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error> {
/// use futures_util::StreamExt;
/// # use kv_codec::{Base64Codec, CodecStoreExt};
/// # let client = async_nats::connect("demo.nats.io:4222").await?;
/// # let jetstream = async_nats::jetstream::new(client);
/// # let kv = jetstream.get_key_value("bucket").await?.with_key_codec(Base64Codec);
/// let mut watch = kv.watch("user.*").await?;
/// while let Some(entry) = watch.next().await {
///     let entry = entry?;
///     println!("{}: {:?}", entry.key, entry.value);
/// }
/// # Ok(())
/// # }
/// ```
pub struct CodecWatch<K, V> {
    pub(crate) inner: Watch,
    pub(crate) key_codec: K,
    pub(crate) value_codec: V,
}

impl<K, V> std::fmt::Debug for CodecWatch<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodecWatch").finish_non_exhaustive()
    }
}

impl<K, V> Stream for CodecWatch<K, V>
where
    K: KeyCodec + Unpin,
    V: ValueCodec + Unpin,
{
    type Item = Result<Entry, KvCodecError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.inner.poll_next_unpin(cx).map(|item| {
            item.map(|result| match result {
                Ok(entry) => decode_entry(&this.key_codec, &this.value_codec, entry),
                Err(err) => Err(KvCodecError::with_source(KvCodecErrorKind::Watcher, err)),
            })
        })
    }
}

/// A stream of decoded historical entries for a key.
///
/// Entries carry the caller-supplied (decoded) key verbatim, like
/// [`CodecStore::entry`](crate::CodecStore::entry). Yields an error item
/// when the underlying watcher fails or a value cannot be decoded.
///
/// # Examples
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error> {
/// use futures_util::StreamExt;
/// # use kv_codec::{Base64Codec, CodecStoreExt};
/// # let client = async_nats::connect("demo.nats.io:4222").await?;
/// # let jetstream = async_nats::jetstream::new(client);
/// # let kv = jetstream.get_key_value("bucket").await?.with_key_codec(Base64Codec);
/// let revisions = kv
///     .history("config.level")
///     .await?
///     .collect::<Vec<_>>()
///     .await;
/// # Ok(())
/// # }
/// ```
pub struct CodecHistory<V> {
    pub(crate) inner: History,
    pub(crate) value_codec: V,
    pub(crate) original_key: String,
}

impl<V> std::fmt::Debug for CodecHistory<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodecHistory").finish_non_exhaustive()
    }
}

impl<V: ValueCodec + Unpin> Stream for CodecHistory<V> {
    type Item = Result<Entry, KvCodecError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.inner.poll_next_unpin(cx).map(|item| {
            item.map(|result| match result {
                Ok(mut entry) => {
                    entry.key = this.original_key.clone();
                    if entry.operation == Operation::Put {
                        entry.value =
                            this.value_codec.decode_value(entry.value).map_err(|err| {
                                KvCodecError::with_source(KvCodecErrorKind::ValueDecode, err)
                            })?;
                    }
                    Ok(entry)
                }
                Err(err) => Err(KvCodecError::with_source(KvCodecErrorKind::Watcher, err)),
            })
        })
    }
}

/// A stream of decoded keys in the bucket.
///
/// Unlike orbit.go's key lister, keys that fail to decode are not skipped
/// silently: they surface as `Err` items. Use
/// `.filter_map(|key| key.ok())` to get the skipping behavior.
///
/// # Examples
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error> {
/// use futures_util::StreamExt;
/// # use kv_codec::{Base64Codec, CodecStoreExt};
/// # let client = async_nats::connect("demo.nats.io:4222").await?;
/// # let jetstream = async_nats::jetstream::new(client);
/// # let kv = jetstream.get_key_value("bucket").await?.with_key_codec(Base64Codec);
/// let keys = kv
///     .keys()
///     .await?
///     .filter_map(|key| async { key.ok() })
///     .collect::<Vec<_>>()
///     .await;
/// # Ok(())
/// # }
/// ```
pub struct CodecKeys<K> {
    pub(crate) inner: Keys,
    pub(crate) key_codec: K,
}

impl<K> std::fmt::Debug for CodecKeys<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodecKeys").finish_non_exhaustive()
    }
}

impl<K: KeyCodec + Unpin> Stream for CodecKeys<K> {
    type Item = Result<String, KvCodecError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.inner.poll_next_unpin(cx).map(|item| {
            item.map(|result| match result {
                Ok(key) => this
                    .key_codec
                    .decode_key(&key)
                    .map_err(|err| KvCodecError::with_source(KvCodecErrorKind::KeyDecode, err)),
                Err(err) => Err(KvCodecError::with_source(KvCodecErrorKind::Watcher, err)),
            })
        })
    }
}
