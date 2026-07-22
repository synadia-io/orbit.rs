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

use std::time::Duration;

use async_nats::jetstream::kv::bucket::Status;
use async_nats::jetstream::kv::{Entry, Operation, Store};
use bytes::Bytes;

use crate::codec::{KeyCodec, NoOpCodec, ValueCodec};
use crate::errors::{
    CodecError, KvCodecError, KvCodecErrorKind, Result, WildcardNotSupportedError,
};
use crate::watch::{CodecHistory, CodecKeys, CodecWatch};

/// A wrapper around [`Store`] that transparently encodes and decodes keys
/// and values using the provided codecs.
///
/// All operations behave like their [`Store`] counterparts, with keys and
/// values encoded before they reach the underlying bucket and decoded on
/// the way back.
///
/// # Examples
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error> {
/// use kv_codec::{Base64Codec, CodecStoreExt, PathCodec};
///
/// let client = async_nats::connect("demo.nats.io:4222").await?;
/// let jetstream = async_nats::jetstream::new(client);
/// let store = jetstream
///     .create_key_value(async_nats::jetstream::kv::Config {
///         bucket: "config".to_string(),
///         ..Default::default()
///     })
///     .await?;
///
/// // Path-style keys, base64-encoded values.
/// let kv = store.with_codecs(PathCodec, Base64Codec);
/// kv.put("/config/app/database", "localhost".into()).await?;
/// let value = kv.get("/config/app/database").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct CodecStore<K = NoOpCodec, V = NoOpCodec>
where
    K: KeyCodec,
    V: ValueCodec,
{
    store: Store,
    key_codec: K,
    value_codec: V,
}

fn key_encode_err(err: CodecError) -> KvCodecError {
    KvCodecError::with_source(KvCodecErrorKind::KeyEncode, err)
}

fn value_encode_err(err: CodecError) -> KvCodecError {
    KvCodecError::with_source(KvCodecErrorKind::ValueEncode, err)
}

fn store_err<E>(err: E) -> KvCodecError
where
    E: std::error::Error + Send + Sync + 'static,
{
    KvCodecError::with_source(KvCodecErrorKind::Store, err)
}

fn filter_err(err: CodecError) -> KvCodecError {
    if err.downcast_ref::<WildcardNotSupportedError>().is_some() {
        KvCodecError::new(KvCodecErrorKind::WildcardNotSupported)
    } else {
        KvCodecError::with_source(KvCodecErrorKind::FilterEncode, err)
    }
}

impl<K: KeyCodec> CodecStore<K, NoOpCodec> {
    /// Creates a codec-enabled KV store that only encodes keys.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use kv_codec::{CodecStore, PathCodec};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let store = jetstream.get_key_value("bucket").await?;
    /// let kv = CodecStore::for_key(store, PathCodec);
    /// kv.put("/etc/hosts", "127.0.0.1".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn for_key(store: Store, key_codec: K) -> Self {
        Self::new(store, key_codec, NoOpCodec)
    }
}

impl<V: ValueCodec> CodecStore<NoOpCodec, V> {
    /// Creates a codec-enabled KV store that only encodes values.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use kv_codec::{Base64Codec, CodecStore};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let store = jetstream.get_key_value("bucket").await?;
    /// let kv = CodecStore::for_value(store, Base64Codec);
    /// kv.put("plain.key", "encoded value".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn for_value(store: Store, value_codec: V) -> Self {
        Self::new(store, NoOpCodec, value_codec)
    }
}

impl<K: KeyCodec, V: ValueCodec> CodecStore<K, V> {
    /// Creates a codec-enabled KV store with the given key and value codecs.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use kv_codec::{Base64Codec, CodecStore, PathCodec};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let store = jetstream.get_key_value("bucket").await?;
    /// let kv = CodecStore::new(store, PathCodec, Base64Codec);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(store: Store, key_codec: K, value_codec: V) -> Self {
        Self {
            store,
            key_codec,
            value_codec,
        }
    }

    /// Returns a reference to the underlying [`Store`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_key_codec(Base64Codec);
    /// // Read the raw, encoded form.
    /// let raw = kv.inner().entry("QWNtZQ").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn inner(&self) -> &Store {
        &self.store
    }

    /// Consumes the wrapper and returns the underlying [`Store`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_key_codec(Base64Codec);
    /// let store = kv.into_inner();
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_inner(self) -> Store {
        self.store
    }

    fn encode_key(&self, key: &str) -> Result<String> {
        self.key_codec.encode_key(key).map_err(key_encode_err)
    }

    fn encode_value(&self, value: Bytes) -> Result<Bytes> {
        self.value_codec
            .encode_value(value)
            .map_err(value_encode_err)
    }

    fn encode_filter(&self, filter: &str) -> Result<String> {
        self.key_codec.encode_filter(filter).map_err(filter_err)
    }

    /// Decodes a retrieved entry, restoring the caller-supplied key.
    fn decode_entry(&self, original_key: &str, mut entry: Entry) -> Result<Entry> {
        entry.key = original_key.to_string();
        if entry.operation == Operation::Put {
            entry.value = self
                .value_codec
                .decode_value(entry.value)
                .map_err(|err| KvCodecError::with_source(KvCodecErrorKind::ValueDecode, err))?;
        }
        Ok(entry)
    }

    /// Returns the status of the underlying bucket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_key_codec(Base64Codec);
    /// let status = kv.status().await?;
    /// println!("bucket: {}", status.bucket);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn status(&self) -> Result<Status> {
        self.store.status().await.map_err(store_err)
    }

    /// Puts a key-value pair into the bucket, encoding both.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// // Keys may contain characters invalid in plain KV keys.
    /// let revision = kv.put("Acme Inc.contact", "info@acme.com".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn put<T: AsRef<str>>(&self, key: T, value: Bytes) -> Result<u64> {
        let key = self.encode_key(key.as_ref())?;
        let value = self.encode_value(value)?;
        self.store.put(key, value).await.map_err(store_err)
    }

    /// Creates a key-value pair only if the key does not exist (or was
    /// deleted/purged).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let revision = kv.create("unique.key", "value".into()).await?;
    /// assert!(kv.create("unique.key", "again".into()).await.is_err());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create<T: AsRef<str>>(&self, key: T, value: Bytes) -> Result<u64> {
        let key = self.encode_key(key.as_ref())?;
        let value = self.encode_value(value)?;
        self.store.create(key, value).await.map_err(store_err)
    }

    /// Like [`CodecStore::create`], with a message TTL. Requires a bucket
    /// with limit markers enabled.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use std::time::Duration;
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// kv.create_with_ttl("session.token", "abc".into(), Duration::from_secs(60))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_with_ttl<T: AsRef<str>>(
        &self,
        key: T,
        value: Bytes,
        ttl: Duration,
    ) -> Result<u64> {
        let key = self.encode_key(key.as_ref())?;
        let value = self.encode_value(value)?;
        self.store
            .create_with_ttl(key, value, ttl)
            .await
            .map_err(store_err)
    }

    /// Updates a key's value if the revision matches.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let revision = kv.put("config.level", "info".into()).await?;
    /// kv.update("config.level", "debug".into(), revision).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update<T: AsRef<str>>(&self, key: T, value: Bytes, revision: u64) -> Result<u64> {
        let key = self.encode_key(key.as_ref())?;
        let value = self.encode_value(value)?;
        self.store
            .update(key, value, revision)
            .await
            .map_err(store_err)
    }

    /// Returns the decoded value for a key, or `None` if the key does not
    /// exist or is marked as deleted/purged.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// if let Some(value) = kv.get("Acme Inc.contact").await? {
    ///     println!("value: {value:?}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get<T: AsRef<str>>(&self, key: T) -> Result<Option<Bytes>> {
        let key = self.encode_key(key.as_ref())?;
        match self.store.get(key).await.map_err(store_err)? {
            Some(value) => self
                .value_codec
                .decode_value(value)
                .map(Some)
                .map_err(|err| KvCodecError::with_source(KvCodecErrorKind::ValueDecode, err)),
            None => Ok(None),
        }
    }

    /// Returns the decoded [`Entry`] for a key. The entry's key is the
    /// caller-supplied (decoded) key.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// if let Some(entry) = kv.entry("Acme Inc.contact").await? {
    ///     println!("{} @ rev {}: {:?}", entry.key, entry.revision, entry.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn entry<T: AsRef<str>>(&self, key: T) -> Result<Option<Entry>> {
        let key = key.as_ref();
        let encoded = self.encode_key(key)?;
        match self.store.entry(encoded).await.map_err(store_err)? {
            Some(entry) => self.decode_entry(key, entry).map(Some),
            None => Ok(None),
        }
    }

    /// Returns the decoded [`Entry`] for a key at the given revision.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let revision = kv.put("config.level", "info".into()).await?;
    /// let entry = kv.entry_for_revision("config.level", revision).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn entry_for_revision<T: AsRef<str>>(
        &self,
        key: T,
        revision: u64,
    ) -> Result<Option<Entry>> {
        let key = key.as_ref();
        let encoded = self.encode_key(key)?;
        match self
            .store
            .entry_for_revision(encoded, revision)
            .await
            .map_err(store_err)?
        {
            Some(entry) => self.decode_entry(key, entry).map(Some),
            None => Ok(None),
        }
    }

    /// Deletes a key, placing a delete marker.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// kv.delete("Acme Inc.contact").await?;
    /// assert_eq!(kv.get("Acme Inc.contact").await?, None);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete<T: AsRef<str>>(&self, key: T) -> Result<()> {
        let key = self.encode_key(key.as_ref())?;
        self.store.delete(key).await.map_err(store_err)
    }

    /// Deletes a key if the revision matches.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let revision = kv.put("config.level", "info".into()).await?;
    /// kv.delete_expect_revision("config.level", Some(revision)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_expect_revision<T: AsRef<str>>(
        &self,
        key: T,
        revision: Option<u64>,
    ) -> Result<()> {
        let key = self.encode_key(key.as_ref())?;
        self.store
            .delete_expect_revision(key, revision)
            .await
            .map_err(store_err)
    }

    /// Destructively purges all revisions of a key, leaving a single purge
    /// marker.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// kv.purge("Acme Inc.contact").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn purge<T: AsRef<str>>(&self, key: T) -> Result<()> {
        let key = self.encode_key(key.as_ref())?;
        self.store.purge(key).await.map_err(store_err)
    }

    /// Like [`CodecStore::purge`], with a TTL on the purge marker.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use std::time::Duration;
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// kv.purge_with_ttl("Acme Inc.contact", Duration::from_secs(60))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn purge_with_ttl<T: AsRef<str>>(&self, key: T, ttl: Duration) -> Result<()> {
        let key = self.encode_key(key.as_ref())?;
        self.store.purge_with_ttl(key, ttl).await.map_err(store_err)
    }

    /// Purges all revisions of a key if the revision matches.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let revision = kv.put("config.level", "info".into()).await?;
    /// kv.purge_expect_revision("config.level", Some(revision)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn purge_expect_revision<T: AsRef<str>>(
        &self,
        key: T,
        revision: Option<u64>,
    ) -> Result<()> {
        let key = self.encode_key(key.as_ref())?;
        self.store
            .purge_expect_revision(key, revision)
            .await
            .map_err(store_err)
    }

    /// Like [`CodecStore::purge_expect_revision`], with a TTL on the purge
    /// marker.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use std::time::Duration;
    /// # use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let revision = kv.put("config.level", "info".into()).await?;
    /// kv.purge_expect_revision_with_ttl("config.level", revision, Duration::from_secs(60))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn purge_expect_revision_with_ttl<T: AsRef<str>>(
        &self,
        key: T,
        revision: u64,
        ttl: Duration,
    ) -> Result<()> {
        let key = self.encode_key(key.as_ref())?;
        self.store
            .purge_expect_revision_with_ttl(key, revision, ttl)
            .await
            .map_err(store_err)
    }
}

impl<K, V> CodecStore<K, V>
where
    K: KeyCodec + Clone,
    V: ValueCodec + Clone,
{
    fn wrap_watch(&self, watch: async_nats::jetstream::kv::Watch) -> CodecWatch<K, V> {
        CodecWatch {
            inner: watch,
            key_codec: self.key_codec.clone(),
            value_codec: self.value_codec.clone(),
        }
    }

    /// Watches keys matching the (possibly wildcarded) pattern, yielding
    /// decoded entries for new updates only.
    ///
    /// Returns [`KvCodecErrorKind::WildcardNotSupported`] when the pattern
    /// contains wildcards and the key codec cannot preserve them. Unlike
    /// orbit.go's `Watch`, current values are not delivered first; use
    /// [`CodecStore::watch_with_history`] for that.
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
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let mut watch = kv.watch("user.*").await?;
    /// while let Some(entry) = watch.next().await {
    ///     println!("{:?}", entry?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch<T: AsRef<str>>(&self, key: T) -> Result<CodecWatch<K, V>> {
        let filter = self.encode_filter(key.as_ref())?;
        self.store
            .watch(filter)
            .await
            .map(|watch| self.wrap_watch(watch))
            .map_err(store_err)
    }

    /// Like [`CodecStore::watch`], but delivers the latest entry per key
    /// first.
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
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let mut watch = kv.watch_with_history("user.*").await?;
    /// while let Some(entry) = watch.next().await {
    ///     println!("{:?}", entry?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_with_history<T: AsRef<str>>(&self, key: T) -> Result<CodecWatch<K, V>> {
        let filter = self.encode_filter(key.as_ref())?;
        self.store
            .watch_with_history(filter)
            .await
            .map(|watch| self.wrap_watch(watch))
            .map_err(store_err)
    }

    /// Like [`CodecStore::watch`], starting from a given revision.
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
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let mut watch = kv.watch_from_revision("user.*", 42).await?;
    /// while let Some(entry) = watch.next().await {
    ///     println!("{:?}", entry?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_from_revision<T: AsRef<str>>(
        &self,
        key: T,
        revision: u64,
    ) -> Result<CodecWatch<K, V>> {
        let filter = self.encode_filter(key.as_ref())?;
        self.store
            .watch_from_revision(filter, revision)
            .await
            .map(|watch| self.wrap_watch(watch))
            .map_err(store_err)
    }

    /// Watches multiple (possibly wildcarded) patterns at once, yielding
    /// decoded entries.
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
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let mut watch = kv.watch_many(["user.*", "admin.1"]).await?;
    /// while let Some(entry) = watch.next().await {
    ///     println!("{:?}", entry?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_many<T, I>(&self, keys: I) -> Result<CodecWatch<K, V>>
    where
        T: AsRef<str>,
        I: IntoIterator<Item = T>,
    {
        let filters = keys
            .into_iter()
            .map(|key| self.encode_filter(key.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        self.store
            .watch_many(filters)
            .await
            .map(|watch| self.wrap_watch(watch))
            .map_err(store_err)
    }

    /// Like [`CodecStore::watch_many`], but delivers the latest entry per
    /// key first.
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
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let mut watch = kv.watch_many_with_history(["user.*", "admin.1"]).await?;
    /// while let Some(entry) = watch.next().await {
    ///     println!("{:?}", entry?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_many_with_history<T, I>(&self, keys: I) -> Result<CodecWatch<K, V>>
    where
        T: AsRef<str>,
        I: IntoIterator<Item = T>,
    {
        let filters = keys
            .into_iter()
            .map(|key| self.encode_filter(key.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        self.store
            .watch_many_with_history(filters)
            .await
            .map(|watch| self.wrap_watch(watch))
            .map_err(store_err)
    }

    /// Watches all keys in the bucket, yielding decoded entries. No filter
    /// encoding is involved.
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
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let mut watch = kv.watch_all().await?;
    /// while let Some(entry) = watch.next().await {
    ///     println!("{:?}", entry?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_all(&self) -> Result<CodecWatch<K, V>> {
        self.store
            .watch_all()
            .await
            .map(|watch| self.wrap_watch(watch))
            .map_err(store_err)
    }

    /// Like [`CodecStore::watch_all`], starting from a given revision.
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
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let mut watch = kv.watch_all_from_revision(42).await?;
    /// while let Some(entry) = watch.next().await {
    ///     println!("{:?}", entry?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_all_from_revision(&self, revision: u64) -> Result<CodecWatch<K, V>> {
        self.store
            .watch_all_from_revision(revision)
            .await
            .map(|watch| self.wrap_watch(watch))
            .map_err(store_err)
    }
}

impl<K, V> CodecStore<K, V>
where
    K: KeyCodec,
    V: ValueCodec + Clone,
{
    /// Returns a stream of decoded historical entries for a key. Entries
    /// carry the caller-supplied key verbatim.
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
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let mut history = kv.history("config.level").await?;
    /// while let Some(entry) = history.next().await {
    ///     let entry = entry?;
    ///     println!("rev {}: {:?}", entry.revision, entry.operation);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn history<T: AsRef<str>>(&self, key: T) -> Result<CodecHistory<V>> {
        let key = key.as_ref();
        let encoded = self.encode_key(key)?;
        self.store
            .history(encoded)
            .await
            .map(|history| CodecHistory {
                inner: history,
                value_codec: self.value_codec.clone(),
                original_key: key.to_string(),
            })
            .map_err(store_err)
    }
}

impl<K, V> CodecStore<K, V>
where
    K: KeyCodec + Clone,
    V: ValueCodec,
{
    /// Returns a stream of decoded keys in the bucket. Keys that fail to
    /// decode surface as `Err` items.
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
    /// # let kv = jetstream.get_key_value("bucket").await?.with_codecs(Base64Codec, Base64Codec);
    /// let mut keys = kv.keys().await?;
    /// while let Some(key) = keys.next().await {
    ///     println!("{}", key?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn keys(&self) -> Result<CodecKeys<K>> {
        self.store
            .keys()
            .await
            .map(|keys| CodecKeys {
                inner: keys,
                key_codec: self.key_codec.clone(),
            })
            .map_err(store_err)
    }
}

/// Extension trait adding codec wrappers to [`Store`].
pub trait CodecStoreExt {
    /// Wraps the store with the given key and value codecs.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use kv_codec::{Base64Codec, CodecStoreExt, PathCodec};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let store = jetstream.get_key_value("bucket").await?;
    /// let kv = store.with_codecs(PathCodec, Base64Codec);
    /// # Ok(())
    /// # }
    /// ```
    fn with_codecs<K: KeyCodec, V: ValueCodec>(
        self,
        key_codec: K,
        value_codec: V,
    ) -> CodecStore<K, V>;

    /// Wraps the store with a key codec only.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use kv_codec::{CodecStoreExt, PathCodec};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let store = jetstream.get_key_value("bucket").await?;
    /// let kv = store.with_key_codec(PathCodec);
    /// # Ok(())
    /// # }
    /// ```
    fn with_key_codec<K: KeyCodec>(self, key_codec: K) -> CodecStore<K, NoOpCodec>;

    /// Wraps the store with a value codec only.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use kv_codec::{Base64Codec, CodecStoreExt};
    /// # let client = async_nats::connect("demo.nats.io:4222").await?;
    /// # let jetstream = async_nats::jetstream::new(client);
    /// # let store = jetstream.get_key_value("bucket").await?;
    /// let kv = store.with_value_codec(Base64Codec);
    /// # Ok(())
    /// # }
    /// ```
    fn with_value_codec<V: ValueCodec>(self, value_codec: V) -> CodecStore<NoOpCodec, V>;
}

impl CodecStoreExt for Store {
    fn with_codecs<K: KeyCodec, V: ValueCodec>(
        self,
        key_codec: K,
        value_codec: V,
    ) -> CodecStore<K, V> {
        CodecStore::new(self, key_codec, value_codec)
    }

    fn with_key_codec<K: KeyCodec>(self, key_codec: K) -> CodecStore<K, NoOpCodec> {
        CodecStore::for_key(self, key_codec)
    }

    fn with_value_codec<V: ValueCodec>(self, value_codec: V) -> CodecStore<NoOpCodec, V> {
        CodecStore::for_value(self, value_codec)
    }
}
