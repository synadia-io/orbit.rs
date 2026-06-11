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

use std::fmt;

/// Boxed error returned by [`KeyCodec`](crate::KeyCodec) and
/// [`ValueCodec`](crate::ValueCodec) implementations.
pub type CodecError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Error type for codec-enabled KV store operations.
pub type KvCodecError = async_nats::error::Error<KvCodecErrorKind>;

/// Result type for codec-enabled KV store operations.
pub type Result<T> = std::result::Result<T, KvCodecError>;

/// Kinds of errors that can occur when working with a codec-enabled KV store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum KvCodecErrorKind {
    /// Failed to encode a key.
    KeyEncode,
    /// Failed to decode a key.
    KeyDecode,
    /// Failed to encode a value.
    ValueEncode,
    /// Failed to decode a value.
    ValueDecode,
    /// Failed to encode a filter pattern.
    FilterEncode,
    /// The key codec does not support wildcard filtering.
    WildcardNotSupported,
    /// The underlying KV store operation failed.
    ///
    /// The original async-nats error is preserved as the source and can be
    /// recovered by downcasting, e.g. to branch on create-if-absent:
    ///
    /// ```no_run
    /// use std::error::Error;
    ///
    /// use async_nats::jetstream::kv::{CreateError, CreateErrorKind};
    /// # fn check(err: kv_codec::KvCodecError) {
    /// let already_exists = err
    ///     .source()
    ///     .and_then(|source| source.downcast_ref::<CreateError>())
    ///     .is_some_and(|create| create.kind() == CreateErrorKind::AlreadyExists);
    /// # }
    /// ```
    Store,
    /// The underlying watcher stream failed.
    Watcher,
}

impl fmt::Display for KvCodecErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyEncode => write!(f, "failed to encode key"),
            Self::KeyDecode => write!(f, "failed to decode key"),
            Self::ValueEncode => write!(f, "failed to encode value"),
            Self::ValueDecode => write!(f, "failed to decode value"),
            Self::FilterEncode => write!(f, "failed to encode filter pattern"),
            Self::WildcardNotSupported => write!(
                f,
                "codec does not support wildcard filtering; use watch_all and filter client-side"
            ),
            Self::Store => write!(f, "key-value operation error"),
            Self::Watcher => write!(f, "watcher stream error"),
        }
    }
}

/// Error returned by the default [`KeyCodec::encode_filter`](crate::KeyCodec::encode_filter)
/// implementation when a filter contains wildcards the codec cannot preserve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WildcardNotSupportedError;

impl fmt::Display for WildcardNotSupportedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "codec does not support wildcard filtering")
    }
}

impl std::error::Error for WildcardNotSupportedError {}
