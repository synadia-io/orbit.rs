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

//! Transparent key and value encoding for NATS JetStream Key-Value stores
//! ([ADR-54](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-54.md)).
//!
//! This crate wraps [`async_nats::jetstream::kv::Store`] with a
//! [`CodecStore`] that encodes keys and values before they are stored and
//! decodes them on retrieval. Typical uses are escaping characters that are
//! invalid in NATS subjects, translating path notation, and end-to-end
//! encryption.
//!
//! # Built-in codecs
//!
//! - [`NoOpCodec`] — passes keys and values through unchanged.
//! - [`Base64Codec`] — URL-safe base64; keys are encoded per token so
//!   server-side wildcard filtering keeps working.
//! - [`PathCodec`] — translates `/foo/bar` style keys to `foo.bar` subjects.
//!
//! Custom codecs implement [`KeyCodec`] and/or [`ValueCodec`]. Codecs can
//! be chained with tuples: `(PathCodec, Base64Codec)` encodes through the
//! path codec first, then base64, and decodes in reverse.
//!
//! # Examples
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), async_nats::Error> {
//! use futures_util::StreamExt;
//! use kv_codec::{Base64Codec, CodecStoreExt};
//!
//! let client = async_nats::connect("demo.nats.io:4222").await?;
//! let jetstream = async_nats::jetstream::new(client);
//! let store = jetstream
//!     .create_key_value(async_nats::jetstream::kv::Config {
//!         bucket: "contacts".to_string(),
//!         ..Default::default()
//!     })
//!     .await?;
//!
//! let kv = store.with_codecs(Base64Codec, Base64Codec);
//!
//! // Keys may contain characters that are invalid in plain KV keys.
//! kv.put("Acme Inc.contact info", "info@acme.com".into())
//!     .await?;
//! let entry = kv.entry("Acme Inc.contact info").await?;
//!
//! // Wildcard watches still work: Base64Codec preserves wildcards.
//! let mut watch = kv.watch("Acme Inc.>").await?;
//! while let Some(entry) = watch.next().await {
//!     println!("{:?}", entry?);
//! }
//! # Ok(())
//! # }
//! ```

mod base64;
mod chain;
mod codec;
mod errors;
mod path;
mod store;
mod watch;

pub use crate::base64::Base64Codec;
pub use crate::codec::{KeyCodec, NoOpCodec, ValueCodec};
pub use crate::errors::{
    CodecError, KvCodecError, KvCodecErrorKind, Result, WildcardNotSupportedError,
};
pub use crate::path::PathCodec;
pub use crate::store::{CodecStore, CodecStoreExt};
pub use crate::watch::{CodecHistory, CodecKeys, CodecWatch};
