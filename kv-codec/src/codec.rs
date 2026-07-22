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

use bytes::Bytes;

use crate::errors::{CodecError, WildcardNotSupportedError};

/// Transforms keys before storage and after retrieval.
///
/// Implementations should be deterministic and reversible:
/// `decode_key(encode_key(key)) == key` for keys in the codec's canonical
/// notation (see [`PathCodec`](crate::PathCodec) for an example of
/// documented lossy edge cases). Encoded keys must be valid NATS KV keys
/// (subject tokens separated by `.`).
///
/// Codecs are stored by value in
/// [`CodecStore`](crate::CodecStore) and cloned into watcher streams; a
/// stateful codec that is not `Clone` (e.g. one holding an encryption key)
/// can be wrapped in [`std::sync::Arc`], which implements both codec
/// traits via blanket impls.
///
/// # Examples
///
/// ```
/// use kv_codec::{CodecError, KeyCodec};
///
/// /// Reverses each key token.
/// struct ReverseCodec;
///
/// impl KeyCodec for ReverseCodec {
///     fn encode_key(&self, key: &str) -> Result<String, CodecError> {
///         Ok(key
///             .split('.')
///             .map(|token| token.chars().rev().collect::<String>())
///             .collect::<Vec<_>>()
///             .join("."))
///     }
///
///     fn decode_key(&self, key: &str) -> Result<String, CodecError> {
///         self.encode_key(key)
///     }
/// }
///
/// assert_eq!(ReverseCodec.encode_key("abc.def").unwrap(), "cba.fed");
/// ```
pub trait KeyCodec: Send + Sync {
    /// Encodes a key for storage.
    fn encode_key(&self, key: &str) -> std::result::Result<String, CodecError>;

    /// Decodes a key retrieved from storage.
    fn decode_key(&self, key: &str) -> std::result::Result<String, CodecError>;

    /// Encodes a filter pattern that may contain `*` or `>` wildcards.
    ///
    /// Unlike [`encode_key`](KeyCodec::encode_key), wildcards must be
    /// preserved in the result so server-side filtering keeps working.
    ///
    /// The default implementation rejects patterns containing a wildcard
    /// character anywhere (matching orbit.go) with
    /// [`WildcardNotSupportedError`] and otherwise delegates to
    /// [`encode_key`](KeyCodec::encode_key). Codecs that can preserve
    /// wildcards should override it.
    ///
    /// # Examples
    ///
    /// ```
    /// use kv_codec::{Base64Codec, KeyCodec, NoOpCodec};
    ///
    /// // Built-ins override the default and keep wildcards intact.
    /// assert_eq!(Base64Codec.encode_filter("user.*").unwrap(), "dXNlcg.*");
    /// assert_eq!(NoOpCodec.encode_filter("user.>").unwrap(), "user.>");
    /// ```
    fn encode_filter(&self, filter: &str) -> std::result::Result<String, CodecError> {
        if filter.contains('*') || filter.contains('>') {
            Err(Box::new(WildcardNotSupportedError))
        } else {
            self.encode_key(filter)
        }
    }
}

/// Transforms values before storage and after retrieval.
///
/// Implementations must be deterministic and reversible:
/// `decode_value(encode_value(value)) == value`. They must also handle
/// empty input, as delete and purge markers carry empty values.
///
/// # Examples
///
/// ```
/// use bytes::Bytes;
/// use kv_codec::{CodecError, ValueCodec};
///
/// /// XORs every byte with a fixed mask.
/// struct XorCodec(u8);
///
/// impl ValueCodec for XorCodec {
///     fn encode_value(&self, value: Bytes) -> Result<Bytes, CodecError> {
///         Ok(value.iter().map(|byte| byte ^ self.0).collect())
///     }
///
///     fn decode_value(&self, value: Bytes) -> Result<Bytes, CodecError> {
///         self.encode_value(value)
///     }
/// }
/// ```
pub trait ValueCodec: Send + Sync {
    /// Encodes a value for storage.
    fn encode_value(&self, value: Bytes) -> std::result::Result<Bytes, CodecError>;

    /// Decodes a value retrieved from storage.
    fn decode_value(&self, value: Bytes) -> std::result::Result<Bytes, CodecError>;
}

macro_rules! forward_key_codec {
    ($wrapper:ty) => {
        impl<T: KeyCodec + ?Sized> KeyCodec for $wrapper {
            fn encode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
                (**self).encode_key(key)
            }

            fn decode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
                (**self).decode_key(key)
            }

            fn encode_filter(&self, filter: &str) -> std::result::Result<String, CodecError> {
                (**self).encode_filter(filter)
            }
        }
    };
}

macro_rules! forward_value_codec {
    ($wrapper:ty) => {
        impl<T: ValueCodec + ?Sized> ValueCodec for $wrapper {
            fn encode_value(&self, value: Bytes) -> std::result::Result<Bytes, CodecError> {
                (**self).encode_value(value)
            }

            fn decode_value(&self, value: Bytes) -> std::result::Result<Bytes, CodecError> {
                (**self).decode_value(value)
            }
        }
    };
}

forward_key_codec!(&T);
forward_key_codec!(std::sync::Arc<T>);
forward_key_codec!(Box<T>);
forward_value_codec!(&T);
forward_value_codec!(std::sync::Arc<T>);
forward_value_codec!(Box<T>);

/// A codec that passes keys and values through unchanged.
///
/// Implements both [`KeyCodec`] and [`ValueCodec`], and preserves wildcards
/// in filter patterns.
///
/// # Examples
///
/// ```
/// use kv_codec::{KeyCodec, NoOpCodec};
///
/// assert_eq!(NoOpCodec.encode_key("foo.bar").unwrap(), "foo.bar");
/// assert_eq!(NoOpCodec.encode_filter("foo.*").unwrap(), "foo.*");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoOpCodec;

impl KeyCodec for NoOpCodec {
    fn encode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
        Ok(key.to_string())
    }

    fn decode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
        Ok(key.to_string())
    }

    fn encode_filter(&self, filter: &str) -> std::result::Result<String, CodecError> {
        Ok(filter.to_string())
    }
}

impl ValueCodec for NoOpCodec {
    fn encode_value(&self, value: Bytes) -> std::result::Result<Bytes, CodecError> {
        Ok(value)
    }

    fn decode_value(&self, value: Bytes) -> std::result::Result<Bytes, CodecError> {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_key_passthrough() {
        let codec = NoOpCodec;
        assert_eq!(codec.encode_key("foo.bar.baz").unwrap(), "foo.bar.baz");
        assert_eq!(codec.decode_key("foo.bar.baz").unwrap(), "foo.bar.baz");
    }

    #[test]
    fn noop_value_passthrough() {
        let codec = NoOpCodec;
        let value = Bytes::from_static(b"test value");
        assert_eq!(codec.encode_value(value.clone()).unwrap(), value);
        assert_eq!(codec.decode_value(value.clone()).unwrap(), value);
    }

    #[test]
    fn noop_filter_preserves_wildcards() {
        let codec = NoOpCodec;
        for filter in ["user.123", "user.*", "user.>", "app.*.config.>"] {
            assert_eq!(codec.encode_filter(filter).unwrap(), filter);
        }
    }

    #[test]
    fn blanket_impls_forward_encode_filter() {
        // If the blanket impls fell back to the default encode_filter
        // instead of forwarding, a wrapped filterable codec would silently
        // lose wildcard support.
        use crate::Base64Codec;

        let by_ref = &Base64Codec;
        assert_eq!(by_ref.encode_filter("user.*").unwrap(), "dXNlcg.*");

        let arc = std::sync::Arc::new(Base64Codec);
        assert_eq!(arc.encode_filter("user.*").unwrap(), "dXNlcg.*");
        assert_eq!(arc.encode_key("user").unwrap(), "dXNlcg");
        assert_eq!(arc.decode_key("dXNlcg").unwrap(), "user");
        assert_eq!(
            arc.encode_value(Bytes::from_static(b"v")).unwrap(),
            Bytes::from_static(b"dg")
        );
        assert_eq!(
            arc.decode_value(Bytes::from_static(b"dg")).unwrap(),
            Bytes::from_static(b"v")
        );

        let boxed: Box<dyn KeyCodec> = Box::new(Base64Codec);
        assert_eq!(boxed.encode_filter("user.>").unwrap(), "dXNlcg.>");
    }

    #[test]
    fn default_encode_filter_rejects_wildcards() {
        struct Plain;
        impl KeyCodec for Plain {
            fn encode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
                Ok(format!("encoded_{key}"))
            }

            fn decode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
                Ok(key.trim_start_matches("encoded_").to_string())
            }
        }

        let codec = Plain;
        assert_eq!(codec.encode_filter("user.123").unwrap(), "encoded_user.123");
        // Wildcards anywhere are rejected, even mid-token (Go parity) —
        // otherwise a literal `*` in the encoded filter would create a
        // watch that can never match a valid key.
        for filter in ["user.*", "user.>", "foo*bar", "foo>bar"] {
            let err = codec.encode_filter(filter).unwrap_err();
            assert!(err.downcast_ref::<WildcardNotSupportedError>().is_some());
        }
    }
}
