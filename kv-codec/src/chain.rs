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

//! Codec chaining via tuples.
//!
//! Tuples of codecs are codecs themselves: encoding applies elements
//! first-to-last, decoding applies them last-to-first. Filters are encoded
//! through every element, so a chain preserves wildcards only if all its
//! elements do.
//!
//! ```no_run
//! use kv_codec::{Base64Codec, KeyCodec, PathCodec};
//!
//! // Translate paths to subjects, then base64-encode each token.
//! let chain = (PathCodec, Base64Codec);
//! let encoded = chain.encode_key("/config/app").unwrap();
//! assert_eq!(chain.decode_key(&encoded).unwrap(), "/config/app");
//! ```

use bytes::Bytes;

use crate::codec::{KeyCodec, ValueCodec};
use crate::errors::CodecError;

macro_rules! impl_codec_tuple {
    ($($name:ident : $index:tt),+) => {
        impl<$($name: KeyCodec),+> KeyCodec for ($($name,)+) {
            fn encode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
                let mut key = key.to_string();
                $(key = self.$index.encode_key(&key)?;)+
                Ok(key)
            }

            fn decode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
                let mut key = key.to_string();
                impl_codec_tuple!(@reverse self, key, decode_key, [$($index),+] []);
                Ok(key)
            }

            fn encode_filter(&self, filter: &str) -> std::result::Result<String, CodecError> {
                let mut filter = filter.to_string();
                $(filter = self.$index.encode_filter(&filter)?;)+
                Ok(filter)
            }
        }

        impl<$($name: ValueCodec),+> ValueCodec for ($($name,)+) {
            fn encode_value(&self, value: Bytes) -> std::result::Result<Bytes, CodecError> {
                let mut value = value;
                $(value = self.$index.encode_value(value)?;)+
                Ok(value)
            }

            fn decode_value(&self, value: Bytes) -> std::result::Result<Bytes, CodecError> {
                let mut value = value;
                impl_codec_tuple!(@reverse self, value, decode_value, [$($index),+] []);
                Ok(value)
            }
        }
    };
    // Reverse the index list, then emit decode calls in that order.
    (@reverse $self:ident, $var:ident, $method:ident, [$head:tt $(, $tail:tt)*] [$($acc:tt)*]) => {
        impl_codec_tuple!(@reverse $self, $var, $method, [$($tail),*] [$head $($acc)*]);
    };
    (@reverse $self:ident, $var:ident, $method:ident, [] [$($acc:tt)*]) => {
        $($var = impl_codec_tuple!(@call $self, $var, $method, $acc)?;)*
    };
    (@call $self:ident, $var:ident, decode_key, $index:tt) => {
        $self.$index.decode_key(&$var)
    };
    (@call $self:ident, $var:ident, decode_value, $index:tt) => {
        $self.$index.decode_value($var)
    };
}

impl_codec_tuple!(A: 0);
impl_codec_tuple!(A: 0, B: 1);
impl_codec_tuple!(A: 0, B: 1, C: 2);
impl_codec_tuple!(A: 0, B: 1, C: 2, D: 3);
impl_codec_tuple!(A: 0, B: 1, C: 2, D: 3, E: 4);

#[cfg(test)]
mod tests {
    use crate::codec::{KeyCodec, NoOpCodec, ValueCodec};
    use crate::errors::{CodecError, WildcardNotSupportedError};
    use crate::{Base64Codec, PathCodec};
    use bytes::Bytes;

    /// Prefixes keys and values with a label; relies on the default
    /// (wildcard-rejecting) `encode_filter`.
    struct PrefixCodec(&'static str);

    impl KeyCodec for PrefixCodec {
        fn encode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
            Ok(format!("{}:{}", self.0, key))
        }

        fn decode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
            key.strip_prefix(&format!("{}:", self.0))
                .map(str::to_string)
                .ok_or_else(|| format!("missing prefix {}", self.0).into())
        }
    }

    impl ValueCodec for PrefixCodec {
        fn encode_value(&self, value: Bytes) -> std::result::Result<Bytes, CodecError> {
            let mut out = self.0.as_bytes().to_vec();
            out.push(b':');
            out.extend_from_slice(&value);
            Ok(out.into())
        }

        fn decode_value(&self, value: Bytes) -> std::result::Result<Bytes, CodecError> {
            let prefix = format!("{}:", self.0);
            value
                .strip_prefix(prefix.as_bytes())
                .map(Bytes::copy_from_slice)
                .ok_or_else(|| format!("missing prefix {}", self.0).into())
        }
    }

    struct FailingCodec;

    impl KeyCodec for FailingCodec {
        fn encode_key(&self, _key: &str) -> std::result::Result<String, CodecError> {
            Err("encode boom".into())
        }

        fn decode_key(&self, _key: &str) -> std::result::Result<String, CodecError> {
            Err("decode boom".into())
        }
    }

    #[test]
    fn key_chain_encodes_first_to_last() {
        let chain = (PrefixCodec("A"), PrefixCodec("B"));
        assert_eq!(chain.encode_key("test").unwrap(), "B:A:test");
        assert_eq!(chain.decode_key("B:A:test").unwrap(), "test");
    }

    #[test]
    fn value_chain_encodes_first_to_last() {
        let chain = (PrefixCodec("A"), PrefixCodec("B"));
        let encoded = chain.encode_value(Bytes::from_static(b"test")).unwrap();
        assert_eq!(&encoded[..], b"B:A:test");
        assert_eq!(&chain.decode_value(encoded).unwrap()[..], b"test");
    }

    #[test]
    fn path_then_base64_roundtrip() {
        let chain = (PathCodec, Base64Codec);
        for key in ["/simple/key", "/foo/bar/baz", "/single", "no/leading/slash"] {
            let encoded = chain.encode_key(key).unwrap();
            assert_ne!(encoded, key);
            assert_eq!(chain.decode_key(&encoded).unwrap(), key);
        }
    }

    #[test]
    fn nested_tuples() {
        let chain = ((PrefixCodec("A"), PrefixCodec("B")), PrefixCodec("C"));
        assert_eq!(chain.encode_key("test").unwrap(), "C:B:A:test");
        assert_eq!(chain.decode_key("C:B:A:test").unwrap(), "test");
    }

    #[test]
    fn triple_chain_decodes_in_reverse() {
        let chain = (PrefixCodec("A"), PrefixCodec("B"), PrefixCodec("C"));
        assert_eq!(chain.encode_key("test").unwrap(), "C:B:A:test");
        assert_eq!(chain.decode_key("C:B:A:test").unwrap(), "test");
    }

    #[test]
    fn chain_filter_through_all_members() {
        let chain = (Base64Codec, NoOpCodec, PathCodec);
        let encoded = chain.encode_filter("orders.*.status").unwrap();
        assert!(encoded.contains('*'));
        assert_eq!(encoded, "b3JkZXJz.*.c3RhdHVz");
    }

    #[test]
    fn chain_filter_rejects_non_filterable_member() {
        let chain = (Base64Codec, PrefixCodec("X"));
        let err = chain.encode_filter("orders.*.status").unwrap_err();
        assert!(err.downcast_ref::<WildcardNotSupportedError>().is_some());
        // Literal filters still pass through non-filterable members.
        assert!(chain.encode_filter("orders.status").is_ok());
    }

    #[test]
    fn chain_propagates_encode_errors() {
        let chain = (FailingCodec, Base64Codec);
        assert!(chain.encode_key("test").is_err());
        // Decode runs last-to-first: base64 decode of "dGVzdA" succeeds,
        // then FailingCodec fails.
        let err = (FailingCodec, Base64Codec)
            .decode_key("dGVzdA")
            .unwrap_err();
        assert_eq!(err.to_string(), "decode boom");
    }
}
