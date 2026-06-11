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

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;

use crate::codec::{KeyCodec, ValueCodec};
use crate::errors::CodecError;

/// Encodes keys and values using URL-safe base64 without padding
/// (matching Go's `base64.RawURLEncoding`).
///
/// Keys are encoded token by token (the parts between dots), preserving the
/// subject hierarchy so server-side filtering keeps working. Values are
/// encoded as a whole. Wildcard tokens (`*`, `>`) in filter patterns are
/// preserved.
///
/// This codec allows keys that would otherwise be invalid, such as keys
/// containing spaces or other special characters.
///
/// Wire-compatible with orbit.go's `Base64Codec`, with two caveats for
/// data written by other clients: decoded key tokens must be valid UTF-8
/// (Go accepts arbitrary bytes), and a foreign token that decodes to text
/// containing a dot produces a key that will not round-trip through
/// [`encode_key`](KeyCodec::encode_key). Not compatible with the legacy
/// nats.js `Base64KeyCodec`, which predates ADR-54 and uses the standard
/// padded base64 alphabet.
///
/// # Examples
///
/// ```
/// use kv_codec::{Base64Codec, KeyCodec};
///
/// // Tokens are encoded individually; wildcards survive in filters.
/// assert_eq!(Base64Codec.encode_key("test.key").unwrap(), "dGVzdA.a2V5");
/// assert_eq!(Base64Codec.decode_key("dGVzdA.a2V5").unwrap(), "test.key");
/// assert_eq!(Base64Codec.encode_filter("user.*").unwrap(), "dXNlcg.*");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Base64Codec;

impl KeyCodec for Base64Codec {
    fn encode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
        Ok(key
            .split('.')
            .map(|token| URL_SAFE_NO_PAD.encode(token))
            .collect::<Vec<_>>()
            .join("."))
    }

    fn decode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
        let tokens = key
            .split('.')
            .map(|token| {
                let decoded = URL_SAFE_NO_PAD
                    .decode(token)
                    .map_err(|err| format!("failed to decode base64 token {token:?}: {err}"))?;
                String::from_utf8(decoded)
                    .map_err(|err| format!("token {token:?} is not valid UTF-8: {err}"))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(tokens.join("."))
    }

    fn encode_filter(&self, filter: &str) -> std::result::Result<String, CodecError> {
        Ok(filter
            .split('.')
            .map(|token| match token {
                "*" | ">" => token.to_string(),
                _ => URL_SAFE_NO_PAD.encode(token),
            })
            .collect::<Vec<_>>()
            .join("."))
    }
}

impl ValueCodec for Base64Codec {
    fn encode_value(&self, value: Bytes) -> std::result::Result<Bytes, CodecError> {
        Ok(URL_SAFE_NO_PAD.encode(&value).into())
    }

    fn decode_value(&self, value: Bytes) -> std::result::Result<Bytes, CodecError> {
        URL_SAFE_NO_PAD
            .decode(&value)
            .map(Bytes::from)
            .map_err(|err| format!("failed to decode base64 value: {err}").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_roundtrip_token_level() {
        let codec = Base64Codec;
        let key = "test.key.with.special.chars!@#$%^&*()";
        let encoded = codec.encode_key(key).unwrap();
        assert_eq!(
            encoded,
            "dGVzdA.a2V5.d2l0aA.c3BlY2lhbA.Y2hhcnMhQCMkJV4mKigp"
        );
        assert_eq!(codec.decode_key(&encoded).unwrap(), key);
    }

    #[test]
    fn value_roundtrip() {
        let codec = Base64Codec;
        let value = Bytes::from_static(b"test value with special chars: !@#$%^&*()");
        let encoded = codec.encode_value(value.clone()).unwrap();
        assert_ne!(encoded, value);
        assert_eq!(codec.decode_value(encoded).unwrap(), value);
    }

    #[test]
    fn empty_value_roundtrip() {
        let codec = Base64Codec;
        let encoded = codec.encode_value(Bytes::new()).unwrap();
        assert!(encoded.is_empty());
        assert!(codec.decode_value(encoded).unwrap().is_empty());
    }

    #[test]
    fn decode_key_invalid_token_errors() {
        let codec = Base64Codec;
        assert!(codec.decode_key("!!!").is_err());
        // "z" has an invalid base64 length.
        assert!(codec.decode_key("z").is_err());
    }

    #[test]
    fn encode_filter_preserves_wildcards() {
        let codec = Base64Codec;
        assert_eq!(codec.encode_filter("user.123").unwrap(), "dXNlcg.MTIz");
        assert_eq!(codec.encode_filter("user.*").unwrap(), "dXNlcg.*");
        assert_eq!(codec.encode_filter("user.>").unwrap(), "dXNlcg.>");
        assert_eq!(
            codec.encode_filter("app.*.config.>").unwrap(),
            "YXBw.*.Y29uZmln.>"
        );
    }

    #[test]
    fn encode_key_does_not_preserve_wildcards() {
        // Only encode_filter treats * and > specially; as key data they
        // are encoded like any other token.
        let codec = Base64Codec;
        assert_eq!(codec.encode_key("user.*").unwrap(), "dXNlcg.Kg");
    }

    #[test]
    fn unicode_key_roundtrip() {
        let codec = Base64Codec;
        let key = "użytkownik.imię";
        let encoded = codec.encode_key(key).unwrap();
        assert_eq!(codec.decode_key(&encoded).unwrap(), key);
    }
}
