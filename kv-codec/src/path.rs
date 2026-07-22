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

use crate::codec::KeyCodec;
use crate::errors::CodecError;

const ROOT_PREFIX: &str = "_root_";
const ROOT_PREFIX_DOT: &str = "_root_.";

/// Translates between path-style keys (`/foo/bar`) and NATS-style keys
/// (`foo.bar`).
///
/// Since NATS subjects cannot start with a dot, a leading `/` is encoded as
/// the `_root_` sentinel (`/foo/bar` becomes `_root_.foo.bar`, `/` alone
/// becomes `_root_`). A single trailing `/` is trimmed and therefore not
/// preserved on decode.
///
/// Round-trips are guaranteed only for keys in path notation (matching
/// orbit.go). Keys containing dots decode differently than they were
/// written (`a.b` is stored as `a.b` but decodes to `a/b`), and keys
/// starting with a literal `_root_` segment decode as if they had a
/// leading `/`.
///
/// Key-only codec: use it together with a [`ValueCodec`](crate::ValueCodec)
/// (or none) for values. Filter patterns are encoded like keys, which
/// preserves wildcards since `*` and `>` contain no separators.
///
/// # Examples
///
/// ```
/// use kv_codec::{KeyCodec, PathCodec};
///
/// assert_eq!(PathCodec.encode_key("/foo/bar").unwrap(), "_root_.foo.bar");
/// assert_eq!(PathCodec.decode_key("_root_.foo.bar").unwrap(), "/foo/bar");
/// assert_eq!(PathCodec.encode_filter("/user/*").unwrap(), "_root_.user.*");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PathCodec;

impl KeyCodec for PathCodec {
    fn encode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
        let key = match key.strip_prefix('/') {
            Some("") => return Ok(ROOT_PREFIX.to_string()),
            Some(rest) => format!("{ROOT_PREFIX}/{rest}"),
            None => key.to_string(),
        };
        let key = key.strip_suffix('/').unwrap_or(&key);
        Ok(key.replace('/', "."))
    }

    fn decode_key(&self, key: &str) -> std::result::Result<String, CodecError> {
        if key == ROOT_PREFIX {
            return Ok("/".to_string());
        }
        if let Some(rest) = key.strip_prefix(ROOT_PREFIX_DOT) {
            return Ok(format!("/{}", rest.replace('.', "/")));
        }
        Ok(key.replace('.', "/"))
    }

    fn encode_filter(&self, filter: &str) -> std::result::Result<String, CodecError> {
        self.encode_key(filter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_table() {
        let codec = PathCodec;
        // (input, stored, decoded-back)
        let cases = [
            ("/foo/bar", "_root_.foo.bar", "/foo/bar"),
            ("foo/bar", "foo.bar", "foo/bar"),
            (
                "/foo/bar/baz/qux",
                "_root_.foo.bar.baz.qux",
                "/foo/bar/baz/qux",
            ),
            ("/foo", "_root_.foo", "/foo"),
            // Trailing slash is lossy.
            ("foo/bar/", "foo.bar", "foo/bar"),
            ("/", "_root_", "/"),
            ("/foo/bar/", "_root_.foo.bar", "/foo/bar"),
        ];
        for (input, stored, decoded) in cases {
            assert_eq!(codec.encode_key(input).unwrap(), stored, "encode {input}");
            assert_eq!(
                codec.decode_key(stored).unwrap(),
                decoded,
                "decode {stored}"
            );
        }
    }

    #[test]
    fn decode_turns_dots_into_slashes() {
        // Decoding is dot-to-slash even if the original never used slashes.
        assert_eq!(PathCodec.decode_key("list.a").unwrap(), "list/a");
    }

    #[test]
    fn only_one_trailing_slash_trimmed() {
        // Go TrimSuffix parity: a second trailing slash leaks through as a
        // trailing dot (and is rejected downstream as an invalid key).
        assert_eq!(PathCodec.encode_key("foo//").unwrap(), "foo.");
        // Double leading slash leaks through as an empty token.
        assert_eq!(PathCodec.encode_key("//foo").unwrap(), "_root_..foo");
    }

    #[test]
    fn encode_filter_preserves_wildcards() {
        let codec = PathCodec;
        assert_eq!(codec.encode_filter("/user/*").unwrap(), "_root_.user.*");
        assert_eq!(
            codec.encode_filter("/app/*/config/>").unwrap(),
            "_root_.app.*.config.>"
        );
        assert_eq!(codec.encode_filter("user/*").unwrap(), "user.*");
    }
}
