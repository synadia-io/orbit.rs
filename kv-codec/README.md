# kv-codec

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Crates.io](https://img.shields.io/crates/v/kv-codec.svg)](https://crates.io/crates/kv-codec)
[![Documentation](https://docs.rs/kv-codec/badge.svg)](https://docs.rs/kv-codec/)
[![Build Status](https://github.com/synadia-io/orbit.rs/actions/workflows/kv-codec.yml/badge.svg?branch=main)](https://github.com/synadia-io/orbit.rs/actions/workflows/kv-codec.yml)

Transparent key and value encoding for NATS JetStream Key-Value stores
([ADR-54](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-54.md)),
for the [async-nats](https://crates.io/crates/async-nats) crate. The Rust
counterpart of [orbit.go's kvcodec](https://github.com/synadia-io/orbit.go/tree/main/kvcodec).

## Overview

`kv-codec` wraps a JetStream Key-Value store to add encoding/decoding with
separate key and value codecs. This enables:

- **Character escaping** — use characters in keys that would normally be invalid
- **Path notation** — use familiar `/path/style` keys while storing NATS subjects
- **Value encoding** — encode values independently from keys (e.g. encryption)
- **Custom transformations** — implement your own codecs
- **Codec chaining** — compose codecs with plain tuples

## Usage

```rust
use kv_codec::{Base64Codec, CodecStoreExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect("demo.nats.io").await?;
    let jetstream = async_nats::jetstream::new(client);

    let store = jetstream
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: "contacts".to_string(),
            ..Default::default()
        })
        .await?;

    // Wrap with separate key and value codecs.
    let kv = store.with_codecs(Base64Codec, Base64Codec);

    // Keys with special characters work seamlessly.
    kv.put("Acme Inc.contact", "info@acme.com".into()).await?;
    let entry = kv.entry("Acme Inc.contact").await?.unwrap();
    println!("key: {}, value: {:?}", entry.key, entry.value);

    Ok(())
}
```

### Constructors

```rust,ignore
// Separate key and value codecs.
let kv = store.with_codecs(Base64Codec, Base64Codec);
// Key codec only — values stored unchanged.
let kv = store.with_key_codec(PathCodec);
// Value codec only — keys stored unchanged.
let kv = store.with_value_codec(Base64Codec);
// Equivalent explicit constructors.
let kv = CodecStore::new(store, Base64Codec, Base64Codec);
let kv = CodecStore::for_key(store, PathCodec);
let kv = CodecStore::for_value(store, Base64Codec);
```

### Built-in codecs

- `NoOpCodec` — passes keys and values through unchanged.
- `Base64Codec` — URL-safe base64 without padding. Keys are encoded per
  token (the parts between dots), so the subject hierarchy and server-side
  wildcard filtering keep working. Values are encoded as a whole.
- `PathCodec` — translates `/foo/bar` to `foo.bar`. A leading `/` is encoded
  as the `_root_` sentinel; a single trailing `/` is trimmed (lossy).

### Custom codecs

Implement `KeyCodec` and/or `ValueCodec`:

```rust,ignore
struct AesCodec { /* ... */ }

impl kv_codec::ValueCodec for AesCodec {
    fn encode_value(&self, value: Bytes) -> Result<Bytes, kv_codec::CodecError> {
        Ok(self.encrypt(&value)?.into())
    }
    fn decode_value(&self, value: Bytes) -> Result<Bytes, kv_codec::CodecError> {
        Ok(self.decrypt(&value)?.into())
    }
}

let kv = store.with_value_codec(AesCodec::new(key));
```

### Wildcard filtering

`watch`, `watch_many` and friends accept patterns with `*` and `>`
wildcards. A key codec supports them by overriding `encode_filter` and
preserving wildcard tokens (as `Base64Codec` and `PathCodec` do). The
default `encode_filter` rejects wildcard patterns with
`KvCodecErrorKind::WildcardNotSupported`; literal patterns always work.

### Codec chaining

Tuples of codecs are codecs. Encoding applies elements first to last,
decoding in reverse:

```rust,ignore
// Translate paths to subjects, then base64-encode each token.
let kv = store.with_codecs((PathCodec, Base64Codec), Base64Codec);
```

A chain supports wildcard filters only if all of its elements do.

## Differences from orbit.go's kvcodec

- `watch()` follows async-nats semantics and yields new updates only;
  Go's `Watch` delivers current values first. Use `watch_with_history`
  for the Go default behavior.
- Chains are tuples, so an empty chain is unrepresentable (`ErrNoCodecs`
  does not exist) and errors are not annotated with a codec index.
- Chains accept literal (wildcard-free) filters even when some members do
  not support wildcard filtering; Go rejects such chains upfront for any
  filter.
- Decode failures are hard errors. Go silently falls back to the encoded
  form for entries and skips undecodable keys when listing; here entries
  fail with `KeyDecode`/`ValueDecode` and `keys()` yields `Err` items.
  Use `keys().filter_map(|key| key.ok())` for the skipping behavior.
- `keys()` is a single stream (no `ListKeys`/`ListKeysFiltered` split) and
  there is no `PutString`/`PurgeDeletes`, mirroring the async-nats API.
- `PathCodec` implements `KeyCodec` only (Go's also passes values through);
  pair it with `NoOpCodec` or any `ValueCodec`.
- Underlying async-nats errors map to `KvCodecErrorKind::Store` with the
  original error preserved as the source — downcast it to branch on kinds
  like `CreateErrorKind::AlreadyExists` (see the `Store` variant docs).

Base64-encoded data is wire-compatible between this crate and orbit.go.
It is **not** compatible with the legacy nats.js `Base64KeyCodec`, which
predates ADR-54 and uses the standard padded base64 alphabet.
