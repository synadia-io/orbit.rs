# AGENTS.md

orbit.rs — async utilities extending the NATS ecosystem in Rust. Pre-v1.0, no API stability guarantees.

## Workspace

Three crates in a Cargo workspace (resolver 2):

| Crate | Version | Purpose |
|---|---|---|
| `nats-extra` | 0.3.0 | Request-many pattern (scatter-gather, streaming responses) |
| `jetstream-extra` | 0.2.1 | Batch publish (atomic) and batch fetch (DIRECT.GET API) |
| `nats-counters` | 0.1.0 | Distributed counters via JetStream (CRDT, BigInt, source tracking) |

A `kv/` stub exists but is not wired into the workspace.

### Dependency quirk

`nats-extra` depends on `async-nats` from git main (needs unreleased `PubAck.value` field).
`jetstream-extra` and `nats-counters` pin `async-nats = "0.45.0"`.
`nats-counters` depends on `jetstream-extra` for batch operations.

## Build, test, lint

```bash
# Build everything
cargo build --all --all-targets

# Run all tests (requires nats-server: go install github.com/nats-io/nats-server/v2@main)
cargo test

# Test a single crate
cargo test -p nats-extra
cargo test -p jetstream-extra
cargo test -p nats-counters

# Clippy (exclude the `nats` crate from workspace)
cargo clippy --benches --tests --examples --all-features --workspace --exclude nats -- --deny clippy::all

# Format (nightly required)
cargo +nightly fmt

# Docs
cargo doc --no-deps --all-features

# Validate README code blocks compile
bash scripts/test_readme_examples.sh
```

Integration tests spawn embedded NATS servers via the `nats_server` crate — no external server needed.

## Architecture

Every crate follows the same pattern: **extension traits** that add methods to async-nats types. Nothing wraps or replaces the core client.

### nats-extra

`RequestManyExt` trait — adds `request_many()` to `async_nats::Client`.
Returns a `Responses` stream. Configurable via builder: `sentinel`, `stall_wait`, `max_messages`, `max_wait`.
Terminates on: max messages, timeouts, sentinel predicate, no-responders status, or subscription close.

Source: `nats-extra/src/request_many.rs`
Tests: `nats-extra/tests/request_many_tests.rs`

### jetstream-extra

Two independent modules:

**Batch publish** (`batch_publish.rs`): `BatchPublishExt` trait adds `batch_publish()` / `batch_publish_all()` to JetStream context. Atomic batch publishing with ack flow control. Max 1000 messages per batch (server limit). Uses `Nats-Batch-Id`, `Nats-Batch-Sequence`, `Nats-Batch-Commit` headers. `BatchPublish<C>` is intentionally `!Send + !Sync` — clone the context, not the batch.

**Batch fetch** (`batch_fetch.rs`): `BatchFetchExt` trait adds `get_batch()` / `get_last_messages_for()`. Uses JetStream DIRECT.GET API (ADR-31) for efficient server-side retrieval. Type-state pattern enforces mutual exclusivity between `.sequence()` and `.start_time()` at compile time.

Tests: `jetstream-extra/tests/batch_publish.rs`, `batch_publish_all.rs`, `batch_publish_errors.rs`, `batch_fetch.rs`

### nats-counters

`CounterExt` trait adds `get_counter()` / `counter_from_stream()` to `jetstream::Context`.
Returns a `Counter` with: `add`, `increment`, `decrement`, `get`, `load`, `get_multiple`.
Counter values stored as JSON (`{"val": "123"}`), uses `num-bigint` for arbitrary precision.
Source tracking via `Nats-Counter-Sources` header — each source stream's contributions are recorded.
Streams **must** have `allow_message_counter: true` and `allow_direct: true`.

Source: `nats-counters/src/counter.rs`, `counter_ext.rs`, `parser.rs`, `errors.rs`
Tests: `nats-counters/tests/counter_basic_tests.rs`, `counter_source_tracking.rs`, `counter_edge_cases_tests.rs`
Shared test helpers: `nats-counters/tests/common/mod.rs`

## CI

Per-crate workflows in `.github/workflows/` (`nats-extra.yml`, `jetstream-extra.yml`, `nats-counters.yml`):
- Test matrix: Ubuntu, macOS, Windows with stable Rust
- Installs Go 1.22 + nats-server from main
- Checks: format, clippy, docs, feature flags (`cargo-hack`), MSRV, examples, minimal versions

Workspace-wide (`general.yml`): license check (`cargo deny`), spell check.

Environment: `RUSTFLAGS="-D warnings"`, `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`.

## Conventions

- Rust edition 2024 (except `kv` which is 2021)
- All operations are async (tokio runtime)
- Error types follow async-nats pattern: `type XxxError = async_nats::error::Error<XxxErrorKind>`
- Licenses allowed: MIT, Apache-2.0, ISC, BSD-2-Clause, BSD-3-Clause (see `deny.toml`)
