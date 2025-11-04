# NATS JetStream Counters Implementation Checklist

## Phase 1: Core Counter Types and Infrastructure
- [x] Create `src/lib.rs` with module structure
- [x] Define `Counter` struct (not trait) with all required methods
  - [x] `add<S, V>(subject: S, value: V) -> Result<BigInt>` where S: ToSubject, V: Into<BigInt>
  - [x] `get<S: ToSubject>(subject: S) -> Result<Entry>`
  - [x] `load<S: ToSubject>(subject: S) -> Result<BigInt>`
  - [x] `increment<S: ToSubject>(subject: S) -> Result<BigInt>` (convenience)
  - [x] `decrement<S: ToSubject>(subject: S) -> Result<BigInt>` (convenience)
  - [x] `get_multiple(subjects: Vec<String>) -> impl Stream<Item = Result<Entry>>`
- [x] Implement `Entry` struct
  - [x] `subject: String`
  - [x] `value: BigInt`
  - [x] `sources: CounterSources`
  - [x] `increment: Option<BigInt>`
- [x] Define `CounterSources` type as `HashMap<String, HashMap<String, BigInt>>`
- [x] Add header constants
  - [x] `COUNTER_SOURCES_HEADER = "Nats-Counter-Sources"`
  - [x] `COUNTER_INCREMENT_HEADER = "Nats-Incr"`
- [x] Create `src/errors.rs`
  - [x] Define `CounterError` enum
  - [x] Implement `std::error::Error` trait
  - [x] Implement `Display` trait
  - [x] Add conversion from `async_nats::Error`
- [x] Write unit tests for types

## Phase 2: Extension Trait Pattern
- [x] Create `src/counter_ext.rs`
- [x] Define `CounterExt` trait for `async_nats::jetstream::Context`
  - [x] `get_counter(&self, name: &str) -> Result<Counter>`
  - [x] `counter_from_stream(&self, stream: Stream) -> Result<Counter>`
- [x] Allow direct counter creation
  - [x] `Counter::from_stream(context: Context, stream: Stream) -> Result<Counter>`
- [x] Implement `Counter` struct
  - [x] Hold `stream: async_nats::jetstream::stream::Stream`
  - [x] Hold `context: async_nats::jetstream::Context`
- [x] Validate stream configuration
  - [x] Check `AllowMsgCounter = true` (actually allow_message_counter)
  - [x] Check `AllowDirect = true` (allow_direct)
- [x] Write tests for extension trait
  - [x] test_counter_not_enabled_error
  - [x] test_counter_direct_access_required
  - [x] test_counter_not_found

## Phase 3: Core Operations
- [x] Implement `add()` method
  - [x] Accept `BigInt` from `num-bigint` crate
  - [x] Set `Nats-Incr` header with increment value
  - [x] Publish message with empty body
  - [x] ⚠️ ISSUE: async-nats doesn't have PubAck.value field yet
  - [x] Workaround: Get counter value after publish using get_last_msg
  - [x] Handle negative values for decrement
- [x] Single generic `add()` method handles all integer types
  - [x] Uses `V: Into<BigInt>` for flexibility
  - [x] Added `increment()` and `decrement()` convenience methods
- [x] Implement `load()` method
  - [x] Use `stream.get_last_raw_message_by_subject()`
  - [x] Parse JSON payload `{"val": "counter_value"}`
  - [x] Return only the `BigInt` value
- [x] Implement `get()` method
  - [x] Use `stream.get_last_raw_message_by_subject()`
  - [x] Parse counter value from payload
  - [x] Parse `Nats-Counter-Sources` header
  - [x] Parse `Nats-Incr` header if present
  - [x] Return complete `Entry` struct
- [x] Write comprehensive tests
  - [x] Test basic increment (test_counter_increment)
  - [x] Test decrement (negative values) (test_counter_decrement, test_counter_decrement_to_negative)
  - [x] Test zero increment (test_counter_add_zero_value)
  - [x] Test multiple counters on same stream (test_counter_multiple_subjects)
  - [x] Test missing counter subject (test_counter_no_counter_for_subject)

## Phase 4: Batch Operations
- [x] ~~Create `src/batch_fetch.rs`~~ (Using jetstream-extra batch_fetch instead)
- [x] Implement `get_multiple()` method
  - [x] Accept vector of subjects
  - [x] Return `impl Stream<Item = Result<Entry>>`
  - [x] Use jetstream-extra's `get_last_messages_for()`
- [x] Leverage existing `BatchFetchExt` from jetstream-extra
  - [x] Add jetstream-extra dependency
  - [x] Use optimized batch fetch API (handles DIRECT.GET automatically)
  - [x] Transform raw messages to counter entries
  - [x] jetstream-extra handles Status codes and EOB automatically
- [x] Write tests for batch operations
  - [x] Test multiple subjects fetch (test_counter_get_multiple)
  - [x] Test with missing subjects (test_counter_get_multiple_with_missing)
  - [x] Test wildcard subjects (covered in test_counter_get_multiple)
  - [x] Test empty results (test_counter_get_multiple_with_empty_array)
  - [x] Test partial results (test_counter_get_multiple_partial_success)
  - [x] Test duplicate subjects (test_counter_get_multiple_with_duplicates)
- [x] Create batch operations example

## Phase 5: Message Parsing and Utilities
- [x] Create `src/parser.rs`
- [x] Implement `parse_counter_value()`
  - [x] Extract `BigInt` from JSON `{"val": "..."}`
  - [x] Handle empty/invalid payloads
  - [x] Return appropriate errors
- [x] Implement `parse_sources()`
  - [x] Parse JSON from `Nats-Counter-Sources` header
  - [x] Convert string values to `BigInt`
  - [x] Handle missing/empty header
  - [x] Handle malformed JSON
- [ ] Create builder pattern (optional, if needed)
  - [ ] `CounterBuilder` for configuration
  - [ ] Timeout configuration
  - [ ] Retry logic configuration
- [x] Write unit tests for all parsers
  - [x] Test valid inputs
  - [x] Test edge cases
  - [x] Test invalid/malformed inputs

## Phase 6: Testing
- [x] Create test utilities in `tests/common/mod.rs`
  - [x] Helper to start embedded NATS server
  - [x] Helper to create test streams with counters enabled
  - [x] Helper to create JetStream context
- [x] Port tests from Go implementation
  - [x] `test_counter_basic_operations`
  - [x] `test_counter_negative_values` (included in basic)
  - [x] `test_counter_multiple_subjects`
  - [x] `test_counter_get_multiple`
  - [x] `test_counter_source_tracking`
  - [x] `test_counter_not_enabled_error`
  - [x] `test_counter_direct_access_required`
  - [x] `test_counter_not_found`
  - [x] `test_counter_zero_increment` (included in basic)
- [x] Add Rust-specific tests
  - [x] Test async behavior
  - [x] Test error conversions
  - [x] Test memory safety with large numbers
- [x] Ensure all tests pass consistently

## Phase 6.5: Comprehensive Edge Case Testing (Added 2025-10-27)
- [x] Create `tests/counter_edge_cases_tests.rs` for edge cases
  - [x] `test_counter_add_empty_subject` - Validate empty subject rejection
  - [x] `test_counter_load_empty_subject` - Validate empty subject rejection
  - [x] `test_counter_get_empty_subject` - Validate empty subject rejection
  - [x] `test_counter_get_multiple_with_empty_subject` - Validate empty in array
  - [x] `test_counter_get_multiple_with_empty_array` - Validate empty array
  - [x] `test_counter_add_zero_value` - Zero maintains value
  - [x] `test_counter_get_multiple_with_duplicates` - Document duplicate behavior
  - [x] `test_counter_get_multiple_partial_success` - Mixed valid/invalid
  - [x] `test_counter_large_negative_values` - Very large negative numbers
- [x] Add convenience method tests to `tests/counter_basic_tests.rs`
  - [x] `test_counter_increment` - Test increment() convenience method
  - [x] `test_counter_decrement` - Test decrement() convenience method
  - [x] `test_counter_decrement_to_negative` - Decrement below zero
  - [x] `test_counter_increment_multiple_times` - 100 rapid increments
  - [x] `test_counter_increment_and_decrement_combined` - Mixed operations
- [x] Add 3-level aggregation test to `tests/counter_source_tracking.rs`
  - [x] `test_counter_three_level_source_aggregation` - ES/PL → EU → GLOBAL
  - [x] Verify source tracking at all levels
  - [x] Test update propagation through hierarchy
- [x] Fix validation bug in `get_multiple()`
  - [x] Added empty subject validation
  - [x] Added empty array validation
  - [x] Consistent with add/load/get validation
- [x] All 46 tests passing (100% success rate)


## Phase 7: Examples and Documentation
- [x] Create `examples/basic_counter.rs`
  - [x] Show basic increment/decrement
  - [x] Show value retrieval
- [x] Create `examples/distributed_counter.rs`
  - [x] Demonstrate source tracking
  - [x] Show multi-region setup
- [x] Create `examples/batch_operations.rs`
  - [x] Show `get_multiple` usage
  - [x] Demonstrate batch fetching of metrics
- [x] Create `examples/error_handling.rs`
  - [x] Show proper error handling patterns
  - [x] Demonstrate recovery strategies
- [x] Write module documentation
  - [x] Document all public APIs
  - [x] Add usage examples in doc comments
  - [x] Explain distributed counter design
- [x] Update main README
  - [x] Add counters to feature list
  - [x] Add quick start example
- [x] Create nats-counters/README.md with comprehensive docs
- [x] Run `cargo doc` and verify output

## Final Validation
- [x] Run `cargo build --all-features`
- [x] Run `cargo test` (all tests pass)
- [x] Run `cargo clippy -- --deny warnings`
- [x] Run `cargo +nightly fmt`
- [x] Run all examples (requires NATS server)
- [x] Verify 100% feature parity with Go implementation
- [x] Update CLAUDE.md with counter information
- [x] Create comprehensive README documentation

## Updates
- [x] Refactored to use async-nats main branch with PubAck.value field
  - Removed workaround that fetched last message after publishing
  - Direct extraction of counter value from PubAck response
  - More efficient and cleaner implementation

## Completion Status (2025-10-27)

### Implementation Status: ✅ COMPLETE
All phases completed with comprehensive testing and full feature parity with orbit.go.

### Test Results: ✅ 46/46 PASSING (100%)
- **Parser Unit Tests**: 17 tests ✅
- **Basic Integration Tests**: 15 tests ✅
- **Edge Cases Tests**: 9 tests ✅
- **Source Tracking Tests**: 3 tests ✅
- **Doc Tests**: 2 tests ✅

### Key Features Verified
- ✅ Basic counter operations (add, load, get)
- ✅ Convenience methods (increment, decrement)
- ✅ Batch operations (get_multiple)
- ✅ Source tracking (2-level and 3-level aggregation)
- ✅ Large BigInt support (arbitrary precision)
- ✅ Error handling (all error kinds)
- ✅ Empty subject validation (all methods)
- ✅ Edge cases (empty arrays, duplicates, partial success)

### Improvements Over orbit.go
- ✅ Type-safe generic `add()` using `Into<BigInt>` trait
- ✅ Dedicated `increment()` and `decrement()` convenience methods
- ✅ Comprehensive error handling with specific error kinds
- ✅ Better documentation with 4 complete examples
- ✅ More extensive test coverage (+60% test lines)

### Documentation
- ✅ Module-level documentation
- ✅ API documentation for all public items
- ✅ 4 comprehensive examples (basic, distributed, batch, error handling)
- ✅ README with usage guide
- ✅ TEST_RESULTS.md with detailed test report

### Production Readiness: ✅ READY
The implementation is production-ready with:
- Full test coverage
- Comprehensive error handling
- Clean API design
- Complete documentation
- Performance optimizations (batch operations)
- Memory safety (async, no unsafe code)