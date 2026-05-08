// Copyright 2026 Synadia Communications Inc.
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

//! Fast-ingest batch publishing for NATS JetStream (ADR-50 fast ingest).
//!
//! Fast ingest is a high-throughput, non-atomic batch publisher. Unlike atomic
//! [`batch_publish`](crate::batch_publish), which stages an entire batch and
//! commits it or drops it, fast ingest persists each message as it arrives and
//! uses a persistent inbox subscription plus server-driven flow control to
//! coordinate throughput across many concurrent publishers.
//!
//! Requires nats-server 2.14 or later and `allow_batched: true` on the stream.
//!
//! # Architecture
//!
//! A [`FastPublisher`] owns its own [`async_nats::Subscriber`] and drives it
//! inline from `add` / `commit` / `close`. There is no background task, no
//! shared state, and no locks. This mirrors the single-task pattern used by
//! `nats-extra/src/request_many.rs`.
//!
//! # Not safe for concurrent use
//!
//! A `FastPublisher` holds per-batch state (sequence counters, cached reply
//! subject prefix, effective flow) and is driven via `&mut self`. Use one
//! publisher per task. Clone the underlying JetStream context if you need
//! independent publishers.

use std::{
    fmt::{self, Debug, Display},
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use async_nats::jetstream::message::OutboundMessage;
use async_nats::subject::ToSubject;
use bytes::Bytes;
use futures::StreamExt;
use futures::task::noop_waker_ref;
use serde::Deserialize;

use crate::batch_publish::BatchPubAck;

// ---------------------------------------------------------------------------
// Public enums
// ---------------------------------------------------------------------------

/// How the server should handle gaps in the batch sequence.
///
/// A gap means one or more messages in the batch were lost in transit between
/// the client and the stream leader (e.g. due to buffer drops under load).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum GapMode {
    /// Allow gaps — the batch continues and the server informs the client via
    /// a gap event. Use this when some message loss is acceptable (metrics,
    /// telemetry).
    Ok,
    /// Fail the batch on the first gap. Use this when in-order, gap-free
    /// delivery is required (object store chunks, ordered events). Default.
    #[default]
    Fail,
}

impl GapMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Fail => "fail",
        }
    }
}

/// Fast-ingest operation codes (match the `$FI` reply-subject tail).
///
/// Encoded as the second-to-last segment of the reply subject before `$FI`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Operation {
    Start = 0,
    Append = 1,
    Commit = 2,
    CommitEob = 3,
    Ping = 4,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Error type for fast-ingest batch publish operations.
pub type FastPublishError = async_nats::error::Error<FastPublishErrorKind>;

/// Kinds of errors that can occur during fast-ingest batch publishing.
///
/// API error codes are verified against `nats-server` 2.14 `errors.json`.
///
/// Marked `#[non_exhaustive]` — adding a new variant in a future release will
/// not be a breaking change. Match on `_` for forward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FastPublishErrorKind {
    /// Stream does not have `allow_batched: true`. (`BATCH_PUBLISH_DISABLED`, 10205)
    NotEnabled,
    /// Reply subject pattern rejected by the server. (`BATCH_PUBLISH_INVALID_PATTERN`, 10206)
    InvalidPattern,
    /// Batch id exceeds 64 characters or is otherwise invalid.
    /// (`BATCH_PUBLISH_INVALID_BATCH_ID`, 10207)
    InvalidBatchId,
    /// Server has forgotten this batch (timed out, leader change in
    /// `GapMode::Fail`, etc.). (`BATCH_PUBLISH_UNKNOWN_BATCH_ID`, 10208)
    UnknownBatchId,
    /// Too many in-flight fast batches on the server.
    /// (`BATCH_PUBLISH_TOO_MANY_INFLIGHT`, 10211)
    TooManyInflight,
    /// A gap was detected while running in [`GapMode::Fail`]. The final ack
    /// will indicate which messages were persisted.
    GapDetected,
    /// Any other server-side error reported via a `BatchFlowErr` message.
    FlowError,
    /// `close()` was called on a publisher that has not received any `add`s.
    EmptyBatch,
    /// `build()` rejected the inbox because it does not have exactly two
    /// tokens. The reply-subject parser requires `<prefix>.<id>` shape.
    InvalidInboxShape,
    /// `build()` rejected a configuration value (e.g. `max_outstanding_acks`
    /// outside `1..=3`).
    InvalidConfig,
    /// Called a method on a publisher that has already committed, closed, or
    /// failed fatally.
    Closed,
    /// Timed out waiting for a flow ack or final pub ack.
    Timeout,
    /// Failed to subscribe to the batch inbox.
    Subscribe,
    /// Failed to publish a batch message.
    Publish,
    /// Failed to parse a server response.
    Serialization,
    /// An internal operation that depends on a runtime invariant was called
    /// before that invariant held — currently only emitted when the publisher
    /// would need to send a ping but no message has been published yet, so
    /// there is no first-subject to address the ping to.
    InvalidState,
    /// Catch-all.
    Other,
}

impl FastPublishErrorKind {
    /// Map a JetStream API error code to the matching fast-ingest error kind.
    pub(crate) fn from_api_error(error: &async_nats::jetstream::Error) -> Self {
        use async_nats::jetstream::ErrorCode;
        match error.error_code() {
            ErrorCode::BATCH_PUBLISH_DISABLED => Self::NotEnabled,
            ErrorCode::BATCH_PUBLISH_INVALID_PATTERN => Self::InvalidPattern,
            ErrorCode::BATCH_PUBLISH_INVALID_BATCH_ID => Self::InvalidBatchId,
            ErrorCode::BATCH_PUBLISH_UNKNOWN_BATCH_ID => Self::UnknownBatchId,
            ErrorCode::BATCH_PUBLISH_TOO_MANY_INFLIGHT => Self::TooManyInflight,
            _ => Self::FlowError,
        }
    }
}

impl Display for FastPublishErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotEnabled => write!(f, "fast batch publish not enabled on stream"),
            Self::InvalidPattern => write!(f, "fast batch publish invalid reply subject pattern"),
            Self::InvalidBatchId => write!(
                f,
                "fast batch publish id is invalid (exceeds 64 characters)"
            ),
            Self::UnknownBatchId => write!(f, "fast batch publish id is unknown to the server"),
            Self::TooManyInflight => write!(f, "too many in-flight fast batches on the server"),
            Self::GapDetected => write!(f, "gap detected in fast batch (gap_mode=fail)"),
            Self::FlowError => write!(f, "fast batch flow error"),
            Self::EmptyBatch => write!(f, "cannot close an empty batch"),
            Self::InvalidInboxShape => {
                write!(f, "inbox must have exactly two tokens (e.g. _INBOX.<id>)")
            }
            Self::InvalidConfig => write!(f, "invalid fast publisher configuration"),
            Self::Closed => write!(f, "fast publisher is closed"),
            Self::Timeout => write!(f, "timeout waiting for fast batch ack"),
            Self::Subscribe => write!(f, "failed to subscribe to fast batch inbox"),
            Self::Publish => write!(f, "failed to publish fast batch message"),
            Self::Serialization => write!(f, "failed to (de)serialize fast batch message"),
            Self::InvalidState => {
                write!(f, "operation not allowed in current fast publisher state")
            }
            Self::Other => write!(f, "other fast batch publish error"),
        }
    }
}

// ---------------------------------------------------------------------------
// Wire protocol structs (server → client)
// ---------------------------------------------------------------------------

/// Flow-control message sent by the server when a batch of messages has been
/// persisted. Wire tag: `"type":"ack"`.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
pub(crate) struct BatchFlowAck {
    /// Highest batch sequence covered by this ack.
    ///
    /// In `GapMode::Fail` this means all messages up to and including this
    /// sequence were persisted. In `GapMode::Ok` some may have been lost.
    #[serde(rename = "seq")]
    pub sequence: u64,
    /// How often the server will send subsequent flow acks (every N messages).
    #[serde(rename = "msgs")]
    pub messages: u16,
}

/// Gap notification sent when the server detects missing messages in a batch.
/// Wire tag: `"type":"gap"`.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
pub(crate) struct BatchFlowGap {
    /// The last sequence the server expected to receive before the gap.
    ///
    /// Messages with sequences `[expected_last_sequence+1, current_sequence)`
    /// were lost.
    #[serde(rename = "last_seq")]
    pub expected_last_sequence: u64,
    /// The sequence of the message that triggered gap detection.
    #[serde(rename = "seq")]
    pub current_sequence: u64,
}

/// Per-message error sent when a batch message fails a server-side check
/// (e.g. expected-last-seq mismatch). Wire tag: `"type":"err"`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BatchFlowErr {
    /// The batch sequence of the message that triggered the error.
    #[serde(rename = "seq")]
    pub sequence: u64,
    /// The full API error as returned by the server.
    pub error: async_nats::jetstream::Error,
}

/// Result of classifying a message received on the batch inbox.
///
/// The classifier dispatches on the `type` field (a serde-tagged enum) and
/// falls back to a full `Response<BatchPubAck>` parse for messages without a
/// `type` discriminator (the terminal pub-ack or an init-time error).
#[derive(Debug)]
pub(crate) enum Classified {
    FlowAck(BatchFlowAck),
    FlowGap(BatchFlowGap),
    FlowErr(BatchFlowErr),
    /// Terminal publish acknowledgment — delivered on `commit`/`close` or on
    /// the single-message immediate-commit fast path.
    PubAck(BatchPubAck),
    /// Init-time API error — returned in response to the `0` (Start) or
    /// `2`/`3` (Commit/CommitEob) operation when the server rejects the batch
    /// before it even begins.
    InitError(async_nats::jetstream::Error),
}

/// Tagged enum for `type`-discriminated messages.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum TaggedFlow {
    Ack(BatchFlowAck),
    Gap(BatchFlowGap),
    Err(BatchFlowErr),
}

/// Classify a payload received on the batch inbox into one of the five
/// possible shapes.
///
/// Implementation strategy: try the tagged-enum deserializer first; if that
/// fails (no `type` field), fall back to parsing the payload as a terminal
/// `Response<BatchPubAck>`. This matches the Go byte-search optimization in
/// spirit while remaining fully serde-driven.
pub(crate) fn classify(payload: &[u8]) -> Result<Classified, FastPublishError> {
    if let Ok(tagged) = serde_json::from_slice::<TaggedFlow>(payload) {
        return Ok(match tagged {
            TaggedFlow::Ack(a) => Classified::FlowAck(a),
            TaggedFlow::Gap(g) => Classified::FlowGap(g),
            TaggedFlow::Err(e) => Classified::FlowErr(e),
        });
    }

    let resp: async_nats::jetstream::response::Response<BatchPubAck> =
        serde_json::from_slice(payload)
            .map_err(|e| FastPublishError::with_source(FastPublishErrorKind::Serialization, e))?;

    Ok(match resp {
        async_nats::jetstream::response::Response::Ok(pa) => Classified::PubAck(pa),
        async_nats::jetstream::response::Response::Err { error } => Classified::InitError(error),
    })
}

// ---------------------------------------------------------------------------
// Reply subject construction
// ---------------------------------------------------------------------------

/// Build the stable prefix of the reply subject:
/// `<inbox>.<flow>.<gap>.`
///
/// The caller appends `<seq>.<op>.$FI` per message via [`build_reply`]. Cached
/// on the publisher and rebuilt only when the server dictates a new flow via
/// a [`BatchFlowAck`].
pub(crate) fn build_reply_prefix(inbox: &str, flow: u16, gap: GapMode) -> String {
    format!("{inbox}.{flow}.{}.", gap.as_str())
}

/// Build a full per-message reply subject: `<prefix><seq>.<op>.$FI`.
///
/// `$FI` marks the subject as a fast-ingest reply to the server's parser
/// (`server/stream.go:getFastBatch`).
pub(crate) fn build_reply(prefix: &str, seq: u64, op: Operation) -> String {
    format!("{prefix}{seq}.{}.$FI", op as u8)
}

/// Validate that an inbox has the shape the fast-ingest reply subject parser
/// requires: exactly two tokens separated by a single dot (e.g. `_INBOX.<id>`).
///
/// The server (`server/stream.go:5174`) parses the reply subject from the
/// right, expecting `<prefix>.<uuid>.<flow>.<gap>.<seq>.<op>.$FI`. Our scheme
/// uses the inbox as the `<prefix>.<uuid>` portion — which only aligns if the
/// inbox itself is exactly two tokens. A custom multi-token inbox prefix
/// (e.g. `_INBOX.myapp.xyz`) would misalign the parser and cause cryptic
/// `InvalidPattern` errors.
pub(crate) fn validate_inbox_shape(inbox: &str) -> Result<(), FastPublishError> {
    let mut parts = inbox.splitn(3, '.');
    let first = parts.next().unwrap_or("");
    let second = parts.next().unwrap_or("");
    let no_third = parts.next().is_none();
    if first.is_empty() || second.is_empty() || !no_third {
        return Err(FastPublishError::new(
            FastPublishErrorKind::InvalidInboxShape,
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Stall formula
// ---------------------------------------------------------------------------

/// Decide whether the publisher should stall before sending the next message.
///
/// Matches the canonical ADR-50 / orbit.go form: wait iff
///
/// ```text
/// last_ack_sequence + effective_flow * max_outstanding_acks <= next_sequence
/// ```
///
/// Equivalently, no wait iff `window > next_sequence`. This uses inclusive
/// comparison (`<=`) so that at the exact boundary
/// `(ack + flow * max) == next_seq` the publisher stalls. That matches the
/// Go reference's `< next_seq` formulation because Go uses `wait_for_ack =
/// ack + flow*max < next_seq` which is `true` at `next_seq > ack + flow*max`.
///
/// Note on formulations:
/// - ADR-50 pseudocode: `waitForAck := lastAck.Sequence + lastAck.Messages*maxOutstandingAcks <= f.batchSeq`
/// - This Rust impl uses the ADR-50 inclusive `<=` form verbatim.
///
/// `effective_flow` and `max_outstanding_acks` are widened to `u64` before
/// multiplication to avoid overflow on pathological inputs.
#[inline]
pub(crate) fn should_stall(
    last_ack_sequence: u64,
    effective_flow: u16,
    max_outstanding_acks: u16,
    next_sequence: u64,
) -> bool {
    let window = last_ack_sequence
        .saturating_add((effective_flow as u64).saturating_mul(max_outstanding_acks as u64));
    window <= next_sequence
}

// ---------------------------------------------------------------------------
// Builder knobs
// ---------------------------------------------------------------------------

/// Default initial flow (ack-every-N) requested from the server.
pub(crate) const DEFAULT_FLOW: u16 = 100;

/// Default max outstanding-acks window size (per ADR-50: "generally optimal").
pub(crate) const DEFAULT_MAX_OUTSTANDING_ACKS: u16 = 2;

/// Minimum allowed value for `max_outstanding_acks`.
pub(crate) const MIN_MAX_OUTSTANDING_ACKS: u16 = 1;

/// Maximum allowed value for `max_outstanding_acks` (ADR-50 recommends 1..=3).
pub(crate) const MAX_MAX_OUTSTANDING_ACKS: u16 = 3;

// ---------------------------------------------------------------------------
// Extension trait
// ---------------------------------------------------------------------------

/// Extension trait adding [`fast_publish`](FastPublishExt::fast_publish) to any
/// JetStream-context-like type.
///
/// Implemented automatically for [`async_nats::jetstream::Context`] and any
/// other type that can provide an [`async_nats::Client`] and a default timeout.
///
/// # Example
///
/// ```no_run
/// # async fn example(js: async_nats::jetstream::Context) -> Result<(), Box<dyn std::error::Error>> {
/// use jetstream_extra::batch_publish_fast::FastPublishExt;
///
/// let mut batch = js.fast_publish().build()?;
/// # let _ = batch;
/// # Ok(())
/// # }
/// ```
pub trait FastPublishExt:
    async_nats::jetstream::context::traits::ClientProvider
    + async_nats::jetstream::context::traits::TimeoutProvider
{
    /// Start building a [`FastPublisher`] bound to the underlying connection
    /// and default timeout of this context.
    fn fast_publish(&self) -> FastPublisherBuilder {
        FastPublisherBuilder::new(self.client(), self.timeout())
    }
}

impl<T> FastPublishExt for T where
    T: async_nats::jetstream::context::traits::ClientProvider
        + async_nats::jetstream::context::traits::TimeoutProvider
{
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Callback type for asynchronous fast-publish errors (gaps, flow errors,
/// per-message server errors). Invoked synchronously on the publisher's task
/// whenever such an event is drained from the inbox. Keep the callback fast
/// and non-blocking.
pub type FastPublishErrorHandler = Box<dyn FnMut(FastPublishError) + Send + 'static>;

/// Builder for a [`FastPublisher`].
///
/// Obtained via [`FastPublishExt::fast_publish`]. Call [`build`](Self::build)
/// to validate configuration and produce a ready-to-use publisher.
///
/// All setters are optional; defaults match ADR-50 recommendations:
/// - `flow = 100` (ack every 100 messages ceiling)
/// - `max_outstanding_acks = 2`
/// - `gap_mode = Fail`
/// - `ack_timeout = <JetStream context default>`
pub struct FastPublisherBuilder {
    client: async_nats::Client,
    flow: u16,
    max_outstanding_acks: u16,
    ack_timeout: Duration,
    gap_mode: GapMode,
    on_error: Option<FastPublishErrorHandler>,
}

impl FastPublisherBuilder {
    /// Create a new builder with default settings.
    ///
    /// Prefer [`FastPublishExt::fast_publish`] for public use.
    pub(crate) fn new(client: async_nats::Client, ack_timeout: Duration) -> Self {
        Self {
            client,
            flow: DEFAULT_FLOW,
            max_outstanding_acks: DEFAULT_MAX_OUTSTANDING_ACKS,
            ack_timeout,
            gap_mode: GapMode::default(),
            on_error: None,
        }
    }

    /// Set the client-requested maximum flow — the upper bound on how often
    /// the server will send flow acks. The server may choose a lower effective
    /// flow.
    ///
    /// Must be at least 1. Values of 0 are clamped to 1.
    pub fn flow(mut self, flow: u16) -> Self {
        self.flow = flow.max(1);
        self
    }

    /// Set the number of flow-ack-batches that can be in flight before the
    /// publisher stalls and waits for an ack. Valid range is `1..=3`.
    ///
    /// - `1` behaves like synchronous async publish throttled to flow N
    /// - `2` is the ADR-50 recommended default (optimal for most cases)
    /// - `3` is useful on higher-RTT links
    ///
    /// Values outside the range are returned as an error from [`build`](Self::build).
    pub fn max_outstanding_acks(mut self, n: u16) -> Self {
        self.max_outstanding_acks = n;
        self
    }

    /// Set the timeout for waiting on flow acks and the final commit ack.
    ///
    /// When the publisher is stalled waiting for an ack, it will auto-send
    /// pings every `ack_timeout / 3` to recover from lost acks, giving up
    /// after the full `ack_timeout` elapses.
    pub fn ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout = timeout;
        self
    }

    /// Set the gap handling mode. Default: [`GapMode::Fail`].
    pub fn gap_mode(mut self, mode: GapMode) -> Self {
        self.gap_mode = mode;
        self
    }

    /// Register a callback invoked for asynchronous events: gap detections,
    /// per-message flow errors, and server-side fast-batch errors.
    ///
    /// The callback is called on the publisher's task synchronously from the
    /// middle of `add` / `commit` / `close`, so it must be fast and
    /// non-blocking.
    pub fn on_error<F>(mut self, handler: F) -> Self
    where
        F: FnMut(FastPublishError) + Send + 'static,
    {
        self.on_error = Some(Box::new(handler));
        self
    }

    /// Validate configuration and produce a [`FastPublisher`].
    ///
    /// The subscription to the batch inbox is NOT created here — it is lazily
    /// opened on the first `add` / `commit` / `close`. This matches the Go
    /// reference implementation and avoids wasted subscriptions when a
    /// publisher is built and then dropped unused.
    ///
    /// # Errors
    ///
    /// - [`FastPublishErrorKind::InvalidConfig`] if `max_outstanding_acks` is
    ///   outside `1..=3`.
    /// - [`FastPublishErrorKind::InvalidInboxShape`] if the client's
    ///   `new_inbox()` does not produce a two-token inbox (required by the
    ///   server's reply-subject parser).
    pub fn build(self) -> Result<FastPublisher, FastPublishError> {
        if !(MIN_MAX_OUTSTANDING_ACKS..=MAX_MAX_OUTSTANDING_ACKS)
            .contains(&self.max_outstanding_acks)
        {
            return Err(FastPublishError::new(FastPublishErrorKind::InvalidConfig));
        }

        let inbox = self.client.new_inbox();
        validate_inbox_shape(&inbox)?;

        let reply_prefix = build_reply_prefix(&inbox, self.flow, self.gap_mode);

        Ok(FastPublisher {
            client: self.client,
            inbox,
            flow: self.flow,
            gap_mode: self.gap_mode,
            max_outstanding_acks: self.max_outstanding_acks,
            ack_timeout: self.ack_timeout,
            reply_prefix,
            subscriber: None,
            sequence: 0,
            effective_flow: self.flow,
            last_ack_sequence: 0,
            initial_ack_received: false,
            pending_pub_ack: None,
            first_subject: None,
            closed: false,
            fatal: None,
            on_error: self.on_error,
        })
    }
}

// ---------------------------------------------------------------------------
// FastPublisher
// ---------------------------------------------------------------------------

/// A non-atomic, high-throughput JetStream batch publisher using the
/// fast-ingest protocol (ADR-50, nats-server 2.14+).
///
/// Obtain via [`FastPublishExt::fast_publish`] → [`FastPublisherBuilder::build`].
///
/// A `FastPublisher` is `Send` but NOT `Sync`: methods require `&mut self` and
/// the publisher must be driven from a single task. Dropping the publisher
/// mid-batch is safe — the underlying [`async_nats::Subscriber`] drops with
/// it, the server-side interest is torn down, and the server will time out
/// the abandoned batch after 10 seconds.
pub struct FastPublisher {
    // --- configuration (immutable after build) ---
    client: async_nats::Client,
    inbox: String,
    flow: u16, // client-requested ceiling
    gap_mode: GapMode,
    max_outstanding_acks: u16,
    ack_timeout: Duration,

    // --- cached reply subject prefix ---
    // "<inbox>.<effective_flow>.<gap>." — rebuilt when effective_flow changes.
    reply_prefix: String,

    // --- lazily-created inbox subscription ---
    subscriber: Option<async_nats::Subscriber>,

    // --- per-batch state ---
    sequence: u64,
    effective_flow: u16, // dictated by server; equals `flow` until first ack
    last_ack_sequence: u64,
    /// Set to true as soon as the server sends the initial `BatchFlowAck` in
    /// response to the first `Start` message. This is tracked separately
    /// from `last_ack_sequence` because the initial ack has `seq:0` (no
    /// messages persisted yet — just confirming the batch is alive).
    initial_ack_received: bool,
    /// Terminal `PubAck` stashed if seen out of band (e.g. single-msg
    /// immediate-commit fast path during `await_first_reply`).
    pending_pub_ack: Option<BatchPubAck>,
    /// Subject of the first published message, used by [`close`](Self::close)
    /// to construct the EOB commit message and by `ping` as the publish
    /// target.
    first_subject: Option<async_nats::Subject>,
    closed: bool,
    fatal: Option<FastPublishErrorKind>,

    // --- async error callback ---
    on_error: Option<FastPublishErrorHandler>,
}

impl FastPublisher {
    /// Returns the number of messages added to (and published in) this batch
    /// so far, excluding any pending commit message.
    pub fn size(&self) -> u64 {
        self.sequence
    }

    /// Returns `true` if the batch has been committed, closed, or failed
    /// fatally.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Returns the batch's inbox, which is also the batch identifier as seen
    /// by the server.
    pub fn batch_id(&self) -> &str {
        &self.inbox
    }

    /// Returns the currently-configured gap mode.
    pub fn gap_mode(&self) -> GapMode {
        self.gap_mode
    }

    /// Returns the highest batch sequence acknowledged by the server so far.
    /// `0` before the first flow ack arrives.
    pub fn last_ack_sequence(&self) -> u64 {
        self.last_ack_sequence
    }
}

impl Debug for FastPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FastPublisher")
            .field("inbox", &self.inbox)
            .field("flow", &self.flow)
            .field("effective_flow", &self.effective_flow)
            .field("gap_mode", &self.gap_mode)
            .field("max_outstanding_acks", &self.max_outstanding_acks)
            .field("sequence", &self.sequence)
            .field("last_ack_sequence", &self.last_ack_sequence)
            .field("closed", &self.closed)
            .field("fatal", &self.fatal)
            .finish()
    }
}

// Compile-time check: `FastPublisher` must be `Send` so users can spawn it
// onto tokio tasks. It is intentionally NOT `Sync`: per-batch state is driven
// through `&mut self` and the publisher is single-consumer.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<FastPublisher>();
};

// ---------------------------------------------------------------------------
// Public return type
// ---------------------------------------------------------------------------

/// Result of a successful [`FastPublisher::add`] / [`FastPublisher::add_message`]
/// call.
///
/// The `ack_sequence` is the highest batch sequence acknowledged by the
/// server so far. In [`GapMode::Fail`] this means all messages up to and
/// including `ack_sequence` were persisted. In [`GapMode::Ok`] there may
/// have been gaps; `ack_sequence` is only a hint about how far the server
/// has progressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FastPubAck {
    /// Batch sequence of the message that was just added.
    pub batch_sequence: u64,
    /// Highest batch sequence acknowledged by the server so far.
    pub ack_sequence: u64,
}

// ---------------------------------------------------------------------------
// State machine: add / commit / close
// ---------------------------------------------------------------------------

/// Convenience discriminator for the `commit_message_inner` call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitKind {
    /// Commit with a final stored message (operation code 2).
    Final,
    /// Commit via end-of-batch — do not store the final message (operation
    /// code 3). Used by [`FastPublisher::close`].
    Eob,
}

impl FastPublisher {
    /// Add a message to the batch with the given subject and payload.
    ///
    /// Convenience wrapper around [`add_message`](Self::add_message) for
    /// callers that don't need custom headers.
    pub async fn add<S: ToSubject>(
        &mut self,
        subject: S,
        payload: Bytes,
    ) -> Result<FastPubAck, FastPublishError> {
        self.add_message(OutboundMessage {
            subject: subject.to_subject(),
            payload,
            headers: None,
        })
        .await
    }

    /// Add a pre-constructed message to the batch.
    ///
    /// The message's `subject`, `payload`, and `headers` fields are forwarded
    /// to the server; the reply subject is always set by the publisher.
    ///
    /// On the first call, a subscription to the batch inbox is created and
    /// the publisher waits for the initial flow ack from the server to
    /// confirm the batch has been accepted.
    ///
    /// # Errors
    ///
    /// - [`FastPublishErrorKind::Closed`] if the publisher has already been
    ///   committed, closed, or failed fatally.
    /// - [`FastPublishErrorKind::Subscribe`] if the initial subscription
    ///   fails.
    /// - [`FastPublishErrorKind::Publish`] if publishing the message fails.
    /// - [`FastPublishErrorKind::Timeout`] if the initial ack does not arrive
    ///   within `ack_timeout`.
    /// - Any mapped API error from the server's init response
    ///   (`NotEnabled`, `InvalidPattern`, `InvalidBatchId`, `UnknownBatchId`,
    ///   `TooManyInflight`).
    pub async fn add_message(
        &mut self,
        msg: OutboundMessage,
    ) -> Result<FastPubAck, FastPublishError> {
        if self.closed {
            return Err(FastPublishError::new(FastPublishErrorKind::Closed));
        }
        if let Some(kind) = self.fatal {
            return Err(FastPublishError::new(kind));
        }

        // Lazy bootstrap on the very first publish.
        self.ensure_subscribed().await?;

        // Drain any events that arrived while the caller was computing the
        // next payload. Cost in the common case: one `Pending` poll.
        self.drain_nonblocking()?;
        if let Some(kind) = self.fatal {
            return Err(FastPublishError::new(kind));
        }

        // Stall gate: if the outstanding-ack window is saturated, wait for a
        // flow ack before emitting the next message. Check BEFORE incrementing
        // sequence so the next-message sequence is what we gate on.
        let next_sequence = self.sequence + 1;
        if should_stall(
            self.last_ack_sequence,
            self.effective_flow,
            self.max_outstanding_acks,
            next_sequence,
        ) {
            self.wait_for_flow_event_with_pings().await?;
            if let Some(kind) = self.fatal {
                return Err(FastPublishError::new(kind));
            }
        }

        self.sequence += 1;
        let op = if self.sequence == 1 {
            Operation::Start
        } else {
            Operation::Append
        };

        if self.first_subject.is_none() {
            self.first_subject = Some(msg.subject.clone());
        }

        let reply = build_reply(&self.reply_prefix, self.sequence, op);
        self.publish_raw(msg, reply).await?;

        if self.sequence == 1 {
            self.await_first_reply().await?;
        }

        // Drain any acks that landed while we were awaiting the first reply
        // or just now publishing.
        self.drain_nonblocking()?;
        if let Some(kind) = self.fatal {
            return Err(FastPublishError::new(kind));
        }

        Ok(FastPubAck {
            batch_sequence: self.sequence,
            ack_sequence: self.last_ack_sequence,
        })
    }

    /// Commit the batch by publishing a final stored message.
    ///
    /// After this returns, the publisher is closed and no further messages
    /// can be added. The returned [`BatchPubAck`] includes the batch id (the
    /// publisher's inbox) and the total number of messages in the batch.
    pub async fn commit<S: ToSubject>(
        self,
        subject: S,
        payload: Bytes,
    ) -> Result<BatchPubAck, FastPublishError> {
        self.commit_message(OutboundMessage {
            subject: subject.to_subject(),
            payload,
            headers: None,
        })
        .await
    }

    /// Commit the batch with a pre-constructed final message.
    pub async fn commit_message(
        mut self,
        msg: OutboundMessage,
    ) -> Result<BatchPubAck, FastPublishError> {
        self.commit_message_inner(msg, CommitKind::Final).await
    }

    /// End the batch without storing a final message (end-of-batch commit).
    ///
    /// Uses the first message's subject as the publish target; the server
    /// does not persist the commit message itself. Returns the same
    /// [`BatchPubAck`] shape as [`commit`](Self::commit).
    ///
    /// Returns [`FastPublishErrorKind::EmptyBatch`] if no messages have been
    /// added yet.
    pub async fn close(mut self) -> Result<BatchPubAck, FastPublishError> {
        if self.closed {
            return Err(FastPublishError::new(FastPublishErrorKind::Closed));
        }
        if self.sequence == 0 {
            return Err(FastPublishError::new(FastPublishErrorKind::EmptyBatch));
        }
        let subject = self
            .first_subject
            .clone()
            .expect("first_subject set once sequence > 0");
        let msg = OutboundMessage {
            subject,
            payload: Bytes::new(),
            headers: None,
        };
        self.commit_message_inner(msg, CommitKind::Eob).await
    }

    // ---- internal helpers --------------------------------------------------

    async fn commit_message_inner(
        &mut self,
        msg: OutboundMessage,
        kind: CommitKind,
    ) -> Result<BatchPubAck, FastPublishError> {
        if self.closed {
            return Err(FastPublishError::new(FastPublishErrorKind::Closed));
        }

        // Lazy bootstrap for first-op-is-commit (single-message immediate
        // commit) — the commit path uses the same subscribe-on-first-publish
        // as add_message.
        self.ensure_subscribed().await?;

        // Drain any pending events first.
        self.drain_nonblocking()?;

        // If a fatal flow error was observed (e.g. FlowErr or GapDetected in
        // Fail mode), the batch is already terminal server-side. Don't publish
        // the commit, but DO drain the inbox to pick up the terminal PubAck
        // the server will send — it tells the user which messages were
        // persisted. If the PubAck never arrives, we time out.
        if let Some(fatal_kind) = self.fatal {
            self.closed = true;
            // Try to get the PubAck for diagnostics; ignore drain errors.
            let _pub_ack = self.drain_until_pub_ack().await.ok();
            return Err(FastPublishError::new(fatal_kind));
        }

        // Stall gate: also apply to commits. A commit is just another
        // message from the server's perspective and counts against the
        // outstanding-ack window.
        let next_sequence = self.sequence + 1;
        if should_stall(
            self.last_ack_sequence,
            self.effective_flow,
            self.max_outstanding_acks,
            next_sequence,
        ) {
            self.wait_for_flow_event_with_pings().await?;
            if let Some(fatal_kind) = self.fatal {
                self.closed = true;
                let _pub_ack = self.drain_until_pub_ack().await.ok();
                return Err(FastPublishError::new(fatal_kind));
            }
        }

        self.sequence += 1;
        let op = match kind {
            CommitKind::Final => Operation::Commit,
            CommitKind::Eob => Operation::CommitEob,
        };

        if self.first_subject.is_none() {
            self.first_subject = Some(msg.subject.clone());
        }

        let reply = build_reply(&self.reply_prefix, self.sequence, op);
        self.publish_raw(msg, reply).await?;
        self.closed = true;

        self.drain_until_pub_ack().await
    }

    /// Create the inbox subscription if it does not exist yet.
    async fn ensure_subscribed(&mut self) -> Result<(), FastPublishError> {
        if self.subscriber.is_some() {
            return Ok(());
        }
        let wildcard = format!("{}.>", self.inbox);
        let sub = self
            .client
            .subscribe(wildcard)
            .await
            .map_err(|e| FastPublishError::with_source(FastPublishErrorKind::Subscribe, e))?;
        self.subscriber = Some(sub);
        Ok(())
    }

    /// Publish a message with a reply subject, fire-and-forget.
    ///
    /// Dispatches to `publish_with_reply_and_headers` when headers are
    /// present, otherwise the simpler `publish_with_reply`.
    ///
    /// Takes `&mut self` to avoid `&FastPublisher` crossing `.await`, which
    /// would require `FastPublisher: Sync` (not satisfied due to the boxed
    /// `FnMut` error handler field).
    async fn publish_raw(
        &mut self,
        msg: OutboundMessage,
        reply: String,
    ) -> Result<(), FastPublishError> {
        let OutboundMessage {
            subject,
            payload,
            headers,
        } = msg;
        let res = match headers {
            Some(h) => {
                self.client
                    .publish_with_reply_and_headers(subject, reply, h, payload)
                    .await
            }
            None => {
                self.client
                    .publish_with_reply(subject, reply, payload)
                    .await
            }
        };
        res.map_err(|e| FastPublishError::with_source(FastPublishErrorKind::Publish, e))
    }

    /// Non-blocking drain: consume all messages currently buffered on the
    /// subscription and apply them to the publisher state.
    ///
    /// The common case (no pending events) costs one `poll_next` returning
    /// `Pending`, which is a few nanoseconds.
    fn drain_nonblocking(&mut self) -> Result<(), FastPublishError> {
        let Some(sub) = self.subscriber.as_mut() else {
            return Ok(());
        };
        let waker = noop_waker_ref();
        let mut cx = TaskContext::from_waker(waker);
        loop {
            match sub.poll_next_unpin(&mut cx) {
                Poll::Ready(Some(msg)) => {
                    let shared = SharedHandlerState {
                        gap_mode: self.gap_mode,
                        effective_flow: &mut self.effective_flow,
                        last_ack_sequence: &mut self.last_ack_sequence,
                        initial_ack_received: &mut self.initial_ack_received,
                        pending_pub_ack: &mut self.pending_pub_ack,
                        fatal: &mut self.fatal,
                        on_error: self.on_error.as_mut(),
                    };
                    handle_inbox_message(shared, msg)?;
                }
                Poll::Ready(None) => {
                    // Subscription ended unexpectedly.
                    self.closed = true;
                    return Err(FastPublishError::new(FastPublishErrorKind::Closed));
                }
                Poll::Pending => return Ok(()),
            }
        }
    }

    /// After the first `Start` message is sent, wait for the server to
    /// either (a) send a `BatchFlowAck` confirming the batch, (b) return an
    /// init-time API error, or (c) send a terminal `PubAck` directly on the
    /// single-message immediate-commit fast path (stashed for later pickup).
    async fn await_first_reply(&mut self) -> Result<(), FastPublishError> {
        let deadline = tokio::time::Instant::now() + self.ack_timeout;
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(FastPublishError::new(FastPublishErrorKind::Timeout));
            }
            let remaining = deadline - now;
            let sub = self
                .subscriber
                .as_mut()
                .expect("subscriber installed by ensure_subscribed");
            let msg = match tokio::time::timeout(remaining, sub.next()).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    self.closed = true;
                    return Err(FastPublishError::new(FastPublishErrorKind::Closed));
                }
                Err(_) => return Err(FastPublishError::new(FastPublishErrorKind::Timeout)),
            };
            let shared = SharedHandlerState {
                gap_mode: self.gap_mode,
                effective_flow: &mut self.effective_flow,
                last_ack_sequence: &mut self.last_ack_sequence,
                initial_ack_received: &mut self.initial_ack_received,
                pending_pub_ack: &mut self.pending_pub_ack,
                fatal: &mut self.fatal,
                on_error: self.on_error.as_mut(),
            };
            handle_inbox_message(shared, msg)?;

            if let Some(kind) = self.fatal {
                return Err(FastPublishError::new(kind));
            }
            if self.pending_pub_ack.is_some() {
                // Single-message immediate-commit fast path — the commit
                // path (drain_until_pub_ack) will pick this up.
                return Ok(());
            }
            if self.initial_ack_received {
                // First BatchFlowAck arrived (even with seq:0) — batch is
                // confirmed and we can continue publishing.
                return Ok(());
            }
            // Otherwise: gap/err/etc. — keep waiting for the terminal signal.
        }
    }

    /// Drain the subscription until a terminal `PubAck` is received (or a
    /// fatal error surfaces).
    async fn drain_until_pub_ack(&mut self) -> Result<BatchPubAck, FastPublishError> {
        // Already stashed by a prior drain or first-reply?
        if let Some(pa) = self.pending_pub_ack.take() {
            self.unsubscribe_best_effort().await;
            return Ok(pa);
        }

        let deadline = tokio::time::Instant::now() + self.ack_timeout;
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(FastPublishError::new(FastPublishErrorKind::Timeout));
            }
            let remaining = deadline - now;
            let sub = self
                .subscriber
                .as_mut()
                .expect("subscriber installed by ensure_subscribed");
            let msg = match tokio::time::timeout(remaining, sub.next()).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    self.closed = true;
                    return Err(FastPublishError::new(FastPublishErrorKind::Closed));
                }
                Err(_) => return Err(FastPublishError::new(FastPublishErrorKind::Timeout)),
            };

            let shared = SharedHandlerState {
                gap_mode: self.gap_mode,
                effective_flow: &mut self.effective_flow,
                last_ack_sequence: &mut self.last_ack_sequence,
                initial_ack_received: &mut self.initial_ack_received,
                pending_pub_ack: &mut self.pending_pub_ack,
                fatal: &mut self.fatal,
                on_error: self.on_error.as_mut(),
            };
            handle_inbox_message(shared, msg)?;

            if let Some(pa) = self.pending_pub_ack.take() {
                self.unsubscribe_best_effort().await;
                return Ok(pa);
            }
            if let Some(kind) = self.fatal.take() {
                self.unsubscribe_best_effort().await;
                return Err(FastPublishError::new(kind));
            }
        }
    }

    async fn unsubscribe_best_effort(&mut self) {
        if let Some(mut sub) = self.subscriber.take() {
            let _ = sub.unsubscribe().await;
        }
    }

    /// Block until the outstanding-ack window has room for another publish,
    /// sending periodic pings to the server to recover from lost acks.
    ///
    /// Matches Go orbit.go PR #32 `waitForStall`: split the ack_timeout into
    /// three intervals so up to two pings fit before the deadline.
    async fn wait_for_flow_event_with_pings(&mut self) -> Result<(), FastPublishError> {
        // Split timeout into 3 intervals for up to 2 pings before giving up.
        // Clamp floor to 100ms so tiny ack_timeouts don't spin.
        let ping_interval = (self.ack_timeout / 3).max(Duration::from_millis(100));
        let deadline = tokio::time::Instant::now() + self.ack_timeout;
        let mut ping_at = tokio::time::Instant::now() + ping_interval;

        loop {
            if let Some(kind) = self.fatal {
                return Err(FastPublishError::new(kind));
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(FastPublishError::new(FastPublishErrorKind::Timeout));
            }

            // Compute the next wake time: either the ping instant or the
            // overall deadline, whichever comes first.
            let next_wake = ping_at.min(deadline);
            let wait = next_wake.saturating_duration_since(now);

            let sub = self
                .subscriber
                .as_mut()
                .expect("subscriber installed before stall");

            match tokio::time::timeout(wait, sub.next()).await {
                Ok(Some(msg)) => {
                    let shared = SharedHandlerState {
                        gap_mode: self.gap_mode,
                        effective_flow: &mut self.effective_flow,
                        last_ack_sequence: &mut self.last_ack_sequence,
                        initial_ack_received: &mut self.initial_ack_received,
                        pending_pub_ack: &mut self.pending_pub_ack,
                        fatal: &mut self.fatal,
                        on_error: self.on_error.as_mut(),
                    };
                    handle_inbox_message(shared, msg)?;

                    let next_sequence = self.sequence + 1;
                    if !should_stall(
                        self.last_ack_sequence,
                        self.effective_flow,
                        self.max_outstanding_acks,
                        next_sequence,
                    ) {
                        // Window has room; we can publish again.
                        return Ok(());
                    }
                    // Still stalled: keep waiting for the next event.
                }
                Ok(None) => {
                    self.closed = true;
                    return Err(FastPublishError::new(FastPublishErrorKind::Closed));
                }
                Err(_) => {
                    // Timed out on this interval. Either send a ping (if the
                    // ping interval expired) or give up (if the overall
                    // deadline expired).
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return Err(FastPublishError::new(FastPublishErrorKind::Timeout));
                    }
                    // Must have hit the ping interval — send a ping and
                    // schedule the next one.
                    self.send_ping().await?;
                    ping_at = tokio::time::Instant::now() + ping_interval;
                }
            }
        }
    }

    /// Send a ping message (op=4) to recover from a possibly-lost ack.
    ///
    /// The ping does NOT increment the batch sequence. It is published to the
    /// first message's subject (required so the server routes it to the same
    /// stream), and triggers the server to resend the latest flow ack.
    ///
    /// Takes `&mut self` — not because the call mutates state, but because
    /// holding `&self` across `.await` would require `FastPublisher: Sync`,
    /// which the boxed `FnMut` error handler field prevents. Callers already
    /// hold `&mut self` so this is cheap.
    async fn send_ping(&mut self) -> Result<(), FastPublishError> {
        let Some(subject) = self.first_subject.clone() else {
            // Cannot ping before the first add — this should never happen
            // because the stall gate only fires after at least one publish.
            return Err(FastPublishError::new(FastPublishErrorKind::InvalidState));
        };
        let reply = build_reply(&self.reply_prefix, self.sequence, Operation::Ping);
        self.client
            .publish_with_reply(subject, reply, Bytes::new())
            .await
            .map_err(|e| FastPublishError::with_source(FastPublishErrorKind::Publish, e))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Handler for inbox messages
// ---------------------------------------------------------------------------

/// Mutable reference bundle passed to [`handle_inbox_message`] so the
/// function can update all the relevant fields without conflicting borrows
/// on `&mut FastPublisher` (which would prevent us from also holding a
/// mutable borrow on `self.subscriber` while processing drained messages).
struct SharedHandlerState<'a> {
    gap_mode: GapMode,
    effective_flow: &'a mut u16,
    last_ack_sequence: &'a mut u64,
    initial_ack_received: &'a mut bool,
    pending_pub_ack: &'a mut Option<BatchPubAck>,
    fatal: &'a mut Option<FastPublishErrorKind>,
    on_error: Option<&'a mut FastPublishErrorHandler>,
}

fn handle_inbox_message(
    mut state: SharedHandlerState<'_>,
    msg: async_nats::Message,
) -> Result<(), FastPublishError> {
    match classify(&msg.payload)? {
        Classified::FlowAck(ack) => {
            // The initial BatchFlowAck for a fresh batch has `seq:0` (no
            // messages persisted yet — it just confirms the batch was
            // accepted). We must track it separately from `last_ack_sequence`
            // because `seq:0` is the same as the default `last_ack_sequence`.
            *state.initial_ack_received = true;
            // Lost-ack handling: any newer seq implicitly acks all below.
            if ack.sequence > *state.last_ack_sequence {
                *state.last_ack_sequence = ack.sequence;
            }
            // Server-dictated flow override. Update the effective flow used by
            // the stall gate. The reply prefix always uses the CLIENT's initial
            // flow ceiling (not the server-dictated value) because ADR-50 says
            // "We add the initial flow [...] for replica followers who might
            // have missed the first message due to limits." The server's
            // getFastBatch parser reads the flow from the reply subject to set
            // maxAckMessages on followers — using the server-dictated (lower)
            // value would cap follower ramp-up incorrectly in clustered setups.
            let new_flow = ack.messages.max(1);
            if new_flow != *state.effective_flow {
                *state.effective_flow = new_flow;
                // NOTE: reply_prefix is NOT rebuilt here — it always contains
                // the initial flow ceiling. Only effective_flow (used by the
                // stall gate) is updated.
            }
        }
        Classified::FlowGap(gap) => {
            tracing::debug!(
                expected_last = gap.expected_last_sequence,
                current = gap.current_sequence,
                "fast batch gap detected"
            );
            if let Some(h) = state.on_error.as_deref_mut() {
                h(FastPublishError::new(FastPublishErrorKind::GapDetected));
            }
            // In GapMode::Fail, stop publishing immediately. The server has
            // already abandoned the batch; further publishes would be silently
            // dropped. The terminal PubAck (containing persisted-message
            // count) will arrive on the next drain/commit.
            if state.gap_mode == GapMode::Fail {
                *state.fatal = Some(FastPublishErrorKind::GapDetected);
            }
        }
        Classified::FlowErr(ferr) => {
            tracing::debug!(
                batch_sequence = ferr.sequence,
                err_code = ferr.error.error_code().0,
                "fast batch flow error"
            );
            let kind = FastPublishErrorKind::from_api_error(&ferr.error);
            if let Some(h) = state.on_error.as_deref_mut() {
                h(FastPublishError::new(kind));
            }
            if state.gap_mode == GapMode::Fail {
                *state.fatal = Some(kind);
            }
        }
        Classified::PubAck(pa) => {
            *state.pending_pub_ack = Some(pa);
        }
        Classified::InitError(err) => {
            *state.fatal = Some(FastPublishErrorKind::from_api_error(&err));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- reply subject format ------------------------------------------------

    #[test]
    fn reply_prefix_format_ok_mode() {
        let p = build_reply_prefix("_INBOX.abc123", 100, GapMode::Ok);
        assert_eq!(p, "_INBOX.abc123.100.ok.");
    }

    #[test]
    fn reply_prefix_format_fail_mode() {
        let p = build_reply_prefix("_INBOX.abc123", 50, GapMode::Fail);
        assert_eq!(p, "_INBOX.abc123.50.fail.");
    }

    #[test]
    fn reply_full_all_operations() {
        let prefix = build_reply_prefix("_INBOX.x", 10, GapMode::Fail);
        for (op, code) in [
            (Operation::Start, 0_u8),
            (Operation::Append, 1),
            (Operation::Commit, 2),
            (Operation::CommitEob, 3),
            (Operation::Ping, 4),
        ] {
            let r = build_reply(&prefix, 42, op);
            assert_eq!(r, format!("_INBOX.x.10.fail.42.{code}.$FI"));
        }
    }

    #[test]
    fn reply_full_both_gap_modes() {
        for (mode, tag) in [(GapMode::Ok, "ok"), (GapMode::Fail, "fail")] {
            let prefix = build_reply_prefix("_INBOX.abc", 25, mode);
            let r = build_reply(&prefix, 1, Operation::Start);
            assert_eq!(r, format!("_INBOX.abc.25.{tag}.1.0.$FI"));
        }
    }

    // -- inbox shape validation ----------------------------------------------

    #[test]
    fn inbox_shape_accepts_two_tokens() {
        assert!(validate_inbox_shape("_INBOX.abc123").is_ok());
        assert!(validate_inbox_shape("X.Y").is_ok());
    }

    #[test]
    fn inbox_shape_rejects_zero_dots() {
        assert!(matches!(
            validate_inbox_shape("INBOX").unwrap_err().kind(),
            FastPublishErrorKind::InvalidInboxShape
        ));
    }

    #[test]
    fn inbox_shape_rejects_three_or_more_tokens() {
        assert!(matches!(
            validate_inbox_shape("_INBOX.myapp.abc123")
                .unwrap_err()
                .kind(),
            FastPublishErrorKind::InvalidInboxShape
        ));
        assert!(matches!(
            validate_inbox_shape("a.b.c.d").unwrap_err().kind(),
            FastPublishErrorKind::InvalidInboxShape
        ));
    }

    #[test]
    fn inbox_shape_rejects_empty_tokens() {
        assert!(validate_inbox_shape(".abc").is_err());
        assert!(validate_inbox_shape("abc.").is_err());
        assert!(validate_inbox_shape("").is_err());
    }

    // -- JSON parsing --------------------------------------------------------

    #[test]
    fn parse_batch_flow_ack() {
        let payload = br#"{"type":"ack","seq":10,"msgs":15}"#;
        match classify(payload).unwrap() {
            Classified::FlowAck(a) => {
                assert_eq!(a.sequence, 10);
                assert_eq!(a.messages, 15);
            }
            other => panic!("expected FlowAck, got {other:?}"),
        }
    }

    #[test]
    fn parse_batch_flow_gap() {
        let payload = br#"{"type":"gap","last_seq":10,"seq":15}"#;
        match classify(payload).unwrap() {
            Classified::FlowGap(g) => {
                assert_eq!(g.expected_last_sequence, 10);
                assert_eq!(g.current_sequence, 15);
            }
            other => panic!("expected FlowGap, got {other:?}"),
        }
    }

    #[test]
    fn parse_batch_flow_err() {
        let payload = br#"{"type":"err","seq":7,"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 1"}}"#;
        match classify(payload).unwrap() {
            Classified::FlowErr(e) => {
                assert_eq!(e.sequence, 7);
                assert_eq!(e.error.error_code().0, 10071);
            }
            other => panic!("expected FlowErr, got {other:?}"),
        }
    }

    #[test]
    fn parse_terminal_pub_ack() {
        let payload = br#"{"stream":"TEST","seq":42,"batch":"inbox-id","count":10}"#;
        match classify(payload).unwrap() {
            Classified::PubAck(pa) => {
                assert_eq!(pa.stream, "TEST");
                assert_eq!(pa.sequence, 42);
                assert_eq!(pa.batch_id, "inbox-id");
                assert_eq!(pa.batch_size, 10);
            }
            other => panic!("expected PubAck, got {other:?}"),
        }
    }

    #[test]
    fn parse_init_error_response() {
        // Server sends init errors as plain JSPubAckResponse with no `type`
        // field — just `{"error":{...}}`. The classifier falls through the
        // tagged-enum parse and hits `Response::Err`.
        let payload =
            br#"{"error":{"code":400,"err_code":10205,"description":"fast batch publish not enabled"}}"#;
        match classify(payload).unwrap() {
            Classified::InitError(err) => {
                assert_eq!(err.error_code().0, 10205);
                assert_eq!(
                    FastPublishErrorKind::from_api_error(&err),
                    FastPublishErrorKind::NotEnabled
                );
            }
            other => panic!("expected InitError, got {other:?}"),
        }
    }

    #[test]
    fn classify_malformed_json_returns_error() {
        let payload = b"not json at all";
        let err = classify(payload).unwrap_err();
        assert!(matches!(err.kind(), FastPublishErrorKind::Serialization));
    }

    // -- stall formula -------------------------------------------------------

    #[test]
    fn stall_no_wait_when_window_is_strictly_greater() {
        // seq=19, ack=0, flow=10, max=2 → window=20, 20 > 19 → no wait
        assert!(!should_stall(0, 10, 2, 19));
    }

    #[test]
    fn stall_waits_at_exact_boundary() {
        // seq=20, ack=0, flow=10, max=2 → window=20, 20 <= 20 → wait
        // This matches ADR-50's `<=` formulation.
        assert!(should_stall(0, 10, 2, 20));
    }

    #[test]
    fn stall_waits_past_boundary() {
        // seq=21 → window=20 <= 21 → wait
        assert!(should_stall(0, 10, 2, 21));
    }

    #[test]
    fn stall_honors_last_ack() {
        // ack=10, flow=10, max=2, seq=30 → window=30, 30 <= 30 → wait
        assert!(should_stall(10, 10, 2, 30));
        // ack=10, flow=10, max=2, seq=29 → window=30, 30 > 29 → no wait
        assert!(!should_stall(10, 10, 2, 29));
    }

    #[test]
    fn stall_with_single_outstanding_ack() {
        // max=1 matches "ack every N" throttling
        assert!(!should_stall(0, 10, 1, 9));
        assert!(should_stall(0, 10, 1, 10));
        assert!(should_stall(0, 10, 1, 11));
    }

    #[test]
    fn stall_with_max_outstanding_three() {
        // max=3 matches higher-RTT throughput mode
        assert!(!should_stall(0, 10, 3, 29));
        assert!(should_stall(0, 10, 3, 30));
    }

    #[test]
    fn stall_saturates_on_pathological_inputs() {
        // Ensure u64 overflow is saturated — should never panic.
        let waited = should_stall(u64::MAX - 5, u16::MAX, u16::MAX, u64::MAX);
        // Window saturates to u64::MAX, which is > u64::MAX is false, so should stall.
        assert!(waited);
    }

    // -- error code mapping --------------------------------------------------

    fn api_err(code: u64) -> async_nats::jetstream::Error {
        // Build a minimal jetstream::Error via JSON round-trip so we don't
        // depend on any constructor that may not exist in the public API.
        let json = format!(r#"{{"code":400,"err_code":{code},"description":"test"}}"#);
        serde_json::from_str(&json).expect("synthetic api error parses")
    }

    #[test]
    fn error_code_mapping_verified_against_server() {
        assert_eq!(
            FastPublishErrorKind::from_api_error(&api_err(10205)),
            FastPublishErrorKind::NotEnabled
        );
        assert_eq!(
            FastPublishErrorKind::from_api_error(&api_err(10206)),
            FastPublishErrorKind::InvalidPattern
        );
        assert_eq!(
            FastPublishErrorKind::from_api_error(&api_err(10207)),
            FastPublishErrorKind::InvalidBatchId
        );
        assert_eq!(
            FastPublishErrorKind::from_api_error(&api_err(10208)),
            FastPublishErrorKind::UnknownBatchId
        );
        assert_eq!(
            FastPublishErrorKind::from_api_error(&api_err(10211)),
            FastPublishErrorKind::TooManyInflight
        );
    }

    #[test]
    fn error_code_mapping_unknown_is_flow_error() {
        assert_eq!(
            FastPublishErrorKind::from_api_error(&api_err(10071)),
            FastPublishErrorKind::FlowError
        );
        assert_eq!(
            FastPublishErrorKind::from_api_error(&api_err(99999)),
            FastPublishErrorKind::FlowError
        );
    }

    // -- display impl --------------------------------------------------------

    #[test]
    fn error_kind_display_non_empty() {
        for kind in [
            FastPublishErrorKind::NotEnabled,
            FastPublishErrorKind::InvalidPattern,
            FastPublishErrorKind::InvalidBatchId,
            FastPublishErrorKind::UnknownBatchId,
            FastPublishErrorKind::TooManyInflight,
            FastPublishErrorKind::GapDetected,
            FastPublishErrorKind::FlowError,
            FastPublishErrorKind::EmptyBatch,
            FastPublishErrorKind::InvalidInboxShape,
            FastPublishErrorKind::InvalidConfig,
            FastPublishErrorKind::Closed,
            FastPublishErrorKind::Timeout,
            FastPublishErrorKind::Subscribe,
            FastPublishErrorKind::Publish,
            FastPublishErrorKind::Serialization,
            FastPublishErrorKind::InvalidState,
            FastPublishErrorKind::Other,
        ] {
            let s = format!("{kind}");
            assert!(!s.is_empty(), "empty Display for {kind:?}");
        }
    }

    // -- default impls -------------------------------------------------------

    #[test]
    fn gap_mode_default_is_fail() {
        assert_eq!(GapMode::default(), GapMode::Fail);
    }

    // -- builder validation --------------------------------------------------
    //
    // These tests construct a bare `FastPublisherBuilder` without a real
    // NATS connection. We use a dummy client obtained by connecting to a
    // bogus address lazily — no I/O is performed because `async_nats::Client`
    // implements the configuration pathways that are hit by `build()`:
    // only `new_inbox()` (pure, no network) is called before validation.
    //
    // Since we cannot synthesize an `async_nats::Client` without a runtime,
    // each test that needs one does so inside a tokio runtime.

    async fn dummy_builder() -> (nats_server::Server, FastPublisherBuilder) {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        (
            server,
            FastPublisherBuilder::new(client, Duration::from_secs(5)),
        )
    }

    #[tokio::test]
    async fn builder_rejects_max_outstanding_zero() {
        let (_s, b) = dummy_builder().await;
        let err = b.max_outstanding_acks(0).build().unwrap_err();
        assert!(matches!(err.kind(), FastPublishErrorKind::InvalidConfig));
    }

    #[tokio::test]
    async fn builder_rejects_max_outstanding_four() {
        let (_s, b) = dummy_builder().await;
        let err = b.max_outstanding_acks(4).build().unwrap_err();
        assert!(matches!(err.kind(), FastPublishErrorKind::InvalidConfig));
    }

    #[tokio::test]
    async fn builder_accepts_all_valid_max_outstanding() {
        for n in 1..=3 {
            let (_s, b) = dummy_builder().await;
            let fp = b.max_outstanding_acks(n).build().expect("valid config");
            assert_eq!(fp.max_outstanding_acks, n);
        }
    }

    #[tokio::test]
    async fn builder_clamps_flow_zero_to_one() {
        let (_s, b) = dummy_builder().await;
        let fp = b.flow(0).build().expect("flow clamped to 1");
        assert_eq!(fp.flow, 1);
    }

    #[tokio::test]
    async fn builder_default_values() {
        let (_s, b) = dummy_builder().await;
        let fp = b.build().expect("defaults build ok");
        assert_eq!(fp.flow, DEFAULT_FLOW);
        assert_eq!(fp.effective_flow, DEFAULT_FLOW);
        assert_eq!(fp.max_outstanding_acks, DEFAULT_MAX_OUTSTANDING_ACKS);
        assert_eq!(fp.gap_mode, GapMode::Fail);
        assert_eq!(fp.sequence, 0);
        assert_eq!(fp.last_ack_sequence, 0);
        assert!(fp.subscriber.is_none());
        assert!(!fp.is_closed());
        assert_eq!(fp.size(), 0);
    }

    #[tokio::test]
    async fn builder_produces_cached_reply_prefix() {
        let (_s, b) = dummy_builder().await;
        let fp = b.flow(42).gap_mode(GapMode::Ok).build().unwrap();
        assert!(fp.reply_prefix.starts_with(&fp.inbox));
        assert!(fp.reply_prefix.ends_with(".42.ok."));
    }

    #[tokio::test]
    async fn builder_batch_id_is_the_inbox() {
        let (_s, b) = dummy_builder().await;
        let fp = b.build().unwrap();
        assert_eq!(fp.batch_id(), fp.inbox);
        assert_eq!(fp.inbox.matches('.').count(), 1);
    }
}
