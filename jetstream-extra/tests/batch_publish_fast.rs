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

//! Integration tests for fast-ingest batch publishing.
//!
//! Requires nats-server 2.14 or later (fast ingest was added in 2.14 / API
//! level 4). Each test spins up an embedded server via the `nats_server`
//! dev-dependency and creates a stream with `allow_batched: true` via a raw
//! `$JS.API.STREAM.CREATE` request since `async-nats 0.45.0`'s `StreamConfig`
//! does not yet expose that field.

#[cfg(test)]
mod fast_publish_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_nats::jetstream::message::OutboundMessage;
    use jetstream_extra::batch_publish_fast::{FastPublishErrorKind, FastPublishExt, GapMode};
    use serde_json::json;

    /// Create a stream with fast-ingest batching enabled.
    ///
    /// Uses a raw `STREAM.CREATE` JetStream API request because async-nats
    /// 0.45.0 does not expose `allow_batched` on its `stream::Config`.
    async fn setup_fast_stream(
        ctx: &async_nats::jetstream::Context,
        name: &str,
        subjects_wildcard: &str,
    ) {
        let _ = ctx.delete_stream(name).await;

        let body = json!({
            "name": name,
            "subjects": [subjects_wildcard],
            "retention": "limits",
            "storage": "file",
            "allow_batched": true,
            "allow_atomic": true,
        });

        let resp: serde_json::Value = ctx
            .request(format!("STREAM.CREATE.{name}"), &body)
            .await
            .expect("STREAM.CREATE succeeds");

        // Surface a server-side error explicitly instead of silently
        // proceeding with a broken stream.
        if let Some(err) = resp.get("error") {
            panic!("STREAM.CREATE returned error: {err}");
        }

        // Sanity-check: the created stream must have allow_batched set.
        // If the server version doesn't understand the field, the stream is
        // still created but without fast-ingest enabled — which would cause
        // mysterious timeouts on the first add rather than a clear error.
        let cfg = resp
            .get("config")
            .expect("STREAM.CREATE response has config");
        let allow_batched = cfg
            .get("allow_batched")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(
            allow_batched,
            "server did not honor allow_batched — response: {resp}"
        );
    }

    async fn connect() -> (nats_server::Server, async_nats::jetstream::Context) {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let ctx = async_nats::jetstream::new(client);
        (server, ctx)
    }

    // -- basic happy path ----------------------------------------------------
    //
    // Mirrors orbit.go PR #32 `TestFastPublisher/basic`.

    #[tokio::test]
    async fn basic() {
        let (_server, js) = connect().await;
        setup_fast_stream(&js, "TEST", "test.>").await;

        let mut batch = js.fast_publish().build().expect("build");

        // Add message 1 — this triggers lazy subscribe + initial flow ack.
        let ack1 = batch
            .add("test.1", "message 1".into())
            .await
            .expect("add 1");
        assert_eq!(ack1.batch_sequence, 1);

        // Add message 2 — append path.
        let ack2 = batch
            .add("test.2", "message 2".into())
            .await
            .expect("add 2");
        assert_eq!(ack2.batch_sequence, 2);

        // Commit with the third message.
        let pub_ack = batch
            .commit("test.3", "message 3".into())
            .await
            .expect("commit");
        assert_eq!(pub_ack.batch_size, 3);
        assert_eq!(pub_ack.stream, "TEST");
        assert!(!pub_ack.batch_id.is_empty());

        // Stream should now have exactly 3 messages.
        let stream = js.get_stream("TEST").await.unwrap();
        let info = stream.get_info().await.unwrap();
        assert_eq!(info.state.messages, 3);
    }

    // -- close (EOB) ---------------------------------------------------------
    //
    // Mirrors orbit.go PR #32 `TestFastPublisher/close`.

    #[tokio::test]
    async fn close_eob() {
        let (_server, js) = connect().await;
        setup_fast_stream(&js, "TEST", "test.>").await;

        let mut batch = js.fast_publish().build().expect("build");
        batch.add("test.1", "message 1".into()).await.unwrap();
        batch.add("test.2", "message 2".into()).await.unwrap();

        let pub_ack = batch.close().await.expect("close");
        assert_eq!(pub_ack.batch_size, 2);

        // Close is EOB: the commit message itself is not stored.
        let stream = js.get_stream("TEST").await.unwrap();
        let info = stream.get_info().await.unwrap();
        assert_eq!(info.state.messages, 2);
    }

    // -- close on empty batch errors -----------------------------------------

    #[tokio::test]
    async fn close_empty_batch_errors() {
        let (_server, js) = connect().await;
        setup_fast_stream(&js, "TEST", "test.>").await;

        let batch = js.fast_publish().build().expect("build");
        let err = batch.close().await.unwrap_err();
        assert!(matches!(err.kind(), FastPublishErrorKind::EmptyBatch));
    }

    // -- flow control --------------------------------------------------------
    //
    // Mirrors orbit.go PR #32 `TestFastPublisher/with_flow_control`.

    #[tokio::test]
    async fn with_flow_control() {
        let (_server, js) = connect().await;
        setup_fast_stream(&js, "TEST", "test.>").await;

        let mut batch = js
            .fast_publish()
            .flow(50)
            .max_outstanding_acks(3)
            .ack_timeout(Duration::from_secs(5))
            .build()
            .expect("build");

        for i in 0..200 {
            batch
                .add("test.msg", format!("data {i}").into())
                .await
                .unwrap_or_else(|e| panic!("add {i} failed: {e:?}"));
        }

        let pub_ack = batch
            .commit("test.final", "final".into())
            .await
            .expect("commit");
        assert_eq!(pub_ack.batch_size, 201);
    }

    // -- continue on gap -----------------------------------------------------
    //
    // Mirrors orbit.go PR #32 `TestFastPublisher/with_continue_on_gap`.
    //
    // This test doesn't actually force a gap (gaps are hard to induce in a
    // deterministic test); it just verifies that GapMode::Ok is accepted and
    // a batch completes normally under it.

    #[tokio::test]
    async fn with_continue_on_gap() {
        let (_server, js) = connect().await;
        setup_fast_stream(&js, "TEST", "test.>").await;

        let mut batch = js
            .fast_publish()
            .gap_mode(GapMode::Ok)
            .build()
            .expect("build");

        for i in 0..5 {
            batch
                .add("test.msg", format!("data {i}").into())
                .await
                .unwrap();
        }

        let pub_ack = batch
            .commit("test.final", "final".into())
            .await
            .expect("commit");
        assert_eq!(pub_ack.batch_size, 6);
    }

    // -- large batch ---------------------------------------------------------
    //
    // Mirrors orbit.go PR #32 `TestFastPublisher_LargeBatch`. This exercises
    // the stall gate and flow-ack handling over a sustained publish.

    #[tokio::test]
    async fn large_batch() {
        let (_server, js) = connect().await;
        setup_fast_stream(&js, "TEST", "test.>").await;

        let mut batch = js
            .fast_publish()
            .flow(100)
            .max_outstanding_acks(2)
            .ack_timeout(Duration::from_secs(10))
            .build()
            .expect("build");

        const N: usize = 10_000;
        for i in 0..N {
            batch
                .add("test.msg", format!("data {i}").into())
                .await
                .unwrap_or_else(|e| panic!("add {i} failed: {e:?}"));
        }

        let pub_ack = batch.close().await.expect("close");
        assert_eq!(pub_ack.batch_size as usize, N);

        let stream = js.get_stream("TEST").await.unwrap();
        let info = stream.get_info().await.unwrap();
        assert_eq!(info.state.messages as usize, N);
    }

    // -- single-msg immediate commit ----------------------------------------
    //
    // The very first operation is `commit`. Server replies with a plain
    // PubAck and no intervening BatchFlowAck — verified against
    // nats-server/server/jetstream_batching_test.go:3010-3019.

    #[tokio::test]
    async fn single_msg_immediate_commit() {
        let (_server, js) = connect().await;
        setup_fast_stream(&js, "TEST", "test.>").await;

        let batch = js.fast_publish().build().expect("build");
        let pub_ack = batch
            .commit("test.only", "single".into())
            .await
            .expect("commit");
        assert_eq!(pub_ack.batch_size, 1);

        let stream = js.get_stream("TEST").await.unwrap();
        let info = stream.get_info().await.unwrap();
        assert_eq!(info.state.messages, 1);
    }

    // -- not enabled on stream -----------------------------------------------

    #[tokio::test]
    async fn not_enabled_on_stream() {
        let (_server, js) = connect().await;
        // Create a stream WITHOUT allow_batched.
        let _ = js.delete_stream("TEST").await;
        js.create_stream(async_nats::jetstream::stream::Config {
            name: "TEST".into(),
            subjects: vec!["test.>".into()],
            ..Default::default()
        })
        .await
        .unwrap();

        let mut batch = js
            .fast_publish()
            .ack_timeout(Duration::from_secs(3))
            .build()
            .expect("build");

        let err = batch.add("test.1", "data".into()).await.unwrap_err();
        assert!(
            matches!(err.kind(), FastPublishErrorKind::NotEnabled),
            "expected NotEnabled, got {err:?}"
        );
    }

    // -- stall gate forced ---------------------------------------------------
    //
    // Force the stall gate to actually fire by asking for a very low flow
    // ceiling. The ack_timeout must be generous enough for the server to
    // process the publish + send the flow ack.

    #[tokio::test]
    async fn stall_gate_fires_with_low_flow() {
        let (_server, js) = connect().await;
        setup_fast_stream(&js, "TEST", "test.>").await;

        let mut batch = js
            .fast_publish()
            .flow(5)
            .max_outstanding_acks(2)
            .ack_timeout(Duration::from_secs(10))
            .build()
            .expect("build");

        // flow=5, max=2 → window=10. Publishing 50 messages will exercise
        // the stall gate multiple times.
        for i in 0..50 {
            batch
                .add("test.msg", format!("data {i}").into())
                .await
                .unwrap_or_else(|e| panic!("add {i} failed: {e:?}"));
        }

        let pub_ack = batch
            .commit("test.done", "done".into())
            .await
            .expect("commit");
        assert_eq!(pub_ack.batch_size, 51);
    }

    // -- concurrent publishers -----------------------------------------------
    //
    // Several publishers writing to the same stream concurrently. All should
    // complete without stepping on each other.

    #[tokio::test]
    async fn concurrent_publishers() {
        let (_server, js) = connect().await;
        setup_fast_stream(&js, "TEST", "test.>").await;

        let mut handles = Vec::new();
        for worker in 0..4 {
            let js_clone = js.clone();
            handles.push(tokio::spawn(async move {
                let mut batch = js_clone
                    .fast_publish()
                    .flow(50)
                    .max_outstanding_acks(2)
                    .ack_timeout(Duration::from_secs(10))
                    .build()
                    .expect("build");
                for i in 0..100 {
                    batch
                        .add("test.msg", format!("w{worker}-{i}").into())
                        .await
                        .expect("add");
                }
                batch
                    .commit("test.done", format!("w{worker}-done").into())
                    .await
                    .expect("commit")
            }));
        }

        for h in handles {
            let ack = h.await.unwrap();
            assert_eq!(ack.batch_size, 101);
        }

        // 4 publishers × 101 messages each = 404 total.
        let stream = js.get_stream("TEST").await.unwrap();
        let info = stream.get_info().await.unwrap();
        assert_eq!(info.state.messages, 404);
    }

    // -- drop mid-batch ------------------------------------------------------
    //
    // Build, add, drop. Must not panic; the server will time out the
    // abandoned batch after 10s. We don't wait for the server cleanup — we
    // just verify the client side drops cleanly.

    #[tokio::test]
    async fn drop_mid_batch() {
        let (_server, js) = connect().await;
        setup_fast_stream(&js, "TEST", "test.>").await;

        {
            let mut batch = js.fast_publish().build().expect("build");
            batch.add("test.1", "data".into()).await.unwrap();
            // Drop at end of scope — no commit/close.
        }

        // Follow up with a fresh publisher on the same stream to prove the
        // client state is healthy (no stuck subscription, no leaked task).
        let mut batch = js.fast_publish().build().expect("build");
        batch.add("test.a", "a".into()).await.unwrap();
        let pub_ack = batch
            .commit("test.b", "b".into())
            .await
            .expect("commit after drop");
        assert_eq!(pub_ack.batch_size, 2);
    }

    // -- BatchFlowErr from header expectation mismatch (Fail mode) -----------
    //
    // First message sets `Nats-Expected-Last-Sequence:99` on an empty stream.
    // The server publishes the message but reports a `BatchFlowErr` on the
    // batch inbox; in `GapMode::Fail` this terminates the batch with
    // `FlowError`. ADR-50 §"Server Errors".

    #[tokio::test]
    async fn flow_err_on_expected_last_sequence_mismatch_fail() {
        let (_server, js) = connect().await;
        setup_fast_stream(&js, "TEST", "test.>").await;

        let mut batch = js
            .fast_publish()
            .ack_timeout(Duration::from_secs(5))
            .build()
            .expect("build");

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Expected-Last-Sequence", "99");
        let msg = OutboundMessage {
            subject: "test.bad".into(),
            payload: "x".into(),
            headers: Some(headers),
        };

        // First add publishes; server emits a BatchFlowErr asynchronously on
        // the inbox. Give the server a moment to deliver it before commit so
        // the publisher classifies the err and sets a fatal kind, rather than
        // commit racing with a not-yet-arrived err and timing out.
        let _ = batch.add_message(msg).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let err = batch
            .commit("test.done", "done".into())
            .await
            .expect_err("commit must fail with FlowError");
        assert_eq!(
            err.kind(),
            FastPublishErrorKind::FlowError,
            "expected FlowError, got {err:?}"
        );
    }

    // -- BatchFlowErr in GapMode::Ok — callback fires, batch continues -------

    #[tokio::test]
    async fn flow_err_callback_fires_in_ok_mode() {
        let (_server, js) = connect().await;
        setup_fast_stream(&js, "TEST", "test.>").await;

        // Capture the actual error kind seen by the callback so we can assert
        // it was a FlowError, not a stray Timeout/Gap.
        let observed_kinds = Arc::new(std::sync::Mutex::new(Vec::<FastPublishErrorKind>::new()));
        let observed_cb = observed_kinds.clone();

        let mut batch = js
            .fast_publish()
            .gap_mode(GapMode::Ok)
            .ack_timeout(Duration::from_secs(5))
            .on_error(move |err| {
                observed_cb.lock().unwrap().push(err.kind());
            })
            .build()
            .expect("build");

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Expected-Last-Sequence", "99");
        let msg = OutboundMessage {
            subject: "test.bad".into(),
            payload: "x".into(),
            headers: Some(headers),
        };
        let _ = batch.add_message(msg).await;

        // Wait for the BatchFlowErr to land on the inbox.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Push more good messages — Ok mode should not abort.
        for i in 0..5 {
            let _ = batch.add("test.ok", format!("data {i}").into()).await;
        }
        let _ = batch.commit("test.done", "done".into()).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let kinds = observed_kinds.lock().unwrap();
        assert!(
            kinds.contains(&FastPublishErrorKind::FlowError),
            "expected callback to observe FlowError at least once; saw {kinds:?}"
        );
    }
}
