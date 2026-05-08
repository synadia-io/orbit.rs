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

//! Benchmark: 100k messages via the four JetStream publish modes.
//!
//! 1. sync — `js.publish().await + ack.await` per message
//! 2. async — bounded-inflight `FuturesUnordered` over PubAck futures
//! 3. atomic batch — `client.batch_publish()` with optional flow control
//! 4. fast — `client.fast_publish()` (server 2.14+, `allow_batched`)
//!
//! All four target an R1 stream, payload ~63B per message. Subjects and
//! payloads are static to avoid per-iteration allocation skewing the loop.
//!
//! ```bash
//! nats-server -js
//! cargo run -p jetstream-extra --example bench_normal_vs_fast --release
//! ```

use std::future::IntoFuture;
use std::time::{Duration, Instant};

use async_nats::jetstream::{self, stream};
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use jetstream_extra::batch_publish::BatchPublishExt;
use jetstream_extra::batch_publish_fast::{FastPublishExt, GapMode};
use serde_json::json;

const TOTAL: usize = 100_000;
const PAYLOAD: &[u8] = b"benchmark payload (~64B) ......................................";
const SUBJECT_SYNC: &str = "bench.sync";
const SUBJECT_ASYNC: &str = "bench.async";
const SUBJECT_ATOMIC: &str = "bench.atomic";
const SUBJECT_FAST: &str = "bench.fast";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The default client capacity (128) and subscription capacity (4096) are
    // tuned for typical workloads. For 100k-message benches with thousands of
    // concurrent PubAck futures we want a bigger write channel.
    let client = async_nats::ConnectOptions::new()
        .client_capacity(8192)
        .subscription_capacity(8192)
        .connect("nats://127.0.0.1:4222")
        .await?;
    let js = jetstream::new(client);

    println!("== payload {}B, {} messages each ==", PAYLOAD.len(), TOTAL);

    let sync_ = bench_sync(&js).await?;
    let async_ = bench_async(&js).await?;
    let core_trick = bench_core_then_js(&js).await?;
    let atomic = bench_atomic(&js).await?;
    let fast = bench_fast(&js).await?;

    println!();
    println!("                       elapsed     msg/s    MB/s");
    print_row("sync       js.publish", sync_);
    print_row("async      js.publish", async_);
    print_row("core+js    last-only ", core_trick);
    print_row("atomic     batch     ", atomic);
    print_row("fast       batch     ", fast);
    Ok(())
}

#[derive(Copy, Clone)]
struct Stats {
    elapsed: Duration,
}

fn print_row(label: &str, s: Stats) {
    let secs = s.elapsed.as_secs_f64();
    let msgs_per_sec = TOTAL as f64 / secs;
    let mb_per_sec = (TOTAL as f64 * PAYLOAD.len() as f64) / secs / 1_048_576.0;
    println!("{label}  {:>8.3}s  {:>8.0}  {:>6.2}", secs, msgs_per_sec, mb_per_sec);
}

fn payload() -> Bytes {
    Bytes::from_static(PAYLOAD)
}

async fn create_stream(
    js: &jetstream::Context,
    name: &str,
    subjects: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = js.delete_stream(name).await;
    js.create_stream(stream::Config {
        name: name.into(),
        subjects: vec![subjects.into()],
        num_replicas: 1,
        ..Default::default()
    })
    .await?;
    Ok(())
}

async fn bench_sync(js: &jetstream::Context) -> Result<Stats, Box<dyn std::error::Error>> {
    println!("\n[sync] R1 stream, await each PubAck");
    create_stream(js, "BENCH_SYNC", "bench.sync").await?;

    let started = Instant::now();
    for _ in 0..TOTAL {
        let ack_fut = js.publish(SUBJECT_SYNC, payload()).await?;
        ack_fut.await?;
    }
    let elapsed = started.elapsed();

    let info = js.get_stream("BENCH_SYNC").await?.get_info().await?;
    assert_eq!(info.state.messages as usize, TOTAL);
    println!("[sync] done in {:.3}s", elapsed.as_secs_f64());
    Ok(Stats { elapsed })
}

async fn bench_async(js: &jetstream::Context) -> Result<Stats, Box<dyn std::error::Error>> {
    const MAX_INFLIGHT: usize = 1024;
    println!("\n[async] R1 stream, max {MAX_INFLIGHT} inflight PubAck futures");
    create_stream(js, "BENCH_ASYNC", "bench.async").await?;

    let started = Instant::now();
    let mut inflight: FuturesUnordered<_> = FuturesUnordered::new();
    for _ in 0..TOTAL {
        if inflight.len() >= MAX_INFLIGHT
            && let Some(res) = inflight.next().await
        {
            res?;
        }
        let ack_fut = js.publish(SUBJECT_ASYNC, payload()).await?;
        inflight.push(ack_fut.into_future());
    }
    while let Some(res) = inflight.next().await {
        res?;
    }
    let elapsed = started.elapsed();

    let info = js.get_stream("BENCH_ASYNC").await?.get_info().await?;
    assert_eq!(info.state.messages as usize, TOTAL);
    println!("[async] done in {:.3}s", elapsed.as_secs_f64());
    Ok(Stats { elapsed })
}

/// Fire core-NATS publish for the first N-1 messages (no JS ack handshake);
/// for the last, use `js.publish` so the returned PubAck barriers the whole
/// run. The stream still captures every message because subjects match — JS
/// captures whatever lands on its subjects regardless of who published it.
/// orbit.go reports this as the fastest path; useful as an upper bound.
async fn bench_core_then_js(
    js: &jetstream::Context,
) -> Result<Stats, Box<dyn std::error::Error>> {
    println!("\n[core+js] R1 stream, core publish first N-1 + js.publish last");
    create_stream(js, "BENCH_CORE", "bench.core").await?;
    let nc = js.clone();

    let started = Instant::now();
    for _ in 0..(TOTAL - 1) {
        nc.client().publish("bench.core", payload()).await?;
    }
    let ack_fut = js.publish("bench.core", payload()).await?;
    ack_fut.await?;
    let elapsed = started.elapsed();

    let info = js.get_stream("BENCH_CORE").await?.get_info().await?;
    assert_eq!(info.state.messages as usize, TOTAL);
    println!("[core+js] done in {:.3}s", elapsed.as_secs_f64());
    Ok(Stats { elapsed })
}

async fn bench_atomic(js: &jetstream::Context) -> Result<Stats, Box<dyn std::error::Error>> {
    println!("\n[atomic] R1 stream, allow_atomic_publish");
    let _ = js.delete_stream("BENCH_ATOMIC").await;
    js.create_stream(stream::Config {
        name: "BENCH_ATOMIC".into(),
        subjects: vec!["bench.atomic".into()],
        num_replicas: 1,
        allow_atomic_publish: true,
        ..Default::default()
    })
    .await?;

    // Atomic batches cap at 1000 messages server-side, so chunk into
    // ceil(TOTAL/1000) batches. Each batch is one round-trip: ack_first on
    // open, fire-and-forget for middles, request on commit. Flow-control via
    // ack_every is left default (no flow ack between first and commit), which
    // is the orbit.go default.
    const BATCH: usize = 1000;
    let mut sent = 0;
    let started = Instant::now();
    while sent < TOTAL {
        let n = (TOTAL - sent).min(BATCH);
        let mut batch = js.batch_publish().build();
        for _ in 0..(n - 1) {
            batch.add(SUBJECT_ATOMIC, payload()).await?;
        }
        batch.commit(SUBJECT_ATOMIC, payload()).await?;
        sent += n;
    }
    let elapsed = started.elapsed();

    let info = js.get_stream("BENCH_ATOMIC").await?.get_info().await?;
    assert_eq!(info.state.messages as usize, TOTAL);
    println!("[atomic] done in {:.3}s ({} batches of {BATCH})", elapsed.as_secs_f64(), TOTAL / BATCH);
    Ok(Stats { elapsed })
}

async fn bench_fast(js: &jetstream::Context) -> Result<Stats, Box<dyn std::error::Error>> {
    println!("\n[fast] R1 stream, allow_batched");
    let _ = js.delete_stream("BENCH_FAST").await;
    let body = json!({
        "name": "BENCH_FAST",
        "subjects": ["bench.fast"],
        "num_replicas": 1,
        "retention": "limits",
        "storage": "file",
        "allow_batched": true,
    });
    let resp: serde_json::Value = js.request("STREAM.CREATE.BENCH_FAST", &body).await?;
    if let Some(err) = resp.get("error") {
        return Err(format!("STREAM.CREATE failed: {err}").into());
    }

    let mut batch = js
        .fast_publish()
        .flow(1000)
        .max_outstanding_acks(2)
        .gap_mode(GapMode::Fail)
        .ack_timeout(Duration::from_secs(10))
        .build()?;

    let started = Instant::now();
    for _ in 0..(TOTAL - 1) {
        batch.add(SUBJECT_FAST, payload()).await?;
    }
    let pub_ack = batch.commit(SUBJECT_FAST, payload()).await?;
    let elapsed = started.elapsed();

    assert_eq!(pub_ack.batch_size as usize, TOTAL);
    let info = js.get_stream("BENCH_FAST").await?.get_info().await?;
    assert_eq!(info.state.messages as usize, TOTAL);
    println!("[fast] done in {:.3}s (batch_id={})", elapsed.as_secs_f64(), pub_ack.batch_id);
    Ok(Stats { elapsed })
}
