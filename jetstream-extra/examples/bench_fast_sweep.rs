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

//! Sweep `flow` × `max_outstanding_acks` for fast-ingest batch publishing.
//!
//! Identifies the configuration that maximizes throughput on a local R1
//! stream. Each cell runs `RUNS` independent batches and reports the median.
//!
//! ```bash
//! nats-server -js
//! cargo run -p jetstream-extra --example bench_fast_sweep --release
//! ```

use std::time::{Duration, Instant};

use async_nats::jetstream::{self};
use bytes::Bytes;
use futures::StreamExt;
use jetstream_extra::batch_publish_fast::{FastPublishExt, GapMode};
use serde_json::json;

const TOTAL: usize = 100_000;
const PAYLOAD_64: &[u8] = b"benchmark payload (~64B) ......................................";
const SUBJECT: &str = "bench.fastsweep";
const RUNS: usize = 3;

const FLOWS: &[u16] = &[500, 1000, 2000, 5000, 10000];
const MAX_ACKS: &[u16] = &[2, 3];
const PAYLOADS: &[(&str, &[u8])] = &[("0B", b""), ("64B", PAYLOAD_64)];

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::ConnectOptions::new()
        .client_capacity(8192)
        .subscription_capacity(8192)
        .connect("nats://127.0.0.1:4222")
        .await?;
    let js = jetstream::new(client);

    // Wipe any leftover sweep streams from prior aborted runs.
    let mut names = js.stream_names();
    while let Some(name) = names.next().await {
        if let Ok(name) = name
            && name.starts_with("BENCH_FAST_SWEEP")
        {
            let _ = js.delete_stream(&name).await;
        }
    }
    drop(names);

    println!("== fast-ingest sweep: {TOTAL} messages × {RUNS} runs each ==");

    for &(plabel, payload_bytes) in PAYLOADS {
        println!();
        println!("=== payload {plabel} ({} bytes) ===", payload_bytes.len());
        println!("        flow  max_acks   median_msg/s   p_min       p_max");

        let mut best: Option<(u16, u16, f64)> = None;
        for &flow in FLOWS {
            for &max_acks in MAX_ACKS {
                let runs = run_n_times(&js, flow, max_acks, payload_bytes, RUNS).await?;
                let median = median(&runs);
                let p_min = runs.iter().cloned().fold(f64::INFINITY, f64::min);
                let p_max = runs.iter().cloned().fold(0.0_f64, f64::max);

                println!(
                    "       {flow:>5}  {max_acks:>8}   {:>12.0}   {:>8.0}   {:>8.0}",
                    median, p_min, p_max
                );

                if best.is_none_or(|(_, _, m)| median > m) {
                    best = Some((flow, max_acks, median));
                }
            }
        }

        if let Some((f, m, t)) = best {
            println!(
                "  ==> best for {plabel}: flow={f}, max_outstanding_acks={m} → {:.0} msg/s",
                t
            );
        }
    }
    Ok(())
}

async fn run_n_times(
    js: &jetstream::Context,
    flow: u16,
    max_acks: u16,
    payload: &'static [u8],
    n: usize,
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(run_once(js, flow, max_acks, payload).await?);
    }
    Ok(out)
}

async fn run_once(
    js: &jetstream::Context,
    flow: u16,
    max_acks: u16,
    payload_bytes: &'static [u8],
) -> Result<f64, Box<dyn std::error::Error>> {
    // Unique stream name per run avoids any stale fast-batch tracker state
    // from a previous iteration's just-deleted stream.
    let stream_name = format!(
        "BENCH_FAST_SWEEP_{flow}_{max_acks}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let subject = format!("{SUBJECT}.{flow}_{max_acks}");
    let body = json!({
        "name": stream_name,
        "subjects": [subject.clone()],
        "num_replicas": 1,
        "retention": "limits",
        "storage": "file",
        "allow_batched": true,
    });
    let resp: serde_json::Value = js
        .request(format!("STREAM.CREATE.{stream_name}"), &body)
        .await?;
    if let Some(err) = resp.get("error") {
        return Err(format!("STREAM.CREATE failed: {err}").into());
    }

    let mut batch = js
        .fast_publish()
        .flow(flow)
        .max_outstanding_acks(max_acks)
        .gap_mode(GapMode::Fail)
        .ack_timeout(Duration::from_secs(30))
        .build()?;

    let payload = || Bytes::from_static(payload_bytes);
    let started = Instant::now();
    for i in 0..(TOTAL - 1) {
        batch
            .add(subject.clone(), payload())
            .await
            .map_err(|e| format!("add {i} failed (flow={flow}, max={max_acks}): {e:?}"))?;
    }
    let pub_ack = batch
        .commit(subject.clone(), payload())
        .await
        .map_err(|e| format!("commit failed (flow={flow}, max={max_acks}): {e:?}"))?;
    let elapsed = started.elapsed();
    assert_eq!(pub_ack.batch_size as usize, TOTAL);

    let _ = js.delete_stream(&stream_name).await;
    Ok(TOTAL as f64 / elapsed.as_secs_f64())
}

fn median(xs: &[f64]) -> f64 {
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n.is_multiple_of(2) {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    } else {
        v[n / 2]
    }
}
