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

//! Example demonstrating batch operations with NATS JetStream counters.
//!
//! This example shows how to efficiently fetch multiple counter values
//! using the optimized batch operations API.

use async_nats::jetstream::stream::Config;
use futures_util::StreamExt;
use nats_counters::CounterExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to NATS
    let client = async_nats::connect("localhost:4222").await?;
    let js = async_nats::jetstream::new(client);

    // Create a counter-enabled stream for metrics
    let config = Config {
        name: "METRICS".to_string(),
        subjects: vec!["metrics.>".to_string()],
        allow_message_counter: true,
        allow_direct: true,
        ..Default::default()
    };

    // Create or update the stream
    let _stream = js.get_or_create_stream(config).await?;

    // Get the counter
    let counter = js.get_counter("METRICS").await?;

    println!("=== Adding Counter Values ===\n");

    // Simulate adding metrics from different services
    let services = ["auth", "api", "db", "cache"];
    let metrics = ["requests", "errors", "latency_ms"];

    for service in &services {
        for metric in &metrics {
            let subject = format!("metrics.{}.{}", service, metric);
            let value = match *metric {
                "requests" => 100 + (service.len() as i64 * 10),
                "errors" => service.len() as i64,
                "latency_ms" => 50 + (service.len() as i64 * 5),
                _ => 0,
            };

            let result = counter.add(subject.clone(), value).await?;
            println!("  {} = {}", subject, result);
        }
    }

    println!("\n=== Fetching Multiple Counters (Batch) ===\n");

    // Build list of subjects to fetch
    let mut subjects = Vec::new();
    for service in &services {
        for metric in &metrics {
            subjects.push(format!("metrics.{}.{}", service, metric));
        }
    }

    // Fetch all counters in a single batch operation
    let mut entries = counter.get_multiple(subjects.clone()).await?;

    println!("Fetched {} subjects in batch:\n", subjects.len());

    while let Some(entry) = entries.next().await {
        let entry = entry?;
        println!("  {} = {}", entry.subject, entry.value);

        // Show the last increment if available
        if let Some(increment) = entry.increment {
            println!("    (last increment: {})", increment);
        }
    }

    println!("\n=== Service Totals ===\n");

    // Get totals for each service using batch operations
    for service in &services {
        let service_subjects: Vec<String> = metrics
            .iter()
            .map(|m| format!("metrics.{}.{}", service, m))
            .collect();

        let mut service_entries = counter.get_multiple(service_subjects).await?;

        let mut total_requests = 0i64;
        let mut total_errors = 0i64;
        let mut total_latency = 0i64;

        while let Some(entry) = service_entries.next().await {
            let entry = entry?;
            if entry.subject.ends_with(".requests") {
                total_requests = entry.value.try_into().unwrap_or(0);
            } else if entry.subject.ends_with(".errors") {
                total_errors = entry.value.try_into().unwrap_or(0);
            } else if entry.subject.ends_with(".latency_ms") {
                total_latency = entry.value.try_into().unwrap_or(0);
            }
        }

        let error_rate = if total_requests > 0 {
            (total_errors as f64 / total_requests as f64) * 100.0
        } else {
            0.0
        };

        println!(
            "  Service '{}': {} requests, {} errors ({:.2}%), {}ms avg latency",
            service, total_requests, total_errors, error_rate, total_latency
        );
    }

    println!("\n=== Partial Batch Fetch ===\n");

    // Fetch only error counters
    let error_subjects: Vec<String> = services
        .iter()
        .map(|s| format!("metrics.{}.errors", s))
        .collect();

    let mut error_entries = counter.get_multiple(error_subjects).await?;

    println!("Error counts by service:");
    while let Some(entry) = error_entries.next().await {
        let entry = entry?;
        println!("  {} = {}", entry.subject, entry.value);
    }

    Ok(())
}
