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

//! Example demonstrating distributed counters with source tracking.
//!
//! This example shows how NATS JetStream counters can be used in a
//! distributed system with multiple regions, where counters from
//! individual regions are aggregated into a global view.

use async_nats::jetstream::stream::{Config, Source, SubjectTransform};
use nats_counters::CounterExt;
use num_bigint::BigInt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to NATS
    let client = async_nats::connect("localhost:4222").await?;
    let js = async_nats::jetstream::new(client);

    println!("=== Setting Up Regional Counters ===\n");

    // Create US-East region stream
    let us_east_config = Config {
        name: "METRICS_US_EAST".to_string(),
        subjects: vec!["metrics.us-east.>".to_string()],
        allow_message_counter: true,
        allow_direct: true,
        ..Default::default()
    };
    js.get_or_create_stream(us_east_config).await?;
    println!("Created US-East metrics stream");

    // Create US-West region stream
    let us_west_config = Config {
        name: "METRICS_US_WEST".to_string(),
        subjects: vec!["metrics.us-west.>".to_string()],
        allow_message_counter: true,
        allow_direct: true,
        ..Default::default()
    };
    js.get_or_create_stream(us_west_config).await?;
    println!("Created US-West metrics stream");

    // Create EU region stream
    let eu_config = Config {
        name: "METRICS_EU".to_string(),
        subjects: vec!["metrics.eu.>".to_string()],
        allow_message_counter: true,
        allow_direct: true,
        ..Default::default()
    };
    js.get_or_create_stream(eu_config).await?;
    println!("Created EU metrics stream");

    // Create Global aggregation stream that sources from all regions
    let global_config = Config {
        name: "METRICS_GLOBAL".to_string(),
        subjects: vec!["metrics.global.>".to_string()],
        allow_message_counter: true,
        allow_direct: true,
        sources: Some(vec![
            Source {
                name: "METRICS_US_EAST".to_string(),
                subject_transforms: vec![SubjectTransform {
                    source: "metrics.us-east.>".to_string(),
                    destination: "metrics.global.>".to_string(),
                }],
                ..Default::default()
            },
            Source {
                name: "METRICS_US_WEST".to_string(),
                subject_transforms: vec![SubjectTransform {
                    source: "metrics.us-west.>".to_string(),
                    destination: "metrics.global.>".to_string(),
                }],
                ..Default::default()
            },
            Source {
                name: "METRICS_EU".to_string(),
                subject_transforms: vec![SubjectTransform {
                    source: "metrics.eu.>".to_string(),
                    destination: "metrics.global.>".to_string(),
                }],
                ..Default::default()
            },
        ]),
        ..Default::default()
    };
    js.get_or_create_stream(global_config).await?;
    println!("Created Global aggregation stream with sources from all regions\n");

    // Get counters for each region
    let us_east_counter = js.get_counter("METRICS_US_EAST").await?;
    let us_west_counter = js.get_counter("METRICS_US_WEST").await?;
    let eu_counter = js.get_counter("METRICS_EU").await?;
    let global_counter = js.get_counter("METRICS_GLOBAL").await?;

    println!("=== Simulating Regional Traffic ===\n");

    // Simulate traffic in US-East
    println!("US-East region:");
    us_east_counter
        .add("metrics.us-east.api.requests", 1500)
        .await?;
    println!("  API requests: 1500");
    us_east_counter
        .add("metrics.us-east.api.errors", 23)
        .await?;
    println!("  API errors: 23");
    us_east_counter
        .add("metrics.us-east.db.queries", 3200)
        .await?;
    println!("  DB queries: 3200\n");

    // Simulate traffic in US-West
    println!("US-West region:");
    us_west_counter
        .add("metrics.us-west.api.requests", 2100)
        .await?;
    println!("  API requests: 2100");
    us_west_counter
        .add("metrics.us-west.api.errors", 15)
        .await?;
    println!("  API errors: 15");
    us_west_counter
        .add("metrics.us-west.db.queries", 4500)
        .await?;
    println!("  DB queries: 4500\n");

    // Simulate traffic in EU
    println!("EU region:");
    eu_counter.add("metrics.eu.api.requests", 800).await?;
    println!("  API requests: 800");
    eu_counter.add("metrics.eu.api.errors", 7).await?;
    println!("  API errors: 7");
    eu_counter.add("metrics.eu.db.queries", 1600).await?;
    println!("  DB queries: 1600\n");

    // Add some global-specific metrics (e.g., CDN stats)
    println!("Global metrics (CDN):");
    global_counter.add("metrics.global.cdn.hits", 50000).await?;
    println!("  CDN hits: 50000");
    global_counter
        .add("metrics.global.cdn.misses", 2000)
        .await?;
    println!("  CDN misses: 2000\n");

    // Wait for stream sourcing to propagate
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    println!("=== Global Aggregated View ===\n");

    // Get aggregated global counters
    let global_api_requests = global_counter.get("metrics.global.api.requests").await?;
    let global_api_errors = global_counter.get("metrics.global.api.errors").await?;
    let global_db_queries = global_counter.get("metrics.global.db.queries").await?;
    let global_cdn_hits = global_counter.get("metrics.global.cdn.hits").await?;
    let global_cdn_misses = global_counter.get("metrics.global.cdn.misses").await?;

    // Display aggregated values
    println!(
        "Total API Requests: {} (aggregated from all regions)",
        global_api_requests.value
    );
    println!(
        "Total API Errors: {} (aggregated from all regions)",
        global_api_errors.value
    );
    println!(
        "Total DB Queries: {} (aggregated from all regions)",
        global_db_queries.value
    );
    println!("CDN Hits: {} (global only)", global_cdn_hits.value);
    println!("CDN Misses: {} (global only)", global_cdn_misses.value);

    // Calculate global error rate
    let error_rate = calculate_error_rate(&global_api_errors.value, &global_api_requests.value);
    println!("\nGlobal API Error Rate: {:.2}%", error_rate);

    // Show source breakdown
    println!("\n=== Source Breakdown ===\n");

    println!("API Requests by region:");
    if !global_api_requests.sources.is_empty() {
        for (stream, subjects) in &global_api_requests.sources {
            for (subject, value) in subjects {
                let region = extract_region(stream);
                println!("  {}: {} (from {})", region, value, subject);
            }
        }
    }

    println!("\nAPI Errors by region:");
    if !global_api_errors.sources.is_empty() {
        for (stream, subjects) in &global_api_errors.sources {
            for (subject, value) in subjects {
                let region = extract_region(stream);
                println!("  {}: {} (from {})", region, value, subject);
            }
        }
    }

    println!("\nDB Queries by region:");
    if !global_db_queries.sources.is_empty() {
        for (stream, subjects) in &global_db_queries.sources {
            for (subject, value) in subjects {
                let region = extract_region(stream);
                println!("  {}: {} (from {})", region, value, subject);
            }
        }
    }

    println!("\n=== Demonstrating Counter Updates ===\n");

    // Show that counters continue to work with updates
    println!("Adding more requests to US-East...");
    let new_value = us_east_counter
        .add("metrics.us-east.api.requests", 500)
        .await?;
    println!("US-East API requests now: {}", new_value);

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let updated_global = global_counter.get("metrics.global.api.requests").await?;
    println!("Global API requests after update: {}", updated_global.value);

    Ok(())
}

fn calculate_error_rate(errors: &BigInt, requests: &BigInt) -> f64 {
    if requests == &BigInt::from(0) {
        return 0.0;
    }

    let errors_i64: i64 = errors.try_into().unwrap_or(0);
    let requests_i64: i64 = requests.try_into().unwrap_or(1);

    (errors_i64 as f64 / requests_i64 as f64) * 100.0
}

fn extract_region(stream_name: &str) -> &str {
    match stream_name {
        "METRICS_US_EAST" => "US-East",
        "METRICS_US_WEST" => "US-West",
        "METRICS_EU" => "EU",
        "METRICS_GLOBAL" => "Global",
        _ => stream_name,
    }
}
