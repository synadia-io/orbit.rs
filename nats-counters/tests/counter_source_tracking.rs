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

mod common;

use async_nats::jetstream::stream::{Config, Source, SubjectTransform};
use nats_counters::CounterExt;
use num_bigint::BigInt;

#[tokio::test]
async fn test_counter_source_tracking() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    // Create ES (Spain) stream
    let es_config = Config {
        name: "COUNT_ES".to_string(),
        subjects: vec!["count.es.*".to_string()],
        allow_message_counter: true,
        allow_direct: true,
        ..Default::default()
    };
    js.create_stream(es_config)
        .await
        .expect("Failed to create ES stream");

    // Create PL (Poland) stream
    let pl_config = Config {
        name: "COUNT_PL".to_string(),
        subjects: vec!["count.pl.*".to_string()],
        allow_message_counter: true,
        allow_direct: true,
        ..Default::default()
    };
    js.create_stream(pl_config)
        .await
        .expect("Failed to create PL stream");

    // Create EU (Europe) stream aggregating ES and PL
    let eu_config = Config {
        name: "COUNT_EU".to_string(),
        subjects: vec!["count.eu.*".to_string()],
        allow_message_counter: true,
        allow_direct: true,
        sources: Some(vec![
            Source {
                name: "COUNT_ES".to_string(),
                subject_transforms: vec![SubjectTransform {
                    source: "count.es.>".to_string(),
                    destination: "count.eu.>".to_string(),
                }],
                ..Default::default()
            },
            Source {
                name: "COUNT_PL".to_string(),
                subject_transforms: vec![SubjectTransform {
                    source: "count.pl.>".to_string(),
                    destination: "count.eu.>".to_string(),
                }],
                ..Default::default()
            },
        ]),
        ..Default::default()
    };
    js.create_stream(eu_config)
        .await
        .expect("Failed to create EU stream");

    // Get counters for each region
    let es_counter = js
        .get_counter("COUNT_ES")
        .await
        .expect("Failed to get ES counter");

    let pl_counter = js
        .get_counter("COUNT_PL")
        .await
        .expect("Failed to get PL counter");

    let eu_counter = js
        .get_counter("COUNT_EU")
        .await
        .expect("Failed to get EU counter");

    // Add values to Spain stream
    es_counter.add("count.es.requests", 100).await.unwrap();
    es_counter.add("count.es.errors", 5).await.unwrap();

    // Add values to Poland stream
    pl_counter.add("count.pl.requests", 150).await.unwrap();
    pl_counter.add("count.pl.errors", 3).await.unwrap();

    // Add direct values to EU stream
    eu_counter.add("count.eu.requests", 50).await.unwrap();
    eu_counter.add("count.eu.errors", 2).await.unwrap();

    // Wait for source aggregation to propagate
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Get aggregated EU counter values with sources
    let eu_requests = eu_counter
        .get("count.eu.requests")
        .await
        .expect("Failed to get EU requests");

    let eu_errors = eu_counter
        .get("count.eu.errors")
        .await
        .expect("Failed to get EU errors");

    // Debug: print actual values to understand aggregation
    println!("EU requests value: {}", eu_requests.value);
    println!("EU requests sources: {:?}", eu_requests.sources);
    println!("EU errors value: {}", eu_errors.value);
    println!("EU errors sources: {:?}", eu_errors.sources);

    // With stream sourcing, the EU stream receives messages from ES and PL streams
    // transformed to count.eu.* subjects, PLUS the direct publishes
    // So EU requests should be: 100 (from ES) + 150 (from PL) + 50 (direct) = 300
    assert_eq!(eu_requests.value, BigInt::from(300));
    assert_eq!(eu_errors.value, BigInt::from(10));

    // Check source tracking - should show contributions from each source stream
    assert!(
        !eu_requests.sources.is_empty(),
        "Sources should be populated for aggregated stream"
    );

    // Verify expected sources for requests (ES and PL should be tracked)
    assert_eq!(
        eu_requests.sources.len(),
        2,
        "Expected 2 source streams (COUNT_ES, COUNT_PL)"
    );

    // Check COUNT_ES source
    assert!(
        eu_requests.sources.contains_key("COUNT_ES"),
        "COUNT_ES should be in sources"
    );
    if let Some(es_sources) = eu_requests.sources.get("COUNT_ES") {
        assert_eq!(
            es_sources.get("count.es.requests"),
            Some(&BigInt::from(100)),
            "COUNT_ES should contribute 100 requests"
        );
    }

    // Check COUNT_PL source
    assert!(
        eu_requests.sources.contains_key("COUNT_PL"),
        "COUNT_PL should be in sources"
    );
    if let Some(pl_sources) = eu_requests.sources.get("COUNT_PL") {
        assert_eq!(
            pl_sources.get("count.pl.requests"),
            Some(&BigInt::from(150)),
            "COUNT_PL should contribute 150 requests"
        );
    }

    // Verify expected sources for errors
    assert_eq!(
        eu_errors.sources.len(),
        2,
        "Expected 2 source streams for errors"
    );

    if let Some(es_sources) = eu_errors.sources.get("COUNT_ES") {
        assert_eq!(
            es_sources.get("count.es.errors"),
            Some(&BigInt::from(5)),
            "COUNT_ES should contribute 5 errors"
        );
    }

    if let Some(pl_sources) = eu_errors.sources.get("COUNT_PL") {
        assert_eq!(
            pl_sources.get("count.pl.errors"),
            Some(&BigInt::from(3)),
            "COUNT_PL should contribute 3 errors"
        );
    }
}

#[tokio::test]
async fn test_counter_sources_single_stream() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    // Create a single stream
    let _stream = common::create_counter_stream(&js, "SINGLE", vec!["single.*".to_string()]).await;

    let counter = js
        .get_counter("SINGLE")
        .await
        .expect("Failed to get counter");

    // Add multiple values to the same subject
    counter.add("single.value", 10).await.unwrap();
    counter.add("single.value", 20).await.unwrap();
    counter.add("single.value", 30).await.unwrap();

    // Get the entry with sources
    let entry = counter
        .get("single.value")
        .await
        .expect("Failed to get entry");

    // Should have final value of 60
    assert_eq!(entry.value, BigInt::from(60));

    // Should have last increment of 30
    assert_eq!(entry.increment, Some(BigInt::from(30)));

    // Debug: print what sources we actually have
    println!("Sources map: {:?}", entry.sources);

    // For a single stream, sources may be empty or contain the stream itself
    // The Go implementation shows sources are populated by the server
    // when there are actual stream sources configured
    if !entry.sources.is_empty() {
        assert!(entry.sources.contains_key("SINGLE"));
        if let Some(sources) = entry.sources.get("SINGLE") {
            assert_eq!(sources.get("single.value"), Some(&BigInt::from(60)));
        }
    }
}

#[tokio::test]
async fn test_counter_three_level_source_aggregation() {
    let server = common::start_jetstream_server();
    let js = common::create_jetstream_context(&server.client_url()).await;

    // Create ES (Spain) stream
    let es_config = Config {
        name: "COUNT_ES".to_string(),
        subjects: vec!["count.es.*".to_string()],
        allow_message_counter: true,
        allow_direct: true,
        ..Default::default()
    };
    js.create_stream(es_config)
        .await
        .expect("Failed to create ES stream");

    // Create PL (Poland) stream
    let pl_config = Config {
        name: "COUNT_PL".to_string(),
        subjects: vec!["count.pl.*".to_string()],
        allow_message_counter: true,
        allow_direct: true,
        ..Default::default()
    };
    js.create_stream(pl_config)
        .await
        .expect("Failed to create PL stream");

    // Create EU (Europe) stream aggregating ES and PL
    let eu_config = Config {
        name: "COUNT_EU".to_string(),
        subjects: vec!["count.eu.*".to_string()],
        allow_message_counter: true,
        allow_direct: true,
        sources: Some(vec![
            Source {
                name: "COUNT_ES".to_string(),
                subject_transforms: vec![SubjectTransform {
                    source: "count.es.>".to_string(),
                    destination: "count.eu.>".to_string(),
                }],
                ..Default::default()
            },
            Source {
                name: "COUNT_PL".to_string(),
                subject_transforms: vec![SubjectTransform {
                    source: "count.pl.>".to_string(),
                    destination: "count.eu.>".to_string(),
                }],
                ..Default::default()
            },
        ]),
        ..Default::default()
    };
    js.create_stream(eu_config)
        .await
        .expect("Failed to create EU stream");

    // Create GLOBAL stream aggregating from EU
    let global_config = Config {
        name: "COUNT_GLOBAL".to_string(),
        subjects: vec!["count.global.*".to_string()],
        allow_message_counter: true,
        allow_direct: true,
        sources: Some(vec![Source {
            name: "COUNT_EU".to_string(),
            subject_transforms: vec![SubjectTransform {
                source: "count.eu.>".to_string(),
                destination: "count.global.>".to_string(),
            }],
            ..Default::default()
        }]),
        ..Default::default()
    };
    js.create_stream(global_config)
        .await
        .expect("Failed to create GLOBAL stream");

    // Get counters for each level
    let es_counter = js
        .get_counter("COUNT_ES")
        .await
        .expect("Failed to get ES counter");

    let pl_counter = js
        .get_counter("COUNT_PL")
        .await
        .expect("Failed to get PL counter");

    let eu_counter = js
        .get_counter("COUNT_EU")
        .await
        .expect("Failed to get EU counter");

    let global_counter = js
        .get_counter("COUNT_GLOBAL")
        .await
        .expect("Failed to get GLOBAL counter");

    // Add values to regional streams (Level 1)
    println!("Adding to regional streams (ES, PL)...");
    es_counter
        .add("count.es.hits", 100)
        .await
        .expect("Failed to add ES hits");
    es_counter
        .add("count.es.views", 200)
        .await
        .expect("Failed to add ES views");

    pl_counter
        .add("count.pl.hits", 150)
        .await
        .expect("Failed to add PL hits");
    pl_counter
        .add("count.pl.views", 50)
        .await
        .expect("Failed to add PL views");

    // Wait for aggregation to EU level
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Verify EU level aggregation (Level 2)
    println!("Checking EU aggregation (Level 2)...");
    let eu_hits = eu_counter
        .get("count.eu.hits")
        .await
        .expect("Failed to get EU hits");
    let eu_views = eu_counter
        .get("count.eu.views")
        .await
        .expect("Failed to get EU views");

    // EU should aggregate ES + PL
    assert_eq!(
        eu_hits.value,
        BigInt::from(250),
        "EU hits should be 100 + 150 = 250"
    );
    assert_eq!(
        eu_views.value,
        BigInt::from(250),
        "EU views should be 200 + 50 = 250"
    );

    // Check EU sources - should show ES and PL contributions
    println!("EU hits sources: {:?}", eu_hits.sources);
    println!("EU views sources: {:?}", eu_views.sources);

    assert!(
        !eu_hits.sources.is_empty(),
        "EU hits sources should be populated"
    );
    assert_eq!(
        eu_hits.sources.len(),
        2,
        "EU hits should have 2 source streams"
    );

    // Verify COUNT_ES contribution to hits
    assert!(
        eu_hits.sources.contains_key("COUNT_ES"),
        "EU hits should track COUNT_ES"
    );
    if let Some(es_sources) = eu_hits.sources.get("COUNT_ES") {
        assert_eq!(
            es_sources.get("count.es.hits"),
            Some(&BigInt::from(100)),
            "COUNT_ES should contribute 100 hits"
        );
    }

    // Verify COUNT_PL contribution to hits
    assert!(
        eu_hits.sources.contains_key("COUNT_PL"),
        "EU hits should track COUNT_PL"
    );
    if let Some(pl_sources) = eu_hits.sources.get("COUNT_PL") {
        assert_eq!(
            pl_sources.get("count.pl.hits"),
            Some(&BigInt::from(150)),
            "COUNT_PL should contribute 150 hits"
        );
    }

    // Verify sources for views
    assert_eq!(
        eu_views.sources.len(),
        2,
        "EU views should have 2 source streams"
    );
    if let Some(es_sources) = eu_views.sources.get("COUNT_ES") {
        assert_eq!(
            es_sources.get("count.es.views"),
            Some(&BigInt::from(200)),
            "COUNT_ES should contribute 200 views"
        );
    }
    if let Some(pl_sources) = eu_views.sources.get("COUNT_PL") {
        assert_eq!(
            pl_sources.get("count.pl.views"),
            Some(&BigInt::from(50)),
            "COUNT_PL should contribute 50 views"
        );
    }

    // Wait for aggregation to GLOBAL level
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Verify GLOBAL level aggregation (Level 3)
    println!("Checking GLOBAL aggregation (Level 3)...");
    let global_hits = global_counter
        .get("count.global.hits")
        .await
        .expect("Failed to get GLOBAL hits");
    let global_views = global_counter
        .get("count.global.views")
        .await
        .expect("Failed to get GLOBAL views");

    // GLOBAL should have same values as EU (since EU is the only source)
    assert_eq!(
        global_hits.value,
        BigInt::from(250),
        "GLOBAL hits should match EU: 250"
    );
    assert_eq!(
        global_views.value,
        BigInt::from(250),
        "GLOBAL views should match EU: 250"
    );

    // Check GLOBAL sources - should show EU contribution
    println!("GLOBAL hits sources: {:?}", global_hits.sources);
    println!("GLOBAL views sources: {:?}", global_views.sources);

    assert!(
        !global_hits.sources.is_empty(),
        "GLOBAL hits sources should be populated"
    );
    assert_eq!(
        global_hits.sources.len(),
        1,
        "GLOBAL should have 1 source stream (COUNT_EU)"
    );

    // Verify COUNT_EU contribution
    assert!(
        global_hits.sources.contains_key("COUNT_EU"),
        "GLOBAL should track COUNT_EU"
    );
    if let Some(eu_source) = global_hits.sources.get("COUNT_EU") {
        assert_eq!(
            eu_source.get("count.eu.hits"),
            Some(&BigInt::from(250)),
            "COUNT_EU should contribute 250 hits to GLOBAL"
        );
    }

    // Verify GLOBAL views sources
    assert_eq!(
        global_views.sources.len(),
        1,
        "GLOBAL views should have 1 source stream"
    );
    if let Some(eu_source) = global_views.sources.get("COUNT_EU") {
        assert_eq!(
            eu_source.get("count.eu.views"),
            Some(&BigInt::from(250)),
            "COUNT_EU should contribute 250 views to GLOBAL"
        );
    }

    // Add more values at regional level and verify propagation
    println!("Adding more values to test propagation...");
    es_counter
        .add("count.es.hits", 50)
        .await
        .expect("Failed to add more ES hits");

    // Wait for propagation through both levels
    tokio::time::sleep(tokio::time::Duration::from_millis(700)).await;

    // Check that EU updated
    let eu_hits_updated = eu_counter.load("count.eu.hits").await.unwrap();
    assert_eq!(
        eu_hits_updated,
        BigInt::from(300),
        "EU hits should update to 300 (250 + 50)"
    );

    // Check that GLOBAL updated
    let global_hits_updated = global_counter.load("count.global.hits").await.unwrap();
    assert_eq!(
        global_hits_updated,
        BigInt::from(300),
        "GLOBAL hits should propagate to 300"
    );

    println!("Three-level aggregation test completed successfully!");
}
