mod mock_helpers;

use bytes::Bytes;
use futures::StreamExt;
use mock_helpers::*;
use schema_validator::registry::{
    AddErrorKind, AddRequest, CompatibilityPolicy, Format, GetErrorKind, GetRequest, Registry,
};
use std::collections::HashMap;
use std::time::Duration;

#[tokio::test]
async fn test_add_request_with_correct_subject() {
    let server = nats_server::run_server("tests/configs/config_with_mapping.conf");
    let client = async_nats::connect(server.client_url()).await.unwrap();

    // Set up a mock responder for ADD
    let mut sub = client
        .subscribe("$SR.mockacc.v1.ADD.test-schema")
        .await
        .unwrap();

    let client_clone = client.clone();
    tokio::spawn(async move {
        if let Some(msg) = sub.next().await {
            // Verify the request payload doesn't contain name
            let payload: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
            assert!(
                !payload.get("name").is_some(),
                "name should not be in payload"
            );
            assert_eq!(payload["format"], "jsonschema");

            // Send back a response
            let response = create_add_response(1, 1);
            let response_bytes = serde_json::to_vec(&response).unwrap();
            if let Some(reply) = msg.reply {
                client_clone
                    .publish(reply, response_bytes.into())
                    .await
                    .unwrap();
            }
        }
    });

    // Give the spawned task time to subscribe
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create registry with account
    let registry = Registry::new(client.clone());

    // Make the ADD request
    let request = AddRequest {
        name: "test-schema".to_string(),
        format: Format::JsonSchema,
        definition: r#"{"type": "object"}"#.to_string(),
        compatibility_policy: CompatibilityPolicy::Backward,
        description: "Test schema".to_string(),
        metadata: HashMap::new(),
    };

    let response = registry.add(request).await.unwrap();
    assert_eq!(response.version, 1);
    assert_eq!(response.revision, 1);
}

#[tokio::test]
async fn test_get_request_with_wildcards() {
    let server = nats_server::run_server("tests/configs/config_with_mapping.conf");
    let client = async_nats::connect(server.client_url()).await.unwrap();

    // Set up a mock responder for GET with wildcards
    let mut sub = client
        .subscribe("$SR.mockacc.v1.GET.*.*.*.*")
        .await
        .unwrap();

    let client_clone = client.clone();
    tokio::spawn(async move {
        if let Some(msg) = sub.next().await {
            // Verify subject format
            let subject_parts: Vec<&str> = msg.subject.split('.').collect();
            assert_eq!(subject_parts[0], "$SR");
            assert_eq!(subject_parts[1], "mockacc");
            assert_eq!(subject_parts[2], "v1");
            assert_eq!(subject_parts[3], "GET");
            assert_eq!(subject_parts[4], "*"); // format wildcard
            assert_eq!(subject_parts[5], "*"); // version wildcard
            assert_eq!(subject_parts[6], "*"); // revision wildcard
            assert_eq!(subject_parts[7], "test-schema");

            // Send back a response
            let response = create_get_response("test-schema", 1, 2);
            let response_bytes = serde_json::to_vec(&response).unwrap();
            if let Some(reply) = msg.reply {
                client_clone
                    .publish(reply, response_bytes.into())
                    .await
                    .unwrap();
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create registry with account
    let registry = Registry::new(client.clone());

    // Make the GET request with wildcards (version and revision = 0)
    let request = GetRequest {
        name: "test-schema".to_string(),
        version: 0,  // Should be converted to "*"
        revision: 0, // Should be converted to "*"
        format: None,
    };

    let response = registry.get(request).await.unwrap();
    assert_eq!(response.schema.name, "test-schema");
    assert_eq!(response.schema.version, 1);
    assert_eq!(response.schema.revision, 2);
}

#[tokio::test]
async fn test_error_handling() {
    let server = nats_server::run_server("tests/configs/config_with_mapping.conf");
    let client = async_nats::connect(server.client_url()).await.unwrap();

    // Test error code 11000 - Invalid Name
    let mut sub_invalid = client.subscribe("$SR.mockacc.v1.ADD.>").await.unwrap();
    let client_c1 = client.clone();
    tokio::spawn(async move {
        if let Some(msg) = sub_invalid.next().await {
            println!("Received ADD request for invalid name");
            if let Some(reply) = msg.reply {
                let mut headers = async_nats::header::HeaderMap::new();
                headers.insert("Nats-Service-Error-Code", "11000");
                headers.insert("Nats-Service-Error", "Invalid name");
                client_c1
                    .publish_with_headers(reply, headers, Bytes::new())
                    .await
                    .unwrap();
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    let registry = Registry::new(client.clone());

    let request = AddRequest {
        name: "invalid".to_string(),
        format: Format::JsonSchema,
        definition: "{}".to_string(),
        compatibility_policy: CompatibilityPolicy::None,
        description: "".to_string(),
        metadata: HashMap::new(),
    };

    let result = registry.add(request).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), AddErrorKind::InvalidName);

    // Test error code 11002 - Schema Not Found
    let mut sub_notfound = client
        .subscribe("$SR.mockacc.v1.GET.*.*.*.notfound")
        .await
        .unwrap();
    let client_c2 = client.clone();
    tokio::spawn(async move {
        if let Some(msg) = sub_notfound.next().await {
            if let Some(reply) = msg.reply {
                let mut headers = async_nats::header::HeaderMap::new();
                headers.insert("Nats-Service-Error-Code", "11002");
                headers.insert("Nats-Service-Error", "Schema not found");
                client_c2
                    .publish_with_headers(reply, headers, Bytes::new())
                    .await
                    .unwrap();
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let get_request = GetRequest {
        name: "notfound".to_string(),
        version: 0,
        revision: 0,
        format: None,
    };

    let result = registry.get(get_request).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), GetErrorKind::SchemaNotFound);
}

