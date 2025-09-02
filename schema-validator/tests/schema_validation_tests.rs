mod mock_helpers;

use bytes::Bytes;
use futures::{pin_mut, StreamExt};
use mock_helpers::*;
use schema_validator::{
    client_ext::{SchemaExt, ValidateStreamExt},
    registry::{Format, GetRequest, Registry, ValidateErrorKind},
};
use std::sync::Arc;

#[tokio::test]
async fn test_message_validation_with_schema_headers() {
    let server = nats_server::run_server("tests/configs/config_with_mapping.conf");
    let client = async_nats::connect(server.client_url()).await.unwrap();

    // Set up mock responder for GET schema
    let mut sub_get = client
        .subscribe("$SR.mockacc.v1.GET.*.*.*.*")
        .await
        .unwrap();
    let client_clone = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub_get.next().await {
            let schema_json = r#"{
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer", "minimum": 0}
                },
                "required": ["name", "age"]
            }"#;

            let mut schema = create_test_schema("person", 1, 1);
            schema.definition = schema_json.to_string();

            let response = mock_helpers::GetResponse { schema };
            let response_bytes = serde_json::to_vec(&response).unwrap();
            if let Some(reply) = msg.reply {
                client_clone
                    .publish(reply, response_bytes.into())
                    .await
                    .unwrap();
            }
        }
    });

    let mut registry = Registry::new(client.clone());

    // Create a message with schema headers
    let mut headers = async_nats::header::HeaderMap::new();
    headers.insert("Nats-Schema", "person");
    headers.insert("Nats-Schema-Version", "1");

    // Valid payload
    let valid_payload = r#"{"name": "John", "age": 30}"#;
    let valid_message = async_nats::Message {
        subject: "test.subject".into(),
        reply: None,
        payload: Bytes::from(valid_payload),
        headers: Some(headers.clone()),
        status: None,
        description: None,
        length: 0,
    };

    // Validate valid message
    let result = registry.validate_message(valid_message).await;
    assert!(result.is_ok());

    // Invalid payload (negative age)
    let invalid_payload = r#"{"name": "John", "age": -5}"#;
    let invalid_message = async_nats::Message {
        subject: "test.subject".into(),
        reply: None,
        payload: Bytes::from(invalid_payload),
        headers: Some(headers),
        status: None,
        description: None,
        length: 0,
    };

    // Validate invalid message
    let result = registry.validate_message(invalid_message).await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().kind(),
        ValidateErrorKind::ValidationFailed
    );
}

#[tokio::test]
async fn test_message_without_schema_headers() {
    let server = nats_server::run_basic_server();
    let client = async_nats::connect(server.client_url()).await.unwrap();

    let mut registry = Registry::new(client.clone());

    // Create a message without schema headers
    let message = async_nats::Message {
        subject: "test.subject".into(),
        reply: None,
        payload: Bytes::from(r#"{"data": "test"}"#),
        headers: None,
        status: None,
        description: None,
        length: 0,
    };

    // Should fail with missing schema headers
    let result = registry.validate_message(message).await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().kind(),
        ValidateErrorKind::MissingSchemaHeaders
    );
}

#[tokio::test]
async fn test_publish_with_schema_extension() {
    let server = nats_server::run_basic_server();
    let client = async_nats::connect(server.client_url()).await.unwrap();

    // Subscribe to test subject
    let mut sub = client.subscribe("test.schema.subject").await.unwrap();

    // Publish with schema headers using extension
    let data = r#"{"name": "test", "value": 123}"#;
    client
        .publish_with_schema("test.schema.subject", "my-schema", 2, Bytes::from(data))
        .await
        .unwrap();

    // Receive and verify message
    let msg = sub.next().await.unwrap();
    assert_eq!(
        msg.headers
            .as_ref()
            .unwrap()
            .get("Nats-Schema")
            .unwrap()
            .as_str(),
        "my-schema"
    );
    assert_eq!(
        msg.headers
            .as_ref()
            .unwrap()
            .get("Nats-Schema-Version")
            .unwrap()
            .as_str(),
        "2"
    );
    assert_eq!(msg.payload, Bytes::from(data));
}

#[tokio::test]
async fn test_stream_validation_extension() {
    let server = nats_server::run_server("tests/configs/config_with_mapping.conf");
    let client = async_nats::connect(server.client_url()).await.unwrap();

    // Set up mock responder for schema
    let mut sub_get = client
        .subscribe("$SR.mockacc.v1.GET.*.*.*.*")
        .await
        .unwrap();
    let client_clone = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub_get.next().await {
            let schema_json = r#"{
                "type": "object",
                "properties": {
                    "value": {"type": "number", "minimum": 0}
                },
                "required": ["value"]
            }"#;

            let mut schema = create_test_schema("data", 1, 1);
            schema.definition = schema_json.to_string();

            let response = mock_helpers::GetResponse { schema };
            let response_bytes = serde_json::to_vec(&response).unwrap();
            if let Some(reply) = msg.reply {
                client_clone
                    .publish(reply, response_bytes.into())
                    .await
                    .unwrap();
            }
        }
    });

    let registry = Arc::new(tokio::sync::Mutex::new(Registry::new(client.clone())));

    // Create subscription
    let sub = client.subscribe("test.stream").await.unwrap();

    // Apply validation extension
    let validated_stream = sub.validated(registry).take(2);
    pin_mut!(validated_stream);

    // Publish valid message
    let mut headers = async_nats::header::HeaderMap::new();
    headers.insert("Nats-Schema", "data");
    headers.insert("Nats-Schema-Version", "1");

    client
        .publish_with_headers(
            "test.stream",
            headers.clone(),
            Bytes::from(r#"{"value": 10}"#),
        )
        .await
        .unwrap();

    // Publish invalid message
    client
        .publish_with_headers("test.stream", headers, Bytes::from(r#"{"value": -5}"#))
        .await
        .unwrap();

    // First message should be valid
    let result1 = validated_stream.next().await.unwrap();
    assert!(result1.is_ok());

    // Second message should fail validation
    let result2 = validated_stream.next().await.unwrap();
    assert!(result2.is_err());
    assert_eq!(
        result2.unwrap_err().kind(),
        ValidateErrorKind::ValidationFailed
    );
}

#[tokio::test]
async fn test_validate_with_different_formats() {
    let server = nats_server::run_basic_server();
    let client = async_nats::connect(server.client_url()).await.unwrap();

    let mut registry = Registry::new(client.clone());

    // Set up mock responder that returns non-JSON schema format
    let mut sub_get = client
        .subscribe("$SR.mockacc.v1.GET.*.*.*.*")
        .await
        .unwrap();
    let client_clone = client.clone();
    tokio::spawn(async move {
        if let Some(msg) = sub_get.next().await {
            let mut schema = create_test_schema("avro-schema", 1, 1);
            schema.format = Format::Avro;
            schema.definition = "avro definition".to_string();

            let response = mock_helpers::GetResponse { schema };
            let response_bytes = serde_json::to_vec(&response).unwrap();
            if let Some(reply) = msg.reply {
                client_clone
                    .publish(reply, response_bytes.into())
                    .await
                    .unwrap();
            }
        }
    });

    // Try to validate with non-JSON schema format
    // Currently only JSON Schema is supported, so this should panic
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async { registry.validate("avro-schema", 1, Bytes::from("{}")).await })
    }));

    // Should panic with "Only JSON Schema is supported"
    assert!(result.is_err());
}

#[tokio::test]
async fn test_schema_cache_key_includes_revision() {
    let server = nats_server::run_server("tests/configs/config_with_mapping.conf");
    let client = async_nats::connect(server.client_url()).await.unwrap();

    let call_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let call_count_clone = call_count.clone();

    // Set up mock responder
    let mut sub_get = client.subscribe("$SR.mockacc.v1.GET.>").await.unwrap();
    let client_clone = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub_get.next().await {
            call_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            // Parse subject to determine revision
            let parts: Vec<&str> = msg.subject.split('.').collect();
            let revision_str = parts[6];
            let revision = if revision_str == "*" {
                1
            } else {
                revision_str.parse().unwrap()
            };

            let schema = create_test_schema("test", 1, revision);
            let response = mock_helpers::GetResponse { schema };
            let response_bytes = serde_json::to_vec(&response).unwrap();
            if let Some(reply) = msg.reply {
                client_clone
                    .publish(reply, response_bytes.into())
                    .await
                    .unwrap();
            }
        }
    });

    let mut registry = Registry::new(client.clone());

    // Get schema with revision 0 (wildcard)
    let schema1 = registry.get_schema("test".to_string(), 1).await.unwrap();
    assert_eq!(schema1.revision, 1);
    assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);

    // Get same schema again - should use cache
    let schema2 = registry.get_schema("test".to_string(), 1).await.unwrap();
    assert_eq!(schema2.revision, 1);
    assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);

    // Get with explicit revision should make new call
    let get_req = GetRequest {
        name: "test".to_string(),
        version: 1,
        revision: 2,
        format: None,
    };
    let response = registry.get(get_req).await.unwrap();
    assert_eq!(response.schema.revision, 2);
    assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_invalid_json_schema_definition() {
    let server = nats_server::run_basic_server();
    let client = async_nats::connect(server.client_url()).await.unwrap();

    // Set up mock responder with invalid JSON schema
    let mut sub_get = client
        .subscribe("$SR.mockacc.v1.GET.*.*.*.*")
        .await
        .unwrap();
    let client_clone = client.clone();
    tokio::spawn(async move {
        if let Some(msg) = sub_get.next().await {
            let mut schema = create_test_schema("bad-schema", 1, 1);
            schema.definition = "not valid json".to_string();

            let response = mock_helpers::GetResponse { schema };
            let response_bytes = serde_json::to_vec(&response).unwrap();
            if let Some(reply) = msg.reply {
                client_clone
                    .publish(reply, response_bytes.into())
                    .await
                    .unwrap();
            }
        }
    });

    let mut registry = Registry::new(client.clone());

    // Try to validate with invalid schema definition
    let result = registry.validate("bad-schema", 1, Bytes::from("{}")).await;
    assert!(result.is_err());
    // Should fail during JSON parsing of the schema definition
}
