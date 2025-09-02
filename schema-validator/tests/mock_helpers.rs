use async_nats::{header::HeaderMap, Message};
use bytes::Bytes;
use schema_validator::registry::{CompatibilityPolicy, Format, Schema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::time::sleep;

// Define our own response types that can be serialized
#[derive(Serialize, Deserialize)]
pub struct AddResponse {
    pub version: u32,
    pub revision: u32,
}

#[derive(Serialize, Deserialize)]
pub struct GetResponse {
    pub schema: Schema,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateResponse {
    pub version: u32,
    pub revision: u32,
}

#[derive(Serialize, Deserialize)]
pub struct ListResponse {
    pub schemas: Vec<Schema>,
}

/// Helper to create a mock response message
pub fn create_response(payload: impl serde::Serialize) -> Message {
    let data = serde_json::to_vec(&payload).unwrap();
    Message {
        subject: "test".into(),
        reply: None,
        payload: Bytes::from(data),
        headers: None,
        status: None,
        description: None,
        length: 0,
    }
}

/// Helper to create an error response message
pub fn create_error_response(code: u32, message: &str) -> Message {
    let mut headers = HeaderMap::new();
    headers.insert("Nats-Service-Error-Code", code.to_string());
    headers.insert("Nats-Service-Error", message);

    Message {
        subject: "test".into(),
        reply: None,
        payload: Bytes::new(),
        headers: Some(headers),
        status: None,
        description: None,
        length: 0,
    }
}

/// Create a test schema
pub fn create_test_schema(name: &str, version: u32, revision: u32) -> Schema {
    Schema {
        name: name.to_string(),
        version,
        revision,
        time: "2024-01-01T00:00:00Z".to_string(),
        format: Format::JsonSchema,
        compatibility_policy: CompatibilityPolicy::Backward,
        description: "Test schema".to_string(),
        metadata: HashMap::new(),
        deleted: false,
        definition: r#"{"type": "object"}"#.to_string(),
    }
}

/// Create a test add response
pub fn create_add_response(version: u32, revision: u32) -> AddResponse {
    AddResponse { version, revision }
}

/// Create a test get response
pub fn create_get_response(name: &str, version: u32, revision: u32) -> GetResponse {
    GetResponse {
        schema: create_test_schema(name, version, revision),
    }
}

/// Create a test update response
pub fn create_update_response(version: u32, revision: u32) -> UpdateResponse {
    UpdateResponse { version, revision }
}

/// Create a test list response
pub fn create_list_response(schemas: Vec<Schema>) -> ListResponse {
    ListResponse { schemas }
}

/// Verify that a subject matches the expected pattern for ADD operation
pub fn verify_add_subject(subject: &str, account: &str, name: &str) -> bool {
    subject == format!("$SR.{}.v1.ADD.{}", account, name)
}

/// Verify that a subject matches the expected pattern for GET operation
pub fn verify_get_subject(
    subject: &str,
    account: &str,
    format: &str,
    version: &str,
    revision: &str,
    name: &str,
) -> bool {
    subject
        == format!(
            "$SR.{}.v1.GET.{}.{}.{}.{}",
            account, format, version, revision, name
        )
}

/// Verify that a subject matches the expected pattern for UPDATE operation
pub fn verify_update_subject(
    subject: &str,
    account: &str,
    format: &str,
    version: &str,
    revision: &str,
    name: &str,
) -> bool {
    subject
        == format!(
            "$SR.{}.v1.UPDATE.{}.{}.{}.{}",
            account, format, version, revision, name
        )
}

/// Verify that a subject matches the expected pattern for LIST operation
pub fn verify_list_subject(subject: &str, account: &str) -> bool {
    subject == format!("$SR.{}.v1.LIST", account)
}

/// Parse a subject to extract components for validation
pub fn parse_subject(subject: &str) -> Result<SubjectComponents, String> {
    let parts: Vec<&str> = subject.split('.').collect();

    if parts.len() < 4 {
        return Err("Invalid subject format".to_string());
    }

    if parts[0] != "$SR" {
        return Err("Subject must start with $SR".to_string());
    }

    let account = parts[1].to_string();
    let version_api = parts[2].to_string();
    let operation = parts[3].to_string();

    if version_api != "v1" {
        return Err("Invalid API version".to_string());
    }

    Ok(SubjectComponents {
        account,
        operation,
        remaining: parts[4..].iter().map(|s| s.to_string()).collect(),
    })
}

pub struct SubjectComponents {
    pub account: String,
    pub operation: String,
    pub remaining: Vec<String>,
}

/// Handle for managing a schema registry process
pub struct SchemaRegistryHandle {
    process: Option<Child>,
}

impl SchemaRegistryHandle {}

impl Drop for SchemaRegistryHandle {
    fn drop(&mut self) {
        if let Some(mut process) = self.process.take() {
            let _ = process.kill();
            let _ = process.wait();
        }
    }
}

/// Start a schema registry instance and return a handle to manage it
///
/// # Arguments
/// * `nats_url` - Optional NATS URL, defaults to "nats://localhost:4222"
/// * `port` - Optional port, defaults to 8080
pub async fn start_schema_registry(nats_url: Option<&str>) -> Result<SchemaRegistryHandle, String> {
    let nats_url = nats_url.unwrap_or("nats://localhost:4222");

    let mut command = Command::new("schema-registry");
    command
        .arg("server")
        .arg("--server")
        .arg(nats_url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    let process = command
        .spawn()
        .map_err(|e| format!("Failed to start schema-registry: {}", e))?;

    // Give the schema registry time to start up
    sleep(Duration::from_millis(500)).await;

    // Optionally verify it's running by checking if we can connect
    // For now, we'll just assume it started successfully after the delay

    Ok(SchemaRegistryHandle {
        process: Some(process),
    })
}

