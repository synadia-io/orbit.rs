use std::{collections::HashMap, fmt::Display};

use async_nats::{self};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::errors::Error;

#[derive(Debug, Serialize)]
pub struct AddRequest {
    pub name: String,
    pub format: Format,
    pub definition: String,
    #[serde(rename = "compat_policy")]
    pub compatibility_policy: CompatibilityPolicy,
    pub description: String,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct AddResponse {
    pub version: u32,
}

#[derive(Debug, Serialize)]
pub struct GetRequest {
    pub name: String,
    pub version: u32,
}

#[derive(Debug, Deserialize)]
pub struct GetResponse {
    pub schema: Schema,
}

#[derive(Debug, Serialize)]
pub struct UpdateRequest {
    pub name: String,
    pub definition: Format,
    pub description: String,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct ListRequest {
    filter: String,
}

#[derive(Debug, Deserialize)]
pub struct ListResponse {
    #[allow(dead_code)]
    schemas: Vec<Schema>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Schema {
    pub name: String,
    pub version: u32,
    #[serde(default)]
    pub revision: Option<u32>,
    pub time: String,
    pub format: Format,
    #[serde(rename = "compat_policy")]
    pub compatibility_policy: CompatibilityPolicy,
    pub description: String,
    #[serde(default)]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(default)]
    pub deleted: bool,
    pub definition: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Format {
    #[serde(rename = "jsonschema")]
    JsonSchema,
    #[serde(rename = "avro")]
    Avro,
    #[serde(rename = "protobuf")]
    Protobuf,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CompatibilityPolicy {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "backward")]
    Backward,
    #[serde(rename = "forward")]
    Forward,
    #[serde(rename = "full")]
    Full,
}

pub struct Registry {
    client: async_nats::Client,
    cache: HashMap<String, Schema>,
}

impl Registry {
    pub fn new(client: async_nats::Client) -> Self {
        Registry {
            client,
            cache: HashMap::new(),
        }
    }

    // This should work when called multiple times.
    pub async fn add(&self, request: AddRequest) -> Result<AddResponse, AddError> {
        let request_bytes = serde_json::to_vec(&request).map(Bytes::from)?;
        let response = self
            .client
            .request(format!("$SR.v1.ADD.{}", request.name), request_bytes)
            .await?;

        let result = serde_json::from_slice(&response.payload)?;
        Ok(result)
    }

    pub async fn get(&self, request: GetRequest) -> Result<GetResponse, GetError> {
        let request_bytes = serde_json::to_vec(&request).map(Bytes::from)?;
        let subject = format!("$SR.v1.GET.{}", request.name);
        
        println!("Sending NATS request to subject: {}", subject);
        println!("Request payload: {}", String::from_utf8_lossy(&request_bytes));
        
        let response = self
            .client
            .request(subject, request_bytes)
            .await?;
            
        let response_text = String::from_utf8_lossy(&response.payload);
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&response_text) {
            println!("Received response payload:");
            println!("{}", serde_json::to_string_pretty(&parsed).unwrap_or_else(|_| response_text.to_string()));
        } else {
            println!("Received response payload: {}", response_text);
        }
        
        let result = serde_json::from_slice(&response.payload)?;
        Ok(result)
    }

    pub async fn update(&self, request: AddRequest) -> Result<AddResponse, AddError> {
        let request_bytes = serde_json::to_vec(&request).map(Bytes::from)?;
        let response = self
            .client
            .request(format!("$SR.v1.UPDATE.{}", request.name), request_bytes)
            .await?;
        let result = serde_json::from_slice(&response.payload)?;
        Ok(result)
    }

    pub async fn list(&self, request: ListRequest) -> Result<ListResponse, GetError> {
        let request_bytes = serde_json::to_vec(&request).map(Bytes::from)?;
        let response = self.client.request("$SR.v1.LIST", request_bytes).await?;
        let result = serde_json::from_slice(&response.payload)?;
        Ok(result)
    }

    pub async fn validate_message(
        &mut self,
        message: async_nats::Message,
    ) -> Result<async_nats::Message, ValidateError> {
        let schema_id = message
            .headers
            .as_ref()
            .and_then(|h| h.get("Nats-Schema"))
            .ok_or_else(|| ValidateError::new(ValidateErrorKind::MissingSchemaHeaders))?;

        self.validate(
            schema_id.as_str(),
            message.payload.clone(),
        )
        .await?;

        Ok(message)
    }

    pub async fn get_schema(&mut self, schema_id: String) -> Result<Schema, ValidateError> {
        if let Some(schema) = self.cache.get(&schema_id).cloned() {
            println!("Cache hit for schema: {}", schema_id);
            return Ok(schema);
        }

        println!("Cache miss for schema: {}", schema_id);
        println!("Requesting schema from registry={}", schema_id);
        
        let schema = self.get(GetRequest { name: schema_id.clone(), version: 1 }).await
            .map_err(|e| {
                println!("Failed to get schema '{}' from registry: {:?}", schema_id, e);
                e
            })?;
        self.cache.insert(
            schema_id.clone(),
            schema.schema.clone(),
        );
        Ok(schema.schema)
    }

    pub async fn validate(
        &mut self,
        schema_id: &str,
        payload: Bytes,
    ) -> Result<(), ValidateError> {
        let schema = self.get_schema(schema_id.to_string()).await?;

        let definition = schema.definition;
        let definition = serde_json::from_str(&definition)
            .map_err(|err| ValidateError::with_source(ValidateErrorKind::Other, err))?;

        match schema.format {
            Format::JsonSchema => {
                let validator = jsonschema::draft202012::new(&definition).map_err(|err| {
                    ValidateError::with_source(ValidateErrorKind::InvalidSchema, err)
                })?;
                let value = serde_json::from_slice(payload.as_ref()).map_err(|err| {
                    ValidateError::with_source(ValidateErrorKind::Deserialization, err)
                })?;

                validator.validate(&value).map_err(|err| {
                    ValidateError::with_source(ValidateErrorKind::ValidationFailed, err.to_string())
                })?;

                return Ok(());
            }
            _ => unimplemented!("Only JSON Schema is supported"),
        }
    }
}

pub type ValidateError = Error<ValidateErrorKind>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValidateErrorKind {
    Deserialization,
    InvalidSchema,
    MissingSchemaHeaders,
    ValidationFailed,
    SchemaRegistryNotFound,
    TimedOut,
    Other,
}

impl Display for ValidateErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidateErrorKind::Deserialization => write!(f, "Deserialization error"),
            ValidateErrorKind::InvalidSchema => write!(f, "Invalid schema"),
            ValidateErrorKind::MissingSchemaHeaders => write!(f, "Missing schema headers"),
            ValidateErrorKind::ValidationFailed => write!(f, "Validation failed"),
            ValidateErrorKind::SchemaRegistryNotFound => write!(f, "Schema registry not found"),
            ValidateErrorKind::TimedOut => write!(f, "Timed out"),
            ValidateErrorKind::Other => write!(f, "Other error"),
        }
    }
}

impl From<GetError> for ValidateError {
    fn from(err: GetError) -> Self {
        match err.kind() {
            GetErrorKind::SchemaRegistryNotFound => {
                ValidateError::new(ValidateErrorKind::SchemaRegistryNotFound)
            }
            GetErrorKind::TimedOut => ValidateError::new(ValidateErrorKind::TimedOut),
            GetErrorKind::Other => ValidateError::with_source(ValidateErrorKind::Other, err),
        }
    }
}

pub type AddError = Error<AddErrorKind>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AddErrorKind {
    SchemaRegistryNotFound,
    TimedOut,
    Other,
}

impl Display for AddErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AddErrorKind::SchemaRegistryNotFound => write!(f, "Schema registry not found"),
            AddErrorKind::TimedOut => write!(f, "Timed out"),
            AddErrorKind::Other => write!(f, "Other error"),
        }
    }
}

impl From<async_nats::RequestError> for AddError {
    fn from(err: async_nats::RequestError) -> Self {
        match err.kind() {
            async_nats::RequestErrorKind::TimedOut => AddError::new(AddErrorKind::TimedOut),
            async_nats::RequestErrorKind::NoResponders => {
                AddError::new(AddErrorKind::SchemaRegistryNotFound)
            }
            async_nats::RequestErrorKind::Other => AddError::with_source(AddErrorKind::Other, err),
        }
    }
}

impl From<serde_json::Error> for AddError {
    fn from(err: serde_json::Error) -> Self {
        AddError::with_source(AddErrorKind::Other, err)
    }
}

pub type GetError = Error<GetErrorKind>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GetErrorKind {
    SchemaRegistryNotFound,
    TimedOut,
    Other,
}

impl Display for GetErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetErrorKind::SchemaRegistryNotFound => write!(f, "Schema registry not found"),
            GetErrorKind::TimedOut => write!(f, "Timed out"),
            GetErrorKind::Other => write!(f, "Other error"),
        }
    }
}

impl From<async_nats::RequestError> for GetError {
    fn from(err: async_nats::RequestError) -> Self {
        match err.kind() {
            async_nats::RequestErrorKind::TimedOut => GetError::new(GetErrorKind::TimedOut),
            async_nats::RequestErrorKind::NoResponders => {
                GetError::new(GetErrorKind::SchemaRegistryNotFound)
            }
            async_nats::RequestErrorKind::Other => GetError::with_source(GetErrorKind::Other, err),
        }
    }
}
impl From<serde_json::Error> for GetError {
    fn from(err: serde_json::Error) -> Self {
        GetError::with_source(GetErrorKind::Other, err)
    }
}

pub type UpdateError = GetError;
