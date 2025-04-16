use std::{collections::HashMap, fmt::Display};

use async_nats::{self, ToServerAddrs};
use bytes::Bytes;
use futures::task::waker;
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
    schemas: Vec<Schema>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    pub name: String,
    pub version: u32,
    pub time: String,
    pub format: Format,
    #[serde(rename = "compat_policy")]
    pub compatibility_policy: CompatibilityPolicy,
    pub description: String,
    pub metadata: HashMap<String, String>,
    #[serde(default)]
    pub deleted: bool,
    pub definition: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Format {
    #[serde(rename = "jsonschema")]
    JsonSchema,
    #[serde(rename = "avro")]
    Avro,
    #[serde(rename = "protobuf")]
    Protobuf,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CompatibilityPolicy {
    None,
    Backward,
    Forward,
    Full,
}

pub struct Registry {
    client: async_nats::Client,
}

impl Registry {
    pub fn new(client: async_nats::Client) -> Self {
        Registry { client }
    }

    // This should work when called multiple times.
    pub async fn add(&self, request: AddRequest) -> Result<AddResponse, AddError> {
        let request_bytes = serde_json::to_vec(&request).map(Bytes::from)?;
        let response = self
            .client
            .request(format!("$SR.v1.ADD.{}", request.name), request_bytes)
            .await?;

        println!("Response: {:?}", response);

        let result = serde_json::from_slice(&response.payload)?;
        Ok(result)
    }

    pub async fn get(&self, request: GetRequest) -> Result<GetResponse, GetError> {
        let request_bytes = serde_json::to_vec(&request).map(Bytes::from)?;
        let response = self
            .client
            .request(format!("$SR.v1.GET.{}", request.name), request_bytes)
            .await?;
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
        &self,
        message: async_nats::Message,
    ) -> Result<async_nats::Message, ValidateError> {
        let schema_name = message
            .headers
            .as_ref()
            .unwrap()
            .get("Nats-Schema-Name")
            .unwrap();
        let schema_version = message
            .headers
            .as_ref()
            .unwrap()
            .get("Nats-Schema-Version")
            .unwrap();
        let schema_version = schema_version.as_str().parse().unwrap();

        self.validate(
            schema_name.as_str(),
            schema_version,
            message.payload.clone(),
        )
        .await?;

        Ok(message)
    }

    pub async fn validate(
        &self,
        schema: &str,
        version: u32,
        payload: Bytes,
    ) -> Result<(), ValidateError> {
        let request = GetRequest {
            name: schema.to_string(),
            version,
        };
        let schema = self.get(request).await?;
        let schema = schema.schema;

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
            _ => todo!(),
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
