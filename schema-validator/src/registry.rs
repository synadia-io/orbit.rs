use std::{collections::HashMap, fmt::Display};

use async_nats::{self, ToServerAddrs};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::errors::Error;

#[derive(Debug, Serialize)]
pub struct AddRequest {
    name: String,
    definition: Format,
    compatibility_policy: CompatibilityPolicy,
    description: String,
    metadata: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct AddResponse {
    version: u32,
}

#[derive(Debug, Serialize)]
pub struct GetRequest {
    name: String,
    version: u32,
}

#[derive(Debug, Deserialize)]
pub struct GetResponse {
    schema: Schema,
}

#[derive(Debug, Serialize)]
pub struct UpdateRequest {
    name: String,
    definition: Format,
    description: String,
    metadata: HashMap<String, String>,
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
    name: String,
    version: u32,
    time: time::OffsetDateTime,
    format: Format,
    #[serde(rename = "compat_policy")]
    compatibility_policy: CompatibilityPolicy,
    description: String,
    metadata: HashMap<String, String>,
    deleted: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Format {
    JsonSchema,
    Avro,
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
    pub async fn new<A: ToServerAddrs>(client: async_nats::Client) -> Self {
        Registry { client }
    }

    pub async fn add(&self, request: AddRequest) -> Result<AddResponse, AddError> {
        let request_bytes = serde_json::to_vec(&request).map(Bytes::from)?;
        let response = self
            .client
            .request(format!("$R.v1.ADD.{}", request.name), request_bytes)
            .await?;
        let result = serde_json::from_slice(&response.payload)?;
        Ok(result)
    }

    pub async fn get(&self, request: GetRequest) -> Result<GetResponse, GetError> {
        let request_bytes = serde_json::to_vec(&request).map(Bytes::from)?;
        let response = self
            .client
            .request(format!("$R.v1.GET.{}", request.name), request_bytes)
            .await?;
        let result = serde_json::from_slice(&response.payload)?;
        Ok(result)
    }

    pub async fn update(&self, request: AddRequest) -> Result<AddResponse, AddError> {
        let request_bytes = serde_json::to_vec(&request).map(Bytes::from)?;
        let response = self
            .client
            .request(format!("$R.v1.UPDATE.{}", request.name), request_bytes)
            .await?;
        let result = serde_json::from_slice(&response.payload)?;
        Ok(result)
    }

    pub async fn list(&self, request: ListRequest) -> Result<ListResponse, GetError> {
        let request_bytes = serde_json::to_vec(&request).map(Bytes::from)?;
        let response = self.client.request("$R.v1.LIST", request_bytes).await?;
        let result = serde_json::from_slice(&response.payload)?;
        Ok(result)
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
