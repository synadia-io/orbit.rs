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
pub enum Format {
    JsonSchema,
    Avro,
    Protobuf,
}

#[derive(Debug, Serialize)]
pub enum CompatibilityPolicy {
    None,
    Backward,
    Forward,
    Full,
}

pub struct Registry {
    client: async_nats::Client,
    name: String,
}

impl Registry {
    pub async fn new<A: ToServerAddrs>(
        name: &str,
        client: async_nats::Client,
    ) -> Result<Self, async_nats::Error> {
        Ok(Registry {
            client,
            name: name.to_string(),
        })
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
