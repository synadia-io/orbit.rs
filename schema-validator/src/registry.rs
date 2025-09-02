use std::{collections::HashMap, fmt::Display};

use async_nats::{self};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::errors::Error;

#[derive(Debug, Serialize)]
pub struct AddRequest {
    #[serde(skip_serializing)]
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
    pub revision: u32,
}

#[derive(Debug, Serialize)]
pub struct GetRequest {
    #[serde(skip_serializing)]
    pub name: String,
    #[serde(skip_serializing)]
    pub version: u32,
    #[serde(skip_serializing)]
    pub revision: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<Format>,
}

#[derive(Debug, Deserialize)]
pub struct GetResponse {
    pub schema: Schema,
}

#[derive(Debug, Serialize)]
pub struct UpdateRequest {
    #[serde(skip_serializing)]
    pub name: String,
    #[serde(skip_serializing)]
    pub version: u32,
    #[serde(skip_serializing)]
    pub revision: u32,
    #[serde(skip_serializing)]
    pub format: Format,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateResponse {
    pub version: u32,
    pub revision: u32,
}

#[derive(Debug, Serialize)]
pub struct ListRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListResponse {
    pub schemas: Vec<Schema>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Schema {
    pub name: String,
    pub version: u32,
    pub revision: u32,
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
    None,
    Backward,
    Forward,
    Full,
}

pub struct Registry {
    client: async_nats::Client,
    cache: HashMap<(String, u32, u32), Schema>,
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
            .request(
                format!("$SR.v1.ADD.{}", request.name),
                request_bytes,
            )
            .await?;

        if let Some(headers) = response.headers {
            if let Some((code, message)) = headers_to_error(&headers) {
                match code {
                    11000 => return Err(AddError::new(AddErrorKind::InvalidName)),
                    11001 => return Err(AddError::new(AddErrorKind::AlreadyExists)),
                    11004 => return Err(AddError::new(AddErrorKind::FormatRequired)),
                    11005 => return Err(AddError::new(AddErrorKind::InvalidDefinition)),
                    11007 => return Err(AddError::new(AddErrorKind::FormatUnsupported)),
                    _ => {
                        return Err(AddError::with_source(
                            AddErrorKind::Other,
                            format!("{}: {}", code, message),
                        ))
                    }
                }
            }
        }
        serde_json::from_slice(&response.payload)
            .map_err(|err| AddError::with_source(AddErrorKind::Other, err))
    }

    pub async fn get(&self, request: GetRequest) -> Result<GetResponse, GetError> {
        let format = request
            .format
            .as_ref()
            .map(|f| format!("{:?}", f).to_lowercase())
            .unwrap_or_else(|| "*".to_string());
        let version = if request.version == 0 {
            "*".to_string()
        } else {
            request.version.to_string()
        };
        let revision = if request.revision == 0 {
            "*".to_string()
        } else {
            request.revision.to_string()
        };

        let request_bytes = serde_json::to_vec(&request).map(Bytes::from)?;
        let response = self
            .client
            .request(
                format!(
                    "$SR.v1.GET.{}.{}.{}.{}",
                    format, version, revision, request.name
                ),
                request_bytes,
            )
            .await?;

        if let Some(headers) = response.headers {
            if let Some((code, message)) = headers_to_error(&headers) {
                match code {
                    11002 => return Err(GetError::new(GetErrorKind::SchemaNotFound)),
                    _ => {
                        return Err(GetError::with_source(
                            GetErrorKind::Other,
                            format!("{}: {}", code, message),
                        ))
                    }
                }
            }
        }

        let result = serde_json::from_slice(&response.payload)?;
        Ok(result)
    }

    pub async fn update(&self, request: UpdateRequest) -> Result<UpdateResponse, UpdateError> {
        let format = format!("{:?}", request.format).to_lowercase();
        let version = if request.version == 0 {
            "*".to_string()
        } else {
            request.version.to_string()
        };
        let revision = if request.revision == 0 {
            "*".to_string()
        } else {
            request.revision.to_string()
        };

        let request_bytes = serde_json::to_vec(&request).map(Bytes::from)?;
        let response = self
            .client
            .request(
                format!(
                    "$SR.v1.UPDATE.{}.{}.{}.{}",
                    format, version, revision, request.name
                ),
                request_bytes,
            )
            .await?;

        if let Some(headers) = response.headers {
            if let Some((code, message)) = headers_to_error(&headers) {
                match code {
                    11002 => return Err(UpdateError::new(UpdateErrorKind::SchemaNotFound)),
                    11005 => return Err(UpdateError::new(UpdateErrorKind::InvalidDefinition)),
                    11007 => return Err(UpdateError::new(UpdateErrorKind::FormatUnsupported)),
                    _ => {
                        return Err(UpdateError::with_source(
                            UpdateErrorKind::Other,
                            format!("{}: {}", code, message),
                        ))
                    }
                }
            }
        }

        let result = serde_json::from_slice(&response.payload)?;
        Ok(result)
    }

    pub async fn list(&self, request: ListRequest) -> Result<ListResponse, GetError> {
        let request_bytes = serde_json::to_vec(&request).map(Bytes::from)?;
        let response = self
            .client
            .request("$SR.v1.LIST".to_string(), request_bytes)
            .await?;

        if let Some(headers) = response.headers {
            if let Some((code, message)) = headers_to_error(&headers) {
                return Err(GetError::with_source(
                    GetErrorKind::Other,
                    format!("{}: {}", code, message),
                ));
            }
        }

        let result = serde_json::from_slice(&response.payload)?;
        Ok(result)
    }

    pub async fn validate_message(
        &mut self,
        message: async_nats::Message,
    ) -> Result<async_nats::Message, ValidateError> {
        let schema_name = message
            .headers
            .as_ref()
            .and_then(|h| h.get("Nats-Schema"))
            .ok_or_else(|| ValidateError::new(ValidateErrorKind::MissingSchemaHeaders))?;

        let schema_version = message
            .headers
            .as_ref()
            .and_then(|h| h.get("Nats-Schema-Version"))
            .ok_or_else(|| ValidateError::new(ValidateErrorKind::MissingSchemaHeaders))?;

        let schema_version = schema_version
            .as_str()
            .parse()
            .map_err(|_| ValidateError::new(ValidateErrorKind::MissingSchemaHeaders))?;

        self.validate(
            schema_name.as_str(),
            schema_version,
            message.payload.clone(),
        )
        .await?;

        Ok(message)
    }

    pub async fn get_schema(&mut self, key: String, version: u32) -> Result<Schema, ValidateError> {
        // Check cache for any revision of this version
        // When using wildcard (revision 0), we cache with revision 0 as key
        if let Some(schema) = self.cache.get(&(key.clone(), version, 0)).cloned() {
            return Ok(schema);
        }

        let schema = self
            .get(GetRequest {
                name: key.clone(),
                version,
                revision: 0, // Get latest revision
                format: None,
            })
            .await?;
        
        // Cache with revision 0 as key since we're using wildcard
        self.cache.insert(
            (key, version, 0),
            schema.schema.clone(),
        );
        Ok(schema.schema)
    }

    pub async fn remove(&self, name: String, version: u32, revision: u32, format: Format) -> Result<(), RemoveError> {
        // Use wildcards for flexible deletion
        let format_str = format!("{:?}", format).to_lowercase();
        let version_str = if version == 0 { "*".to_string() } else { version.to_string() };
        let revision_str = if revision == 0 { "*".to_string() } else { revision.to_string() };
        
        let subject = format!(
            "$SR.v1.REMOVE.{}.{}.{}.{}",
            format_str, version_str, revision_str, name
        );
        
        let response = self.client.request(subject, Bytes::new()).await?;
        
        if let Some(headers) = response.headers {
            if let Some((code, message)) = headers_to_error(&headers) {
                match code {
                    11002 => return Err(RemoveError::new(RemoveErrorKind::SchemaNotFound)),
                    _ => {
                        return Err(RemoveError::with_source(
                            RemoveErrorKind::Other,
                            format!("{}: {}", code, message),
                        ))
                    }
                }
            }
        }
        
        Ok(())
    }

    pub async fn validate(
        &mut self,
        schema: &str,
        version: u32,
        payload: Bytes,
    ) -> Result<(), ValidateError> {
        let schema = self.get_schema(schema.to_string(), version).await?;

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

                Ok(())
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
    SchemaNotFound,
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
            ValidateErrorKind::SchemaNotFound => write!(f, "Schema not found"),
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
            GetErrorKind::SchemaNotFound => ValidateError::new(ValidateErrorKind::SchemaNotFound),
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
    AlreadyExists,
    FormatRequired,
    Other,
    InvalidName,
    InvalidDefinition,
    FormatUnsupported,
}

impl Display for AddErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AddErrorKind::SchemaRegistryNotFound => write!(f, "Schema registry not found"),
            AddErrorKind::TimedOut => write!(f, "Timed out"),
            AddErrorKind::Other => write!(f, "Other error"),
            AddErrorKind::AlreadyExists => write!(f, "Schema already exists"),
            AddErrorKind::FormatRequired => write!(f, "Format required"),
            AddErrorKind::InvalidName => write!(f, "Invalid name"),
            AddErrorKind::InvalidDefinition => write!(f, "Invalid definition"),
            AddErrorKind::FormatUnsupported => write!(f, "Unsupported schema format"),
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

fn headers_to_error(headers: &async_nats::header::HeaderMap) -> Option<(u32, String)> {
    let code = headers
        .get("Nats-Service-Error-Code")
        .and_then(|h| h.as_str().parse().ok())?;

    let message = headers
        .get("Nats-Service-Error")
        .map(|message| message.to_string())?;

    Some((code, message))
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
    SchemaNotFound,
    TimedOut,
    Other,
}

impl Display for GetErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetErrorKind::SchemaRegistryNotFound => write!(f, "Schema registry not found"),
            GetErrorKind::SchemaNotFound => write!(f, "Schema not found"),
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

pub type UpdateError = Error<UpdateErrorKind>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UpdateErrorKind {
    SchemaRegistryNotFound,
    SchemaNotFound,
    InvalidDefinition,
    FormatUnsupported,
    TimedOut,
    Other,
}

impl Display for UpdateErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateErrorKind::SchemaRegistryNotFound => write!(f, "Schema registry not found"),
            UpdateErrorKind::SchemaNotFound => write!(f, "Schema not found"),
            UpdateErrorKind::InvalidDefinition => write!(f, "Invalid definition"),
            UpdateErrorKind::FormatUnsupported => write!(f, "Unsupported schema format"),
            UpdateErrorKind::TimedOut => write!(f, "Timed out"),
            UpdateErrorKind::Other => write!(f, "Other error"),
        }
    }
}

impl From<async_nats::RequestError> for UpdateError {
    fn from(err: async_nats::RequestError) -> Self {
        match err.kind() {
            async_nats::RequestErrorKind::TimedOut => UpdateError::new(UpdateErrorKind::TimedOut),
            async_nats::RequestErrorKind::NoResponders => {
                UpdateError::new(UpdateErrorKind::SchemaRegistryNotFound)
            }
            async_nats::RequestErrorKind::Other => {
                UpdateError::with_source(UpdateErrorKind::Other, err)
            }
        }
    }
}

impl From<serde_json::Error> for UpdateError {
    fn from(err: serde_json::Error) -> Self {
        UpdateError::with_source(UpdateErrorKind::Other, err)
    }
}

pub type RemoveError = Error<RemoveErrorKind>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RemoveErrorKind {
    SchemaRegistryNotFound,
    SchemaNotFound,
    TimedOut,
    Other,
}

impl Display for RemoveErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RemoveErrorKind::SchemaRegistryNotFound => write!(f, "Schema registry not found"),
            RemoveErrorKind::SchemaNotFound => write!(f, "Schema not found"),
            RemoveErrorKind::TimedOut => write!(f, "Timed out"),
            RemoveErrorKind::Other => write!(f, "Other error"),
        }
    }
}

impl From<async_nats::RequestError> for RemoveError {
    fn from(err: async_nats::RequestError) -> Self {
        match err.kind() {
            async_nats::RequestErrorKind::TimedOut => RemoveError::new(RemoveErrorKind::TimedOut),
            async_nats::RequestErrorKind::NoResponders => {
                RemoveError::new(RemoveErrorKind::SchemaRegistryNotFound)
            }
            async_nats::RequestErrorKind::Other => {
                RemoveError::with_source(RemoveErrorKind::Other, err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_serialization() {
        // Test AddRequest - name should not be serialized
        let add_req = AddRequest {
            name: "test-schema".to_string(),
            format: Format::JsonSchema,
            definition: "{}".to_string(),
            compatibility_policy: CompatibilityPolicy::Backward,
            description: "test".to_string(),
            metadata: HashMap::new(),
        };
        let json = serde_json::to_value(&add_req).unwrap();
        assert!(json.get("name").is_none(), "name should not be serialized");
        assert_eq!(json["format"], "jsonschema");
        assert_eq!(json["compat_policy"], "Backward");

        // Test GetRequest - name, version, revision should not be serialized
        let get_req = GetRequest {
            name: "test".to_string(),
            version: 1,
            revision: 2,
            format: Some(Format::JsonSchema),
        };
        let json = serde_json::to_value(&get_req).unwrap();
        assert!(json.get("name").is_none(), "name should not be serialized");
        assert!(json.get("version").is_none(), "version should not be serialized");
        assert!(json.get("revision").is_none(), "revision should not be serialized");
        assert_eq!(json["format"], "jsonschema", "format should be serialized when Some");
        
        // Test GetRequest with None format - should not be in JSON
        let get_req_no_format = GetRequest {
            name: "test".to_string(),
            version: 1,
            revision: 2,
            format: None,
        };
        let json_no_format = serde_json::to_value(&get_req_no_format).unwrap();
        assert!(json_no_format.get("format").is_none(), "format should not be serialized when None");

        // Test UpdateRequest - only optional fields should be serialized
        let update_req = UpdateRequest {
            name: "test".to_string(),
            version: 1,
            revision: 2,
            format: Format::JsonSchema,
            definition: Some("new def".to_string()),
            description: None,
            metadata: None,
        };
        let json = serde_json::to_value(&update_req).unwrap();
        assert!(json.get("name").is_none());
        assert!(json.get("version").is_none());
        assert!(json.get("revision").is_none());
        assert!(json.get("format").is_none());
        assert_eq!(json["definition"], "new def");
        assert!(json.get("description").is_none());
        assert!(json.get("metadata").is_none());
    }

    #[test]
    fn test_response_deserialization() {
        // Test AddResponse
        let json = r#"{"version": 1, "revision": 2}"#;
        let resp: AddResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.version, 1);
        assert_eq!(resp.revision, 2);

        // Test Schema with revision
        let schema_json = r#"{
            "name": "test",
            "version": 1,
            "revision": 3,
            "time": "2024-01-01T00:00:00Z",
            "format": "jsonschema",
            "compat_policy": "Backward",
            "description": "test schema",
            "metadata": {},
            "definition": "{}"
        }"#;
        let schema: Schema = serde_json::from_str(schema_json).unwrap();
        assert_eq!(schema.name, "test");
        assert_eq!(schema.version, 1);
        assert_eq!(schema.revision, 3);
    }

    #[test]
    fn test_error_code_mapping() {
        let headers = |code: &str| {
            let mut h = async_nats::header::HeaderMap::new();
            h.insert("Nats-Service-Error-Code", code);
            h.insert("Nats-Service-Error", "error message");
            h
        };

        // Test error code extraction
        assert_eq!(headers_to_error(&headers("11000")).unwrap().0, 11000);
        assert_eq!(headers_to_error(&headers("11001")).unwrap().0, 11001);
        assert_eq!(headers_to_error(&headers("11002")).unwrap().0, 11002);
        assert_eq!(headers_to_error(&headers("11004")).unwrap().0, 11004);
        assert_eq!(headers_to_error(&headers("11005")).unwrap().0, 11005);
        assert_eq!(headers_to_error(&headers("11007")).unwrap().0, 11007);
    }

    #[test]
    fn test_format_serialization() {
        assert_eq!(serde_json::to_string(&Format::JsonSchema).unwrap(), "\"jsonschema\"");
        assert_eq!(serde_json::to_string(&Format::Avro).unwrap(), "\"avro\"");
        assert_eq!(serde_json::to_string(&Format::Protobuf).unwrap(), "\"protobuf\"");
    }

    #[test]
    fn test_compatibility_policy_serialization() {
        assert_eq!(serde_json::to_string(&CompatibilityPolicy::None).unwrap(), "\"None\"");
        assert_eq!(serde_json::to_string(&CompatibilityPolicy::Backward).unwrap(), "\"Backward\"");
        assert_eq!(serde_json::to_string(&CompatibilityPolicy::Forward).unwrap(), "\"Forward\"");
        assert_eq!(serde_json::to_string(&CompatibilityPolicy::Full).unwrap(), "\"Full\"");
    }
}
