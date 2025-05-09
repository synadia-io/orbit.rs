use std::sync::Arc;

use async_nats::{subject::ToSubject, Message};
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;

use crate::registry::{Registry, ValidateError};

#[async_trait]
pub trait SchemaExt {
    async fn publish_with_schema<S: ToSubject + Send>(
        &self,
        subject: S,
        schema: &str,
        schema_version: u32,
        data: bytes::Bytes,
    ) -> Result<(), Error>;
}

type Error = Box<dyn std::error::Error + Send + Sync>;

#[async_trait]
impl SchemaExt for async_nats::Client {
    async fn publish_with_schema<S: ToSubject + Send>(
        &self,
        subject: S,
        schema: &str,
        schema_version: u32,
        data: bytes::Bytes,
    ) -> Result<(), Error> {
        let mut headers = async_nats::header::HeaderMap::new();
        headers.insert("Nats-Schema", schema);
        headers.insert("Nats-Schema-Version", schema_version.to_string());
        self.publish_with_headers(subject, headers, data).await?;
        Ok(())
    }
}

pub trait ValidateStreamExt: Stream<Item = Message> + Sized {
    /// Turn a `Stream<Item = Message>` into
    /// a `Stream<Item = Result<Message, ValidateError>>`
    /// by running `registry.validate_message(...)` on each element.
    fn validated(
        self,
        registry: Arc<tokio::sync::Mutex<Registry>>,
    ) -> impl Stream<Item = Result<Message, ValidateError>> {
        self.then(move |msg| {
            let registry = registry.clone();
            async move { registry.lock().await.validate_message(msg).await }
        })
    }
}

impl<S: Stream<Item = Message>> ValidateStreamExt for S {}
