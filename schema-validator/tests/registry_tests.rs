#[cfg(test)]
mod registry {
    use std::{collections::HashMap, path::PathBuf, sync::Arc};

    use futures::{pin_mut, StreamExt, TryStreamExt};
    use schema_validator::{
        client_ext::{SchemaExt, ValidateStreamExt},
        registry::{self, AddRequest, Format},
    };
    use serde_json::json;

    #[tokio::test]
    async fn schema_roundtrip() {
        // let server = nats_server::run_server("tests/configs/jetstream.conf");
        let nc = async_nats::connect("localhost:4222").await.unwrap();

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        // A fixture to have some schema in the registry.
        let schema = tokio::fs::read_to_string(path.join("tests/schemas/jsonschema.json"))
            .await
            .unwrap();

        let registry = registry::Registry::new(nc.clone());
        // Add the schema to the registry.
        registry
            .add(AddRequest {
                name: "schema".to_string(),
                format: Format::JsonSchema,
                definition: schema,
                compatibility_policy: registry::CompatibilityPolicy::Backward,
                description: "desc".to_string(),
                metadata: HashMap::new(),
            })
            .await
            .unwrap();

        let sub = nc
            .subscribe("subject.>")
            .await
            .unwrap()
            .validated(Arc::new(tokio::sync::Mutex::new(registry)))
            .take(1);

        pin_mut!(sub);

        let payload = json!({
            "firstName": "John",
            "lastName": "Doe",
            "age": 21
        });
        let data = bytes::Bytes::from(serde_json::to_vec(&payload).unwrap());

        let bad_payload = json!({
            "firstName": "John",
            "lastName": "Doe",
            "age": -1
        });
        let bad_data = bytes::Bytes::from(serde_json::to_vec(&bad_payload).unwrap());

        // Publish a message with payload that does not match the schema.
        // nc.publish("subject.bad", bad_data).await.unwrap();

        // Publish a message with payload that matches the schema.
        nc.publish_with_schema("subject.schema", "schema", 1, data)
            .await
            .unwrap();

        while let Some(message) = sub.next().await {
            print!("Message: {:?}", message);
        }

        while let Some(message) = sub.try_next().await.unwrap() {
            // For messages that have a proper schema, assert that the schema is valid.
            print!("Message: {:?}", message);
        }

        // while let Some(message) = sub.next().await {
        //     // For messages that have a proper schema, assert that the schema is valid.
        //     if message.subject.as_ref() == "subject.good"
        //         || message.subject.as_ref() == "subject.schema"
        //     {
        //         registry.validate_message(message).await.unwrap();
        //     // Otherwise, assert error.
        //     } else {
        //         registry.validate_message(message).await.unwrap_err();
        //     }
        // }
    }
}
