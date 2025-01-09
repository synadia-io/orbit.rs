#[cfg(test)]
mod registry {
    use std::{collections::HashMap, path::PathBuf};

    use bytes::Bytes;
    use futures::{pin_mut, FutureExt, StreamExt, TryStreamExt};
    use schema_validator::registry::{self, AddRequest, Format, GetRequest};
    use serde_json::json;

    #[tokio::test]
    async fn publish() {
        // let server = nats_server::run_server("tests/configs/jetstream.conf");
        let nc = async_nats::connect("localhost:4222").await.unwrap();

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let registry = registry::Registry::new(nc.clone());

        let schema = tokio::fs::read_to_string(path.join("tests/schemas/jsonschema.json"))
            .await
            .unwrap();

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

        // Nats-Schema-Name: "schema_name"

        // "events.>"
        // "event.create"- schemaA
        // "event.update" - schemaB

        // let sub: T = nc.subscribe("events.>")

        // enum t {
        //     schemaA
        //     schemeB
        // }

        let mut sub = nc.subscribe("subject").await.unwrap();

        let payload = json!({
            "firstName": "John",
            "lastName": "Doe",
            "age": 21
        });
        let data = serde_json::to_string(&payload).unwrap();
        // registry.validate("schema", 1, data).await.unwrap();
        // nc.publish("subject", payload).await.unwrap();

        while let Some(message) = sub
            .next()
            .await
            .and_then(|message| Some(registry.validate("schema", 1, message.payload)))
        {
            // println!("{:?}", message);
        }
    }

    #[tokio::test]
    async fn schema() {
        // let schema = json!({
        // "$id": "https://example.com/person.schema.json",
        // "$schema": "https://json-schema.org/draft/2020-12/schema",
        // "title": "Person",
        // "type": "object",
        // "properties": {
        //   "firstName": {
        //     "type": "string",
        //     "description": "The person's first name."
        //   },
        //   "lastName": {
        //     "type": "string",
        //     "description": "The person's last name."
        //   },
        //   "age": {
        //     "description": "Age in years which must be equal to or greater than zero.",
        //     "type": "integer",
        //     "minimum": 0
        //   }
        // }
        //       });

        // let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        // let schema2 = tokio::fs::read_to_string(path.join("tests/schemas/jsonschema.json"))
        //     .await
        //     .unwrap();

        // let schema2 = serde_json::from_str(&schema2).unwrap();

        // let validator = jsonschema::draft202012::new(&schema2).unwrap();
    }
}
