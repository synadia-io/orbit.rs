#[cfg(test)]
mod registry {
    use std::{collections::HashMap, path::PathBuf, str::FromStr};

    use async_nats::HeaderValue;
    use futures::StreamExt;
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

        let mut sub = nc.subscribe("subject.>").await.unwrap().take(2);

        let payload = json!({
            "firstName": "John",
            "lastName": "Doe",
            "age": 21
        });

        let bad_payload = json!({
            "firstName": "John",
            "lastName": "Doe",
            "age": -1
        });

        let data = bytes::Bytes::from(serde_json::to_vec(&payload).unwrap());
        let bad_data = bytes::Bytes::from(serde_json::to_vec(&bad_payload).unwrap());
        nc.publish("subject.good", data.clone()).await.unwrap();
        nc.publish("subject.bad", bad_data).await.unwrap();

        let mut headers = async_nats::header::HeaderMap::new();
        headers.insert("Nats-Schema", "schema");
        headers.insert("Nats-Schema-Version", "1");
        nc.publish_with_headers("subject.schema", headers, data)
            .await
            .unwrap();

        while let Some(message) = sub.next().await {
            let (schema_name, schema_version) = if message.headers.as_ref().is_some() {
                let schema_name = message
                    .headers
                    .as_ref()
                    .unwrap()
                    .get("Nats-Schema")
                    .and_then(|h| Some(h.to_owned()))
                    .unwrap_or_else(|| HeaderValue::from_str("schema").unwrap())
                    .to_string();

                let schema_version = message
                    .headers
                    .as_ref()
                    .unwrap()
                    .get("Nats-Schema-Version")
                    .and_then(|h| Some(h.to_owned()))
                    .unwrap_or_else(|| HeaderValue::from_str("1").unwrap())
                    .to_string();
                let schema_version = str::parse(&schema_version).unwrap();

                (schema_name, schema_version)
            } else {
                ("schema".to_string(), 1)
            };

            let schema = registry
                .get(GetRequest {
                    name: schema_name.to_string(),
                    version: str::parse(&schema_version.to_string()).unwrap(),
                })
                .await
                .unwrap();

            let schema_value = serde_json::from_str(&schema.schema.definition).unwrap();
            println!("schema_value: {:?}", schema_value);
            let schema = jsonschema::draft202012::new(&schema_value).unwrap();

            let instance = serde_json::from_slice(message.payload.as_ref()).unwrap();
            println!("instance: {:?}", instance);

            if message.subject.as_ref() == "subject.good"
                || message.subject.as_ref() == "subject.schema"
            {
                // validate the message
                schema.validate(&instance).unwrap();
                registry
                    .validate("schema", 1, message.payload.clone())
                    .await
                    .unwrap();
            } else {
                // validate the message
                schema.validate(&instance).unwrap_err();
            }
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
