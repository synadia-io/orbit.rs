use async_nats;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use schema_validator::{
    registry::Registry,
    client_ext::{SchemaExt, ValidateStreamExt},
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration, path::Path, fs};
use tokio::time::sleep;

#[derive(Parser)]
#[command(name = "schema-test")]
#[command(about = "NATS schema validation testing tool")]
struct Cli {
    /// NATS server URL
    #[arg(short, long, default_value = "localhost:4222")]
    server: String,

    /// NATS credentials file
    #[arg(short, long)]
    creds: Option<String>,

    /// Schema ID to use (format: jsonschema.1.1.Demo)
    #[arg(long)]
    schema_id: String,

    /// Subject to publish/subscribe to
    #[arg(long, default_value = "person.events")]
    subject: String,

    /// Number of messages to publish/receive
    #[arg(short = 'n', long, default_value = "1")]
    count: usize,

    /// Custom payload: file path or JSON string
    #[arg(long)]
    payload: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Publish test messages with schema validation
    Publish,
    /// Subscribe and validate incoming messages
    Subscribe,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Build NATS connection options
    let mut connect_options = async_nats::ConnectOptions::new();
    
    // Apply credentials file if specified
    if let Some(creds_file) = &cli.creds {
        connect_options = connect_options.credentials_file(creds_file).await?;
    }

    // Determine server URL
    let server_url = if cli.server.starts_with("nats://") || cli.server.starts_with("tls://") {
        cli.server.clone()
    } else {
        format!("nats://{}", cli.server)
    };

    // Connect to NATS
    let client = connect_options.connect(&server_url).await?;
    println!("Connected to NATS server: {}", server_url);

    match cli.command {
        Commands::Publish => {
            run_publisher(client, cli.subject, cli.schema_id, cli.count, cli.payload).await?;
        }
        Commands::Subscribe => {
            run_subscriber(client, cli.subject, cli.schema_id, cli.count).await?;
        }
    }

    Ok(())
}

async fn run_publisher(
    client: async_nats::Client,
    subject: String,
    schema_id: String,
    count: usize,
    payload: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Publishing {} messages to subject: {}", count, subject);
    println!("Using schema ID: {}", schema_id);
    
    // Use the full schema_id for everything
    let full_schema_id = schema_id.clone();
    let schema_version = 1; // Default version for demo purposes

    // Load or parse payload data
    let payload_data = if let Some(payload_str) = payload {
        // Check if it's a file path or JSON string
        if Path::new(&payload_str).exists() {
            // It's a file path
            let file_content = fs::read_to_string(&payload_str)?;
            serde_json::from_str::<Value>(&file_content)?
        } else {
            // It's a JSON string
            serde_json::from_str::<Value>(&payload_str)?
        }
    } else {
        // Default test data - valid person record
        json!({
            "firstName": "John",
            "lastName": "Doe", 
            "age": 30
        })
    };

    for i in 0..count {
        let payload = Bytes::from(serde_json::to_vec(&payload_data)?);
        
        // println!("Publishing message {}: {}", i + 1, serde_json::to_string(&payload_data)?);
        // println!("  Subject: {}", subject);
        // println!("  Schema ID: {}", full_schema_id);
        // println!("  Schema Version: {}", schema_version);
        
        match client.clone().publish_with_schema(
            subject.clone(),
            &full_schema_id,
            schema_version,
            payload,
        ).await {
            Ok(_) => println!("Published successfully"),
            Err(e) => println!("Publish failed: {:?}", e),
        }
        
        // Small delay between messages
        if i < count - 1 {
            sleep(Duration::from_millis(200)).await;
        }
    }

    // Give NATS time to send the last message
    sleep(Duration::from_millis(100)).await;
    
    Ok(())
}

async fn run_subscriber(
    client: async_nats::Client,
    subject: String,
    schema_id: String,
    _count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Subscribing to subject: {}", subject);
    println!("Using schema ID: {}", schema_id);

    // Initialize schema registry
    let registry = Registry::new(client.clone());
    
    // Create a validating subscriber
    let subscription = client
        .subscribe(subject.clone())
        .await?
        .validated(Arc::new(tokio::sync::Mutex::new(registry)));

    println!("Subscriber started, waiting for messages...");
    
    futures::pin_mut!(subscription);
    let mut received = 0;
    
    while let Some(result) = subscription.next().await {
        received += 1;
        match result {
            Ok(message) => {
                println!("✅ Message {} - Valid:", received);
                println!("  Subject: {}", message.subject);
                println!("  Payload: {}", String::from_utf8_lossy(&message.payload));
                if let Some(headers) = &message.headers {
                    println!("  Headers:");
                    for (key, values) in headers.iter() {
                        for value in values {
                            println!("    {}: {}", key, value);
                        }
                    }
                }
                println!();
            }
            Err(error) => {
                println!("❌ Message {} - Validation error:", received);
                println!("  Error: {}", error);
                println!();
            }
        }
    }
    
    println!("Subscriber finished - received {} messages", received);
    Ok(())
}


