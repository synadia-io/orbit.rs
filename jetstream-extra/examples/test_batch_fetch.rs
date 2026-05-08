use async_nats::jetstream;
use futures::StreamExt;
use jetstream_extra::batch_fetch::BatchFetchExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to NATS
    let client = async_nats::connect("localhost:4222").await?;
    let context = jetstream::new(client);

    // Try to get a batch (will fail because server doesn't support yet)
    match context.get_batch("test_stream", 20).send().await {
        Ok(mut stream) => {
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(m) => println!("Got message: seq={}", m.sequence),
                    Err(e) => println!("Stream error: {:?}", e),
                }
            }
        }
        Err(e) => {
            println!("Expected error (server doesn't support batch get): {:?}", e);
        }
    }

    Ok(())
}
