// Copyright 2024 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_nats::jetstream;
use futures::future::join_all;
use jetstream_extra::publisher::{Publisher, PublishMode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to NATS
    let client = async_nats::connect("localhost:4222").await?;
    let jetstream = jetstream::new(client);
    
    // Ensure stream exists
    let _ = jetstream
        .create_stream(jetstream::stream::Config {
            name: "EVENTS".to_string(),
            subjects: vec!["events.*".to_string()],
            ..Default::default()
        })
        .await;
    
    // Create a publisher with max 10 in-flight messages
    let publisher = Publisher::builder(jetstream)
        .max_in_flight(10)
        .mode(PublishMode::WaitForPermit)
        .build();
    
    // Publish messages concurrently
    let mut futures = Vec::new();
    
    for i in 0..50 {
        let publisher = publisher.clone();
        let future = tokio::spawn(async move {
            let ack_future = publisher
                .publish(format!("events.{}", i % 5), format!("Message {}", i).into())
                .await?;
                
            // The semaphore permit is held until we await the ack
            let ack = ack_future.await?;
            println!("Published message {} to stream {} with sequence {}", 
                     i, ack.stream, ack.sequence);
            
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
        
        futures.push(future);
    }
    
    // Wait for all publishes to complete
    let results = join_all(futures).await;
    
    for (i, result) in results.iter().enumerate() {
        if let Err(e) = result {
            eprintln!("Task {} failed: {}", i, e);
        }
    }
    
    println!("All messages published successfully!");
    
    Ok(())
}