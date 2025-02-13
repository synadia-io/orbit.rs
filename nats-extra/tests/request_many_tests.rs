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

#[cfg(feature = "request_many")]
mod request_many {
    use std::time::Duration;

    use futures::StreamExt;

    #[tokio::test]
    async fn request_many() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        use nats_extra::request_many::RequestManyExt;

        // request many with sentinel
        let mut requests = client.subscribe("test").await.unwrap();
        let mut responses = client
            .request_many()
            .sentinel(|msg| msg.payload.is_empty())
            .send("test", "data".into())
            .await
            .unwrap();

        let request = requests.next().await.unwrap();

        for _ in 0..100 {
            client
                .publish(request.reply.clone().unwrap(), "data".into())
                .await
                .unwrap();
        }
        client
            .publish(request.reply.unwrap(), "".into())
            .await
            .unwrap();

        let mut count = 0;
        while responses.next().await.is_some() {
            count += 1;
        }
        assert_eq!(count, 100);
        assert_eq!(
            responses.termination_reason(),
            Some(nats_extra::request_many::TerminationReason::Sentinel)
        );
        requests.unsubscribe().await.unwrap();

        // request many with max messages
        let mut requests = client.subscribe("test").await.unwrap();
        let mut responses = client
            .request_many()
            .max_messages(20)
            .send("test", "data".into())
            .await
            .unwrap();

        let request = requests.next().await.unwrap();

        for _ in 1..=100 {
            client
                .publish(request.reply.clone().unwrap(), "data".into())
                .await
                .unwrap();
        }

        let mut count = 0;
        while responses.next().await.is_some() {
            count += 1;
        }
        assert_eq!(count, 20);
        let termination = responses.termination_reason();
        assert_eq!(
            termination,
            Some(nats_extra::request_many::TerminationReason::MaxMessages)
        );
        requests.unsubscribe().await.unwrap();

        // request many with stall
        let mut requests = client.subscribe("test").await.unwrap();
        let mut responses = client
            .request_many()
            .stall_wait(Duration::from_millis(100))
            .send("test", "data".into())
            .await
            .unwrap();

        tokio::task::spawn({
            let client = client.clone();
            async move {
                let request = requests.next().await.unwrap();
                for i in 1..=100 {
                    if i == 51 {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    client
                        .publish(request.reply.clone().unwrap(), "data".into())
                        .await
                        .unwrap();
                }
                requests.unsubscribe().await.unwrap();
            }
        });
        let mut count = 0;
        while responses.next().await.is_some() {
            count += 1;
        }
        assert_eq!(count, 50);
        assert_eq!(
            responses.termination_reason(),
            Some(nats_extra::request_many::TerminationReason::StallWait)
        );

        // request many with max wait
        let mut requests = client.subscribe("test").await.unwrap();
        let mut responses = client
            .request_many()
            .max_wait(Some(Duration::from_secs(5)))
            .send("test", "data".into())
            .await
            .unwrap();

        tokio::task::spawn({
            let client = client.clone();
            async move {
                let request = requests.next().await.unwrap();
                for i in 1..=100 {
                    if i == 21 {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                    client
                        .publish(request.reply.clone().unwrap(), "data".into())
                        .await
                        .unwrap();
                }
            }
        });
        let mut count = 0;
        while responses.next().await.is_some() {
            count += 1;
        }
        assert_eq!(count, 20);
        assert_eq!(
            responses.termination_reason(),
            Some(nats_extra::request_many::TerminationReason::MaxWait)
        );

        // no responders
        let mut responses = client
            .request_many()
            .send("noone_listening", "data".into())
            .await
            .unwrap();

        let mut count = 0;
        while responses.next().await.is_some() {
            count += 1;
        }
        assert_eq!(count, 0);
        assert_eq!(
            responses.termination_reason(),
            Some(nats_extra::request_many::TerminationReason::NoResponders)
        );
    }
}
