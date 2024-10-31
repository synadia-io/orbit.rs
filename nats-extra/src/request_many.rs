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

use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use async_nats::{subject::ToSubject, Client, RequestError, Subscriber};
use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt};

pub trait RequestManyExt {
    fn request_many(&self) -> RequestMany;
}

impl RequestManyExt for Client {
    fn request_many(&self) -> RequestMany {
        RequestMany::new(self.clone(), self.timeout())
    }
}

pub struct RequestMany {
    client: Client,
    sentinel: Option<Box<dyn Fn(&async_nats::Message) -> bool + 'static>>,
    max_wait: Option<Duration>,
    stall_wait: Option<Duration>,
    max_messags: Option<usize>,
}

impl RequestMany {
    pub fn new(client: Client, max_wait: Option<Duration>) -> Self {
        RequestMany {
            client,
            sentinel: None,
            max_wait,
            stall_wait: None,
            max_messags: None,
        }
    }

    pub fn sentinel(mut self, sentinel: impl Fn(&async_nats::Message) -> bool + 'static) -> Self {
        self.sentinel = Some(Box::new(sentinel));
        self
    }

    pub fn stall_wait(mut self, stall_wait: Duration) -> Self {
        self.stall_wait = Some(stall_wait);
        self
    }

    pub fn max_messages(mut self, max_messages: usize) -> Self {
        self.max_messags = Some(max_messages);
        self
    }

    pub fn max_wait(mut self, max_wait: Option<Duration>) -> Self {
        self.max_wait = max_wait;
        self
    }

    pub async fn send<S: ToSubject>(
        self,
        subject: S,
        payload: Bytes,
    ) -> Result<Responses, RequestError> {
        let response_subject = self.client.new_inbox();
        let responses = self.client.subscribe(response_subject.clone()).await?;
        self.client
            .publish_with_reply(subject, response_subject, payload)
            .await?;

        let timer = self
            .max_wait
            .map(|max_wait| Box::pin(tokio::time::sleep(max_wait)));

        Ok(Responses {
            timer,
            stall: None,
            responses,
            messages_received: 0,
            sentinel: self.sentinel,
            max_messages: self.max_messags,
            stall_wait: self.stall_wait,
        })
    }
}

pub struct Responses {
    responses: Subscriber,
    messages_received: usize,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    stall: Option<Pin<Box<tokio::time::Sleep>>>,
    sentinel: Option<Box<dyn Fn(&async_nats::Message) -> bool + 'static>>,
    max_messages: Option<usize>,
    stall_wait: Option<Duration>,
}

impl Stream for Responses {
    type Item = async_nats::Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // max_wait
        if let Some(timer) = self.timer.as_mut() {
            match timer.poll_unpin(cx) {
                Poll::Ready(_) => {
                    return Poll::Ready(None);
                }
                Poll::Pending => {}
            }
        }

        // max_messages
        if let Some(max_messages) = self.max_messages {
            if self.messages_received >= max_messages {
                return Poll::Ready(None);
            }
        }

        // stall_wait
        if let Some(stall) = self.stall_wait {
            let stall = self
                .stall
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(stall)));

            match stall.as_mut().poll_unpin(cx) {
                Poll::Ready(_) => {
                    return Poll::Ready(None);
                }
                Poll::Pending => {}
            }
        }

        match self.responses.poll_next_unpin(cx) {
            Poll::Ready(message) => match message {
                Some(message) => {
                    self.messages_received += 1;

                    // reset timer
                    self.stall = None;

                    // sentinel
                    match self.sentinel {
                        Some(ref sentinel) => {
                            if sentinel(&message) {
                                Poll::Ready(None)
                            } else {
                                return Poll::Ready(Some(message));
                            }
                        }
                        None => Poll::Ready(Some(message)),
                    }
                }
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}