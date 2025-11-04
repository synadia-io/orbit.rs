// Copyright 2025 Synadia Communications Inc.
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

//! Extension trait for JetStream Context to work with counters.

use crate::{Counter, Result};
use async_nats::jetstream::{self, stream::Stream};
use std::future::Future;

/// Extension trait for JetStream Context that enables counter operations.
///
/// This trait adds counter-specific methods to the JetStream context.
pub trait CounterExt {
    /// Gets a counter for the specified stream.
    fn get_counter(&self, name: &str) -> impl Future<Output = Result<Counter>> + Send;

    /// Creates a counter from an existing stream.
    fn counter_from_stream(&self, stream: Stream) -> impl Future<Output = Result<Counter>> + Send;
}

impl CounterExt for jetstream::Context {
    #[allow(clippy::manual_async_fn)]
    fn get_counter(&self, name: &str) -> impl Future<Output = Result<Counter>> + Send {
        let context = self.clone();
        let name = name.to_string();
        async move {
            let stream = context.get_stream(&name).await.map_err(|e| {
                crate::errors::CounterError::with_source(
                    crate::errors::CounterErrorKind::GetStream,
                    e,
                )
            })?;
            Counter::from_stream(context, stream).await
        }
    }

    fn counter_from_stream(&self, stream: Stream) -> impl Future<Output = Result<Counter>> + Send {
        Counter::from_stream(self.clone(), stream)
    }
}
