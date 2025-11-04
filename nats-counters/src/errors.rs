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

use std::fmt;

/// Error type for counter operations.
pub type CounterError = async_nats::error::Error<CounterErrorKind>;

/// Result type for counter operations.
pub type Result<T> = std::result::Result<T, CounterError>;

/// Kinds of errors that can occur when working with NATS counters.
#[derive(Debug, Clone, PartialEq)]
pub enum CounterErrorKind {
    /// Stream is not configured for counters (AllowMsgCounter must be true).
    CounterNotEnabled,
    /// Stream must be configured for direct access (AllowDirect must be true).
    DirectAccessRequired,
    /// Invalid counter value.
    InvalidCounterValue,
    /// Counter stream not found.
    CounterNotFound,
    /// Counter not initialized for the given subject.
    NoCounterForSubject,
    /// Invalid counter value in response.
    InvalidResponseValue,
    /// Missing counter value in response.
    MissingResponseValue,
    /// Failed to parse counter sources.
    InvalidSources,
    /// JSON parsing error.
    Serialization,
    /// Stream operation error.
    Stream,
    Publish,
    GetStream,
    Request,
    Other,
}

impl fmt::Display for CounterErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CounterNotEnabled => {
                write!(
                    f,
                    "stream is not configured for counters (AllowMsgCounter must be true)"
                )
            }
            Self::DirectAccessRequired => {
                write!(
                    f,
                    "stream must be configured for direct access (AllowDirect must be true)"
                )
            }
            Self::InvalidCounterValue => write!(f, "invalid counter value"),
            Self::CounterNotFound => write!(f, "counter not found"),
            Self::NoCounterForSubject => write!(f, "counter not initialized for subject"),
            Self::InvalidResponseValue => write!(f, "invalid counter value in response"),
            Self::MissingResponseValue => write!(f, "counter increment response missing value"),
            Self::InvalidSources => write!(f, "failed to parse counter sources"),
            Self::Serialization => write!(f, "serialization error"),
            Self::Stream => write!(f, "stream operation error"),
            Self::Publish => write!(f, "publish operation error"),
            Self::GetStream => write!(f, "get stream error"),
            Self::Request => write!(f, "request error"),
            Self::Other => write!(f, "other error"),
        }
    }
}
