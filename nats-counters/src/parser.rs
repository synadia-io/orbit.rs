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

//! Parser utilities for counter values and sources.

use crate::CounterSources;
use crate::errors::{CounterError, CounterErrorKind, Result};
use async_nats::HeaderMap;
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

/// JSON structure for counter value payload.
#[derive(Debug, Serialize, Deserialize)]
struct CounterPayload {
    val: String,
}

/// Parses a counter value from message data.
///
/// Counter values are stored as JSON in the format: `{"val": "123"}`
///
/// # Arguments
///
/// * `data` - The raw message data containing the JSON payload
///
/// # Returns
///
/// A `BigInt` representing the counter value.
pub fn parse_counter_value(data: &[u8]) -> Result<BigInt> {
    if data.is_empty() {
        return Err(CounterError::new(CounterErrorKind::InvalidCounterValue));
    }

    let payload: CounterPayload = serde_json::from_slice(data)
        .map_err(|e| CounterError::with_source(CounterErrorKind::Serialization, e))?;

    BigInt::from_str(&payload.val)
        .map_err(|_| CounterError::new(CounterErrorKind::InvalidResponseValue))
}

/// Parses a counter value from PubAck value field.
///
/// When publishing to a stream with counters enabled, the PubAck response
/// includes a `value` field containing the current counter value as a string.
///
/// # Arguments
///
/// * `value` - Optional string value from PubAck
///
/// # Returns
///
/// A `BigInt` representing the counter value.
pub fn parse_counter_value_from_string(value: Option<String>) -> Result<BigInt> {
    match value {
        Some(val_str) => BigInt::from_str(&val_str)
            .map_err(|_| CounterError::new(CounterErrorKind::InvalidResponseValue)),
        None => Err(CounterError::new(CounterErrorKind::MissingResponseValue)),
    }
}

/// Parses counter sources from message headers.
///
/// Sources are stored in the `Nats-Counter-Sources` header as JSON.
/// The format is: `{"source1": {"subject1": "value1", "subject2": "value2"}}`
///
/// # Arguments
///
/// * `headers` - The message headers containing the sources information
///
/// # Returns
///
/// A `CounterSources` map or `None` if no sources header is present.
pub fn parse_sources(headers: &HeaderMap) -> Result<CounterSources> {
    let sources_header = headers.get(crate::COUNTER_SOURCES_HEADER);

    match sources_header {
        Some(header_value) => {
            let header_str = header_value.as_str();

            // Parse as JSON map of source -> subject -> value (as strings)
            let sources_json: HashMap<String, HashMap<String, String>> =
                serde_json::from_str(header_str)
                    .map_err(|e| CounterError::with_source(CounterErrorKind::Serialization, e))?;

            // Convert string values to BigInt
            let mut sources = HashMap::new();
            for (source_id, subjects) in sources_json {
                let mut subject_values = HashMap::new();
                for (subject, value_str) in subjects {
                    let value = BigInt::from_str(&value_str)
                        .map_err(|_| CounterError::new(CounterErrorKind::InvalidSources))?;
                    subject_values.insert(subject, value);
                }
                sources.insert(source_id, subject_values);
            }

            Ok(sources)
        }
        None => Ok(HashMap::new()),
    }
}

/// Parses an increment value from message headers.
///
/// The increment value is stored in the `Nats-Incr` header.
///
/// # Arguments
///
/// * `headers` - The message headers containing the increment value
///
/// # Returns
///
/// An optional `BigInt` representing the increment value.
pub fn parse_increment(headers: &HeaderMap) -> Result<Option<BigInt>> {
    let increment_header = headers.get(crate::COUNTER_INCREMENT_HEADER);

    match increment_header {
        Some(header_value) => {
            let increment_str = header_value.as_str();

            let increment = BigInt::from_str(increment_str)
                .map_err(|_| CounterError::new(CounterErrorKind::InvalidResponseValue))?;

            Ok(Some(increment))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_counter_value_valid() {
        let data = br#"{"val": "123"}"#;
        let result = parse_counter_value(data).unwrap();
        assert_eq!(result, BigInt::from(123));
    }

    #[test]
    fn test_parse_counter_value_negative() {
        let data = br#"{"val": "-456"}"#;
        let result = parse_counter_value(data).unwrap();
        assert_eq!(result, BigInt::from(-456));
    }

    #[test]
    fn test_parse_counter_value_large() {
        let data = br#"{"val": "999999999999999999999999999999"}"#;
        let result = parse_counter_value(data).unwrap();
        assert_eq!(
            result,
            BigInt::from_str("999999999999999999999999999999").unwrap()
        );
    }

    #[test]
    fn test_parse_counter_value_empty() {
        let data = b"";
        let result = parse_counter_value(data);
        assert!(matches!(result, Err(e) if e.kind() == CounterErrorKind::InvalidCounterValue));
    }

    #[test]
    fn test_parse_counter_value_invalid_json() {
        let data = b"not json";
        let result = parse_counter_value(data);
        assert!(matches!(result, Err(e) if e.kind() == CounterErrorKind::Serialization));
    }

    #[test]
    fn test_parse_counter_value_invalid_number() {
        let data = br#"{"val": "not a number"}"#;
        let result = parse_counter_value(data);
        assert!(matches!(result, Err(e) if e.kind() == CounterErrorKind::InvalidResponseValue));
    }

    #[test]
    fn test_parse_sources_empty() {
        let headers = HeaderMap::new();
        let result = parse_sources(&headers).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_sources_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            crate::COUNTER_SOURCES_HEADER,
            r#"{"stream1": {"subject1": "100", "subject2": "200"}}"#,
        );

        let result = parse_sources(&headers).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("stream1"));

        let stream1 = &result["stream1"];
        assert_eq!(stream1.len(), 2);
        assert_eq!(stream1["subject1"], BigInt::from(100));
        assert_eq!(stream1["subject2"], BigInt::from(200));
    }

    #[test]
    fn test_parse_sources_multiple_streams() {
        let mut headers = HeaderMap::new();
        headers.insert(
            crate::COUNTER_SOURCES_HEADER,
            r#"{"stream1": {"sub1": "10"}, "stream2": {"sub2": "20"}}"#,
        );

        let result = parse_sources(&headers).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result["stream1"]["sub1"], BigInt::from(10));
        assert_eq!(result["stream2"]["sub2"], BigInt::from(20));
    }

    #[test]
    fn test_parse_increment_present() {
        let mut headers = HeaderMap::new();
        headers.insert(crate::COUNTER_INCREMENT_HEADER, "42");

        let result = parse_increment(&headers).unwrap();
        assert_eq!(result, Some(BigInt::from(42)));
    }

    #[test]
    fn test_parse_increment_negative() {
        let mut headers = HeaderMap::new();
        headers.insert(crate::COUNTER_INCREMENT_HEADER, "-10");

        let result = parse_increment(&headers).unwrap();
        assert_eq!(result, Some(BigInt::from(-10)));
    }

    #[test]
    fn test_parse_increment_absent() {
        let headers = HeaderMap::new();
        let result = parse_increment(&headers).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_increment_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert(crate::COUNTER_INCREMENT_HEADER, "not_a_number");

        let result = parse_increment(&headers);
        assert!(matches!(result, Err(e) if e.kind() == CounterErrorKind::InvalidResponseValue));
    }

    #[test]
    fn test_parse_counter_value_from_string_valid() {
        let value = Some("42".to_string());
        let result = parse_counter_value_from_string(value).unwrap();
        assert_eq!(result, BigInt::from(42));
    }

    #[test]
    fn test_parse_counter_value_from_string_large() {
        let value = Some("999999999999999999999999".to_string());
        let result = parse_counter_value_from_string(value).unwrap();
        assert_eq!(
            result,
            BigInt::from_str("999999999999999999999999").unwrap()
        );
    }

    #[test]
    fn test_parse_counter_value_from_string_none() {
        let result = parse_counter_value_from_string(None);
        assert!(matches!(result, Err(e) if e.kind() == CounterErrorKind::MissingResponseValue));
    }

    #[test]
    fn test_parse_counter_value_from_string_invalid() {
        let value = Some("not_a_number".to_string());
        let result = parse_counter_value_from_string(value);
        assert!(matches!(result, Err(e) if e.kind() == CounterErrorKind::InvalidResponseValue));
    }
}
