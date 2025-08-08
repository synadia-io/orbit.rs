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

//! # Orbit.rs nats-extra
//!
//! This crate extends functionality for the [async-nats](https://docs.rs/async-nats)
//! crate. It is more opinionated and versioned separately, allowing for more
//! flexible API development.

#![deny(unreachable_pub)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::private_intra_doc_links)]
#![deny(rustdoc::invalid_codeblock_attributes)]
#![deny(rustdoc::invalid_rust_codeblocks)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

pub mod publisher;
