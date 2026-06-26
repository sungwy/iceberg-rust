// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Shared helpers for the iceberg-testing conformance suite.
//!
//! The fixtures are language-neutral: a static input plus the expected result the spec
//! fixes for it, pinned in the `iceberg-testing/` submodule. Each surface has its own test
//! module. Cases this crate does not satisfy yet are listed as expected failures with a
//! tracking issue. The submodule must be checked out (`git submodule update --init`);
//! otherwise the tests skip.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde_json::Value;

/// Root of the pinned fixtures, relative to the iceberg crate.
pub fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../iceberg-testing/table-spec")
}

pub fn load_jsonl(path: &Path) -> Vec<Value> {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("valid JSONL"))
        .collect()
}

/// Record one case against its expected-failure list: a listed case is allowed to fail; any
/// other failure is a real conformance miss.
pub fn record(
    id: &str,
    outcome: Result<(), String>,
    xfail: &HashMap<&str, &str>,
    failures: &mut Vec<String>,
) {
    match (outcome, xfail.get(id)) {
        (Ok(()), None) => {}
        (Ok(()), Some(reason)) => {
            println!("XPASS (remove from expected-failure list): {id} -- {reason}")
        }
        (Err(_), Some(reason)) => println!("expected failure: {id} -- {reason}"),
        (Err(msg), None) => failures.push(format!("{id}: {msg}")),
    }
}
