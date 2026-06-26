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

//! Type-string conformance: parse the input and re-serialize to the canonical schema-JSON form.

mod common;

use std::collections::HashMap;

use common::{fixtures, load_jsonl, record};
use iceberg::spec::PrimitiveType;
use serde_json::Value;

// Cases iceberg-rust does not satisfy yet: geometry/geography are not implemented,
// fixed[...] rejects internal whitespace, and decimal precision > 38 is not rejected.
// (Decimal whitespace, by contrast, is accepted.)
fn expected_failures() -> HashMap<&'static str, &'static str> {
    HashMap::from([
        (
            "geometry-default",
            "geometry type not supported yet; apache/iceberg-rust#1884",
        ),
        (
            "geometry-unquoted-crs",
            "geometry type not supported yet; apache/iceberg-rust#1884",
        ),
        (
            "fixed-space-around",
            "rejects whitespace inside fixed[...] brackets; type-string whitespace, cf. apache/iceberg#16798",
        ),
        (
            "decimal-precision-over-max",
            "does not reject decimal precision > 38 (spec: precision must be 38 or less; Java rejects it)",
        ),
    ])
}

#[test]
fn type_strings() {
    let dir = fixtures().join("types");
    if !dir.exists() {
        eprintln!("iceberg-testing submodule absent; skipping (git submodule update --init)");
        return;
    }
    let xfail = expected_failures();
    let mut failures = Vec::new();
    for kind in ["decimal", "fixed", "geometry"] {
        for case in load_jsonl(&dir.join(kind).join("cases.jsonl")) {
            let id = case["id"].as_str().unwrap();
            record(id, check(&case), &xfail, &mut failures);
        }
    }
    assert!(
        failures.is_empty(),
        "type-string conformance failures:\n{}",
        failures.join("\n")
    );
}

fn check(case: &Value) -> Result<(), String> {
    let input = case["input"].as_str().unwrap();
    let parsed: Result<PrimitiveType, _> = serde_json::from_value(Value::String(input.to_string()));
    if !case["accept"].as_bool().unwrap() {
        return match parsed {
            Ok(t) => Err(format!("expected reject, parsed to {t:?}")),
            Err(_) => Ok(()),
        };
    }
    let parsed = parsed.map_err(|e| format!("expected parse, got error: {e}"))?;
    if let Some(fields) = case.get("parsed") {
        let ok = match &parsed {
            PrimitiveType::Decimal { precision, scale } => {
                u64::from(*precision) == fields["precision"].as_u64().unwrap()
                    && u64::from(*scale) == fields["scale"].as_u64().unwrap()
            }
            PrimitiveType::Fixed(length) => *length == fields["length"].as_u64().unwrap(),
            _ => true,
        };
        if !ok {
            return Err(format!("parsed {parsed:?} != {fields}"));
        }
    }
    // Re-serialize through serde, the schema-JSON form, not Display.
    let canonical = case["canonical"].as_str().unwrap();
    match serde_json::to_value(&parsed).map_err(|e| e.to_string())? {
        Value::String(s) if s == canonical => Ok(()),
        other => Err(format!("serialized {other} != canonical {canonical:?}")),
    }
}
