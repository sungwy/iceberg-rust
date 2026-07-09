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

//! Bucket-transform conformance: the Appendix B 32-bit hash that bucket[N] builds on.

mod common;

use std::collections::HashMap;

use common::{fixtures, load_jsonl, record};
use iceberg::spec::{Datum, PrimitiveLiteral, Transform};
use iceberg::transform::{BoxedTransformFunction, create_transform_function};
use serde_json::Value;

const BUCKETS: i64 = 2_000_003; // large prime; comparing bucket[N] avoids needing the raw hash API

#[test]
fn bucket_hash() {
    let path = fixtures().join("transforms/bucket/cases.jsonl");
    if !path.exists() {
        eprintln!("iceberg-testing submodule absent; skipping (git submodule update --init)");
        return;
    }
    // iceberg-rust already serializes the minimal-byte decimal, so there are no expected
    // failures here -- including the byte-boundary decimals some implementations get wrong.
    let xfail = HashMap::new();
    let func = create_transform_function(&Transform::Bucket(BUCKETS as u32)).unwrap();

    let mut failures = Vec::new();
    for case in load_jsonl(&path) {
        let id = case["id"].as_str().unwrap();
        record(id, check(&case, &func), &xfail, &mut failures);
    }
    assert!(
        failures.is_empty(),
        "bucket-hash conformance failures:\n{}",
        failures.join("\n")
    );
}

fn check(case: &Value, func: &BoxedTransformFunction) -> Result<(), String> {
    let datum = datum(
        case["type"].as_str().unwrap(),
        case["value"].as_str().unwrap(),
    );
    let result = func
        .transform_literal_result(&datum)
        .map_err(|e| e.to_string())?;
    let got = match result.literal() {
        PrimitiveLiteral::Int(bucket) => i64::from(*bucket),
        other => return Err(format!("bucket result not an int: {other:?}")),
    };
    let expected = (case["hash"].as_i64().unwrap() & 0x7FFF_FFFF) % BUCKETS;
    if got == expected {
        Ok(())
    } else {
        Err(format!("bucket {got} != expected {expected}"))
    }
}

/// Build the typed value the bucket transform hashes from the fixture's string form.
fn datum(type_str: &str, value: &str) -> Datum {
    match type_str {
        "int" => Datum::int(value.parse::<i32>().unwrap()),
        "long" => Datum::long(value.parse::<i64>().unwrap()),
        "string" => Datum::string(value),
        "date" => Datum::date_from_str(value).unwrap(),
        "time" => Datum::time_from_str(value).unwrap(),
        "timestamp" => Datum::timestamp_from_str(value).unwrap(),
        "uuid" => Datum::uuid_from_str(value).unwrap(),
        "binary" => Datum::binary(hex(value)),
        t if t.starts_with("decimal") => Datum::decimal_from_str(value).unwrap(),
        t if t.starts_with("fixed") => Datum::fixed(hex(value)),
        other => panic!("unsupported type {other}"),
    }
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex"))
        .collect()
}
