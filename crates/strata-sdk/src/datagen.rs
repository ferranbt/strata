//! Deterministic row generation from a schema.
//!
//! [`Generator`] is pure — `(schema, row index) -> row` — so a page is just a
//! range, with no state and no I/O. Values are **monotonic in the row index**, so a
//! `key` column is unique and a `cursor` column is ascending: the ordering the
//! paging invariant depends on.

use std::ops::Range;

use anyhow::Result;
use strata_schema::{DataType, Field, Schema};
use serde_json::{Map, Value, json};

use crate::record::{Batch, DataStream, stringify_text_columns};

pub struct Generator {
    schema: Schema,
    seed: usize,
}

impl Generator {
    pub fn new(schema: &Schema) -> Result<Self> {
        Ok(Generator {
            schema: schema.clone(),
            seed: 0,
        })
    }

    pub fn seed(mut self, seed: usize) -> Self {
        self.seed = seed;
        self
    }

    fn row(&self, i: usize) -> Value {
        let i = i + self.seed;
        let mut obj = Map::new();
        for field in &self.schema.fields {
            obj.insert(field.name.clone(), value_for(field, i));
        }
        Value::Object(obj)
    }

    /// The rows in `range`, Arrow-encoded — one page.
    pub fn rows(&self, range: Range<usize>) -> Result<Batch> {
        let mut rows: Vec<Value> = range.map(|i| self.row(i)).collect();
        stringify_text_columns(&self.schema, &mut rows);
        Batch::encode(&self.schema, &rows)
    }

    /// The first `n` rows as a single-page [`DataStream`], ready to `put`.
    pub fn stream(&self, n: usize) -> Result<DataStream> {
        Ok(DataStream::once(self.schema.clone(), self.rows(0..n)?))
    }
}

/// A deterministic value for `field` at row `i` — monotonic in `i`, so a `key`
/// column is unique and a `cursor` column is ascending.
fn value_for(field: &Field, i: usize) -> Value {
    value_of(&field.data_type, &field.name, i)
}

/// The same as `value_for` but addressed by type rather than by field, so a `List`'s element and a
/// `Struct`'s members can recurse.
fn value_of(data_type: &DataType, name: &str, i: usize) -> Value {
    match data_type {
        DataType::Bool => json!(i.is_multiple_of(2)),
        DataType::Int64 => json!(i as i64),
        DataType::UInt64 => json!(i as u64),
        DataType::Float64 => json!(i as f64),
        DataType::Decimal => json!(i.to_string()),
        DataType::String => json!(format!("{name}-{i}")),
        DataType::Timestamp => json!(iso_timestamp(i)),
        DataType::Date => json!(iso_date(i)),
        DataType::Json => json!({ "i": i }),
        DataType::List(inner) => json!([value_of(inner, name, i)]),
        DataType::Struct(fields) => Value::Object(
            fields
                .iter()
                .map(|f| (f.name.clone(), value_for(f, i)))
                .collect(),
        ),
        // No deterministic rendering that survives the Arrow `Binary` mapping.
        DataType::Bytes => Value::Null,
    }
}

/// `2025-01-01T00:00:00Z` plus `i` seconds, RFC 3339.
fn iso_timestamp(i: usize) -> String {
    const BASE: i64 = 1_735_689_600; // 2025-01-01T00:00:00Z
    chrono::DateTime::from_timestamp(BASE + i as i64, 0)
        .expect("in range")
        .to_rfc3339()
}

/// `2025-01-01` plus `i` days, `YYYY-MM-DD`.
fn iso_date(i: usize) -> String {
    let base = chrono::NaiveDate::from_ymd_opt(2025, 1, 1).expect("valid");
    (base + chrono::Duration::days(i as i64)).to_string()
}
