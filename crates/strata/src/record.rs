//! The Arrow record layer: strata's internal data currency.
//!
//! Inside the framework, data travels as Arrow record batches — never JSON. This
//! module owns the one mapping between the native [`schema::DataType`] and an
//! Arrow [`Schema`] (the `schema` crate itself stays Arrow-agnostic). JSON only
//! appears at named edges outside here: CLI display and JSON-native backends.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow::datatypes::{
    DataType as ArrowType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use schema::{Annotations, DataType, Schema as StrataSchema};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::page::Cursor;

/// A batch of rows in strata's internal Arrow form — the currency that flows
/// reader → writer. `schema` is carried once; `batches` are the rows.
///
/// The two methods here are the *only* sanctioned conversions between Arrow and
/// other representations: [`encode`](Records::encode) (typed handler output →
/// Arrow) and [`to_json_rows`](Records::to_json_rows) (Arrow → JSON, for display
/// only). JSON → Arrow is deliberately absent: it belongs inside the specific
/// JSON-native providers that own that edge, not here.
#[derive(Debug, Clone)]
pub struct Records {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl Records {
    /// Encode typed handler output (`items: &[T]`) into Arrow, driven by the
    /// endpoint's resolved [`Schema`](StrataSchema). The single typed→Arrow site —
    /// used by `list` dispatch so provider structs never travel as JSON.
    pub fn encode<T: Serialize>(schema: StrataSchema, items: &[T]) -> Result<Records> {
        let fields = strata_schema_to_arrow_fields(&schema);
        let field_refs: Vec<FieldRef> = fields.iter().cloned().collect();
        let arrow_schema = Arc::new(Schema::new(fields));
        let arrays = serde_arrow::to_arrow(&field_refs, items)
            .map_err(|e| anyhow!("arrow encode failed: {e}"))?;
        let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)?;
        Ok(Records {
            schema: arrow_schema,
            batches: vec![batch],
        })
    }

    /// Total row count across the batches — the "did this page bring anything?"
    /// signal the sync layer reads to detect the tail of an unbounded (`NextLink`)
    /// walk, where the cursor never goes `None`.
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Decode to JSON rows — the Arrow→JSON **display** edge (e.g. `strata call`
    /// output) and the interim sink bridge. Never call this for internal
    /// transport; that stays Arrow.
    pub fn to_json_rows(&self) -> Result<Vec<Value>> {
        let mut rows = Vec::new();
        for batch in &self.batches {
            let batch_rows: Vec<Value> = serde_arrow::from_record_batch(batch)
                .map_err(|e| anyhow!("arrow decode failed: {e}"))?;
            rows.extend(batch_rows);
        }
        // serde_arrow decodes temporal columns to raw integers (µs / epoch days);
        // render them as ISO strings so they're readable and re-parseable by a
        // sink (a `timestamptz`/`date` column can't ingest a bare integer).
        temporals_to_iso(&self.schema, &mut rows);
        Ok(rows)
    }

    /// Decode the batch's rows into typed values — the Arrow→`T` convenience for
    /// callers (and tests) that know the row type.
    pub fn decode<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        self.to_json_rows()?
            .into_iter()
            .map(|v| Ok(serde_json::from_value(v)?))
            .collect()
    }
}

/// Rewrite `Timestamp`/`Date` columns (decoded as integers) into ISO strings,
/// keyed off the Arrow `schema`.
fn temporals_to_iso(schema: &Schema, rows: &mut [Value]) {
    use chrono::DateTime;
    for field in schema.fields() {
        let to_iso: fn(i64) -> Option<String> = match field.data_type() {
            ArrowType::Timestamp(TimeUnit::Second, _) => {
                |v| DateTime::from_timestamp(v, 0).map(|d| d.to_rfc3339())
            }
            ArrowType::Timestamp(TimeUnit::Millisecond, _) => {
                |v| DateTime::from_timestamp_millis(v).map(|d| d.to_rfc3339())
            }
            ArrowType::Timestamp(TimeUnit::Microsecond, _) => {
                |v| DateTime::from_timestamp_micros(v).map(|d| d.to_rfc3339())
            }
            ArrowType::Timestamp(TimeUnit::Nanosecond, _) => {
                |v| Some(DateTime::from_timestamp_nanos(v).to_rfc3339())
            }
            ArrowType::Date32 => {
                |v| DateTime::from_timestamp(v * 86_400, 0).map(|d| d.date_naive().to_string())
            }
            _ => continue,
        };
        for row in rows.iter_mut() {
            if let Value::Object(map) = row
                && let Some(slot) = map.get_mut(field.name())
                && let Some(ts) = slot.as_i64()
                && let Some(iso) = to_iso(ts)
            {
                *slot = Value::String(iso);
            }
        }
    }
}

/// A page of Arrow rows produced directly by a handler — the return of an
/// Arrow-native `list` (see `Route::list_records`). Used when the row schema is
/// dynamic (e.g. a SQL table's columns), so the provider reads its own schema and
/// builds the batch rather than the framework encoding a static type.
pub struct RecordPage {
    pub data: Records,
    pub cursor: Cursor,
}

/// Prepare a source provider's JSON rows for [`Records::encode`] against
/// `schema`: `String`, `Decimal`, and `Json` columns are stored as Arrow `Utf8`,
/// so a value that arrives as a number/object (e.g. Postgres `numeric` renders as
/// a number, `jsonb` as an object) is stringified to fit its string column.
/// Genuinely nested columns (`List`/`Struct`) map to native Arrow and are left
/// alone. A no-op for drivers that already return these columns as text.
pub fn stringify_text_columns(schema: &StrataSchema, rows: &mut [Value]) {
    for row in rows.iter_mut() {
        let Value::Object(map) = row else { continue };
        for field in schema.fields.iter() {
            let stored_as_text = matches!(
                field.data_type,
                DataType::String | DataType::Decimal | DataType::Json
            );
            if !stored_as_text {
                continue;
            }
            if let Some(value) = map.get_mut(&field.name)
                && !value.is_string()
                && !value.is_null()
            {
                *value = Value::String(value.to_string());
            }
        }
    }
}

fn arrow_type(data_type: &DataType) -> ArrowType {
    match data_type {
        DataType::Bool => ArrowType::Boolean,
        DataType::Int64 => ArrowType::Int64,
        DataType::UInt64 => ArrowType::UInt64,
        DataType::Float64 => ArrowType::Float64,
        DataType::String => ArrowType::Utf8,
        DataType::Bytes => ArrowType::Binary,
        // Native temporal types: serde_arrow parses the string values into these
        // on the way out (and formats them back to strings on `DoPut`).
        DataType::Timestamp => ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Date => ArrowType::Date32,
        // Decimal has no precision/scale in the IR, so it rides as text.
        DataType::Decimal => ArrowType::Utf8,
        // Arbitrary JSON has no fixed Arrow type; carry it as text.
        DataType::Json => ArrowType::Utf8,
        DataType::List(inner) => {
            ArrowType::List(Arc::new(Field::new("item", arrow_type(inner), true)))
        }
        DataType::Struct(fields) => ArrowType::Struct(arrow_fields(fields)),
    }
}

pub fn strata_schema_to_arrow_schema(schema: &StrataSchema) -> Schema {
    let fields = strata_schema_to_arrow_fields(schema);
    Schema::new(fields)
}

fn strata_schema_to_arrow_fields(schema: &StrataSchema) -> Fields {
    arrow_fields(&schema.fields)
}

fn arrow_fields(fields: &[schema::Field]) -> Fields {
    let fields: Vec<FieldRef> = fields
        .iter()
        .map(|f| {
            let mut field = Field::new(&f.name, arrow_type(&f.data_type), f.nullable);
            field.set_metadata(f.annotations.to_map());

            Arc::new(field)
        })
        .collect();
    Fields::from(fields)
}

/// The inverse: an Arrow schema as a native [`Schema`](StrataSchema) — the row
/// `(schema, rows)` shape a writer expects when ingesting an Arrow batch stream
/// (e.g. Flight `DoPut`).
pub fn arrow_schema_to_strata(schema: &Schema) -> StrataSchema {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let mut field = schema::Field::new(
                f.name(),
                data_type_from_arrow(f.data_type()),
                f.is_nullable(),
            );

            field.set_annotations(Annotations::from(f.metadata().clone()));

            field
        })
        .collect();
    StrataSchema::new(fields)
}

fn data_type_from_arrow(ty: &ArrowType) -> DataType {
    match ty {
        ArrowType::Boolean => DataType::Bool,
        ArrowType::Int8 | ArrowType::Int16 | ArrowType::Int32 | ArrowType::Int64 => DataType::Int64,
        ArrowType::UInt8 | ArrowType::UInt16 | ArrowType::UInt32 | ArrowType::UInt64 => {
            DataType::UInt64
        }
        ArrowType::Float16 | ArrowType::Float32 | ArrowType::Float64 => DataType::Float64,
        ArrowType::Utf8 | ArrowType::LargeUtf8 => DataType::String,
        ArrowType::Binary | ArrowType::LargeBinary => DataType::Bytes,
        ArrowType::Timestamp(_, _) => DataType::Timestamp,
        ArrowType::Date32 | ArrowType::Date64 => DataType::Date,
        // Nested/other Arrow types round-trip as opaque JSON.
        _ => DataType::Json,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporal_maps_to_native_arrow_and_parses_strings() {
        let strata = StrataSchema::new(vec![
            schema::Field::new("d", DataType::Date, true),
            schema::Field::new("ts", DataType::Timestamp, true),
        ]);
        let arrow = strata_schema_to_arrow_schema(&strata);
        assert!(matches!(arrow.field(0).data_type(), ArrowType::Date32));
        assert!(matches!(
            arrow.field(1).data_type(),
            ArrowType::Timestamp(_, _)
        ));

        // serde_arrow parses the string values into the native temporal arrays.
        let rows = vec![serde_json::json!({"d": "2025-09-18", "ts": "2025-01-03T12:00:00Z"})];
        let fields: Vec<FieldRef> = arrow.fields().iter().cloned().collect();
        let arrays = serde_arrow::to_arrow(&fields, &rows).expect("parse temporal strings");
        assert_eq!(arrays[0].len(), 1);

        // The reverse maps native Arrow temporal back to the temporal DataTypes.
        let back = arrow_schema_to_strata(&arrow);
        assert_eq!(back.fields[0].data_type, DataType::Date);
        assert_eq!(back.fields[1].data_type, DataType::Timestamp);
    }

    #[test]
    fn encode_then_json_rows_round_trips() {
        let schema = StrataSchema::new(vec![
            schema::Field::new("n", DataType::Int64, false),
            schema::Field::new("s", DataType::String, true),
        ]);
        let items = vec![
            serde_json::json!({"n": 1, "s": "a"}),
            serde_json::json!({"n": 2, "s": null}),
        ];
        let records = Records::encode(schema, &items).unwrap();
        assert_eq!(records.batches[0].num_rows(), 2);
        assert_eq!(records.to_json_rows().unwrap(), items);
    }
}
