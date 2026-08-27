use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use arrow::datatypes::{DataType as ArrowType, Field, FieldRef, Fields, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_array::Array;
use arrow_schema::Schema;
use futures::stream::{BoxStream, StreamExt};
use strata_schema::{Annotations, DataType, HasSchema, Schema as StrataSchema};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::page::Cursor;

pub struct DataStream {
    pub schema: StrataSchema,
    pub chunks: BoxStream<'static, Result<BatchPage>>,
}

impl DataStream {
    pub async fn first(mut self) -> Result<Option<BatchPage>> {
        self.chunks.next().await.transpose()
    }

    /// A stream of exactly one page — the shape a caller that already has all the
    /// rows in hand writes into a sink.
    pub fn once(schema: StrataSchema, data: Batch) -> DataStream {
        let page = BatchPage { data, cursor: None };
        DataStream {
            schema,
            chunks: futures::stream::once(async move { Ok(page) }).boxed(),
        }
    }

    /// One page built from typed `rows`: the schema is `T`'s, with whatever
    /// `#[schema(key)]` annotations it declares, and the rows are encoded against it.
    pub fn of<T: Serialize + HasSchema>(rows: &[T]) -> Result<DataStream> {
        let schema = T::schema();
        let data = Batch::encode(&schema, rows)?;
        Ok(DataStream::once(schema, data))
    }
}
pub struct BatchPage {
    pub data: Batch,
    pub cursor: Option<Cursor>,
}

#[derive(Debug, Clone)]
pub struct Batch {
    columns: Vec<Arc<dyn Array>>,
}

impl Batch {
    pub fn to_json_rows(&self, schema: &StrataSchema) -> Result<Vec<Value>> {
        let arrow_schema = strata_schema_to_arrow_schema(schema);
        let mut rows: Vec<Value> = serde_arrow::from_arrow(arrow_schema.fields(), &self.columns)
            .map_err(|e| anyhow!("arrow decode failed: {e}"))?;

        // serde_arrow decodes temporal columns to raw integers (µs / epoch days);
        // render them as ISO strings so they're readable and re-parseable by a
        // sink (a `timestamptz`/`date` column can't ingest a bare integer).
        temporals_to_iso(&arrow_schema, &mut rows);
        Ok(rows)
    }

    pub fn decode<T: DeserializeOwned>(&self, schema: &StrataSchema) -> Result<Vec<T>> {
        self.to_json_rows(schema)?
            .into_iter()
            .map(|row| Ok(serde_json::from_value(row)?))
            .collect()
    }

    pub fn encode<T: Serialize>(schema: &StrataSchema, items: &[T]) -> Result<Batch> {
        let fields = strata_schema_to_arrow_fields(schema);
        let columns = serde_arrow::to_arrow(&fields, items)
            .map_err(|e| anyhow!("arrow encode failed: {e}"))?;
        Ok(Batch { columns })
    }

    pub fn row_count(&self) -> usize {
        self.columns.first().map(|c| c.len()).unwrap_or(0)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Batch {
        Batch {
            columns: batch.columns().to_vec(),
        }
    }

    pub fn to_record_batch(&self, schema: &StrataSchema) -> Result<RecordBatch> {
        let arrow_schema = Arc::new(strata_schema_to_arrow_schema(schema));
        Ok(RecordBatch::try_new(arrow_schema, self.columns.clone())?)
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

fn arrow_fields(fields: &[strata_schema::Field]) -> Fields {
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
            let mut field = strata_schema::Field::new(
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

/// How a sink should apply a written dataset. Rides as metadata on the existing
/// `put` verb (the reserved `disposition` query param) rather than a new verb, so
/// every sink shares one write surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub enum Disposition {
    /// Insert every row (the historical behavior). Re-running adds duplicates.
    #[default]
    Append,
    /// Idempotent write-by-key: upsert each row on the dataset's key fields
    /// (insert, or update the non-key columns on conflict). Requires the schema
    /// to declare a key; this is what makes a re-fetching pipe dedup itself.
    Merge,
}

impl Disposition {
    /// The reserved `put` query param that selects the disposition.
    pub const PARAM: &str = "disposition";

    /// Parse the param value (`append` | `merge`/`upsert`); defaults to `Append`
    /// when absent.
    pub fn from_param(value: Option<&str>) -> Result<Self> {
        match value {
            None | Some("append") => Ok(Disposition::Append),
            Some("merge") | Some("upsert") => Ok(Disposition::Merge),
            Some(other) => bail!("unknown write disposition `{other}` (append|merge)"),
        }
    }

    /// The param value (inverse of [`from_param`](Self::from_param)).
    pub fn as_param(self) -> &'static str {
        match self {
            Disposition::Append => "append",
            Disposition::Merge => "merge",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporal_maps_to_native_arrow_and_parses_strings() {
        let strata = StrataSchema::new(vec![
            strata_schema::Field::new("d", DataType::Date, true),
            strata_schema::Field::new("ts", DataType::Timestamp, true),
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
            strata_schema::Field::new("n", DataType::Int64, false),
            strata_schema::Field::new("s", DataType::String, true),
        ]);
        let items = vec![
            serde_json::json!({"n": 1, "s": "a"}),
            serde_json::json!({"n": 2, "s": null}),
        ];
        let batch = Batch::encode(&schema, &items).unwrap();
        assert_eq!(batch.row_count(), 2);
        assert_eq!(batch.to_json_rows(&schema).unwrap(), items);
    }
}
