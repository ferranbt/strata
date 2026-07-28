//! A native, typed schema IR for strata.
//!
//! [`DataType`] is the single source of truth for an endpoint's shape. It is
//! produced from Rust types via [`HasSchema`] (hand-written or, more usually,
//! `#[derive(HasSchema)]`) or built at runtime (e.g. from a SQL catalog). It is
//! deliberately Arrow-agnostic: the crate converts *to* JSON Schema here, and
//! the Flight layer converts `DataType` to Arrow separately, so nothing about
//! the wire format leaks into the model.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

pub use schema_macro::HasSchema;

mod temporal;
pub use temporal::{Date, Timestamp};

/// An ordered set of string key/value annotations carried on a [`Field`] — the
/// normalization metadata strata layers onto a schema.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Annotations(BTreeMap<String, String>);

impl Annotations {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn to_map(&self) -> HashMap<String, String> {
        self.0.clone().into_iter().collect()
    }

    /// Set annotation `key` to `val`, builder-style in place.
    fn annotate(&mut self, key: impl Into<String>, val: impl Into<String>) -> &mut Self {
        self.0.insert(key.into(), val.into());
        self
    }

    /// Whether annotation `key` is present.
    fn has(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
}

impl From<HashMap<String, String>> for Annotations {
    fn from(map: HashMap<String, String>) -> Self {
        Self(map.into_iter().collect())
    }
}

/// The type of a value an endpoint returns. Serializable so a schema can travel
/// alongside data (e.g. piping a reader's `(schema, rows)` into a writer).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Bool,
    Int64,
    UInt64,
    Float64,
    /// An exact decimal number, carried as a string to preserve precision.
    Decimal,
    String,
    Bytes,
    /// An RFC 3339 date-time, carried as its string form.
    Timestamp,
    /// A calendar date (`YYYY-MM-DD`), carried as its string form.
    Date,
    /// Arbitrary JSON whose shape isn't statically known (e.g. a `jsonb` column,
    /// or `serde_json::Value`).
    Json,
    List(Box<DataType>),
    Struct(Vec<Field>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn empty() -> Self {
        Self { fields: vec![] }
    }

    pub fn to_datatype(&self) -> DataType {
        DataType::Struct(self.fields.clone())
    }

    pub fn to_json_schema(&self) -> Value {
        to_json_schema(&self.to_datatype())
    }

    pub fn get_key_fields(&self) -> Vec<String> {
        self.fields
            .iter()
            .filter(|val| val.is_key())
            .map(|val| val.name.clone())
            .collect()
    }
}

/// One field of a [`DataType::Struct`].
///
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Annotations::is_empty")]
    pub annotations: Annotations,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl Field {
    /// Annotation key marking a field as part of the row's merge/upsert key.
    pub const KEY: &'static str = "key";
    /// Annotation key marking a field as the watermark column for pagination.
    pub const CURSOR: &'static str = "cursor";

    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.into(),
            data_type,
            nullable,
            annotations: Annotations::default(),
            description: None,
        }
    }

    pub fn set_annotations(&mut self, annotations: Annotations) {
        self.annotations = annotations;
    }

    pub fn annotate(&mut self, key: impl Into<String>, val: impl Into<String>) {
        self.annotations.annotate(key, val);
    }

    pub fn is_key(&self) -> bool {
        self.annotations.has(Self::KEY)
    }

    pub fn is_cursor(&self) -> bool {
        self.annotations.has(Self::CURSOR)
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// A Rust type that describes itself as a [`Schema`] — an entity/row shape. Derived
/// for structs with `#[derive(HasSchema)]`.
pub trait HasSchema {
    fn schema() -> Schema;
}

/// A type that maps to a single [`DataType`] — the cell type of a field. Implemented
/// by the primitive and wrapper types below, and (via the derive) by every
/// [`HasSchema`] entity, so a struct can nest as a field's `DataType::Struct`.
pub trait HasDataType {
    fn data_type() -> DataType;
}

macro_rules! leaf {
    ($($t:ty => $dt:expr),* $(,)?) => {
        $(impl HasDataType for $t { fn data_type() -> DataType { $dt } })*
    };
}

leaf! {
    bool => DataType::Bool,
    i8 => DataType::Int64, i16 => DataType::Int64, i32 => DataType::Int64, i64 => DataType::Int64, isize => DataType::Int64,
    u8 => DataType::UInt64, u16 => DataType::UInt64, u32 => DataType::UInt64, u64 => DataType::UInt64, usize => DataType::UInt64,
    f32 => DataType::Float64, f64 => DataType::Float64,
    String => DataType::String, str => DataType::String,
    serde_json::Value => DataType::Json,
}

/// An exact decimal number carried as its string form (to preserve precision).
/// Schema type [`DataType::Decimal`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Decimal(pub String);

impl HasDataType for Decimal {
    fn data_type() -> DataType {
        DataType::Decimal
    }
}

impl<T: HasDataType> HasDataType for Vec<T> {
    fn data_type() -> DataType {
        DataType::List(Box::new(T::data_type()))
    }
}

impl<T: HasDataType> HasDataType for Option<T> {
    // Field-level nullability is set by the derive when it sees `Option<_>`;
    // this impl exists for `Option` used inside other generics.
    fn data_type() -> DataType {
        T::data_type()
    }
}

impl<T: HasDataType + ?Sized> HasDataType for &T {
    fn data_type() -> DataType {
        T::data_type()
    }
}

/// Render a [`DataType`] as a JSON Schema (draft 2020-12) value, for the
/// `strata schema` introspection output.
pub fn to_json_schema(data_type: &DataType) -> Value {
    match data_type {
        DataType::Bool => json!({ "type": "boolean" }),
        DataType::Int64 => json!({ "type": "integer", "format": "int64" }),
        DataType::UInt64 => json!({ "type": "integer", "format": "uint64", "minimum": 0 }),
        DataType::Float64 => json!({ "type": "number" }),
        DataType::Decimal => json!({ "type": "string", "format": "decimal" }),
        DataType::String => json!({ "type": "string" }),
        DataType::Bytes => json!({ "type": "string", "contentEncoding": "base64" }),
        DataType::Timestamp => json!({ "type": "string", "format": "date-time" }),
        DataType::Date => json!({ "type": "string", "format": "date" }),
        DataType::Json => json!(true),
        DataType::List(item) => json!({ "type": "array", "items": to_json_schema(item) }),
        DataType::Struct(fields) => {
            let mut properties = Map::new();
            let mut required = Vec::new();
            for field in fields {
                let mut schema = to_json_schema(&field.data_type);
                if field.nullable {
                    schema = json!({ "anyOf": [schema, { "type": "null" }] });
                } else {
                    required.push(Value::String(field.name.clone()));
                }
                if let Some(description) = &field.description {
                    match &mut schema {
                        Value::Object(map) => {
                            map.insert("description".to_string(), json!(description));
                        }
                        // `Json` renders as bare `true`; wrap it so the
                        // description isn't lost.
                        other => *other = json!({ "description": description }),
                    }
                }
                properties.insert(field.name.clone(), schema);
            }
            json!({ "type": "object", "properties": properties, "required": required })
        }
    }
}

/// Fluent schema construction: declare columns, mark the key and cursor.
///
/// ```ignore
/// let schema = Shape::new()
///     .column("id", DataType::Int64).key()
///     .column("created_at", DataType::Timestamp).cursor()
///     .build();
/// ```
#[derive(Default)]
pub struct SchemaBuilder {
    fields: Vec<Field>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        SchemaBuilder::default()
    }

    /// Append a (non-nullable) column.
    pub fn column(mut self, name: &str, data_type: DataType) -> Self {
        self.fields.push(Field::new(name, data_type, false));
        self
    }

    /// Mark the most-recently-added column as the row key.
    pub fn key(mut self) -> Self {
        self.mark(Field::KEY);
        self
    }

    /// Mark the most-recently-added column as the pagination cursor.
    pub fn cursor(mut self) -> Self {
        self.mark(Field::CURSOR);
        self
    }

    fn mark(&mut self, annotation: &str) {
        if let Some(field) = self.fields.last_mut() {
            field.annotations.annotate(annotation, "true");
        }
    }

    /// The row schema: `List<Struct<..>>` with the declared annotations.
    pub fn build(&self) -> Schema {
        Schema::new(self.fields.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_schema_of_struct() {
        let dt = DataType::Struct(vec![
            Field::new("id", DataType::UInt64, false).with_description("unique id"),
            Field::new("name", DataType::String, true).with_description("display name"),
            Field::new("tags", DataType::List(Box::new(DataType::String)), false),
        ]);
        let js = to_json_schema(&dt);
        assert_eq!(js["type"], "object");
        assert_eq!(js["properties"]["id"]["format"], "uint64");
        assert_eq!(js["properties"]["id"]["description"], "unique id");
        assert!(js["properties"]["name"]["anyOf"].is_array());
        // Description rides alongside the nullable `anyOf` wrapper.
        assert_eq!(js["properties"]["name"]["description"], "display name");
        assert_eq!(js["properties"]["tags"]["type"], "array");
        assert!(js["properties"]["tags"].get("description").is_none());
        assert_eq!(js["required"], json!(["id", "tags"]));
    }
}
