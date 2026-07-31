//! Type conversions between strata and Iceberg: the schema mapping in both
//! directions, plus the Arrow batch alignment the iceberg writer/reader needs.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Schema as ArrowSchema;
use iceberg::spec::{
    ListType, NestedField, NestedFieldRef, PrimitiveType, Schema as IcebergSchema, StructType, Type,
};
use schema::{DataType, Schema};

pub(super) fn strata_to_iceberg_schema(schema: &Schema) -> Result<IcebergSchema> {
    let mut next_id = 0;
    let nested = struct_fields(&schema.fields, &mut next_id);
    Ok(IcebergSchema::builder().with_fields(nested).build()?)
}

fn struct_fields(fields: &[schema::Field], next_id: &mut i32) -> Vec<NestedFieldRef> {
    fields
        .iter()
        .map(|f| {
            *next_id += 1;
            let id = *next_id;
            let ty = iceberg_type(&f.data_type, next_id);
            Arc::new(if f.nullable {
                NestedField::optional(id, &f.name, ty)
            } else {
                NestedField::required(id, &f.name, ty)
            })
        })
        .collect()
}

fn iceberg_type(dt: &DataType, next_id: &mut i32) -> Type {
    match dt {
        DataType::Bool => Type::Primitive(PrimitiveType::Boolean),
        DataType::Int64 | DataType::UInt64 => Type::Primitive(PrimitiveType::Long),
        DataType::Float64 => Type::Primitive(PrimitiveType::Double),
        DataType::String => Type::Primitive(PrimitiveType::String),
        DataType::Bytes => Type::Primitive(PrimitiveType::Binary),
        DataType::Date => Type::Primitive(PrimitiveType::Date),
        DataType::Timestamp => Type::Primitive(PrimitiveType::Timestamptz),
        // No Iceberg equivalent: arbitrary JSON, and a decimal whose precision/scale
        // the strata IR doesn't carry. Both ride as text.
        DataType::Decimal | DataType::Json => Type::Primitive(PrimitiveType::String),
        DataType::List(inner) => {
            *next_id += 1;
            let id = *next_id;
            let element = iceberg_type(inner, next_id);
            Type::List(ListType::new(Arc::new(NestedField::list_element(
                id, element, true,
            ))))
        }
        DataType::Struct(fields) => Type::Struct(StructType::new(struct_fields(fields, next_id))),
    }
}

pub(super) fn iceberg_to_strata_schema(schema: &IcebergSchema) -> Schema {
    Schema::new(strata_fields(schema.as_struct().fields()))
}

fn strata_fields(fields: &[NestedFieldRef]) -> Vec<schema::Field> {
    fields
        .iter()
        .map(|f| schema::Field::new(&f.name, strata_type(&f.field_type), !f.required))
        .collect()
}

fn strata_type(ty: &Type) -> DataType {
    match ty {
        Type::Primitive(PrimitiveType::Boolean) => DataType::Bool,
        Type::Primitive(PrimitiveType::Int | PrimitiveType::Long) => DataType::Int64,
        Type::Primitive(PrimitiveType::Float | PrimitiveType::Double) => DataType::Float64,
        Type::Primitive(PrimitiveType::String) => DataType::String,
        Type::Primitive(PrimitiveType::Binary | PrimitiveType::Fixed(_)) => DataType::Bytes,
        Type::Primitive(PrimitiveType::Date) => DataType::Date,
        Type::Primitive(PrimitiveType::Timestamp | PrimitiveType::Timestamptz) => {
            DataType::Timestamp
        }
        Type::List(list) => DataType::List(Box::new(strata_type(&list.element_field.field_type))),
        Type::Struct(fields) => DataType::Struct(strata_fields(fields.fields())),
        _ => DataType::Json,
    }
}

/// Re-wrap `batch` to the table's Arrow `target` schema: select each column by name
/// in the schema's order and cast it to the target type. Arrays are reused; this
/// only reconciles column order and metadata (field ids, timestamp timezone) so the
/// iceberg writer accepts the batch — no data copy beyond the cheap tz/width fixups.
pub(super) fn align_to(target: &Arc<ArrowSchema>, batch: &RecordBatch) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let column = batch
            .column_by_name(field.name())
            .ok_or_else(|| anyhow!("dataset is missing column `{}`", field.name()))?;
        columns.push(arrow::compute::cast(column, field.data_type())?);
    }
    Ok(RecordBatch::try_new(target.clone(), columns)?)
}
