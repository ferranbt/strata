//! GraphQL query surface over the registry.
//!
//! A read-only consumer of [`Registry`]/[`ProviderObject`], like [`flight`](crate::flight):
//! it builds a dynamic schema from every `queryable` list endpoint and resolves
//! fields by calling `read` through the router. No provider or SQL knowledge lives
//! here; a table is discovered by listing `/tables` and reading its resolved schema.
//!
//! Each table becomes a query field returning `[Row]`. Args: `where` (a JSON
//! predicate compiled to the read's `?filter=`) and `limit`. The requested columns
//! come from the selection set and drive `?fields=` (projection). One page is
//! served per query (the first chunk).

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use async_graphql::Value as GqlValue;
use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object, Scalar, Schema, TypeRef,
};
use async_graphql_axum::GraphQL;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use schema::{DataType, Schema as StrataSchema};
use serde_json::Value;

use crate::Registry;

/// Custom scalars: `Long` for 64-bit ints (GraphQL `Int` is 32-bit), `JSON` for
/// nested/opaque values and the `where` predicate.
const LONG: &str = "Long";
const JSON: &str = "JSON";

/// Serve the GraphQL schema over HTTP: `POST /graphql` runs queries, `GET /graphql`
/// is the GraphiQL explorer, `GET /schema` returns the SDL (every type and field).
pub async fn serve(registry: Arc<Registry>, addr: std::net::SocketAddr) -> Result<()> {
    let schema = build_schema(&registry).await?;
    let sdl = schema.sdl();
    let app = axum::Router::new()
        .route("/graphql", get(graphiql).post_service(GraphQL::new(schema)))
        .route("/schema", get(move || std::future::ready(sdl.clone())));
    tracing::info!("strata GraphQL server on http://{addr}/graphql (SDL at /schema)");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn graphiql() -> impl IntoResponse {
    Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint("/graphql")
            .finish(),
    )
}

/// Build the dynamic schema: one query field per queryable table across all mounts.
/// Snapshotted at startup (tables added later need a rebuild).
async fn build_schema(registry: &Arc<Registry>) -> Result<Schema> {
    let mut tables_meta = Vec::new();
    for mount in registry.names() {
        for table in tables(registry, &mount).await {
            let path = format!("/tables/{table}");
            let provider = registry.get(&mount)?;
            let Ok(endpoint) = provider.resolve(&path).await else {
                continue;
            };
            if !endpoint.metadata.queryable {
                continue;
            }
            let type_name = format!("{mount}_{table}");
            tables_meta.push((mount.clone(), table, type_name, endpoint.response));
        }
    }
    let known: HashSet<String> = tables_meta.iter().map(|(_, _, t, _)| t.clone()).collect();

    let mut query = Object::new("Query");
    let mut objects = Vec::new();
    for (mount, table, type_name, row_schema) in &tables_meta {
        objects.push(row_object(
            type_name,
            row_schema,
            registry.clone(),
            mount.clone(),
            &known,
        ));
        query = query.field(table_field(
            type_name,
            registry.clone(),
            mount.clone(),
            table.clone(),
            row_schema,
        ));
    }

    let mut builder = Schema::build("Query", None, None)
        .register(Scalar::new(LONG))
        .register(Scalar::new(JSON))
        .register(query);
    for object in objects {
        builder = builder.register(object);
    }
    Ok(builder.finish()?)
}

/// The table names of a mount that exposes `/tables`, or empty if it has none.
async fn tables(registry: &Arc<Registry>, mount: &str) -> Vec<String> {
    let Ok(provider) = registry.get(mount) else {
        return Vec::new();
    };
    let Ok(stream) = provider.read("/tables").await else {
        return Vec::new();
    };
    let schema = stream.schema.clone();
    let Ok(Some(chunk)) = stream.first().await else {
        return Vec::new();
    };
    chunk
        .data
        .to_json_rows(&schema)
        .unwrap_or_default()
        .iter()
        .filter_map(|row| row.get("name")?.as_str().map(String::from))
        .collect()
}

/// A GraphQL object type for a row: one field per column, each reading its value
/// out of the parent JSON row. All fields nullable (providers return nulls freely).
/// A column with a `ref` annotation also gets a nested field pointing at the
/// referenced table's type, resolved by a filtered read.
fn row_object(
    type_name: &str,
    row_schema: &StrataSchema,
    registry: Arc<Registry>,
    mount: String,
    known: &HashSet<String>,
) -> Object {
    let mut object = Object::new(type_name);
    for field in &row_schema.fields {
        let key = field.name.clone();
        object = object.field(Field::new(
            field.name.clone(),
            type_ref(&field.data_type),
            move |ctx| {
                let key = key.clone();
                FieldFuture::new(async move {
                    let row = ctx.parent_value.try_downcast_ref::<Value>()?;
                    match row.get(&key) {
                        None | Some(Value::Null) => Ok(None),
                        Some(value) => Ok(Some(FieldValue::value(
                            GqlValue::from_json(value.clone()).unwrap_or(GqlValue::Null),
                        ))),
                    }
                })
            },
        ));

        if let Some((target_table, target_col)) = field.ref_target().and_then(|t| t.split_once('.'))
            && known.contains(&format!("{mount}_{target_table}"))
        {
            object = object.field(nested_field(
                &field.name,
                &format!("{mount}_{target_table}"),
                target_table,
                target_col,
                registry.clone(),
                mount.clone(),
            ));
        }
    }
    object
}

fn nested_field(
    column: &str,
    target_type: &str,
    target_table: &str,
    target_col: &str,
    registry: Arc<Registry>,
    mount: String,
) -> Field {
    let name = nested_name(column);
    let column = column.to_string();
    let target_table = target_table.to_string();
    let target_col = target_col.to_string();
    Field::new(name, TypeRef::named(target_type), move |ctx| {
        let registry = registry.clone();
        let mount = mount.clone();
        let column = column.clone();
        let target_table = target_table.clone();
        let target_col = target_col.clone();
        FieldFuture::new(async move {
            let row = ctx.parent_value.try_downcast_ref::<Value>()?;
            let fk = match row.get(&column) {
                Some(value) if !value.is_null() => value.clone(),
                _ => return Ok(None),
            };
            let filter =
                serde_json::json!({ "cmp": { "field": target_col, "op": "eq", "value": fk } });
            let encoded = urlencoding::encode(&filter.to_string()).into_owned();
            let path = format!("/tables/{target_table}?filter={encoded}&limit=1");
            let stream = registry.get(&mount)?.read(&path).await?;
            let schema = stream.schema.clone();
            let rows = match stream.first().await? {
                Some(chunk) => chunk.data.to_json_rows(&schema)?,
                None => Vec::new(),
            };
            Ok(rows.into_iter().next().map(FieldValue::owned_any))
        })
    })
}

/// The `Query.<mount>_<table>` field: read one page, honoring `where` and `limit`,
/// projecting to the selected columns.
fn table_field(
    type_name: &str,
    registry: Arc<Registry>,
    mount: String,
    table: String,
    row_schema: &StrataSchema,
) -> Field {
    let field_name = type_name.to_string();
    let columns: HashSet<String> = row_schema.fields.iter().map(|f| f.name.clone()).collect();
    let rel_to_fk: std::collections::HashMap<String, String> = row_schema
        .fields
        .iter()
        .filter(|f| f.ref_target().is_some())
        .map(|f| (nested_name(&f.name), f.name.clone()))
        .collect();
    Field::new(field_name, TypeRef::named_nn_list(type_name), move |ctx| {
        let registry = registry.clone();
        let (mount, table) = (mount.clone(), table.clone());
        let (columns, rel_to_fk) = (columns.clone(), rel_to_fk.clone());
        FieldFuture::new(async move {
            let mut fields: Vec<String> = Vec::new();
            for selected in ctx.field().selection_set() {
                let name = selected.name();
                if columns.contains(name) {
                    fields.push(name.to_string());
                } else if let Some(fk) = rel_to_fk.get(name)
                    && !fields.contains(fk)
                {
                    fields.push(fk.clone());
                }
            }

            let filter = ctx
                .args
                .get("where")
                .map(|v| v.deserialize::<Value>())
                .transpose()?;
            let limit = ctx.args.get("limit").and_then(|v| v.u64().ok());

            let path = read_path(&table, filter.as_ref(), &fields, limit);
            let provider = registry.get(&mount)?;
            let stream = provider.read(&path).await?;
            let schema = stream.schema.clone();
            let rows = match stream.first().await? {
                Some(chunk) => chunk.data.to_json_rows(&schema)?,
                None => Vec::new(),
            };
            Ok(Some(FieldValue::list(
                rows.into_iter().map(FieldValue::owned_any),
            )))
        })
    })
    .argument(InputValue::new("where", TypeRef::named(JSON)))
    .argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
}

fn nested_name(column: &str) -> String {
    column
        .strip_suffix("_id")
        .filter(|s| !s.is_empty())
        .map_or_else(|| format!("{column}_ref"), String::from)
}

/// `/tables/<table>` with `filter`/`fields`/`limit` query params set.
fn read_path(table: &str, filter: Option<&Value>, fields: &[String], limit: Option<u64>) -> String {
    let mut params: Vec<(String, String)> = Vec::new();
    if let Some(filter) = filter {
        params.push(("filter".into(), filter.to_string()));
    }
    if !fields.is_empty() {
        params.push(("fields".into(), fields.join(",")));
    }
    if let Some(limit) = limit {
        params.push(("limit".into(), limit.to_string()));
    }
    match serde_urlencoded::to_string(&params) {
        Ok(q) if !q.is_empty() => format!("/tables/{table}?{q}"),
        _ => format!("/tables/{table}"),
    }
}

/// Map a [`DataType`] to a GraphQL output type. Integers use `Long`; temporals and
/// decimals ride as `String`; nested/opaque values as `JSON`.
fn type_ref(data_type: &DataType) -> TypeRef {
    let name = match data_type {
        DataType::Bool => TypeRef::BOOLEAN,
        DataType::Int64 | DataType::UInt64 => LONG,
        DataType::Float64 => TypeRef::FLOAT,
        DataType::String
        | DataType::Decimal
        | DataType::Timestamp
        | DataType::Date
        | DataType::Bytes => TypeRef::STRING,
        DataType::Json | DataType::List(_) | DataType::Struct(_) => JSON,
    };
    TypeRef::named(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProviderConfig;
    use crate::pipe::{run_pass, store::NoPipeStore};
    use crate::providers::dummy::Dummy;
    use crate::providers::sqlite::Sqlite;
    use schema::{DataType, HasSchema, SchemaBuilder};
    use serde_json::json;
    use strata_types::{Endpoint, Pipe};

    #[tokio::test]
    async fn queries_a_piped_sqlite_table() -> Result<()> {
        // TODO: Improve this setup
        let db_path = std::env::temp_dir().join("strata_graphql_test.sqlite");
        let _ = std::fs::remove_file(&db_path);

        let mut registry = Registry::new();
        let dummy_cfg: ProviderConfig = serde_json::from_value(json!({ "backend": "dummy" }))?;
        registry.mount::<Dummy>("dummy", &dummy_cfg)?;
        let sqlite_cfg: ProviderConfig = serde_json::from_value(
            json!({ "backend": "sqlite", "path": db_path.to_str().unwrap() }),
        )?;
        registry.mount::<Sqlite>("local", &sqlite_cfg)?;

        let row_schema = SchemaBuilder::new()
            .column("id", DataType::Int64)
            .key()
            .column("name", DataType::String)
            .build();
        let encoded = urlencoding::encode(&serde_json::to_string(&row_schema)?).into_owned();
        let mut pipe = Pipe::new(
            Endpoint::new("dummy", format!("/data?schema={encoded}&rows=25")),
            Endpoint::new("local", "/tables/bench"),
        );
        run_pass(&registry, &NoPipeStore, &mut pipe).await?;

        let registry = Arc::new(registry);
        let schema = build_schema(&registry).await?;

        let run = |query: &'static str| {
            let schema = schema.clone();
            async move {
                let response = schema.execute(query).await;
                assert!(
                    response.errors.is_empty(),
                    "graphql errors: {:?}",
                    response.errors
                );
                response.data.into_json().unwrap()
            }
        };

        // Projection: selecting only `id` returns objects with just that key.
        let projected = run("{ local_bench(limit: 25) { id } }").await;
        let rows = projected["local_bench"].as_array().unwrap();
        assert_eq!(rows.len(), 25);
        assert!(
            rows.iter()
                .all(|r| r.as_object().unwrap().keys().eq(["id"]))
        );

        // Filter: `id >= 20` returns a strict, non-empty subset of the 25 rows.
        let filtered = run(
            "{ local_bench(where: {cmp: {field: \"id\", op: \"gte\", value: 20}}, limit: 25) { id name } }",
        )
        .await;
        let matched = filtered["local_bench"].as_array().unwrap().len();
        assert!(
            (1..25).contains(&matched),
            "filter should narrow the set, got {matched}"
        );

        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    struct StaticSource(std::collections::HashMap<String, StrataSchema>);

    impl crate::router::SchemaSource for StaticSource {
        fn schema(
            &self,
            path: String,
        ) -> crate::router::BoxFuture<'static, Result<Option<StrataSchema>>> {
            let found = self.0.get(&path).cloned();
            Box::pin(async move { Ok(found) })
        }
    }

    async fn put<T: serde::Serialize + schema::HasSchema>(
        registry: &Registry,
        path: &str,
        rows: &[T],
    ) -> Result<()> {
        let body = crate::Body {
            data: Some(crate::DataStream::of(rows)?),
            meta: Value::Null,
        };
        registry
            .get("local")?
            .invoke(crate::Method::Put, path, Some(body))
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn resolves_a_declared_relationship() -> Result<()> {
        #[derive(serde::Serialize, schema::HasSchema)]
        struct Customer {
            #[schema(key)]
            id: i64,
            name: String,
        }
        #[derive(serde::Serialize, schema::HasSchema)]
        struct Order {
            #[schema(key)]
            id: i64,
            customer_id: i64,
        }
        #[derive(serde::Serialize, schema::HasSchema)]
        struct OrderRel {
            #[schema(key)]
            id: i64,
            #[schema(references = "customers.id")]
            customer_id: i64,
        }

        let db_path = std::env::temp_dir().join("strata_graphql_rel.sqlite");
        let _ = std::fs::remove_file(&db_path);

        let mut registry = Registry::new();
        let cfg: ProviderConfig = serde_json::from_value(
            json!({ "backend": "sqlite", "path": db_path.to_str().unwrap() }),
        )?;
        registry.mount::<Sqlite>("local", &cfg)?;

        put(
            &registry,
            "/tables/customers",
            &[Customer { id: 1, name: "acme".into() }],
        )
        .await?;
        put(
            &registry,
            "/tables/orders",
            &[Order { id: 10, customer_id: 1 }],
        )
        .await?;

        let mut declared = std::collections::HashMap::new();
        declared.insert("/tables/orders".to_string(), OrderRel::schema());
        registry.set_schema_source("local", Arc::new(StaticSource(declared)))?;

        let registry = Arc::new(registry);
        let schema = build_schema(&registry).await?;

        let response = schema
            .execute("{ local_orders { id customer { id name } } }")
            .await;
        assert!(response.errors.is_empty(), "graphql errors: {:?}", response.errors);
        let data = response.data.into_json().unwrap();
        let order = &data["local_orders"][0];
        assert_eq!(order["id"], 10);
        assert_eq!(order["customer"]["id"], 1);
        assert_eq!(order["customer"]["name"], "acme");

        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }
}
