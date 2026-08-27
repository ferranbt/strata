use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRequest {
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListResponse {
    pub endpoints: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountsResponse {
    pub mounts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRequest {
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResponse {
    pub schema: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Query {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    pub filter: Option<String>,
    pub fields: Option<String>,
    pub cursor_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadRequest {
    pub path: String,
    #[serde(default)]
    pub query: Query,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor {
    pub next: Option<String>,
    pub total: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResponse {
    pub rows: Vec<Value>,
    pub cursor: Cursor,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRequest {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetResponse {
    pub entity: Option<Value>,
    pub output: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRequest {
    pub path: String,
    pub body: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateResponse {
    pub entity: Option<Value>,
    pub output: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutRequest {
    pub path: String,
    pub rows: Vec<Value>,
    pub schema: Option<Value>,
    pub disposition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutResponse {
    pub output: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipeRequest {
    pub source: String,
    pub destination: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipeResponse {
    pub rows_written: u64,
}
