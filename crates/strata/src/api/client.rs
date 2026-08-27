use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{
    CreateResponse, Error, GetResponse, ListResponse, MountsResponse, PipeRequest, PipeResponse,
    PutResponse, Query, ReadResponse, Result, SchemaResponse,
};

pub struct Client {
    base: String,
    http: reqwest::Client,
}

impl Client {
    pub fn new(base: impl Into<String>) -> Self {
        Client {
            base: base.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    pub async fn mounts(&self) -> Result<MountsResponse> {
        self.get(self.url("/v1/mounts")).await
    }

    pub async fn list(&self, path: Option<&str>) -> Result<ListResponse> {
        self.get(self.under("/v1/endpoints", path)).await
    }

    pub async fn schema(&self, path: Option<&str>) -> Result<SchemaResponse> {
        self.get(self.under("/v1/schema", path)).await
    }

    pub async fn read(&self, path: &str, query: &Query) -> Result<ReadResponse> {
        let url = self.under("/v1/data", Some(path));
        self.decode(self.http.get(url).query(query)).await
    }

    pub async fn entity(&self, path: &str) -> Result<GetResponse> {
        self.get(self.under("/v1/entity", Some(path))).await
    }

    pub async fn create(&self, path: &str, body: &Value) -> Result<CreateResponse> {
        let url = self.under("/v1/data", Some(path));
        self.decode(self.http.post(url).json(body)).await
    }

    pub async fn put(
        &self,
        path: &str,
        rows: &[Value],
        schema: Option<&Value>,
        disposition: Option<&str>,
    ) -> Result<PutResponse> {
        let url = self.under("/v1/data", Some(path));
        let body = serde_json::json!({
            "rows": rows,
            "schema": schema,
            "disposition": disposition,
        });
        self.decode(self.http.put(url).json(&body)).await
    }

    pub async fn pipe(&self, source: &str, destination: &str) -> Result<PipeResponse> {
        let request = PipeRequest {
            source: source.to_string(),
            destination: destination.to_string(),
        };
        let url = self.url("/v1/pipes/run");
        self.decode(self.http.post(url).json(&request)).await
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    fn under(&self, base: &str, path: Option<&str>) -> String {
        match path {
            Some(path) => self.url(&format!("{base}/{}", path.trim_start_matches('/'))),
            None => self.url(base),
        }
    }

    async fn get<T: DeserializeOwned>(&self, url: String) -> Result<T> {
        self.decode(self.http.get(url)).await
    }

    async fn decode<T: DeserializeOwned>(&self, request: reqwest::RequestBuilder) -> Result<T> {
        let response = request.send().await.map_err(|e| {
            Error::internal(format!(
                "cannot reach the strata server at {}: {e}",
                self.base
            ))
        })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| Error::internal(e.to_string()))?;
        if !status.is_success() {
            return Err(serde_json::from_str::<Error>(&body)
                .unwrap_or_else(|_| Error::internal(format!("{status}: {body}"))));
        }
        serde_json::from_str(&body)
            .map_err(|e| Error::internal(format!("decoding response: {e}: {body}")))
    }
}
