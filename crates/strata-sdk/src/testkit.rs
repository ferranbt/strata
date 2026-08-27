//! A typed in-process client for exercising a single provider's endpoints in
//! tests. It mounts one [`Provider`] on its own [`Router`] and calls endpoints by
//! path, decoding each [`Response`] into the type you name at the call site — so a
//! test reads like `let r: WriteResult = pg.put("/tables/x", data).await?`.

use futures::StreamExt;
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use strata_schema::Schema;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::provider::Provider;
use crate::record::DataStream;
use crate::router::{Body, Method, Response, Router};

pub struct Client<S> {
    state: Arc<S>,
    router: Router<S>,
}

impl<S: Provider> Client<S> {
    pub fn mount(config: &crate::config::ProviderConfig) -> Result<Self> {
        let state = Arc::new(S::new(config)?);
        let mut router = Router::new();
        S::register(&mut router);
        Ok(Client { state, router })
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let r = self.dispatch(Method::Get, path, None).await?;
        Ok(serde_json::from_value(r.entity.unwrap_or(Value::Null))?)
    }

    pub async fn list<T>(&self, path: &str) -> Result<ListConsumer<T>> {
        let data_stream = self.router.dispatch_read(self.state.clone(), path).await?;
        Ok(ListConsumer {
            data_stream,
            phantom: PhantomData,
        })
    }

    pub async fn create<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: B,
    ) -> Result<T> {
        let body = Body {
            data: None,
            meta: serde_json::to_value(body)?,
        };
        let r = self.dispatch(Method::Create, path, Some(body)).await?;
        Ok(serde_json::from_value(r.entity.unwrap_or(Value::Null))?)
    }

    pub async fn put<T: DeserializeOwned>(&self, path: &str, data: DataStream) -> Result<T> {
        let body = Body {
            data: Some(data),
            meta: Value::Null,
        };
        let r = self.dispatch(Method::Put, path, Some(body)).await?;
        Ok(serde_json::from_value(r.output)?)
    }

    async fn dispatch(&self, method: Method, path: &str, body: Option<Body>) -> Result<Response> {
        self.router
            .dispatch(self.state.clone(), method, path, body)
            .await
    }
}

pub struct ListConsumer<T> {
    data_stream: DataStream,
    phantom: PhantomData<T>,
}

impl<T: DeserializeOwned> ListConsumer<T> {
    pub fn schema(&self) -> &Schema {
        &self.data_stream.schema
    }

    pub async fn consume_all(&mut self) -> anyhow::Result<Vec<T>> {
        let mut got = Vec::new();
        while let Some(page) = self.try_next().await? {
            got.extend(page);
        }
        Ok(got)
    }

    pub async fn next(&mut self) -> anyhow::Result<Vec<T>> {
        self.try_next()
            .await?
            .ok_or_else(|| anyhow!("returned no page"))
    }

    pub async fn try_next(&mut self) -> anyhow::Result<Option<Vec<T>>> {
        let Some(page) = self.data_stream.chunks.next().await.transpose()? else {
            return Ok(None);
        };
        page.data.decode::<T>(self.schema()).map(Some)
    }
}
