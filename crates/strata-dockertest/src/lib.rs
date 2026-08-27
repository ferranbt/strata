//! A fluent wrapper over [`testcontainers`] for integration tests.
//!
//! ```no_run
//! # async fn ex() -> anyhow::Result<()> {
//! use strata_dockertest::Dockertest;
//! use std::time::Duration;
//!
//! let ch = Dockertest::image("clickhouse/clickhouse-server")
//!     .tag("latest")
//!     .port(8123)
//!     .retry_interval(Duration::from_millis(500))
//!     .retry_attempts(60)
//!     .retry(|ep| async move { ping(&ep.url("http", 8123)).await })
//!     .build()
//!     .await?;
//!
//! let url = ch.url("http", 8123);
//! # Ok(()) }
//! # async fn ping(_: &str) -> bool { true }
//! ```
//!
//! The container is removed when the returned [`Running`] handle is dropped.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::IntoContainerPort, runners::AsyncRunner,
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
type ReadyCheck = Box<dyn Fn(Endpoint) -> BoxFuture<'static, bool> + Send + Sync>;

/// Where a running container is reachable: its host and the host ports its exposed
/// container ports were mapped to.
#[derive(Clone, Debug)]
pub struct Endpoint {
    host: String,
    ports: HashMap<u16, u16>,
}

impl Endpoint {
    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self, container_port: u16) -> u16 {
        self.ports
            .get(&container_port)
            .copied()
            .unwrap_or_else(|| panic!("container port {container_port} was not exposed"))
    }

    pub fn url(&self, scheme: &str, container_port: u16) -> String {
        format!("{scheme}://{}:{}", self.host, self.port(container_port))
    }
}

/// Fluent builder that starts one throwaway container.
pub struct Dockertest {
    image: String,
    tag: String,
    env: Vec<(String, String)>,
    ports: Vec<u16>,
    ready: Option<ReadyCheck>,
    attempts: usize,
    interval: Duration,
}

impl Dockertest {
    pub fn image(image: impl Into<String>) -> Self {
        Dockertest {
            image: image.into(),
            tag: "latest".into(),
            env: Vec::new(),
            ports: Vec::new(),
            ready: None,
            attempts: 30,
            interval: Duration::from_millis(500),
        }
    }

    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = tag.into();
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// Expose a container port, mapped to a random host port. Repeatable.
    pub fn port(mut self, container_port: u16) -> Self {
        self.ports.push(container_port);
        self
    }

    /// Readiness check, retried until it returns `true` or attempts run out.
    pub fn retry<F, Fut>(mut self, check: F) -> Self
    where
        F: Fn(Endpoint) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        self.ready = Some(Box::new(move |ep| Box::pin(check(ep))));
        self
    }

    pub fn retry_attempts(mut self, attempts: usize) -> Self {
        self.attempts = attempts;
        self
    }

    pub fn retry_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub async fn build(self) -> Result<Running> {
        // SAFETY: set before starting the container; no concurrent readers here.
        unsafe { std::env::set_var("TESTCONTAINERS_RYUK_DISABLED", "true") };

        let mut image = GenericImage::new(&self.image, &self.tag);
        for &p in &self.ports {
            image = image.with_exposed_port(p.tcp());
        }
        let mut request = testcontainers::ContainerRequest::from(image);
        for (k, v) in &self.env {
            request = request.with_env_var(k, v);
        }

        let container = request
            .start()
            .await
            .with_context(|| format!("starting {}:{}", self.image, self.tag))?;

        let host = container
            .get_host()
            .await
            .context("container host")?
            .to_string();
        let mut ports = HashMap::new();
        for &p in &self.ports {
            let mapped = container
                .get_host_port_ipv4(p.tcp())
                .await
                .with_context(|| format!("mapping port {p}"))?;
            ports.insert(p, mapped);
        }
        let endpoint = Endpoint { host, ports };

        if let Some(check) = &self.ready {
            let mut ok = false;
            for _ in 0..self.attempts {
                if check(endpoint.clone()).await {
                    ok = true;
                    break;
                }
                tokio::time::sleep(self.interval).await;
            }
            if !ok {
                bail!(
                    "{}:{} not ready after {} attempts",
                    self.image,
                    self.tag,
                    self.attempts
                );
            }
        }

        Ok(Running {
            _container: container,
            endpoint,
        })
    }
}

/// A live container. Dropping it removes the container.
pub struct Running {
    _container: ContainerAsync<GenericImage>,
    endpoint: Endpoint,
}

impl Running {
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub fn host(&self) -> &str {
        self.endpoint.host()
    }

    pub fn port(&self, container_port: u16) -> u16 {
        self.endpoint.port(container_port)
    }

    pub fn url(&self, scheme: &str, container_port: u16) -> String {
        self.endpoint.url(scheme, container_port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn starts_and_exposes_a_port() -> Result<()> {
        let c = Dockertest::image("nginx")
            .tag("alpine")
            .port(80)
            .retry(|ep| async move {
                tokio::net::TcpStream::connect((ep.host().to_string(), ep.port(80)))
                    .await
                    .is_ok()
            })
            .build()
            .await?;

        assert!(c.port(80) > 0);
        tokio::net::TcpStream::connect((c.host().to_string(), c.port(80))).await?;
        Ok(())
    }
}
