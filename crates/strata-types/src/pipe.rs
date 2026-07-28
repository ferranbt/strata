use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    pub mount: String,
    pub path: String,
}

impl Endpoint {
    pub fn new(mount: impl Into<String>, path: impl Into<String>) -> Self {
        Endpoint {
            mount: mount.into(),
            path: path.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pipe {
    #[serde(default = "Uuid::new_v4")]
    pub id: Uuid,
    pub source: Endpoint,
    pub destination: Endpoint,
    #[serde(default)]
    pub cursor: Option<String>,
}

impl Pipe {
    pub fn new(source: Endpoint, destination: Endpoint) -> Self {
        Pipe {
            id: Uuid::new_v4(),
            source,
            destination,
            cursor: None,
        }
    }
}
