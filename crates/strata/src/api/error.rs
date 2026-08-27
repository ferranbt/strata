use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    NotFound,
    Invalid,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Error {
    pub kind: Kind,
    pub message: String,
}

impl Error {
    pub fn not_found(message: impl Into<String>) -> Self {
        Error {
            kind: Kind::NotFound,
            message: message.into(),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Error {
            kind: Kind::Invalid,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Error {
            kind: Kind::Internal,
            message: message.into(),
        }
    }

    pub fn status(&self) -> StatusCode {
        match self.kind {
            Kind::NotFound => StatusCode::NOT_FOUND,
            Kind::Invalid => StatusCode::BAD_REQUEST,
            Kind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        let message = format!("{error:#}");
        match message.contains("nothing mounted at") {
            true => Error::not_found(message),
            false => Error::internal(message),
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (self.status(), axum::Json(self)).into_response()
    }
}
