/// YouTube Data API + OAuth2 client.
///
/// Handles OAuth2 authorization flow, token refresh with SQLite persistence,
/// and YouTube Live Streaming API queries.
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod device_flow;
pub mod oauth;
pub mod quota;
pub mod streams;

#[derive(Debug, Error)]
pub enum YouTubeError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("OAuth error: {0}")]
    OAuth(String),
    #[error("token expired and refresh failed: {0}")]
    TokenExpired(String),
    #[error("not authenticated — complete OAuth flow first")]
    NotAuthenticated,
    #[error("API error: {status} - {message}")]
    Api { status: u16, message: String },
    #[error("{0}")]
    Other(String),
    #[error("DB error: {0}")]
    Db(String),
}

pub type Result<T> = std::result::Result<T, YouTubeError>;

/// YouTube OAuth configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct YouTubeConfig {
    pub client_id: String,
    pub client_secret: String,
}

/// Stored OAuth tokens.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub token_uri: String,
    pub client_id: String,
    pub client_secret: String,
    pub scopes: String,
    pub expires_at: Option<String>,
}

#[cfg(test)]
mod device_flow_tests;
#[cfg(test)]
mod quota_tests;
#[cfg(test)]
mod streams_for_label_tests;
#[cfg(test)]
mod streams_pagination_tests;
