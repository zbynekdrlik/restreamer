//! Facebook Graph API client for Live Video ingestion-health monitoring (#166).
//!
//! Mirrors `rs-youtube`: a thin read-only client over the Graph API's
//! `live_videos` edge, used to ask Facebook whether it is actually decoding the
//! stream we push. Facebook's RTMP edge silently discards bytes pushed to a
//! persistent key that has no bound `live_video` (it never closes the socket,
//! never errors), so every LOCAL push signal stays green while FB decodes
//! nothing. The only ground truth is asking FB — exactly as YouTube's health
//! column asks the YT Data API.
//!
//! No OAuth/refresh-token flow here: Facebook Page Access Tokens are
//! never-expiring, so the caller passes the token straight through.

use thiserror::Error;

pub mod live_video;

#[derive(Debug, Error)]
pub enum FacebookError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error: {status} - {message}")]
    Api { status: u16, message: String },
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, FacebookError>;

#[cfg(test)]
mod live_video_http_tests;
