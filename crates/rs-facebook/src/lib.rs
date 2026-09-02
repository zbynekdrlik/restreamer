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
    /// A transport-level reqwest error, ALREADY stripped of its URL. The Page
    /// Access Token is never sent in the URL (it goes in the `Authorization`
    /// header), but `reqwest::Error`'s `Display` appends the request URL, so we
    /// additionally call `without_url()` before storing the message — a
    /// belt-and-suspenders guarantee the never-expiring token can never reach a
    /// log line (#166 review).
    #[error("HTTP error: {0}")]
    Http(String),
    /// A non-2xx Graph response. `code` is the Graph API `error.code`
    /// (e.g. 190 = invalid/expired token, 10/200-299 = permission) — NOT the
    /// HTTP status, which is almost always 400 for these.
    #[error("API error: http {status} code {code:?} - {message}")]
    Api {
        status: u16,
        code: Option<i64>,
        message: String,
    },
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, FacebookError>;

#[cfg(test)]
mod live_video_http_tests;
