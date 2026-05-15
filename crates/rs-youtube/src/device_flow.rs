//! Google OAuth 2.0 Device Code Flow (RFC 8628) client.
//! Two HTTP calls: `request_device_code` (operator-facing prompt) and
//! `poll_token` (background poll until grant/deny/expire).

use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeviceCodeResponse {
    pub device_code: String,
    pub user_code: String,
    pub verification_url: String,
    pub expires_in: i64,
    pub interval: i64,
}

#[derive(Debug)]
pub enum PollResponse {
    Pending,
    SlowDown,
    Denied,
    Expired,
    Granted {
        access_token: String,
        refresh_token: String,
        expires_in: Option<i64>,
    },
    Error(String),
}

#[derive(Debug)]
pub enum PollDecision {
    Continue,
    DoubleInterval,
    TerminalDenied,
    TerminalExpired,
    TerminalGranted {
        access_token: String,
        refresh_token: String,
        expires_in: Option<i64>,
    },
    TerminalError(String),
}

#[derive(thiserror::Error, Debug)]
pub enum DeviceFlowError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("response parse error: {0}")]
    Parse(String),
}

pub async fn request_device_code(
    base_url: &str,
    client_id: &str,
    scope: &str,
) -> Result<DeviceCodeResponse, DeviceFlowError> {
    let client = Client::new();
    let resp = client
        .post(format!("{base_url}/device/code"))
        .form(&[("client_id", client_id), ("scope", scope)])
        .send()
        .await?;
    let body = resp.text().await?;
    serde_json::from_str::<DeviceCodeResponse>(&body)
        .map_err(|e| DeviceFlowError::Parse(format!("{e}: body={body}")))
}

#[derive(Deserialize)]
struct TokenSuccess {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: Option<i64>,
}

#[derive(Deserialize)]
struct TokenError {
    error: String,
}

pub async fn poll_token(
    base_url: &str,
    client_id: &str,
    client_secret: &str,
    device_code: &str,
) -> Result<PollResponse, DeviceFlowError> {
    let client = Client::new();
    let resp = client
        .post(format!("{base_url}/token"))
        .form(&[
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("device_code", device_code),
            ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
        ])
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await?;

    if status.is_success() {
        let ts: TokenSuccess = serde_json::from_str(&body)
            .map_err(|e| DeviceFlowError::Parse(format!("{e}: body={body}")))?;
        let refresh_token = ts.refresh_token.ok_or_else(|| {
            DeviceFlowError::Parse("token response missing refresh_token".to_string())
        })?;
        return Ok(PollResponse::Granted {
            access_token: ts.access_token,
            refresh_token,
            expires_in: ts.expires_in,
        });
    }

    match serde_json::from_str::<TokenError>(&body) {
        Ok(te) => match te.error.as_str() {
            "authorization_pending" => Ok(PollResponse::Pending),
            "slow_down" => Ok(PollResponse::SlowDown),
            "access_denied" => Ok(PollResponse::Denied),
            "expired_token" => Ok(PollResponse::Expired),
            other => Ok(PollResponse::Error(other.to_string())),
        },
        Err(_) => Ok(PollResponse::Error(format!("HTTP {status}: {body}"))),
    }
}

pub fn poll_decision(resp: &PollResponse) -> PollDecision {
    match resp {
        PollResponse::Pending => PollDecision::Continue,
        PollResponse::SlowDown => PollDecision::DoubleInterval,
        PollResponse::Denied => PollDecision::TerminalDenied,
        PollResponse::Expired => PollDecision::TerminalExpired,
        PollResponse::Granted {
            access_token,
            refresh_token,
            expires_in,
        } => PollDecision::TerminalGranted {
            access_token: access_token.clone(),
            refresh_token: refresh_token.clone(),
            expires_in: *expires_in,
        },
        PollResponse::Error(e) => PollDecision::TerminalError(e.clone()),
    }
}
