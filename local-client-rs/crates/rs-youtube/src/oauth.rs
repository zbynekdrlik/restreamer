/// YouTube OAuth2 flow implementation.

use crate::{OAuthTokens, Result, YouTubeConfig, YouTubeError};
use reqwest::Client;
use serde::{Deserialize, Serialize};

const AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const SCOPES: &str = "https://www.googleapis.com/auth/youtube.readonly";

/// Generate the OAuth2 authorization URL.
pub fn authorization_url(config: &YouTubeConfig, redirect_uri: &str) -> String {
    format!(
        "{AUTH_URL}?client_id={}&redirect_uri={}&response_type=code&scope={}&access_type=offline&prompt=consent",
        urlencoding(&config.client_id),
        urlencoding(redirect_uri),
        urlencoding(SCOPES),
    )
}

fn urlencoding(s: &str) -> String {
    s.replace(':', "%3A")
        .replace('/', "%2F")
        .replace(' ', "+")
        .replace('@', "%40")
}

#[derive(Debug, Serialize)]
struct TokenExchangeRequest<'a> {
    code: &'a str,
    client_id: &'a str,
    client_secret: &'a str,
    redirect_uri: &'a str,
    grant_type: &'a str,
}

#[derive(Debug, Serialize)]
struct RefreshRequest<'a> {
    refresh_token: &'a str,
    client_id: &'a str,
    client_secret: &'a str,
    grant_type: &'a str,
}

#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_in: Option<u64>,
    pub token_type: String,
    pub scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OAuthErrorResponse {
    error: String,
    error_description: Option<String>,
}

/// Exchange an authorization code for tokens.
pub async fn exchange_code(
    config: &YouTubeConfig,
    code: &str,
    redirect_uri: &str,
) -> Result<TokenResponse> {
    let client = Client::new();
    let resp = client
        .post(TOKEN_URL)
        .form(&TokenExchangeRequest {
            code,
            client_id: &config.client_id,
            client_secret: &config.client_secret,
            redirect_uri,
            grant_type: "authorization_code",
        })
        .send()
        .await?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        if let Ok(err) = serde_json::from_str::<OAuthErrorResponse>(&body) {
            return Err(YouTubeError::OAuth(format!(
                "{}: {}",
                err.error,
                err.error_description.unwrap_or_default()
            )));
        }
        return Err(YouTubeError::OAuth(body));
    }

    Ok(resp.json().await?)
}

/// Refresh an expired access token.
pub async fn refresh_access_token(tokens: &OAuthTokens) -> Result<TokenResponse> {
    let client = Client::new();
    let resp = client
        .post(&tokens.token_uri)
        .form(&RefreshRequest {
            refresh_token: &tokens.refresh_token,
            client_id: &tokens.client_id,
            client_secret: &tokens.client_secret,
            grant_type: "refresh_token",
        })
        .send()
        .await?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(YouTubeError::TokenExpired(format!(
            "OAuth refresh failed: {body}. Token may have expired. \
             Re-authorize or move Google Cloud project to Production."
        )));
    }

    Ok(resp.json().await?)
}

/// Check if token is expired based on expires_at timestamp.
pub fn is_token_expired(expires_at: Option<&str>) -> bool {
    match expires_at {
        None => true,
        Some(s) => {
            if let Ok(dt) = s.parse::<chrono::DateTime<chrono::Utc>>() {
                dt < chrono::Utc::now()
            } else {
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_url_format() {
        let config = YouTubeConfig {
            client_id: "test-client-id".to_string(),
            client_secret: "test-value".to_string(),
        };
        let url = authorization_url(&config, "http://localhost:8910/callback");
        assert!(url.contains("test-client-id"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("access_type=offline"));
        assert!(url.contains("prompt=consent"));
    }

    #[test]
    fn is_token_expired_none() {
        assert!(is_token_expired(None));
    }

    #[test]
    fn is_token_expired_past() {
        assert!(is_token_expired(Some("2020-01-01T00:00:00Z")));
    }

    #[test]
    fn is_token_expired_future() {
        assert!(!is_token_expired(Some("2099-01-01T00:00:00Z")));
    }

    #[test]
    fn is_token_expired_invalid() {
        assert!(is_token_expired(Some("not-a-date")));
    }

    #[test]
    fn token_response_deserialize() {
        let json = serde_json::json!({
            "access_token": "test-access-value",
            "refresh_token": "test-refresh-value",
            "expires_in": 3600,
            "token_type": "Bearer",
            "scope": "https://www.googleapis.com/auth/youtube.readonly"
        });
        let resp: TokenResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.access_token, "test-access-value");
        assert_eq!(resp.refresh_token.as_deref(), Some("test-refresh-value"));
        assert_eq!(resp.expires_in, Some(3600));
    }
}
