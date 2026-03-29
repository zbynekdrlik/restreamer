//! YouTube status checking via the DeliveryOrchestrator.
//! Split from delivery.rs to keep files under 1000 lines.

use tracing::warn;

use rs_core::db;
use rs_youtube::oauth;
use rs_youtube::streams;

use crate::delivery::{DeliveryOrchestrator, YouTubeStatus};

impl DeliveryOrchestrator {
    /// Check YouTube stream receiving status using stored OAuth tokens.
    pub async fn check_youtube_status(&self) -> YouTubeStatus {
        let tokens = match db::get_youtube_oauth(self.pool()).await {
            Ok(Some(t)) => t,
            Ok(None) => {
                return YouTubeStatus {
                    authenticated: false,
                    stream_receiving: None,
                    error: Some("No YouTube OAuth tokens configured".to_string()),
                };
            }
            Err(e) => {
                return YouTubeStatus {
                    authenticated: false,
                    stream_receiving: None,
                    error: Some(format!("DB error: {e}")),
                };
            }
        };

        // Check if token needs refresh
        let access_token = if oauth::is_token_expired(tokens.expires_at.as_deref()) {
            let oauth_tokens = rs_youtube::OAuthTokens {
                access_token: tokens.access_token.clone(),
                refresh_token: tokens.refresh_token.clone(),
                token_uri: tokens.token_uri.clone(),
                client_id: tokens.client_id.clone(),
                client_secret: tokens.client_secret.clone(),
                scopes: tokens.scopes.clone(),
                expires_at: tokens.expires_at.clone(),
            };

            match oauth::refresh_access_token(&oauth_tokens).await {
                Ok(resp) => {
                    let new_expires = resp.expires_in.map(|secs| {
                        (chrono::Utc::now() + chrono::Duration::seconds(secs as i64)).to_rfc3339()
                    });

                    if let Err(e) = db::upsert_youtube_oauth(
                        self.pool(),
                        &resp.access_token,
                        resp.refresh_token
                            .as_deref()
                            .unwrap_or(&tokens.refresh_token),
                        &tokens.token_uri,
                        &tokens.client_id,
                        &tokens.client_secret,
                        &tokens.scopes,
                        new_expires.as_deref(),
                    )
                    .await
                    {
                        warn!("Failed to save refreshed token: {e}");
                    }

                    resp.access_token
                }
                Err(e) => {
                    return YouTubeStatus {
                        authenticated: true,
                        stream_receiving: None,
                        error: Some(format!("Token refresh failed: {e}")),
                    };
                }
            }
        } else {
            tokens.access_token.clone()
        };

        match streams::is_stream_receiving(&access_token).await {
            Ok(receiving) => YouTubeStatus {
                authenticated: true,
                stream_receiving: Some(receiving),
                error: None,
            },
            Err(e) => YouTubeStatus {
                authenticated: true,
                stream_receiving: None,
                error: Some(format!("YouTube API error: {e}")),
            },
        }
    }

    /// List YouTube live streams for diagnostics.
    pub async fn list_youtube_streams(
        &self,
    ) -> anyhow::Result<Vec<rs_youtube::streams::LiveStream>> {
        let tokens = db::get_youtube_oauth(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("No YouTube OAuth tokens"))?;

        let access_token = if oauth::is_token_expired(tokens.expires_at.as_deref()) {
            let oauth_tokens = rs_youtube::OAuthTokens {
                access_token: tokens.access_token.clone(),
                refresh_token: tokens.refresh_token.clone(),
                token_uri: tokens.token_uri.clone(),
                client_id: tokens.client_id.clone(),
                client_secret: tokens.client_secret.clone(),
                scopes: tokens.scopes.clone(),
                expires_at: tokens.expires_at.clone(),
            };
            oauth::refresh_access_token(&oauth_tokens)
                .await?
                .access_token
        } else {
            tokens.access_token
        };

        Ok(streams::list_live_streams(&access_token).await?)
    }

    pub async fn get_broadcast_statuses(&self) -> anyhow::Result<Vec<(String, String)>> {
        let tokens = db::get_youtube_oauth(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("No YouTube OAuth tokens"))?;

        let access_token = if oauth::is_token_expired(tokens.expires_at.as_deref()) {
            let oauth_tokens = rs_youtube::OAuthTokens {
                access_token: tokens.access_token.clone(),
                refresh_token: tokens.refresh_token.clone(),
                token_uri: tokens.token_uri.clone(),
                client_id: tokens.client_id.clone(),
                client_secret: tokens.client_secret.clone(),
                scopes: tokens.scopes.clone(),
                expires_at: tokens.expires_at.clone(),
            };
            oauth::refresh_access_token(&oauth_tokens)
                .await?
                .access_token
        } else {
            tokens.access_token
        };

        Ok(streams::get_broadcast_statuses(&access_token).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_core::config::Config;
    use rs_core::db;

    #[tokio::test]
    async fn youtube_status_no_tokens() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let mut config = Config::for_testing();
        config.hetzner.api_token = "test-token".to_string();
        let orch = DeliveryOrchestrator::new(pool, config).unwrap();

        let status = orch.check_youtube_status().await;
        assert!(!status.authenticated);
        assert!(status.error.is_some());
    }
}
