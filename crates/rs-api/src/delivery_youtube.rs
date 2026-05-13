//! Per-label YouTube refresh helper used by the health probe.
//! The legacy single-channel `check_youtube_status` is replaced by
//! `youtube::check_all_youtube_status`.

use tracing::warn;

use rs_core::db::youtube_oauth as yo;
use rs_youtube::oauth;

use crate::delivery::DeliveryOrchestrator;

impl DeliveryOrchestrator {
    /// Refresh OAuth tokens for a single label if expired. Returns the access
    /// token. Used by code paths that previously called the deleted
    /// `db::get_youtube_oauth` directly.
    pub async fn refresh_token_for_label(&self, label: &str) -> Option<String> {
        let oauth_row = yo::get_oauth_by_label(self.pool(), label)
            .await
            .ok()
            .flatten()?;
        if oauth_row.refresh_token.is_empty() {
            return None;
        }
        if !oauth::is_token_expired(oauth_row.expires_at.as_deref()) {
            return Some(oauth_row.access_token);
        }
        let tokens = rs_youtube::OAuthTokens {
            access_token: oauth_row.access_token.clone(),
            refresh_token: oauth_row.refresh_token.clone(),
            token_uri: oauth_row.token_uri.clone(),
            client_id: oauth_row.client_id.clone(),
            client_secret: oauth_row.client_secret.clone(),
            scopes: oauth_row.scopes.clone(),
            expires_at: oauth_row.expires_at.clone(),
        };
        let refreshed = match oauth::refresh_access_token(&tokens).await {
            Ok(r) => r,
            Err(e) => {
                warn!("refresh_token_for_label '{label}' failed: {e}");
                return None;
            }
        };
        let new_expires = refreshed.expires_in.map(|s| {
            (chrono::Utc::now() + chrono::Duration::seconds(s)).to_rfc3339()
        });
        if let Err(e) = yo::upsert_oauth_by_label(
            self.pool(),
            label,
            &refreshed.access_token,
            refreshed
                .refresh_token
                .as_deref()
                .unwrap_or(&oauth_row.refresh_token),
            &oauth_row.token_uri,
            &oauth_row.client_id,
            &oauth_row.client_secret,
            &oauth_row.scopes,
            new_expires.as_deref(),
        )
        .await
        {
            warn!("upsert after refresh failed for '{label}': {e}");
        }
        Some(refreshed.access_token)
    }

    /// List YouTube live streams for diagnostics (default label).
    pub async fn list_youtube_streams(
        &self,
    ) -> anyhow::Result<Vec<rs_youtube::streams::LiveStream>> {
        rs_youtube::streams::list_streams_for_label(self.pool(), "default")
            .await
            .map_err(Into::into)
    }

    pub async fn get_broadcast_statuses(&self) -> anyhow::Result<Vec<(String, String)>> {
        let access_token = self
            .refresh_token_for_label("default")
            .await
            .ok_or_else(|| anyhow::anyhow!("No YouTube OAuth tokens for label 'default'"))?;
        rs_youtube::streams::get_broadcast_statuses(&access_token)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_core::config::Config;
    use rs_core::db;

    #[tokio::test]
    async fn refresh_token_for_label_returns_none_when_no_tokens() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let mut config = Config::for_testing();
        config.hetzner.api_token = "test-token".to_string();
        let orch = DeliveryOrchestrator::new(pool, config).unwrap();

        let result = orch.refresh_token_for_label("default").await;
        assert!(result.is_none());
    }
}
