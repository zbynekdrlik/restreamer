//! Multi-account YouTube OAuth ops. Each grant is keyed by a unique `label`.
//!
//! The single-row legacy ops in `db::v2` (`get_youtube_oauth`,
//! `upsert_youtube_oauth`) keep working as the `label = 'default'` path.

use crate::error::Result;
use crate::models::YouTubeOAuth;
use sqlx::Row;
use sqlx::sqlite::SqlitePool;

const SELECT_COLS: &str = "id, label, access_token, refresh_token, token_uri, client_id, client_secret, scopes, \
     expires_at, channel_id, connected_at";

fn row_to_oauth(r: sqlx::sqlite::SqliteRow) -> YouTubeOAuth {
    YouTubeOAuth {
        id: r.get("id"),
        label: r.get("label"),
        access_token: r.get("access_token"),
        refresh_token: r.get("refresh_token"),
        token_uri: r.get("token_uri"),
        client_id: r.get("client_id"),
        client_secret: r.get("client_secret"),
        scopes: r.get("scopes"),
        expires_at: r.get("expires_at"),
        channel_id: r.get("channel_id"),
        connected_at: r.get("connected_at"),
    }
}

pub async fn get_oauth_by_label(pool: &SqlitePool, label: &str) -> Result<Option<YouTubeOAuth>> {
    let q = format!("SELECT {SELECT_COLS} FROM youtube_oauth WHERE label = ?1");
    let row = sqlx::query(&q).bind(label).fetch_optional(pool).await?;
    Ok(row.map(row_to_oauth))
}

pub async fn get_oauth_by_id(pool: &SqlitePool, id: i64) -> Result<Option<YouTubeOAuth>> {
    let q = format!("SELECT {SELECT_COLS} FROM youtube_oauth WHERE id = ?1");
    let row = sqlx::query(&q).bind(id).fetch_optional(pool).await?;
    Ok(row.map(row_to_oauth))
}

pub async fn list_oauths(pool: &SqlitePool) -> Result<Vec<YouTubeOAuth>> {
    let q = format!("SELECT {SELECT_COLS} FROM youtube_oauth ORDER BY label");
    let rows = sqlx::query(&q).fetch_all(pool).await?;
    Ok(rows.into_iter().map(row_to_oauth).collect())
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert_oauth_by_label(
    pool: &SqlitePool,
    label: &str,
    access_token: &str,
    refresh_token: &str,
    token_uri: &str,
    client_id: &str,
    client_secret: &str,
    scopes: &str,
    expires_at: Option<&str>,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO youtube_oauth
            (label, access_token, refresh_token, token_uri, client_id, client_secret, scopes, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(label) DO UPDATE SET
            access_token = excluded.access_token,
            refresh_token = excluded.refresh_token,
            token_uri = excluded.token_uri,
            client_id = excluded.client_id,
            client_secret = excluded.client_secret,
            scopes = excluded.scopes,
            expires_at = excluded.expires_at
         RETURNING id",
    )
    .bind(label)
    .bind(access_token)
    .bind(refresh_token)
    .bind(token_uri)
    .bind(client_id)
    .bind(client_secret)
    .bind(scopes)
    .bind(expires_at)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}
