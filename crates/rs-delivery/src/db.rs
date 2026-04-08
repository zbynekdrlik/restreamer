/// In-memory SQLite database for VPS chunk metadata.
/// Tracks duration_ms per chunk, read from S3 object metadata headers.
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::str::FromStr;

pub async fn init_pool() -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")?;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS chunks (
            sequence_number INTEGER PRIMARY KEY,
            duration_ms     INTEGER NOT NULL,
            size_bytes      INTEGER NOT NULL DEFAULT 0,
            fetched_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )",
    )
    .execute(&pool)
    .await?;
    Ok(pool)
}

pub async fn insert_chunk(
    pool: &SqlitePool,
    seq: i64,
    duration_ms: i64,
    size_bytes: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT OR REPLACE INTO chunks (sequence_number, duration_ms, size_bytes) VALUES (?1, ?2, ?3)",
    )
    .bind(seq)
    .bind(duration_ms)
    .bind(size_bytes)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_total_duration_ms(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
    let row = sqlx::query("SELECT COALESCE(SUM(duration_ms), 0) as total FROM chunks")
        .fetch_one(pool)
        .await?;
    Ok(row.get::<i64, _>("total"))
}

pub async fn get_chunk_duration(pool: &SqlitePool, seq: i64) -> Result<Option<i64>, sqlx::Error> {
    let row = sqlx::query("SELECT duration_ms FROM chunks WHERE sequence_number = ?1")
        .bind(seq)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.get::<i64, _>("duration_ms")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn init_and_insert() {
        let pool = init_pool().await.unwrap();
        insert_chunk(&pool, 1, 2000, 50000).await.unwrap();
        insert_chunk(&pool, 2, 1500, 40000).await.unwrap();
        let total = get_total_duration_ms(&pool).await.unwrap();
        assert_eq!(total, 3500);
    }

    #[tokio::test]
    async fn get_chunk_duration_returns_none_for_missing() {
        let pool = init_pool().await.unwrap();
        assert_eq!(get_chunk_duration(&pool, 1).await.unwrap(), None);
        insert_chunk(&pool, 1, 2100, 50000).await.unwrap();
        assert_eq!(get_chunk_duration(&pool, 1).await.unwrap(), Some(2100));
    }

    #[tokio::test]
    async fn empty_db_returns_zero_duration() {
        let pool = init_pool().await.unwrap();
        assert_eq!(get_total_duration_ms(&pool).await.unwrap(), 0);
    }
}
