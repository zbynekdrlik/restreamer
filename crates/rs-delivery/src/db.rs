/// In-memory SQLite database for VPS chunk metadata.
/// Tracks duration_ms per chunk, parsed from S3 key filenames.
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

/// Parse S3 key to extract (sequence_number, duration_ms).
/// New format: "{event}/{seq}_{duration_ms}_{event}.bin"
/// Legacy format: "{event}/{seq}_{event}.bin" → returns duration_ms = 0
pub fn parse_chunk_key(key: &str) -> Option<(i64, i64)> {
    let filename = key.rsplit('/').next()?;
    let stem = filename.strip_suffix(".bin")?;
    let parts: Vec<&str> = stem.splitn(3, '_').collect();
    match parts.len() {
        3 => {
            let seq: i64 = parts[0].parse().ok()?;
            let dur: i64 = parts[1].parse().ok()?;
            Some((seq, dur))
        }
        2 => {
            let seq: i64 = parts[0].parse().ok()?;
            Some((seq, 0))
        }
        _ => None,
    }
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

    #[test]
    fn parse_new_format_key() {
        let (seq, dur) = parse_chunk_key("evt-123/42_2100_evt-123.bin").unwrap();
        assert_eq!(seq, 42);
        assert_eq!(dur, 2100);
    }

    #[test]
    fn parse_legacy_format_key() {
        let (seq, dur) = parse_chunk_key("evt-123/42_evt-123.bin").unwrap();
        assert_eq!(seq, 42);
        assert_eq!(dur, 0);
    }

    #[test]
    fn parse_invalid_key() {
        assert!(parse_chunk_key("garbage").is_none());
        assert!(parse_chunk_key("").is_none());
    }

    #[tokio::test]
    async fn empty_db_returns_zero_duration() {
        let pool = init_pool().await.unwrap();
        assert_eq!(get_total_duration_ms(&pool).await.unwrap(), 0);
    }
}
