/// S3 chunk fetcher for sequential chunk retrieval.
use crate::api::S3Config;
use s3::Bucket;
use s3::Region;
use s3::creds::Credentials;
use thiserror::Error;

/// Typed errors for S3 fetching operations.
#[derive(Debug, Error)]
pub enum S3FetchError {
    #[error("S3 credentials error: {0}")]
    Credentials(String),
    #[error("S3 bucket error: {0}")]
    Bucket(String),
    #[error("S3 fetch error: {0}")]
    Fetch(String),
}

/// Chunk data with duration from S3 object metadata header.
pub struct ChunkData {
    pub data: Vec<u8>,
    pub duration_ms: i64,
}

pub struct S3Fetcher {
    bucket: Box<Bucket>,
    event_identifier: String,
}

impl S3Fetcher {
    pub fn new(config: &S3Config, event_identifier: &str) -> Result<Self, S3FetchError> {
        let region = Region::Custom {
            region: config.region.clone(),
            endpoint: config.endpoint.clone(),
        };
        let credentials = Credentials::new(
            Some(&config.access_key_id),
            Some(&config.secret_access_key),
            None,
            None,
            None,
        )
        .map_err(|e| S3FetchError::Credentials(e.to_string()))?;

        let bucket = Bucket::new(&config.bucket, region, credentials)
            .map_err(|e| S3FetchError::Bucket(e.to_string()))?
            .with_path_style();

        Ok(Self {
            bucket,
            event_identifier: event_identifier.to_string(),
        })
    }

    /// Fetch a chunk with metadata (duration_ms from S3 object metadata header).
    /// Uses direct GET with key `{event}/{seq}.bin`.
    #[mutants::skip] // Requires real S3 — tested by E2E
    pub async fn fetch_chunk_with_meta(
        &self,
        chunk_id: i64,
    ) -> Result<Option<ChunkData>, S3FetchError> {
        let key = format!("{}/{}.bin", self.event_identifier, chunk_id);

        match self.bucket.get_object(&key).await {
            Ok(response) if response.status_code() == 200 => {
                let duration_ms = response
                    .headers()
                    .get("x-amz-meta-duration-ms")
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(0);
                Ok(Some(ChunkData {
                    data: response.to_vec(),
                    duration_ms,
                }))
            }
            Ok(response) if response.status_code() == 404 => Ok(None),
            Ok(response) => Err(S3FetchError::Fetch(format!(
                "status {}",
                response.status_code()
            ))),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("404") || err_str.contains("NoSuchKey") {
                    Ok(None)
                } else {
                    Err(S3FetchError::Fetch(err_str))
                }
            }
        }
    }

    /// Get chunk duration via HEAD request (no data download).
    /// Returns `Ok(Some(duration_ms))` for 200, `Ok(None)` for 404.
    #[mutants::skip] // Requires real S3 — tested by E2E
    pub async fn head_chunk_duration(&self, chunk_id: i64) -> Result<Option<i64>, S3FetchError> {
        let key = format!("{}/{}.bin", self.event_identifier, chunk_id);

        match self.bucket.head_object(&key).await {
            Ok((head, 200)) => {
                let duration_ms = head
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get("duration-ms"))
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(0);
                Ok(Some(duration_ms))
            }
            Ok((_, 404)) => Ok(None),
            Ok((_, code)) => Err(S3FetchError::Fetch(format!("HEAD status {}", code))),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("404") || err_str.contains("NoSuchKey") {
                    Ok(None)
                } else {
                    Err(S3FetchError::Fetch(err_str))
                }
            }
        }
    }

    /// Fetch a chunk by sequential ID. Returns None if not found (404).
    #[mutants::skip] // Requires real S3 — tested by E2E
    pub async fn fetch_chunk(&self, chunk_id: i64) -> Result<Option<Vec<u8>>, S3FetchError> {
        let key = format!("{}/{}.bin", self.event_identifier, chunk_id);

        match self.bucket.get_object(&key).await {
            Ok(response) if response.status_code() == 200 => Ok(Some(response.to_vec())),
            Ok(response) if response.status_code() == 404 => Ok(None),
            Ok(response) => Err(S3FetchError::Fetch(format!(
                "status {}",
                response.status_code()
            ))),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("404") || err_str.contains("NoSuchKey") {
                    Ok(None)
                } else {
                    Err(S3FetchError::Fetch(err_str))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn chunk_key_format() {
        // Direct key format: {event}/{seq}.bin
        let key = format!("{}/{}.bin", "evt-123", 42);
        assert_eq!(key, "evt-123/42.bin");
    }
}
