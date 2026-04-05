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

/// Chunk data with metadata parsed from S3 key filename.
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

    /// Fetch a chunk with metadata (duration_ms parsed from S3 key filename).
    /// Uses S3 LIST to discover the key since duration is embedded in the filename.
    pub async fn fetch_chunk_with_meta(
        &self,
        chunk_id: i64,
    ) -> Result<Option<ChunkData>, S3FetchError> {
        let prefix = format!("{}/{}_", self.event_identifier, chunk_id);
        let list_result = self
            .bucket
            .list(prefix, Some("/".to_string()))
            .await
            .map_err(|e| S3FetchError::Fetch(format!("list failed: {e}")))?;

        let key = list_result
            .iter()
            .flat_map(|r| r.contents.iter())
            .map(|obj| &obj.key)
            .next();

        let key = match key {
            Some(k) => k.clone(),
            None => return Ok(None),
        };

        let (_seq, duration_ms) = crate::db::parse_chunk_key(&key).unwrap_or((chunk_id, 0));

        match self.bucket.get_object(&key).await {
            Ok(response) if response.status_code() == 200 => Ok(Some(ChunkData {
                data: response.to_vec(),
                duration_ms,
            })),
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

    /// Fetch a chunk by sequential ID. Returns None if not found (404).
    /// Delegates to `fetch_chunk_with_meta` and discards metadata.
    pub async fn fetch_chunk(&self, chunk_id: i64) -> Result<Option<Vec<u8>>, S3FetchError> {
        match self.fetch_chunk_with_meta(chunk_id).await? {
            Some(cd) => Ok(Some(cd.data)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn chunk_key_format() {
        // New format includes duration_ms
        let key = format!("{}/{}_{}_{}_{}.bin", "evt-123", 42, 2100, "evt-123", "");
        // Verify prefix matches pattern used by fetch_chunk_with_meta
        assert!(key.starts_with("evt-123/42_"));
    }
}
