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

/// Chunk data with duration + lifecycle stages from S3 object metadata.
pub struct ChunkData {
    pub data: Vec<u8>,
    pub duration_ms: i64,
    /// Stage A: host clock millis since epoch when the chunker wrote the
    /// chunk to local FS. NULL/None when the chunk was uploaded by a
    /// pre-lifecycle host. Cross-host with VPS clock — see spec section 4.3.
    pub host_emit_ts: Option<i64>,
    /// Stage B: host clock millis since epoch when the uploader received
    /// the S3 200 OK. NULL/None for legacy chunks.
    pub s3_upload_complete_ts: Option<i64>,
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
    pub async fn fetch_chunk_with_meta(
        &self,
        chunk_id: i64,
    ) -> Result<Option<ChunkData>, S3FetchError> {
        let key = format!("{}/{}.bin", self.event_identifier, chunk_id);

        match self.bucket.get_object(&key).await {
            Ok(response) if response.status_code() == 200 => {
                let headers = response.headers();
                let duration_ms = headers
                    .get("x-amz-meta-duration-ms")
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(0);
                let host_emit_ts = headers
                    .get("x-amz-meta-host-emit-ts")
                    .and_then(|v| v.parse::<i64>().ok());
                let s3_upload_complete_ts = headers
                    .get("x-amz-meta-s3-complete-ts")
                    .and_then(|v| v.parse::<i64>().ok());
                Ok(Some(ChunkData {
                    data: response.to_vec(),
                    duration_ms,
                    host_emit_ts,
                    s3_upload_complete_ts,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_key_format() {
        // Direct key format: {event}/{seq}.bin
        let key = format!("{}/{}.bin", "evt-123", 42);
        assert_eq!(key, "evt-123/42.bin");
    }

    #[test]
    fn chunk_data_has_lifecycle_header_fields() {
        // Compile-time assertion: ChunkData carries host_emit_ts and
        // s3_upload_complete_ts (both Option<i64> millis since epoch).
        // The fetcher backfills them from x-amz-meta-* response headers.
        let cd = ChunkData {
            data: vec![],
            duration_ms: 2000,
            host_emit_ts: Some(1715380800000),
            s3_upload_complete_ts: Some(1715380800120),
        };
        assert_eq!(cd.host_emit_ts, Some(1715380800000));
        assert_eq!(cd.s3_upload_complete_ts, Some(1715380800120));
    }
}
