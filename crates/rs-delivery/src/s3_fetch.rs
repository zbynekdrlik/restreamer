/// S3 chunk fetcher for sequential chunk retrieval.
use crate::api::S3Config;
use s3::Bucket;
use s3::Region;
use s3::creds::Credentials;
use std::time::Duration;
use thiserror::Error;

/// Per-GET request timeout for S3 chunk fetches. Without a client-side timeout a
/// wedged/blackhole GET hangs on the reqwest client until the OS TCP timeout
/// (minutes), holding a `max_concurrent` fetch semaphore permit — which starves
/// #252 recovery fetches (endpoint never returns to live) and blocks the #276
/// live-edge hot path. 20s clears the measured legit-GET p99 (8.2–16.4s,
/// #275/#276) with margin so slow-but-completing GETs still succeed, while
/// failing a true wedge fast into the existing retry-with-backoff.
const S3_GET_REQUEST_TIMEOUT_SECS: u64 = 20;

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
    /// chunk to local FS. Backfilled from `x-amz-meta-host-emit-ts` on
    /// the S3 GET response. NULL/None when the chunk was uploaded by a
    /// pre-lifecycle host. Cross-host with VPS clock — see spec section 4.3.
    pub host_emit_ts: Option<i64>,
    /// Stage B: host clock millis since epoch when the uploader received
    /// the S3 200 OK. NOT carried via S3 header (the value is unknown
    /// at PUT time — only after PUT returns). The VPS backfills this
    /// field from the host's `chunk_records.s3_upload_complete_ts` DB
    /// row inside `LifecycleAwarePusher` (Task 18). Always None on the
    /// S3 fetch path; populated downstream.
    pub s3_upload_complete_ts: Option<i64>,
}

pub struct S3Fetcher {
    bucket: Box<Bucket>,
    event_identifier: String,
}

impl S3Fetcher {
    pub fn new(config: &S3Config, event_identifier: &str) -> Result<Self, S3FetchError> {
        Self::new_with_timeout(
            config,
            event_identifier,
            Duration::from_secs(S3_GET_REQUEST_TIMEOUT_SECS),
        )
    }

    /// Construct a fetcher with an explicit per-GET request timeout. Dependency
    /// injection seam so the timeout behavior is unit-testable at a fast bound;
    /// production callers use `new` (which passes `S3_GET_REQUEST_TIMEOUT_SECS`).
    fn new_with_timeout(
        config: &S3Config,
        event_identifier: &str,
        request_timeout: Duration,
    ) -> Result<Self, S3FetchError> {
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

        let mut bucket = Bucket::new(&config.bucket, region, credentials)
            .map_err(|e| S3FetchError::Bucket(e.to_string()))?
            .with_path_style();
        // Bound the per-GET fetch so a wedged connection fails fast into retry.
        bucket.set_request_timeout(Some(request_timeout));

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
                // s3_upload_complete_ts (stage B) cannot be carried via
                // S3 header — unknown at PUT time. Always None here;
                // backfilled from chunk_records DB in LifecycleAwarePusher.
                Ok(Some(ChunkData {
                    data: response.to_vec(),
                    duration_ms,
                    host_emit_ts,
                    s3_upload_complete_ts: None,
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

    /// #252/#276 regression: a wedged/blackhole S3 GET MUST be bounded by a
    /// client-side request timeout, not hang until the OS TCP timeout (minutes)
    /// holding a fetch semaphore permit (which starves crash-recovery fetches so
    /// a non-fast endpoint never returns to live — the #252 gap — and blocks the
    /// #276 live-edge hot path).
    ///
    /// A local `TcpListener` ACCEPTS the connection (so connect succeeds and the
    /// GET is sent) but never writes a response — the exact wedged nbg1 GET.
    /// Deterministic + fast: an injected 1s bound; the outer 8s ceiling detects a
    /// regression (unbounded GET -> ceiling trips -> RED; bounded -> the fetcher
    /// returns Err(Fetch) in ~1s -> GREEN). This asserts EFFECTIVE behavior, so
    /// it also fails a naive `set_request_timeout` "fix" (a no-op on the tokio
    /// backend — it sets a dead struct field, never rebuilding the reqwest
    /// client; only `with_request_timeout` rebuilds it).
    #[tokio::test]
    async fn wedged_s3_get_is_bounded_by_request_timeout() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _accept = tokio::spawn(async move {
            // Hold every accepted socket open, never respond — a true blackhole.
            let mut held = Vec::new();
            loop {
                match listener.accept().await {
                    Ok((sock, _)) => held.push(sock),
                    Err(_) => break,
                }
            }
        });

        let config = crate::api::S3Config {
            bucket: "b".to_string(),
            region: "us-east-1".to_string(),
            endpoint: format!("http://{addr}"),
            access_key_id: "k".to_string(),
            secret_access_key: "s".to_string(),
        };

        let fetcher = S3Fetcher::new_with_timeout(&config, "evt", Duration::from_secs(1)).unwrap();

        let result =
            tokio::time::timeout(Duration::from_secs(8), fetcher.fetch_chunk_with_meta(1)).await;

        assert!(
            result.is_ok(),
            "S3 GET must be bounded by a client-side request timeout, not hang on a \
             wedged connection (held a fetch permit -> #252 recovery starvation)"
        );
        assert!(
            matches!(result.unwrap(), Err(S3FetchError::Fetch(_))),
            "a wedged GET must surface as a fetch error (timeout), not a false 404/None"
        );
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
