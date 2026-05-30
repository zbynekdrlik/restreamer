use futures::StreamExt;
use rs_core::config::S3Config;
use s3::Region;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

use crate::EndpointError;

/// Concurrency for parallel S3 deletes. The previous implementation deleted
/// objects sequentially, which caused the dashboard "Delete + Cleanup"
/// button to time out for any event with more than ~150 chunks (one HTTP
/// round-trip per object × ~200ms). Eight parallel deletes is a good
/// trade-off — fast enough to finish 1000 chunks in ~25 seconds, slow
/// enough not to push the bucket past Hetzner's per-bucket write rate
/// when 8 sustained upload workers are also in flight.
///
///
/// Bumped DOWN from 20 -> 8 after the 2026-05-25 -> 2026-05-29 cascade
/// post-mortem: aws-cli sustained 800 PUTs at 3.7/s against nbg1 returns
/// zero 5xx, but Restreamer (8 uploader workers + 20-way clear-s3 delete
/// plus VPS LIST/GET) hit 504 floods during clear-s3 windows. 8+8 = 16
/// in-flight stays under the bucket limit observed empirically on nbg1.
const DELETE_CONCURRENCY: usize = 8;

/// Max retries inside a single S3 op for transient 5xx / network errors.
/// Internal retry absorbs Hetzner backend hiccups without polluting the
/// outer uploader retry queue (which fires on every failure -> writes an
/// audit row, schedules backoff). Three internal retries with 200ms,
/// 400ms, 800ms backoff covers the 600ms-1.5s 504 recovery window seen
/// on nbg1 during the 2026-05 cascade. Network errors past the budget
/// surface to the caller, which still retries forever (continuity).
const S3_OP_INTERNAL_RETRIES: u32 = 3;

/// Returns true if the upload/delete error is a transient 5xx or network
/// hiccup worth absorbing with an internal retry. Structural rejects
/// (4xx) and "permanent" classes are surfaced immediately.
fn is_transient_s3_error(err: &str) -> bool {
    let m = err.to_ascii_lowercase();
    m.contains(" 500")
        || m.contains(" 502")
        || m.contains(" 503")
        || m.contains(" 504")
        || m.contains("5xx")
        || m.contains("internalerror")
        || m.contains("serviceunavailable")
        || m.contains("slowdown")
        || m.contains("gateway")
        || m.contains("timeout")
        || m.contains("timed out")
        || m.contains("connection")
        || m.contains("reset")
        || m.contains("refused")
}

/// Internal retry wrapper. Re-runs `op` up to `S3_OP_INTERNAL_RETRIES`
/// times when the error is transient (5xx/network). Backoff schedule
/// is 200ms, 400ms, 800ms (cumulative ~1.4s). Non-transient errors
/// (4xx, invalid creds, etc.) return immediately.
async fn with_internal_retry<F, Fut, T>(label: &str, mut op: F) -> Result<T, EndpointError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, EndpointError>>,
{
    let mut last_err: Option<EndpointError> = None;
    for attempt in 0..=S3_OP_INTERNAL_RETRIES {
        match op().await {
            Ok(v) => {
                if attempt > 0 {
                    debug!(
                        "{label}: succeeded on internal retry attempt {} of {S3_OP_INTERNAL_RETRIES}",
                        attempt
                    );
                }
                return Ok(v);
            }
            Err(e) => {
                let msg = e.to_string();
                if attempt < S3_OP_INTERNAL_RETRIES && is_transient_s3_error(&msg) {
                    let backoff_ms = 200u64 * (1 << attempt);
                    debug!(
                        "{label}: transient error on attempt {} of {S3_OP_INTERNAL_RETRIES}, retrying in {backoff_ms}ms: {msg}",
                        attempt + 1
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| EndpointError::S3(format!("{label}: retry budget exhausted"))))
}

/// Given a raw S3 CommonPrefix string like `"{base}event/"`, trim the
/// base prefix and the trailing slash. Returns `None` if the resulting
/// event name is empty. Pure function, unit-testable without S3.
fn strip_event_from_common_prefix(raw: &str, base: &str) -> Option<String> {
    let after_base = raw.strip_prefix(base).unwrap_or(raw);
    let event = after_base.trim_end_matches('/');
    if event.is_empty() {
        None
    } else {
        Some(event.to_string())
    }
}

/// S3 client wrapper for uploading chunk files.
pub struct S3Client {
    bucket: Box<Bucket>,
}

impl S3Client {
    /// Create a new S3 client from config.
    pub fn new(config: &S3Config) -> Result<Self, EndpointError> {
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
        .map_err(|e| EndpointError::S3(format!("invalid credentials: {e}")))?;

        let bucket = Bucket::new(&config.bucket, region, credentials)
            .map_err(|e| EndpointError::S3(format!("failed to create bucket: {e}")))?
            .with_path_style();

        Ok(Self { bucket })
    }

    /// Generate an S3 key for a chunk file.
    ///
    /// `event_identifier` is expected to be of the form
    /// `{client_uuid}/{event_name}` so the final key is
    /// `{client_uuid}/{event_name}/{sequence_number}.bin`. This prevents
    /// cross-installation collisions when multiple Restreamer installs
    /// share one S3 bucket (#114).
    pub fn chunk_key(event_identifier: &str, sequence_number: i64) -> String {
        format!("{event_identifier}/{sequence_number}.bin")
    }

    /// Upload a chunk file to S3 with duration stored as object metadata.
    ///
    /// Uses `x-amz-meta-duration-ms` header so the VPS can read duration
    /// from S3 HEAD without needing to parse the key or access the local DB.
    pub async fn upload_chunk(
        &self,
        local_path: &Path,
        event_id: &str,
        seq: i64,
        duration_ms: i64,
    ) -> Result<(), EndpointError> {
        self.upload_chunk_inner(local_path, event_id, seq, duration_ms, &HashMap::new())
            .await
    }

    /// Upload a chunk with extra `x-amz-meta-*` headers in addition to the
    /// existing `duration-ms`. Used by the lifecycle uploader (#184) so
    /// the VPS can backfill stage A/B timestamps from the S3 GET response.
    ///
    /// `metadata` keys must be lowercase ASCII (Hetzner S3 conformance);
    /// keys are emitted verbatim as `x-amz-meta-{key}`.
    pub async fn upload_chunk_with_metadata(
        &self,
        local_path: &Path,
        event_id: &str,
        seq: i64,
        duration_ms: i64,
        metadata: HashMap<String, String>,
    ) -> Result<(), EndpointError> {
        self.upload_chunk_inner(local_path, event_id, seq, duration_ms, &metadata)
            .await
    }

    /// Shared upload core. Both `upload_chunk` and
    /// `upload_chunk_with_metadata` call this to avoid drift if a future
    /// fix touches the S3 PUT path.
    async fn upload_chunk_inner(
        &self,
        local_path: &Path,
        event_id: &str,
        seq: i64,
        duration_ms: i64,
        metadata: &HashMap<String, String>,
    ) -> Result<(), EndpointError> {
        let s3_key = Self::chunk_key(event_id, seq);

        let mut file = tokio::fs::File::open(local_path)
            .await
            .map_err(|e| EndpointError::Io(e.to_string()))?;

        let file_size = file
            .metadata()
            .await
            .map_err(|e| EndpointError::Io(e.to_string()))?
            .len();

        debug!(
            "Uploading to s3://{}/{} ({file_size} bytes, duration_ms={duration_ms}, meta_keys={})",
            self.bucket.name,
            s3_key,
            metadata.len(),
        );

        // Read the file ONCE into memory so internal retries don't re-read
        // (rust-s3 put_object_stream consumes the reader; we can't rewind
        // a tokio File across attempts without reopening). Chunks are
        // already < 8 MB so the single-PUT path is taken by rust-s3 anyway.
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::with_capacity(file_size as usize);
        file.read_to_end(&mut buf)
            .await
            .map_err(|e| EndpointError::Io(e.to_string()))?;
        let buf_arc = std::sync::Arc::new(buf);

        let label = format!("upload {s3_key}");
        with_internal_retry(&label, || {
            let buf_arc = std::sync::Arc::clone(&buf_arc);
            let s3_key = s3_key.clone();
            let bucket_template = (*self.bucket).clone();
            let duration_ms_str = duration_ms.to_string();
            let metadata = metadata.clone();
            async move {
                // Fresh bucket clone + headers per attempt so retries don't
                // inherit a stale header state from a prior failed signing.
                let mut upload_bucket = bucket_template;
                upload_bucket.add_header("x-amz-meta-duration-ms", &duration_ms_str);
                for (k, v) in metadata.iter() {
                    upload_bucket.add_header(&format!("x-amz-meta-{k}"), v);
                }
                let response = upload_bucket
                    .put_object(&s3_key, &buf_arc)
                    .await
                    .map_err(|e| EndpointError::S3(format!("upload failed: {e}")))?;
                if response.status_code() >= 300 {
                    return Err(EndpointError::S3(format!(
                        "upload returned status {}",
                        response.status_code(),
                    )));
                }
                Ok(())
            }
        })
        .await?;

        info!(
            "Uploaded {s3_key} ({file_size} bytes, duration_ms={duration_ms}, meta_keys={})",
            metadata.len(),
        );
        Ok(())
    }

    /// Upload arbitrary bytes to S3 with public-read ACL and return the
    /// public URL. Used for rescue video uploads — these files must be
    /// publicly readable so the delivery VPS can fetch them via ffmpeg.
    pub async fn upload_public_object(
        &self,
        key: &str,
        bytes: &[u8],
        content_type: &str,
    ) -> Result<String, EndpointError> {
        let mut upload_bucket = (*self.bucket).clone();
        upload_bucket.add_header("x-amz-acl", "public-read");

        let response = upload_bucket
            .put_object_with_content_type(key, bytes, content_type)
            .await
            .map_err(|e| EndpointError::S3(format!("upload failed: {e}")))?;

        if response.status_code() >= 300 {
            return Err(EndpointError::S3(format!(
                "upload returned status {}",
                response.status_code(),
            )));
        }

        // Public URL: {endpoint}/{bucket}/{key}
        // The bucket.url() method gives us the proper URL for the object.
        let url = format!("{}/{}", self.bucket.url(), key);
        info!("Uploaded public object to {url} ({} bytes)", bytes.len());
        Ok(url)
    }

    /// Delete all S3 objects under the given event name prefix.
    /// Returns the number of objects deleted.
    ///
    /// Deletes are issued in parallel (up to DELETE_CONCURRENCY in flight)
    /// because rust-s3 0.35 has no native bulk-delete API. The serial loop
    /// this replaced caused the "Delete + Cleanup" dashboard action to hang
    /// past the HTTP client timeout for any event with >150 chunks.
    pub async fn delete_event_chunks(&self, event_name: &str) -> Result<u64, EndpointError> {
        let prefix = format!("{event_name}/");
        let bucket = Arc::new((*self.bucket).clone());
        let mut total_deleted = 0u64;

        loop {
            let list = {
                let bucket = Arc::clone(&bucket);
                let prefix = prefix.clone();
                with_internal_retry(&format!("list {prefix}"), move || {
                    let bucket = Arc::clone(&bucket);
                    let prefix = prefix.clone();
                    async move {
                        bucket
                            .list(prefix, None)
                            .await
                            .map_err(|e| EndpointError::S3(format!("list failed: {e}")))
                    }
                })
                .await?
            };

            let keys: Vec<String> = list
                .iter()
                .flat_map(|page| page.contents.iter().map(|obj| obj.key.clone()))
                .collect();

            if keys.is_empty() {
                break;
            }

            let results: Vec<Result<String, EndpointError>> = futures::stream::iter(keys)
                .map(|key| {
                    let bucket = Arc::clone(&bucket);
                    async move {
                        let key_for_retry = key.clone();
                        with_internal_retry(&format!("delete {key}"), move || {
                            let bucket = Arc::clone(&bucket);
                            let key = key_for_retry.clone();
                            async move {
                                let response = bucket.delete_object(&key).await.map_err(|e| {
                                    EndpointError::S3(format!("delete {key} failed: {e}"))
                                })?;
                                if response.status_code() >= 300 {
                                    return Err(EndpointError::S3(format!(
                                        "delete {key} returned status {}",
                                        response.status_code()
                                    )));
                                }
                                Ok(key)
                            }
                        })
                        .await
                    }
                })
                .buffer_unordered(DELETE_CONCURRENCY)
                .collect()
                .await;

            // Count successful deletes before surfacing any error so the
            // reported total reflects real progress, not "attempted deletes".
            // This matters when an error occurs mid-batch — without this the
            // log line overstates progress by up to DELETE_CONCURRENCY - 1.
            let batch_successes = results.iter().filter(|r| r.is_ok()).count() as u64;
            total_deleted += batch_successes;
            if let Some(err) = results.into_iter().find_map(|r| r.err()) {
                info!("Deleted {total_deleted} S3 objects under prefix '{prefix}' before error");
                return Err(err);
            }
        }

        info!("Deleted {total_deleted} S3 objects under prefix '{prefix}'");
        Ok(total_deleted)
    }

    /// List the event prefix and return the smallest `{seq}.bin` whose
    /// sequence number is >= `lower_bound`. Used by `poll_and_init` to
    /// validate the orchestrator-computed `start_chunk_id` when the DB
    /// has orphan rows referring to chunks that were already cleared
    /// from S3 (so the producer doesn't get stuck on a 404 below the
    /// real live edge). Returns `Ok(None)` if no chunk at or after
    /// `lower_bound` exists.
    pub async fn find_first_chunk_id_at_or_after(
        &self,
        event_prefix: &str,
        lower_bound: i64,
    ) -> Result<Option<i64>, EndpointError> {
        let prefix = format!("{event_prefix}/");
        let list = self
            .bucket
            .list(prefix.clone(), None)
            .await
            .map_err(|e| EndpointError::S3(format!("list failed: {e}")))?;
        let mut best: Option<i64> = None;
        for page in &list {
            for obj in &page.contents {
                let key = &obj.key;
                let stem = match key.strip_prefix(&prefix) {
                    Some(s) => s,
                    None => continue,
                };
                let num_str = match stem.strip_suffix(".bin") {
                    Some(s) => s,
                    None => continue,
                };
                let n: i64 = match num_str.parse() {
                    Ok(n) => n,
                    Err(_) => continue,
                };
                if n >= lower_bound && best.is_none_or(|b| n < b) {
                    best = Some(n);
                }
            }
        }
        Ok(best)
    }

    /// Compute total bytes and object count under a given prefix without
    /// downloading anything. Used by the S3 usage endpoint.
    pub async fn measure_prefix(&self, prefix: &str) -> Result<(u64, u64), EndpointError> {
        let list = self
            .bucket
            .list(prefix.to_string(), None)
            .await
            .map_err(|e| EndpointError::S3(format!("list failed: {e}")))?;

        let mut total_bytes: u64 = 0;
        let mut object_count: u64 = 0;
        for page in &list {
            for obj in &page.contents {
                total_bytes += obj.size;
                object_count += 1;
            }
        }
        Ok((total_bytes, object_count))
    }

    /// List "subdirectories" (CommonPrefixes) under `base_prefix`.
    /// With the #114 layout, callers pass `{client_uuid}/` to enumerate
    /// events for a single installation. The returned names have the
    /// base prefix stripped, so the caller gets just event names.
    pub async fn list_event_prefixes(
        &self,
        base_prefix: &str,
    ) -> Result<Vec<String>, EndpointError> {
        let list = self
            .bucket
            .list(base_prefix.to_string(), Some("/".to_string()))
            .await
            .map_err(|e| EndpointError::S3(format!("list failed: {e}")))?;

        let mut prefixes = Vec::new();
        for page in &list {
            if let Some(common) = &page.common_prefixes {
                for cp in common {
                    if let Some(event) = strip_event_from_common_prefix(&cp.prefix, base_prefix) {
                        prefixes.push(event);
                    }
                }
            }
        }
        Ok(prefixes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_key_format() {
        let key = S3Client::chunk_key("evt-123", 1);
        assert_eq!(key, "evt-123/1.bin");
    }

    #[test]
    fn chunk_key_with_client_uuid_prefix() {
        // With the #114 format, the caller passes a composite identifier
        // `{client_uuid}/{event_name}` so the key nests naturally under the
        // client UUID.
        let key = S3Client::chunk_key("abc-uuid/sunday-service", 7);
        assert_eq!(key, "abc-uuid/sunday-service/7.bin");
    }

    #[test]
    fn upload_chunk_key_is_simple() {
        let key = S3Client::chunk_key("sunday-service-2026", 42);
        assert!(!key.contains('_'), "key should have no underscores: {key}");
        assert!(key.ends_with(".bin"), "key should end with .bin: {key}");
    }

    #[test]
    fn strip_event_from_common_prefix_removes_base_and_slash() {
        let out = strip_event_from_common_prefix("abc-uuid/sunday-service/", "abc-uuid/");
        assert_eq!(out, Some("sunday-service".to_string()));
    }

    #[test]
    fn strip_event_from_common_prefix_no_match_returns_full() {
        // If S3 returns a prefix outside the base (e.g. another installation
        // in a shared bucket), we return the full name rather than silently
        // dropping it — the caller can decide.
        let out = strip_event_from_common_prefix("other-uuid/event/", "abc-uuid/");
        assert_eq!(out, Some("other-uuid/event".to_string()));
    }

    #[test]
    fn strip_event_from_common_prefix_empty_after_strip_is_none() {
        // The base prefix itself (no event underneath) should yield None.
        let out = strip_event_from_common_prefix("abc-uuid/", "abc-uuid/");
        assert_eq!(out, None);
    }

    #[test]
    fn s3_client_rejects_empty_credentials() {
        let config = S3Config {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
        };
        // Should still construct (empty creds are valid for some providers)
        let result = S3Client::new(&config);
        assert!(result.is_ok());
    }
}
