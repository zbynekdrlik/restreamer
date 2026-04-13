use futures::StreamExt;
use rs_core::config::S3Config;
use s3::Region;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

use crate::EndpointError;

/// Concurrency for parallel S3 deletes. The previous implementation deleted
/// objects sequentially, which caused the dashboard "Delete + Cleanup"
/// button to time out for any event with more than ~150 chunks (one HTTP
/// round-trip per object × ~200ms). Twenty parallel deletes is a good
/// trade-off — fast enough to finish 1000 chunks in ~10 seconds, slow
/// enough not to flood the S3 endpoint.
const DELETE_CONCURRENCY: usize = 20;

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
    /// Format: `{event_id}/{sequence_number}.bin`
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
        let s3_key = Self::chunk_key(event_id, seq);

        let mut file = tokio::fs::File::open(local_path)
            .await
            .map_err(|e| EndpointError::Io(e.to_string()))?;

        let metadata = file
            .metadata()
            .await
            .map_err(|e| EndpointError::Io(e.to_string()))?;
        let file_size = metadata.len();

        debug!(
            "Uploading to s3://{}/{} ({file_size} bytes, duration_ms={duration_ms})",
            self.bucket.name, s3_key,
        );

        // Clone bucket to add per-upload metadata without leaking headers
        let mut upload_bucket = (*self.bucket).clone();
        upload_bucket.add_header("x-amz-meta-duration-ms", &duration_ms.to_string());

        let response = upload_bucket
            .put_object_stream(&mut file, &s3_key)
            .await
            .map_err(|e| EndpointError::S3(format!("upload failed: {e}")))?;

        if response.status_code() >= 300 {
            return Err(EndpointError::S3(format!(
                "upload returned status {}",
                response.status_code(),
            )));
        }

        info!("Uploaded {s3_key} ({file_size} bytes, duration_ms={duration_ms})");
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
            let list = bucket
                .list(prefix.clone(), None)
                .await
                .map_err(|e| EndpointError::S3(format!("list failed: {e}")))?;

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
                        let response = bucket
                            .delete_object(&key)
                            .await
                            .map_err(|e| EndpointError::S3(format!("delete {key} failed: {e}")))?;
                        if response.status_code() >= 300 {
                            return Err(EndpointError::S3(format!(
                                "delete {key} returned status {}",
                                response.status_code()
                            )));
                        }
                        Ok(key)
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

    /// List all top-level "directories" (CommonPrefixes) in the bucket.
    /// Each entry corresponds to one event_identifier folder.
    pub async fn list_event_prefixes(&self) -> Result<Vec<String>, EndpointError> {
        let list = self
            .bucket
            .list("".to_string(), Some("/".to_string()))
            .await
            .map_err(|e| EndpointError::S3(format!("list failed: {e}")))?;

        let mut prefixes = Vec::new();
        for page in &list {
            if let Some(common) = &page.common_prefixes {
                for cp in common {
                    let prefix = cp.prefix.trim_end_matches('/').to_string();
                    if !prefix.is_empty() {
                        prefixes.push(prefix);
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
    fn upload_chunk_key_is_simple() {
        let key = S3Client::chunk_key("sunday-service-2026", 42);
        assert!(!key.contains('_'), "key should have no underscores: {key}");
        assert!(key.ends_with(".bin"), "key should end with .bin: {key}");
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
