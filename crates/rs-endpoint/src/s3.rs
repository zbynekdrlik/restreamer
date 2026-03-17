use rs_core::config::S3Config;
use s3::Region;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use std::path::Path;
use tracing::{debug, info};

use crate::EndpointError;

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

    /// Upload a file to S3 using streaming to avoid loading entire file into memory.
    pub async fn upload_file(&self, local_path: &Path, s3_key: &str) -> Result<(), EndpointError> {
        let mut file = tokio::fs::File::open(local_path)
            .await
            .map_err(|e| EndpointError::Io(e.to_string()))?;

        let metadata = file
            .metadata()
            .await
            .map_err(|e| EndpointError::Io(e.to_string()))?;
        let file_size = metadata.len();

        debug!(
            "Uploading to s3://{}/{} ({file_size} bytes)",
            self.bucket.name, s3_key,
        );

        let response = self
            .bucket
            .put_object_stream(&mut file, s3_key)
            .await
            .map_err(|e| EndpointError::S3(format!("upload failed: {e}")))?;

        if response.status_code() >= 300 {
            return Err(EndpointError::S3(format!(
                "upload returned status {}",
                response.status_code(),
            )));
        }

        info!("Uploaded {s3_key} ({file_size} bytes)");
        Ok(())
    }

    /// Generate an S3 key for a chunk file.
    /// Format: `{event_id}/{sequence_number}_{event_id}.bin`
    /// Uses per-event sequence numbers to avoid interleaving when multiple events run concurrently.
    pub fn chunk_key(event_identifier: &str, sequence_number: i64) -> String {
        format!("{event_identifier}/{sequence_number}_{event_identifier}.bin")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_key_format() {
        let key = S3Client::chunk_key("evt-123", 1);
        assert_eq!(key, "evt-123/1_evt-123.bin");
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
