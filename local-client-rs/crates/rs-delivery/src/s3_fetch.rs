/// S3 chunk fetcher for sequential chunk retrieval.

use crate::api::S3Config;
use s3::creds::Credentials;
use s3::Region;
use s3::Bucket;

pub struct S3Fetcher {
    bucket: Box<Bucket>,
    event_identifier: String,
}

impl S3Fetcher {
    pub fn new(config: &S3Config, event_identifier: &str) -> Result<Self, String> {
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
        .map_err(|e| format!("S3 credentials error: {e}"))?;

        let bucket = Bucket::new(&config.bucket, region, credentials)
            .map_err(|e| format!("S3 bucket error: {e}"))?
            .with_path_style();

        Ok(Self {
            bucket,
            event_identifier: event_identifier.to_string(),
        })
    }

    /// Fetch a chunk by sequential ID. Returns None if not found (404).
    pub async fn fetch_chunk(&self, chunk_id: i64) -> Result<Option<Vec<u8>>, String> {
        let key = format!(
            "{}/{}_{}.bin",
            self.event_identifier, chunk_id, self.event_identifier
        );

        match self.bucket.get_object(&key).await {
            Ok(response) => {
                if response.status_code() == 200 {
                    Ok(Some(response.to_vec()))
                } else if response.status_code() == 404 {
                    Ok(None)
                } else {
                    Err(format!(
                        "S3 error: status {}",
                        response.status_code()
                    ))
                }
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("404") || err_str.contains("NoSuchKey") {
                    Ok(None)
                } else {
                    Err(format!("S3 fetch error: {e}"))
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
        let key = format!("{}/{}_{}.bin", "evt-123", 42, "evt-123");
        assert_eq!(key, "evt-123/42_evt-123.bin");
    }
}
