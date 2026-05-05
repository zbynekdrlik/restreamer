//! DownloadService -- bandwidth-managed S3 chunk downloader with dedup.

use std::sync::Arc;

pub struct DownloadService {
    _placeholder: (),
}

impl DownloadService {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { _placeholder: () })
    }
}
