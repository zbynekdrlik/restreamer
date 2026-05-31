//! Trait impls glueing real `S3Fetcher` + `FfmpegProcess` to the
//! [`ChunkFetcher`] / [`OutputProcess`] / [`OutputProcessFactory`]
//! abstractions used by `endpoint_task::consumer_task`.
//!
//! Extracted from `endpoint_task.rs` to keep that file under the
//! 1000-line CI cap (see review finding #5).

use async_trait::async_trait;
use rs_ffmpeg::{FfmpegProcess, ServiceType};

use crate::endpoint_task::{ChunkFetcher, OutputProcess, OutputProcessFactory};
use crate::s3_fetch::S3Fetcher;

impl ChunkFetcher for S3Fetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        S3Fetcher::fetch_chunk_with_meta(self, chunk_id)
            .await
            .map(|opt| opt.map(|cd| (cd.data, cd.duration_ms)))
            .map_err(|e| e.to_string())
    }
    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        S3Fetcher::head_chunk_duration(self, chunk_id)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Real ffmpeg process implementing OutputProcess.
#[async_trait]
impl OutputProcess for FfmpegProcess {
    fn is_alive(&mut self) -> bool {
        self.try_exit_code().is_none()
    }

    async fn write(&mut self, data: &[u8]) -> Result<(), String> {
        rs_ffmpeg::FfmpegProcess::write(self, data)
            .await
            .map_err(|e| e.to_string())
    }

    async fn kill(&mut self) {
        rs_ffmpeg::FfmpegProcess::kill(self).await;
    }

    fn last_stderr_line(&self) -> Option<String> {
        rs_ffmpeg::FfmpegProcess::stderr_tail(self)
    }
}

/// Real ffmpeg process factory.
pub struct FfmpegProcessFactory;

impl OutputProcessFactory for FfmpegProcessFactory {
    fn spawn(
        &self,
        service_type: ServiceType,
        stream_key: &str,
        alias: &str,
    ) -> Result<Box<dyn OutputProcess>, String> {
        FfmpegProcess::spawn(service_type, stream_key, alias)
            .map(|p| Box::new(p) as Box<dyn OutputProcess>)
            .map_err(|e| e.to_string())
    }
}
