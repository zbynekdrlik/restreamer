use bytes::BytesMut;
use xmpegts::define::epsi_stream_type;
use xmpegts::define::{MPEG_FLAG_H264_H265_WITH_AUD, MPEG_FLAG_IDR_FRAME};
use xmpegts::ts::TsMuxer as XiuTsMuxer;

/// MPEG-TS muxer wrapping the xmpegts crate for proper transport stream output.
///
/// Produces valid MPEG-TS with PAT/PMT tables, PES headers, and correct
/// H.264/AAC codec handling. This replaces the previous simplified wrapper
/// that only created raw 188-byte packets without proper program information.
pub struct TsMuxer {
    inner: XiuTsMuxer,
    video_pid: Option<u16>,
    audio_pid: Option<u16>,
}

impl TsMuxer {
    pub fn new() -> Self {
        Self {
            inner: XiuTsMuxer::new(),
            video_pid: None,
            audio_pid: None,
        }
    }

    /// Register H.264 video and AAC audio streams in the muxer.
    /// Must be called before writing any media data.
    pub fn init_streams(&mut self) -> Result<(), crate::InpointError> {
        let video_pid = self
            .inner
            .add_stream(epsi_stream_type::PSI_STREAM_H264, BytesMut::new())
            .map_err(|e| crate::InpointError::Muxer(format!("add H264 stream: {e}")))?;
        let audio_pid = self
            .inner
            .add_stream(epsi_stream_type::PSI_STREAM_AAC, BytesMut::new())
            .map_err(|e| crate::InpointError::Muxer(format!("add AAC stream: {e}")))?;
        self.video_pid = Some(video_pid);
        self.audio_pid = Some(audio_pid);
        Ok(())
    }

    /// Write an H.264 video frame. PTS/DTS are in milliseconds (scaled to 90kHz internally).
    pub fn write_video(
        &mut self,
        pts: i64,
        dts: i64,
        is_keyframe: bool,
        data: BytesMut,
    ) -> Result<(), crate::InpointError> {
        let pid = self
            .video_pid
            .ok_or_else(|| crate::InpointError::Muxer("video stream not initialized".into()))?;
        let flags = if is_keyframe {
            MPEG_FLAG_IDR_FRAME | MPEG_FLAG_H264_H265_WITH_AUD
        } else {
            MPEG_FLAG_H264_H265_WITH_AUD
        };
        self.inner
            .write(pid, pts * 90, dts * 90, flags, data)
            .map_err(|e| crate::InpointError::Muxer(format!("write video: {e}")))?;
        Ok(())
    }

    /// Write an AAC audio frame. PTS/DTS are in milliseconds (scaled to 90kHz internally).
    pub fn write_audio(
        &mut self,
        pts: i64,
        dts: i64,
        data: BytesMut,
    ) -> Result<(), crate::InpointError> {
        let pid = self
            .audio_pid
            .ok_or_else(|| crate::InpointError::Muxer("audio stream not initialized".into()))?;
        self.inner
            .write(pid, pts * 90, dts * 90, 0, data)
            .map_err(|e| crate::InpointError::Muxer(format!("write audio: {e}")))?;
        Ok(())
    }

    /// Extract accumulated MPEG-TS output bytes.
    pub fn get_data(&mut self) -> BytesMut {
        self.inner.get_data()
    }

    /// Reset the muxer to initial state.
    pub fn reset(&mut self) {
        self.inner = XiuTsMuxer::new();
        self.video_pid = None;
        self.audio_pid = None;
    }
}

impl Default for TsMuxer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_streams_succeeds() {
        let mut muxer = TsMuxer::new();
        assert!(muxer.init_streams().is_ok());
        assert!(muxer.video_pid.is_some());
        assert!(muxer.audio_pid.is_some());
    }

    #[test]
    fn write_video_without_init_fails() {
        let mut muxer = TsMuxer::new();
        let result = muxer.write_video(0, 0, true, BytesMut::from(&[0u8; 100][..]));
        assert!(result.is_err());
    }

    #[test]
    fn write_audio_without_init_fails() {
        let mut muxer = TsMuxer::new();
        let result = muxer.write_audio(0, 0, BytesMut::from(&[0u8; 100][..]));
        assert!(result.is_err());
    }

    #[test]
    fn write_video_produces_ts_output() {
        let mut muxer = TsMuxer::new();
        muxer.init_streams().unwrap();

        // Write a video frame (simulated H.264 NALU with start code)
        let mut data = BytesMut::new();
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01, 0x65]); // IDR NALU
        data.extend_from_slice(&vec![0xAA; 200]);
        muxer.write_video(0, 0, true, data).unwrap();

        let output = muxer.get_data();
        // Should produce MPEG-TS packets (188 bytes each)
        assert!(!output.is_empty());
        assert_eq!(output.len() % 188, 0);
        // First byte should be TS sync byte
        assert_eq!(output[0], 0x47);
    }

    #[test]
    fn write_audio_produces_ts_output() {
        let mut muxer = TsMuxer::new();
        muxer.init_streams().unwrap();

        let mut data = BytesMut::new();
        data.extend_from_slice(&vec![0xBB; 100]); // simulated AAC frame
        muxer.write_audio(0, 0, data).unwrap();

        let output = muxer.get_data();
        assert!(!output.is_empty());
        assert_eq!(output.len() % 188, 0);
        assert_eq!(output[0], 0x47);
    }

    #[test]
    fn reset_clears_state() {
        let mut muxer = TsMuxer::new();
        muxer.init_streams().unwrap();

        let mut data = BytesMut::new();
        data.extend_from_slice(&vec![0xCC; 200]);
        muxer.write_video(0, 0, true, data).unwrap();
        let _ = muxer.get_data();

        muxer.reset();
        assert!(muxer.video_pid.is_none());
        assert!(muxer.audio_pid.is_none());

        let output = muxer.get_data();
        assert!(output.is_empty());
    }
}
