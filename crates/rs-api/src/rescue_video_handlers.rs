//! Handler for uploading rescue videos to S3.
//!
//! The frontend sends a multipart/form-data request with a "file" field;
//! the bytes are written to a temp file, transcoded ONCE via ffmpeg to a
//! normalized 1080p30 H.264 main + AAC 48 kHz stereo FLV, validated with
//! ffprobe, and only then uploaded to S3 at `rescue-videos/<uuid>.flv`
//! with public-read ACL. The returned URL is written into the event or
//! template's rescue_video_url field by the caller.
//!
//! Why transcode at upload time:
//! - The rs-delivery VPS pushes the rescue video as-is via the rust RTMP
//!   pusher (no ffmpeg installed on the VPS). The S3 object MUST already
//!   be a valid FLV with codecs YouTube/FB accept; we cannot rely on the
//!   operator uploading the right container/codec combo.
//! - stream.lan has ffmpeg available (the chunker already depends on it),
//!   so doing the heavy work here is free and keeps the outage-time path
//!   ffmpeg-less.
//!
//! Public-read ACL is required so the delivery VPS can fetch the FLV via
//! plain HTTPS (no S3 credentials on the VPS for this path).

use axum::extract::{Multipart, State};
use axum::http::StatusCode;
use axum::response::Json;
use rs_endpoint::s3::S3Client;
use serde::Serialize;
use tracing::{error, info};
use uuid::Uuid;

use crate::state::AppState;

/// Cap on uploaded INPUT size (200 MB). We transcode down to a much
/// smaller FLV; this just stops the operator from accidentally uploading
/// a 4 GB master. Literal value (not arithmetic) so cargo-mutants can't
/// generate surviving mutations on the multiplication operators.
const MAX_INPUT_BYTES: usize = 209_715_200; // 200 * 1024 * 1024

/// Cap on the transcoded FLV (50 MB). Rescue videos loop, so anything
/// longer than ~5 min @ 1.5 Mbit is wasted disk + bandwidth on the VPS.
/// Literal value for the same cargo-mutants reason.
const MAX_FLV_BYTES: usize = 52_428_800; // 50 * 1024 * 1024

#[derive(Serialize)]
pub struct UploadResponse {
    pub url: String,
}

#[derive(Serialize)]
pub struct ErrorBody {
    pub error: String,
}

fn err(code: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (code, Json(ErrorBody { error: msg.into() }))
}

/// Return the last `n` lines of `s`, joined with `\n`. Used to surface
/// the tail of ffmpeg stderr to the operator on transcode failure.
fn tail_lines(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join("\n")
}

pub async fn upload_rescue_video(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, (StatusCode, Json<ErrorBody>)> {
    let s3_client = S3Client::new(&state.config.s3).map_err(|e| {
        error!("Failed to create S3 client: {e}");
        err(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("S3 client init failed: {e}"),
        )
    })?;

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        err(
            StatusCode::BAD_REQUEST,
            format!("multipart parse error: {e}"),
        )
    })? {
        let name = field.name().unwrap_or("").to_string();
        if name != "file" {
            continue;
        }

        let bytes = field.bytes().await.map_err(|e| {
            err(
                StatusCode::BAD_REQUEST,
                format!("failed to read file body: {e}"),
            )
        })?;

        if bytes.is_empty() {
            return Err(err(StatusCode::BAD_REQUEST, "file is empty"));
        }
        if bytes.len() > MAX_INPUT_BYTES {
            return Err(err(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "input too large: {} bytes (max {})",
                    bytes.len(),
                    MAX_INPUT_BYTES
                ),
            ));
        }

        // Write input to temp file. ffmpeg needs a path or pipe; we pick
        // a path because the input may not be seekable from a pipe for
        // some containers (e.g. MP4 with the moov atom at the end).
        let input_tmp = tempfile::Builder::new()
            .prefix("rescue-input-")
            .tempfile()
            .map_err(|e| {
                err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("tempfile create (input): {e}"),
                )
            })?;
        std::fs::write(input_tmp.path(), &bytes).map_err(|e| {
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("tempfile write (input): {e}"),
            )
        })?;

        // Output temp with explicit .flv suffix so ffmpeg infers the
        // container correctly even if the `-f flv` is dropped someday.
        let output_tmp = tempfile::Builder::new()
            .prefix("rescue-output-")
            .suffix(".flv")
            .tempfile()
            .map_err(|e| {
                err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("tempfile create (output): {e}"),
                )
            })?;

        let input_path = input_tmp.path().to_string_lossy().to_string();
        let output_path = output_tmp.path().to_string_lossy().to_string();

        // ONE-TIME transcode. Args chosen to match what the chunker and
        // YT/FB ingest happily accept:
        // - libx264 main profile (broadest decoder support)
        // - yuv420p pixel format (REQUIRED: main profile rejects 4:4:4
        //   input; without this we crash on any high-bit / 4:4:4 source)
        // - 30 fps + 60-frame keyframe interval (2s GOP) — matches our
        //   live chunker so the player switches smoothly
        // - 1500 kbit video, AAC 48 kHz stereo 64 kbit audio
        let transcode = tokio::process::Command::new("ffmpeg")
            .args([
                "-y",
                "-i",
                &input_path,
                "-c:v",
                "libx264",
                "-profile:v",
                "main",
                "-preset",
                "medium",
                "-pix_fmt",
                "yuv420p",
                "-r",
                "30",
                "-g",
                "60",
                "-b:v",
                "1500k",
                "-c:a",
                "aac",
                "-ar",
                "48000",
                "-ac",
                "2",
                "-b:a",
                "64k",
                "-f",
                "flv",
                &output_path,
            ])
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .map_err(|e| {
                error!("ffmpeg spawn failed: {e}");
                err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("ffmpeg spawn failed: {e}"),
                )
            })?;

        if !transcode.status.success() {
            let stderr_str = String::from_utf8_lossy(&transcode.stderr);
            let tail = tail_lines(&stderr_str, 20);
            error!(
                "Rescue transcode failed (input {} bytes): {tail}",
                bytes.len()
            );
            return Err(err(
                StatusCode::BAD_REQUEST,
                format!("transcode failed:\n{tail}"),
            ));
        }

        // Validate the produced FLV. `-v error` keeps stderr quiet on
        // success and the non-zero exit on any structural problem.
        let probe = tokio::process::Command::new("ffprobe")
            .args(["-v", "error", "-i", &output_path])
            .output()
            .await
            .map_err(|e| {
                error!("ffprobe spawn failed: {e}");
                err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("ffprobe spawn failed: {e}"),
                )
            })?;
        if !probe.status.success() {
            let stderr_str = String::from_utf8_lossy(&probe.stderr);
            let tail = tail_lines(&stderr_str, 20);
            error!("Transcoded FLV failed ffprobe validation: {tail}");
            return Err(err(
                StatusCode::BAD_REQUEST,
                format!("transcoded FLV failed ffprobe validation:\n{tail}"),
            ));
        }

        let flv_bytes = std::fs::read(output_tmp.path()).map_err(|e| {
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("read transcoded file: {e}"),
            )
        })?;
        if flv_bytes.len() > MAX_FLV_BYTES {
            return Err(err(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "transcoded FLV too large: {} bytes (max {})",
                    flv_bytes.len(),
                    MAX_FLV_BYTES
                ),
            ));
        }

        let key = format!("rescue-videos/{}.flv", Uuid::new_v4());

        let url = s3_client
            .upload_public_object(&key, &flv_bytes, "video/x-flv")
            .await
            .map_err(|e| {
                error!("S3 upload failed: {e}");
                err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("S3 upload failed: {e}"),
                )
            })?;

        info!(
            "Rescue video transcoded + uploaded: {} ({} input bytes -> {} FLV bytes)",
            url,
            bytes.len(),
            flv_bytes.len()
        );

        return Ok(Json(UploadResponse { url }));
    }

    Err(err(
        StatusCode::BAD_REQUEST,
        "missing 'file' field in multipart body",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note on test scope: end-to-end coverage of the transcode pipeline
    // lives in the CI E2E gate (Task 11) where ffmpeg is actually
    // available. The handler hits S3 + ffmpeg + ffprobe, none of which
    // are usefully mocked at the unit level, so we keep unit tests
    // limited to the helpers and the byte-cap constants. Lock the
    // constants and the tail helper so accidental edits or
    // mutation-test changes are caught here.

    #[test]
    fn max_input_bytes_is_200_mib() {
        assert_eq!(MAX_INPUT_BYTES, 200 * 1024 * 1024);
    }

    #[test]
    fn max_flv_bytes_is_50_mib() {
        assert_eq!(MAX_FLV_BYTES, 50 * 1024 * 1024);
    }

    // Compile-time guard: output cap must stay smaller than input cap,
    // otherwise the operator can't upload anything that would transcode
    // within budget without first hitting MAX_INPUT_BYTES. Using a const
    // assertion (not `assert!(...)` in a #[test]) because clippy folds
    // the runtime form to `assert!(true)` and rejects it.
    const _: () = assert!(MAX_INPUT_BYTES > MAX_FLV_BYTES);

    #[test]
    fn tail_lines_returns_last_n() {
        let s = "a\nb\nc\nd\ne";
        assert_eq!(tail_lines(s, 3), "c\nd\ne");
    }

    #[test]
    fn tail_lines_with_n_greater_than_total_returns_all() {
        let s = "a\nb";
        assert_eq!(tail_lines(s, 10), "a\nb");
    }

    #[test]
    fn tail_lines_empty_input() {
        assert_eq!(tail_lines("", 5), "");
    }

    #[test]
    fn tail_lines_single_line() {
        assert_eq!(tail_lines("only-line", 20), "only-line");
    }
}
