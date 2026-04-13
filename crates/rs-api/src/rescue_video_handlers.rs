//! Handler for uploading rescue videos to S3.
//!
//! The frontend sends a multipart/form-data request with a "file" field;
//! the video bytes are streamed to S3 at `rescue-videos/<uuid>.<ext>`
//! with public-read ACL. The returned URL is written into the event or
//! template's rescue_video_url field by the caller.
//!
//! Public-read ACL is required so the delivery VPS can fetch the video
//! via ffmpeg's HTTP input (no S3 credentials on the VPS for this path).

use axum::extract::{Multipart, State};
use axum::http::StatusCode;
use axum::response::Json;
use rs_endpoint::s3::S3Client;
use serde::Serialize;
use tracing::{error, info};
use uuid::Uuid;

use crate::state::AppState;

/// Cap on rescue video upload size. A few seconds of H.264+AAC at 1080p
/// comfortably fits here; anything much larger is probably a misconfig.
const MAX_RESCUE_VIDEO_BYTES: usize = 100 * 1024 * 1024; // 100 MB

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

fn extension_from_content_type(ct: &str) -> &'static str {
    match ct {
        "video/mp4" | "application/mp4" => "mp4",
        "video/webm" => "webm",
        "video/quicktime" => "mov",
        "video/x-matroska" => "mkv",
        _ => "mp4",
    }
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

        let content_type = field
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_string();

        let bytes = field.bytes().await.map_err(|e| {
            err(
                StatusCode::BAD_REQUEST,
                format!("failed to read file body: {e}"),
            )
        })?;

        if bytes.is_empty() {
            return Err(err(StatusCode::BAD_REQUEST, "file is empty"));
        }
        if bytes.len() > MAX_RESCUE_VIDEO_BYTES {
            return Err(err(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "file too large: {} bytes (max {})",
                    bytes.len(),
                    MAX_RESCUE_VIDEO_BYTES
                ),
            ));
        }

        let ext = extension_from_content_type(&content_type);
        let key = format!("rescue-videos/{}.{ext}", Uuid::new_v4());

        let url = s3_client
            .upload_public_object(&key, &bytes, &content_type)
            .await
            .map_err(|e| {
                error!("S3 upload failed: {e}");
                err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("S3 upload failed: {e}"),
                )
            })?;

        info!(
            "Rescue video uploaded: {} ({} bytes, content_type={})",
            url,
            bytes.len(),
            content_type
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

    #[test]
    fn extension_mp4() {
        assert_eq!(extension_from_content_type("video/mp4"), "mp4");
    }

    #[test]
    fn extension_webm() {
        assert_eq!(extension_from_content_type("video/webm"), "webm");
    }

    #[test]
    fn extension_unknown_falls_back_to_mp4() {
        assert_eq!(
            extension_from_content_type("application/octet-stream"),
            "mp4"
        );
    }
}
