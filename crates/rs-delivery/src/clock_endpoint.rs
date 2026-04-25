//! GET /clock — returns the VPS wall-clock time for skew probing.

use axum::response::Json;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ClockResponse {
    pub vps_ms: i64,
}

pub async fn get_clock() -> Json<ClockResponse> {
    let vps_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    Json(ClockResponse { vps_ms })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, http::Request, http::StatusCode, routing::get};
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn clock_endpoint_returns_current_wall_clock_ms() {
        let app = Router::new().route("/clock", get(get_clock));

        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/clock")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let after = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: ClockResponse = serde_json::from_slice(&body_bytes).unwrap();

        assert!(
            body.vps_ms >= before && body.vps_ms <= after,
            "vps_ms {} outside [{before}, {after}]",
            body.vps_ms
        );
    }
}
