use serde::Serialize;

const DEFAULT_SERVICE_URL: &str = "http://127.0.0.1:8910";

#[derive(Debug, Serialize)]
pub struct ServiceStatusResponse {
    pub connected: bool,
    pub status: Option<serde_json::Value>,
    pub error: Option<String>,
}

#[tauri::command]
pub async fn get_service_status() -> ServiceStatusResponse {
    let url = format!("{DEFAULT_SERVICE_URL}/api/v1/status");
    match reqwest::get(&url).await {
        Ok(response) => match response.json::<serde_json::Value>().await {
            Ok(status) => ServiceStatusResponse {
                connected: true,
                status: Some(status),
                error: None,
            },
            Err(e) => ServiceStatusResponse {
                connected: true,
                status: None,
                error: Some(format!("Failed to parse response: {e}")),
            },
        },
        Err(e) => ServiceStatusResponse {
            connected: false,
            status: None,
            error: Some(format!("Service unreachable: {e}")),
        },
    }
}

#[tauri::command]
pub fn get_service_url() -> String {
    DEFAULT_SERVICE_URL.to_string()
}
