pub mod handlers;
pub mod router;
pub mod state;
pub mod websocket;

use std::net::SocketAddr;

use tokio::net::TcpListener;
use tracing::info;

use crate::state::AppState;

/// Start the API server on the given address.
pub async fn serve(state: AppState, addr: SocketAddr) -> anyhow::Result<SocketAddr> {
    let app = router::build_router(state);
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    info!("API server listening on {local_addr}");

    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    Ok(local_addr)
}
