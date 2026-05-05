//! EndpointPositionRegistry -- tracks per-endpoint chunk_id for eviction.

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EndpointWindow {
    pub alias: String,
    pub current_chunk_id: i64,
    pub cache_window_chunks: i64,
}

pub struct EndpointPositionRegistry {
    _placeholder: (),
}

impl EndpointPositionRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { _placeholder: () })
    }
}
