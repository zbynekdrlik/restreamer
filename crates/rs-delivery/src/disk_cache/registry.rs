//! ChunkRegistry -- in-memory chunk-availability tracker with async wake.
//!
//! Owns the source of truth for "is chunk N on disk and ready to read?".
//! `DownloadService` calls `mark_available` after the file rename;
//! `EndpointReader` calls `wait_for_chunk` to block until ready.

use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum ChunkAvailability {
    Available { size_bytes: u64 },
    NotFound,
    InFlight,
    Evicted,
}

pub struct ChunkRegistry {
    // implemented in Task 4
    _placeholder: (),
}

impl ChunkRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { _placeholder: () })
    }
}
