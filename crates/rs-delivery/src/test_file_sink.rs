//! TEST_FILE loopback RTMP accept-and-discard sink (#192).
//!
//! RED skeleton: the interface the delivery binary wires up and the tests
//! drive. The sink does not bind anything yet, so a TEST_FILE push still
//! dies with "connection refused" exactly as it does on the VPS today.

use std::future::Future;
use std::net::SocketAddr;

use serde::Serialize;

/// Where a TEST_FILE endpoint's Rust pusher (and its rescue loop) dials.
pub const TEST_FILE_SINK_ADDR: &str = "127.0.0.1:1935";

/// Point-in-time copy of the sink's counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct SinkCountersSnapshot {
    pub connections_accepted: u64,
    pub active_connections: u64,
    pub publishes: u64,
    pub unpublishes: u64,
    pub tags_received: u64,
    pub bytes_received: u64,
}

/// A running sink's bound address and counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SinkStatus {
    pub local_addr: SocketAddr,
    pub counters: SinkCountersSnapshot,
}

/// Whether an endpoint set (given as its `service_type` strings) needs the sink.
pub fn wants_test_file_sink<'a>(service_types: impl IntoIterator<Item = &'a str>) -> bool {
    let _ = service_types.into_iter();
    false
}

/// Owner of the (at most one) running sink.
pub struct TestFileSinkSlot {
    addr: String,
}

impl TestFileSinkSlot {
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }

    pub fn production() -> Self {
        Self::new(TEST_FILE_SINK_ADDR)
    }

    pub async fn reconcile(&self, wanted: impl Future<Output = bool>) {
        let _ = (&self.addr, wanted.await);
    }

    pub async fn status(&self) -> Option<SinkStatus> {
        None
    }
}

#[cfg(test)]
#[path = "test_file_sink_tests.rs"]
mod tests;
