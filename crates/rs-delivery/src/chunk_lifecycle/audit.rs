//! Audit-row emit helpers for the lifecycle module (#184).
//! Implemented in Task 12.

#![allow(dead_code)]

use super::timings::ChunkLifecycleTimings;
use crate::audit_ring::AuditRing;
use std::sync::Arc;

pub fn emit_lifecycle_sample(_ring: &Arc<AuditRing>, _t: &ChunkLifecycleTimings) {
    unimplemented!("Task 12")
}

pub fn emit_lifecycle_breach(_ring: &Arc<AuditRing>, _t: &ChunkLifecycleTimings) {
    unimplemented!("Task 12")
}

pub fn emit_lifecycle_predeath(_ring: &Arc<AuditRing>, _ts: &[ChunkLifecycleTimings]) {
    unimplemented!("Task 12")
}
