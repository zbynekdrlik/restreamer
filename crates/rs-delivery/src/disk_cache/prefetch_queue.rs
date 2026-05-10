//! PrefetchQueue — bounded async FIFO between fetcher and pusher.
//! See spec §3.3. Implementation in Task 14.

#![allow(dead_code, unused_imports)]

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone, thiserror::Error)]
#[error("prefetch queue closed")]
pub struct QueueClosed;

pub struct PrefetchQueue<T: Send + 'static> {
    capacity: usize,
    inner: Mutex<VecDeque<T>>,
    not_full: Notify,
    not_empty: Notify,
    closed: AtomicBool,
}

impl<T: Send + 'static> PrefetchQueue<T> {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            inner: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
            not_full: Notify::new(),
            not_empty: Notify::new(),
            closed: AtomicBool::new(false),
        })
    }

    pub async fn push_back(&self, _item: T) -> Result<(), QueueClosed> {
        unimplemented!("Task 14")
    }

    pub async fn pop_front(&self) -> Result<T, QueueClosed> {
        unimplemented!("Task 14")
    }

    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn close(&self) {
        unimplemented!("Task 14")
    }
}
