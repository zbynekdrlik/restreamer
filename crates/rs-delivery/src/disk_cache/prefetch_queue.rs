//! PrefetchQueue — bounded async FIFO between disk_cache fetcher and
//! pusher. See spec §3.3 for design rationale.
//!
//! K=0 is a synchronous rendezvous channel — push and pop must meet
//! before either returns. Used by non-fast endpoints to preserve
//! today's zero-buffer behavior.
//!
//! K>=1 buffers up to `capacity` items. Reader awaits `not_full`;
//! pusher awaits `not_empty`.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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

    /// Reader-side: push at back. Awaits `not_full` if at capacity.
    /// For K=0 rendezvous, push always blocks until a matching pop drains it.
    pub async fn push_back(&self, item: T) -> Result<(), QueueClosed> {
        if self.capacity == 0 {
            return self.rendezvous_push(item).await;
        }
        let mut item_slot = Some(item);
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(QueueClosed);
            }
            let notified = self.not_full.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut g = self.inner.lock().await;
                if g.len() < self.capacity {
                    g.push_back(item_slot.take().expect("loop invariant"));
                    drop(g);
                    self.not_empty.notify_one();
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    /// Pusher-side: pop front. Awaits `not_empty` if drained.
    pub async fn pop_front(&self) -> Result<T, QueueClosed> {
        if self.capacity == 0 {
            return self.rendezvous_pop().await;
        }
        loop {
            // If closed, drain whatever remains; once empty -> Err.
            if self.closed.load(Ordering::Acquire) {
                let mut g = self.inner.lock().await;
                if let Some(it) = g.pop_front() {
                    drop(g);
                    self.not_full.notify_one();
                    return Ok(it);
                }
                return Err(QueueClosed);
            }
            let notified = self.not_empty.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut g = self.inner.lock().await;
                if let Some(it) = g.pop_front() {
                    drop(g);
                    self.not_full.notify_one();
                    return Ok(it);
                }
            }
            notified.await;
        }
    }

    /// K=0 push: park the item in the slot, wait for pop to consume.
    /// Returns Ok only after a matching pop has drained the slot.
    async fn rendezvous_push(&self, item: T) -> Result<(), QueueClosed> {
        if self.closed.load(Ordering::Acquire) {
            return Err(QueueClosed);
        }
        // Phase 1: place the item once the slot is empty.
        let mut item_slot = Some(item);
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(QueueClosed);
            }
            let notified = self.not_full.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut g = self.inner.lock().await;
                if g.is_empty() {
                    g.push_back(item_slot.take().expect("loop invariant"));
                    drop(g);
                    self.not_empty.notify_one();
                    break;
                }
            }
            notified.await;
        }
        // Phase 2: wait for the matching pop to drain the slot.
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(QueueClosed);
            }
            let notified = self.not_full.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let g = self.inner.lock().await;
                if g.is_empty() {
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    /// K=0 pop: take the parked item and notify the pusher its rendezvous
    /// completed.
    async fn rendezvous_pop(&self) -> Result<T, QueueClosed> {
        loop {
            if self.closed.load(Ordering::Acquire) {
                let mut g = self.inner.lock().await;
                if let Some(it) = g.pop_front() {
                    drop(g);
                    self.not_full.notify_one();
                    return Ok(it);
                }
                return Err(QueueClosed);
            }
            let notified = self.not_empty.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut g = self.inner.lock().await;
                if let Some(it) = g.pop_front() {
                    drop(g);
                    self.not_full.notify_one();
                    return Ok(it);
                }
            }
            notified.await;
        }
    }

    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Whether `close()` has been called. Used by background producers
    /// (e.g. PrefetchReader) to break out of internal retry loops
    /// without waiting on the next push_back.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Close the queue. Wakes all waiters; subsequent push_back/pop_front
    /// return `Err(QueueClosed)` after any remaining items are drained.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.not_full.notify_waiters();
        self.not_empty.notify_waiters();
    }
}
