use super::prefetch_queue::{PrefetchQueue, QueueClosed};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn fifo_order_for_k_eq_3() {
    let q: Arc<PrefetchQueue<i64>> = PrefetchQueue::new(3);
    q.push_back(1).await.unwrap();
    q.push_back(2).await.unwrap();
    q.push_back(3).await.unwrap();
    assert_eq!(q.pop_front().await.unwrap(), 1);
    assert_eq!(q.pop_front().await.unwrap(), 2);
    assert_eq!(q.pop_front().await.unwrap(), 3);
}

#[tokio::test]
async fn push_blocks_when_at_capacity_until_pop_drains_one() {
    let q: Arc<PrefetchQueue<i64>> = PrefetchQueue::new(2);
    q.push_back(1).await.unwrap();
    q.push_back(2).await.unwrap();
    let q2 = Arc::clone(&q);
    let push_task = tokio::spawn(async move { q2.push_back(3).await });
    // Yield to let push_task start. After 50ms, the third push should NOT
    // have completed (queue is at capacity).
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!push_task.is_finished());
    // Drain one slot — push_task must wake and complete.
    assert_eq!(q.pop_front().await.unwrap(), 1);
    push_task.await.unwrap().unwrap();
    assert_eq!(q.pop_front().await.unwrap(), 2);
    assert_eq!(q.pop_front().await.unwrap(), 3);
}

#[tokio::test]
async fn pop_blocks_when_empty_until_push_arrives() {
    let q: Arc<PrefetchQueue<i64>> = PrefetchQueue::new(2);
    let q2 = Arc::clone(&q);
    let pop_task = tokio::spawn(async move { q2.pop_front().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!pop_task.is_finished());
    q.push_back(42).await.unwrap();
    let got = pop_task.await.unwrap().unwrap();
    assert_eq!(got, 42);
}

#[tokio::test]
async fn close_unblocks_pending_push_and_pop_with_err() {
    let q: Arc<PrefetchQueue<i64>> = PrefetchQueue::new(1);
    q.push_back(1).await.unwrap();
    let q2 = Arc::clone(&q);
    let push_task = tokio::spawn(async move { q2.push_back(2).await });
    let q3 = Arc::clone(&q);
    let pop_task = tokio::spawn(async move {
        // First pop drains slot, second pop blocks then sees Closed.
        let _ = q3.pop_front().await;
        q3.pop_front().await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    q.close();
    let push_res = push_task.await.unwrap();
    let pop_res = pop_task.await.unwrap();
    assert!(matches!(push_res, Err(QueueClosed)));
    assert!(matches!(pop_res, Err(QueueClosed)));
}

#[tokio::test]
async fn k_zero_is_synchronous_rendezvous() {
    let q: Arc<PrefetchQueue<i64>> = PrefetchQueue::new(0);
    let q2 = Arc::clone(&q);
    let push_task = tokio::spawn(async move { q2.push_back(7).await });
    // K=0 means push must NOT complete until a matching pop is in flight.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !push_task.is_finished(),
        "K=0 push must rendezvous with pop"
    );
    let got = q.pop_front().await.unwrap();
    assert_eq!(got, 7);
    push_task.await.unwrap().unwrap();
    assert_eq!(q.len().await, 0, "K=0 queue never holds anything");
}

#[tokio::test]
async fn len_and_capacity_observable_for_dashboard() {
    let q: Arc<PrefetchQueue<i64>> = PrefetchQueue::new(4);
    assert_eq!(q.capacity(), 4);
    assert_eq!(q.len().await, 0);
    q.push_back(1).await.unwrap();
    q.push_back(2).await.unwrap();
    assert_eq!(q.len().await, 2);
}
