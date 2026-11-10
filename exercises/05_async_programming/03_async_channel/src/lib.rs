//! # Async Channel
//!
//! In this exercise, you will use `tokio::sync::mpsc` async channels to implement producer-consumer pattern.
//!
//! ## Concepts
//! - `tokio::sync::mpsc::channel` creates bounded async channels
//! - Async `send` and `recv`
//! - Channel closing mechanism (receiver returns None after all senders are dropped)

use tokio::sync::mpsc;

/// Async producer-consumer:
/// - Create a producer task that sends each element from items sequentially
/// - Create a consumer task that receives all elements and collects them into Vec for return
///
/// Hint: Set channel capacity to items.len().max(1)
pub async fn producer_consumer(items: Vec<String>) -> Vec<String> {
    // TODO: Create channel with mpsc::channel
    // TODO: Spawn producer task: iterate through items, send each one
    // TODO: Spawn consumer task: loop recv until channel closes, collect results
    // TODO: Wait for consumer to complete and return results
    //todo!()
    // channel容量不能是0，所以空数组时使用1
    let capacity = items.len().max(1);
    let (tx, mut rx) = mpsc::channel(capacity);

    // 生产者：依次发送所有元素
    let producer = tokio::spawn(async move {
        for item in items {
            tx.send(item)
                .await
                .expect("consumer was dropped");
        }
        // 任务结束后，tx自动销毁，通道关闭
    });

    // 消费者：一直接收，直到所有Sender都被销毁
    let consumer = tokio::spawn(async move {
        let mut result = Vec::new();

        while let Some(item) = rx.recv().await {
            result.push(item);
        }

        result
    });

    producer.await.expect("producer task panicked");
    consumer.await.expect("consumer task panicked")
}

/// Fan‑in pattern: multiple producers, one consumer.
/// Create `n_producers` producers, each sending `"producer {id}: message"`.
/// Consumer collects all messages, sorts them, and returns.
pub async fn fan_in(n_producers: usize) -> Vec<String> {
    // TODO: Create mpsc channel
    // TODO: Spawn n_producers producer tasks
    //       Each sends format!("producer {id}: message")
    // TODO: Drop the original sender (important! otherwise channel won't close)
    // TODO: Consumer loops receiving, collects and sorts
    //todo!()
    let capacity = n_producers.max(1);
    let (tx, mut rx) = mpsc::channel(capacity);
    let mut producers = Vec::with_capacity(n_producers);

    for id in 0..n_producers {
        // 每个生产者都需要自己的Sender
        let producer_tx = tx.clone();

        producers.push(tokio::spawn(async move {
            let message = format!("producer {id}: message");

            producer_tx
                .send(message)
                .await
                .expect("consumer was dropped");
        }));
    }

    // 必须销毁最初的Sender。
    // 之后所有生产者任务结束，通道才能真正关闭。
    drop(tx);

    let mut results = Vec::with_capacity(n_producers);

    while let Some(message) = rx.recv().await {
        results.push(message);
    }

    // 检查生产者任务有没有发生panic
    for producer in producers {
        producer.await.expect("producer task panicked");
    }

    // 多个生产者的发送顺序不确定，排序后再返回
    results.sort();
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_producer_consumer() {
        let items = vec!["hello".into(), "async".into(), "world".into()];
        let result = producer_consumer(items.clone()).await;
        assert_eq!(result, items);
    }

    #[tokio::test]
    async fn test_producer_consumer_empty() {
        let result = producer_consumer(vec![]).await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_fan_in() {
        let result = fan_in(3).await;
        assert_eq!(
            result,
            vec![
                "producer 0: message",
                "producer 1: message",
                "producer 2: message",
            ]
        );
    }

    #[tokio::test]
    async fn test_fan_in_single() {
        let result = fan_in(1).await;
        assert_eq!(result, vec!["producer 0: message"]);
    }
}
