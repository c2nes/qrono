use async_std::task::JoinHandle;
use async_std::{channel, task};
use futures::stream::StreamExt;
use redis::RedisResult;

async fn bench() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1:16379/")?;
    let conn = client.get_multiplexed_async_connection().await?;
    let (s, r) = channel::bounded::<JoinHandle<RedisResult<(i64, i64)>>>(10);
    let complete = task::spawn(r.for_each(|handle| async {
        handle.await.unwrap();
    }));

    for _ in 0..1_000_000 {
        let mut conn = conn.clone();
        let handle = task::spawn(async move {
            redis::cmd("ENQUEUE")
                .arg("my-queue")
                .arg("my-value-123")
                .query_async::<_, (i64, i64)>(&mut conn)
                .await
        });

        s.send(handle).await.expect("channel should not be closed")
    }

    s.close();
    complete.await;

    Ok(())
}

fn main() {
    async_std::task::block_on(bench()).unwrap();
    println!("Done!");
}
