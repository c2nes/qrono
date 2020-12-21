use futures::stream::StreamExt;
use tokio::task::JoinHandle;

async fn bench() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1:16379/")?;
    let conn = client.get_multiplexed_async_connection().await?;
    let (s, r) = tokio::sync::mpsc::channel::<JoinHandle<()>>(10);
    let complete = tokio::spawn(r.for_each(|handle| async {
        handle.await.unwrap();
    }));

    let s = s;
    for _ in 0..1_000_000 {
        let mut conn = conn.clone();
        let handle = tokio::spawn(async move {
            redis::cmd("ENQUEUE")
                .arg("my-queue")
                .arg("my-value-123")
                .query_async::<_, (i64, i64)>(&mut conn)
                .await
                .unwrap();
        });

        s.send(handle).await.expect("channel should not be closed")
    }

    drop(s);
    complete.await.unwrap();

    Ok(())
}

#[tokio::main]
async fn main() {
    bench().await.unwrap();
    println!("Done!");
}
