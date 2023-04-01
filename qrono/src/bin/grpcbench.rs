use std::time::Instant;

use log::info;
use qrono::grpc::messages::EnqueueRequest;
use tokio_stream::wrappers::ReceiverStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut client = qrono::grpc::QronoClient::connect("grpc://localhost:16381").await?;
    for _ in 0..100 {
        let start = Instant::now();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let mut name = "q".to_string();
            for i in 0..1_000_000i32 {
                let val = i.to_be_bytes().to_vec();
                tx.send(EnqueueRequest {
                    queue: name.clone(),
                    value: val,
                    deadline: None,
                })
                .await
                .unwrap();
                name = String::new();
            }
            info!("Sender done");
        });
        let mut responses = client
            .enqueue_many(ReceiverStream::new(rx))
            .await?
            .into_inner();
        let mut i = 1;
        loop {
            match responses.message().await {
                Ok(Some(resp)) => {
                    if i % 100_000 == 0 {
                        info!("Receive! {}: {:?}", i, resp);
                    }
                }
                Ok(None) => break,
                Err(err) => panic!("{:?}", err),
            }
            i += 1;
        }
        dbg!(Instant::now() - start);
    }
    dbg!(client);
    Ok(())
}
