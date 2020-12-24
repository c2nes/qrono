use std::net::SocketAddr;

use bytes::BytesMut;
use futures::stream::StreamExt;
use redis_async::resp::RespCodec;
use redis_async::resp_array;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::codec::{Encoder, FramedRead};

async fn bench() -> Result<(), std::io::Error> {
    let addr: SocketAddr = "127.0.0.1:16379".parse().unwrap();
    let mut client = TcpStream::connect(addr).await?;
    let (rx, mut tx) = client.split();
    let mut total = 0;

    // redis-benchmark is pretty simple
    // - Accept a command "cmd" and pipeline length "P"
    // - Pre-build a buffer with "cmd" repeated "P" times
    // - Write the output buffer entirely
    // - Read all responses entirely
    // - Repeat until the requested number of commands have been issued
    let cmd = resp_array!["ENQUEUE", "my-queue", "my-value-123"];
    let mut codec = RespCodec;
    let mut obuf = BytesMut::new();

    for _ in 0..10 {
        codec.encode(cmd.clone(), &mut obuf).unwrap();
    }

    let mut reader = FramedRead::new(rx, codec);

    while total < 1_000_000 {
        tx.write_all(&obuf).await?;
        for _ in 0..10 {
            reader.next().await.unwrap().unwrap();
        }
        total += 10;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    bench().await.unwrap();
    println!("Done!");
}
