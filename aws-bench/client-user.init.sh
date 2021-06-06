#!/bin/bash
set -euo pipefail

cd "$HOME"

# Install the Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"

# Clone qrono
git clone https://github.com/c2nes/qrono.git

# Build and install redis tools
curl -fsLO 'https://download.redis.io/releases/redis-6.2.3.tar.gz'
tar -xzf redis-6.2.3.tar.gz
tmux new-window -d -c "$HOME/redis-6.2.3" -n redis
sleep 0.5; tmux send-keys -l -t redis $'make && sudo make install\n'

# Build qrono-bench
cd qrono/qrono-bench
cargo build --release

tmux new-window -c "$HOME/qrono/qrono-bench" -n benchmark
sleep 0.5; tmux send-keys -l -t benchmark 'target/release/qrono-bench -t server.qrono.test:16379 -r 250000 -n 2000000 -c 2 -C 500 --wait-to-consume --queue-name q'
