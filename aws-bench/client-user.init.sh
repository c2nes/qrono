#!/bin/bash
set -euo pipefail

cd "$HOME"

# Install the Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"

# Clone qrono
git clone https://github.com/c2nes/qrono.git

# Build qrono-bench
cargo install --path qrono/qrono-rs/qrono-bench

tmux new-window -c "$HOME/qrono/qrono-rs" -n benchmark
sleep 0.5; tmux send-keys -l -t benchmark 'qrono-bench -t server.qrono.test:16379 -r 1e6 -n 10M --mode publish-then-delete --pipeline 100 -P 10 q'
