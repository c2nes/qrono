#!/usr/bin/env bash

set -euo pipefail

cd "$HOME"

# Symlink go/gofmt to gv for the ubuntu user...really seems like cloudinit
# should be able to write symlinks, but here we are...
ln -sv gv bin/go
ln -sv gv bin/gofmt

# For local tools, including Maven
mkdir -p local

# Install async-profiler
curl -fsL 'https://github.com/jvm-profiling-tools/async-profiler/releases/download/v2.0/async-profiler-2.0-linux-x64.tar.gz' \
    | tar xzf - -C /var/lib/qrono

# Install the Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"

# Clone qrono
git clone https://github.com/c2nes/qrono.git

# Build and install Qrono
cargo install --path qrono/qrono-rs/qrono --bin qrono &

# Build perf from source
(
    cd /var/lib/qrono
    mkdir tmp
    cd tmp
    sudo sed -i.backup -e 's/# deb-src/deb-src/' /etc/apt/sources.list
    sudo apt-get update
    apt-get source linux-image-$(uname -r)
    cd linux-aws-*/tools/perf
    sudo make install WERROR=0 prefix=/usr/local
    cd /var/lib/qrono
    sudo rm -rf tmp
) &

wait

# Start server in tmux
tmux new-window -d -c "$HOME/qrono" -n server
tmux send-keys -l -t server $'RUST_LOG=debug RUST_BACKTRACE=full qrono --listen 0.0.0.0:16379 --data /var/lib/qrono --rayon --working-set-stripes=4 --wal-sync-period=1000\n'

# Start top and enable some colors
tmux new-window -n top
tmux send-keys -l -t top $'top\n'
sleep 0.5; tmux send-keys -l -t top z
sleep 0.1; tmux send-keys -l -t top x
sleep 0.1; tmux send-keys -l -t top W
# Run iostat alongside top
tmux split-window -h -t top.0
tmux send-keys -l -t top.1 $'iostat -xdh /dev/nvme1n1 5\n'

# Switch to watching server startup
tmux select-window -t server
