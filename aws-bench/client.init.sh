#!/bin/bash

set -euo pipefail

yum update -y
yum install -y git gcc tmux

runuser -u ec2-user -- bash -s <<'EOF'
#!/bin/bash
set -euo pipefail

cd "$HOME"

# tmux config
cat <<'TMUX_EOF' > .tmux.conf
set -g prefix '^o'
bind-key '^o' last-window
set -g default-terminal "xterm-256color"
set -gs escape-time 10
set -g visual-activity off
set -g visual-bell off
set -g visual-silence off
setw -g monitor-activity off
set -g bell-action none
TMUX_EOF

# Install the Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Clone qrono
git clone https://github.com/c2nes/qrono.git

cd qrono/qrono-bench

# Start a tmux session
tmux new-session -d -n build
sleep 0.5; tmux send-keys -l -t build $'cargo build --release\n'

tmux new-window -d -n benchmark
sleep 0.5; tmux send-keys -l -t benchmark './target/release/qrono-bench -t server.qrono.test:16379 -r 250000 -n 2000000 -c 2 -C 500 --wait-to-consume --queue-name q'
EOF
