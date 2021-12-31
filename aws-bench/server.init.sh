#!/bin/bash

set -euo pipefail

# Grant ubuntu docker access
usermod -a -G docker ubuntu

# Adjust sysctls for async-profiler
sysctl kernel.perf_event_paranoid=-1
sysctl kernel.kptr_restrict=0

# Assign /var/lib/qrono to ubuntu
chown ubuntu:ubuntu /var/lib/qrono

# Start user level initialization in tmux
runuser -u ubuntu -- bash -s <<'EOF'
set -euo pipefail
cd "$HOME"
tmux new-session -d -n init; sleep 1
tmux send-keys -l -t init $'user.init.sh\n'
EOF
