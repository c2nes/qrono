#!/bin/bash

set -euo pipefail

# Install docker
yum update -y
amazon-linux-extras install -y docker
systemctl start docker.service

# Grant ec2-user docker access
usermod -a -G docker ec2-user

# Adjust sysctls for async-profiler
sysctl kernel.perf_event_paranoid=1
sysctl kernel.kptr_restrict=0

# Install async-profiler
curl -fsL 'https://github.com/jvm-profiling-tools/async-profiler/releases/download/v2.0/async-profiler-2.0-linux-x64.tar.gz' \
    | tar xzf - -C /var/lib/qrono

# Start user level initialization in tmux
runuser -u ec2-user -- bash -s <<'EOF'
set -euo pipefail
cd "$HOME"
tmux new-session -d -n init; sleep 1
tmux send-keys -l -t init $'user.init.sh\n'
EOF
