#!/bin/bash

set -euo pipefail

# Install docker
yum update -y
amazon-linux-extras install -y docker
systemctl start docker.service

# Grant ec2-user docker access
usermod -a -G docker ec2-user

# Start user level initialization in tmux
runuser -u ec2-user -- bash -s <<'EOF'
set -euo pipefail
cd "$HOME"
tmux new-session -d -n init; sleep 1
tmux send-keys -l -t init $'user.init.sh\n'
EOF
