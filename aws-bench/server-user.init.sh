#!/usr/bin/env bash

set -euo pipefail

cd "$HOME"

# Symlink go/gofmt to gv for ec2-user...really seems like cloudinit should be
# able to write symlinks, but here we are...
ln -sv gv bin/go
ln -sv gv bin/gofmt

# For local tools, including Maven
mkdir -p local

# Install Maven
MVN_VERSION="3.6.3"
MVN_URL="https://downloads.apache.org/maven/maven-3/${MVN_VERSION}/binaries/apache-maven-${MVN_VERSION}-bin.tar.gz"
curl -fsL "$MVN_URL" | tar -C local -xzf -
ln -sv "$HOME/local/apache-maven-${MVN_VERSION}/bin/mvn" bin/mvn

# Clone qrono
git clone https://github.com/c2nes/qrono.git

# Build Qrono server JAR
(cd qrono && mvn -B package)

# Build Qrono gateway binary
(cd qrono/gateway && go build .)

# Start server in tmux
tmux new-window -d -c "$HOME/qrono" -n server
tmux send-keys -l -t server $'docker-run\n'

# Start top and enable some colors
tmux new-window -n top
tmux send-keys -l -t top $'top\n'
sleep 0.5; tmux send-keys -l -t top z
sleep 0.1; tmux send-keys -l -t top x
sleep 0.1; tmux send-keys -l -t top W

# Monitor "q" size
tmux new-window -n q-size
tmux send-keys -l -t q-size $'watch -c "curl -s localhost:16380/v1/queues/q | jq -C ."\n'

# Start a shell
tmux new-window -n shell

# Switch to watching server startup
tmux select-window -t server
