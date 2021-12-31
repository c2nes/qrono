#!/bin/bash

set -euo pipefail

# Start user level initialization in tmux
runuser -u ubuntu -- bash -s <<'EOF'
set -euo pipefail
cd "$HOME"
tmux new-session -d -n init; sleep 1
tmux send-keys -l -t init $'user.init.sh\n'
EOF
