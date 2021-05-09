#!/bin/bash

set -euo pipefail

# See https://adoptopenjdk.net/installation.html#linux-pkg-rpm
# Support for amazonlinux is not mentioned explicitly, but is available.
cat <<'EOF' > /etc/yum.repos.d/adoptopenjdk.repo
[AdoptOpenJDK]
name=AdoptOpenJDK
baseurl=http://adoptopenjdk.jfrog.io/adoptopenjdk/rpm/amazonlinux/$releasever/$basearch
enabled=1
gpgcheck=1
gpgkey=https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public
EOF

# See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/docker-basics.html
yum update -y
yum install -y git tmux jq adoptopenjdk-11-hotspot
amazon-linux-extras install -y docker
service docker start
usermod -a -G docker ec2-user

# See https://golang.org/doc/install
curl -fsL "https://golang.org/dl/go1.15.6.linux-amd64.tar.gz" | tar -C /usr/local -xzf -

cat <<'EOF' > /etc/profile.d/golang.sh
export PATH=$PATH:/usr/local/go/bin
EOF

MVN_VERSION="3.6.3"
MVN_URL="https://downloads.apache.org/maven/maven-3/${MVN_VERSION}/binaries/apache-maven-${MVN_VERSION}-bin.tar.gz"
curl -fsL "$MVN_URL" | tar -C /opt -xzf -

cat <<EOF > /etc/profile.d/mvn.sh
export PATH=\$PATH:/opt/apache-maven-${MVN_VERSION}/bin
EOF

runuser -u ec2-user -- bash -l -s <<'EOF'
#!/bin/bash
set -euo pipefail

cd "$HOME"

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

# Clone qrono
git clone https://github.com/c2nes/qrono.git

cd qrono
mvn -B package

(
    cd gateway
    go build .
)

cat <<'DOCKER_RUN_EOF' > docker-run
#!/bin/bash

set -euo pipefail

docker build -t qrono/qrono:local .
docker rm -f qrono || true
exec docker run -it --rm \
       -v /var/lib/qrono:/var/lib/qrono \
       --name qrono \
       --network host \
       qrono/qrono:local
DOCKER_RUN_EOF

chmod +x docker-run

# Start server in tmux
tmux new-session -d -n server
tmux send-keys -l -t server $'./docker-run\n'

# Start top and enable some colors
tmux new-window -n top
tmux send-keys -l -t top $'top\n'
sleep 0.5; tmux send-keys -l -t top z
sleep 0.1; tmux send-keys -l -t top x
sleep 0.1; tmux send-keys -l -t top W

# Monitor "q" size
tmux new-window -n q-size
tmux send-keys -l -t q-size $'watch -c "curl -s localhost:16380/v1/queues/q | jq -C ."\n'

# Finally, start a shell
tmux new-window -n shell
EOF
