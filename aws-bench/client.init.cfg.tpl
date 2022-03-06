fqdn: client.qrono.test

runcmd:
  - "echo ${server_ip} server.qrono.test >> /etc/hosts"

package_update: true
packages:
- build-essential
- git
- tmux
- redis-tools

write_files:
# The write-files module is configured to run before the users-groups modules so
# we place these files in /etc/skel instead of putting them directly into
# /home/ubuntu.
- path: /etc/skel/.tmux.conf
  content: "${filebase64("${root}/tmux.conf")}"
  owner: root:root
  permissions: '0644'
  encoding: b64

# bash profile
- path: /etc/skel/.bash_profile
  content: "${filebase64("${root}/bash_profile")}"
  owner: root:root
  permissions: '0644'
  encoding: b64

# ubuntu user init script
- path: /etc/skel/bin/user.init.sh
  content: "${filebase64("${root}/client-user.init.sh")}"
  owner: root:root
  permissions: '0755'
  encoding: b64

# There is no obvious way to create a directory by itself so we create an empty
# file in the bin directory to ensure the bin/ directory exists.
- path: /etc/skel/bin/.keepdir
  owner: root:root
