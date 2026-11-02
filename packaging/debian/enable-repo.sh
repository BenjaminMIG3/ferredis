#!/usr/bin/env bash
# Active le dépôt apt Ferredis → ensuite : sudo apt-get install ferredis
set -euo pipefail
if [[ "$(id -u)" -ne 0 ]]; then
  echo "Exécuter en root : sudo $0" >&2
  exit 1
fi

LIST=/etc/apt/sources.list.d/ferredis.list
cat > "$LIST" <<'EOF'
deb [arch=amd64 trusted=yes] https://benjaminmig3.github.io/ferredis/apt stable main
EOF

apt-get update
echo "Dépôt activé. Installation :"
echo "  sudo apt-get install -y ferredis"
echo "  sudo systemctl enable --now ferredis"
