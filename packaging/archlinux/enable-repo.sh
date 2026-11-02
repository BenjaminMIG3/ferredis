#!/usr/bin/env bash
# Active le dépôt pacman Ferredis → ensuite : sudo pacman -Syu ferredis
set -euo pipefail
if [[ "$(id -u)" -ne 0 ]]; then
  echo "Exécuter en root : sudo $0" >&2
  exit 1
fi

CONF=/etc/pacman.d/ferredis.conf
cat > "$CONF" <<'EOF'
[ferredis]
SigLevel = Optional TrustAll
Server = https://benjamindusunceli.github.io/ferredis/archlinux/$arch
EOF

if ! grep -q 'Include = /etc/pacman.d/ferredis.conf' /etc/pacman.conf; then
  printf '\n# Ferredis custom repo\nInclude = /etc/pacman.d/ferredis.conf\n' >> /etc/pacman.conf
fi

pacman -Sy
echo "Dépôt activé. Installation :"
echo "  sudo pacman -S ferredis"
echo "  sudo systemctl enable --now ferredis"
