#!/usr/bin/env bash
# Installation générique Linux (systemd) — Arch, Debian, Fedora, openSUSE, etc.
# Usage (root) :
#   sudo ./packaging/linux/install.sh [--prefix /usr] [--enable] [--start]
# Depuis un tarball dist/linux :
#   sudo ./install.sh --from-dir . --enable --start
set -euo pipefail

PREFIX="/usr"
ENABLE=0
START=0
FROM_DIR=""
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix) PREFIX="$2"; shift 2 ;;
    --from-dir) FROM_DIR="$2"; shift 2 ;;
    --enable) ENABLE=1; shift ;;
    --start) START=1; shift ;;
    -h|--help)
      echo "Usage: sudo $0 [--prefix /usr] [--from-dir DIR] [--enable] [--start]"
      exit 0
      ;;
    *) echo "Option inconnue: $1" >&2; exit 1 ;;
  esac
done

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Exécuter en root (sudo)." >&2
  exit 1
fi

if [[ -n "$FROM_DIR" ]]; then
  BIN_DIR="$(cd "$FROM_DIR" && pwd)"
  UNIT_SRC="${BIN_DIR}/ferredis.service"
  CONF_SRC="${BIN_DIR}/ferredis.conf"
  SYSUSERS_SRC="${BIN_DIR}/ferredis.sysusers"
  TMPFILES_SRC="${BIN_DIR}/ferredis.tmpfiles"
else
  ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
  BIN_DIR="${ROOT}/target/release"
  UNIT_SRC="${SCRIPT_DIR}/ferredis.service"
  CONF_SRC="${SCRIPT_DIR}/ferredis.conf"
  SYSUSERS_SRC="${SCRIPT_DIR}/ferredis.sysusers"
  TMPFILES_SRC="${SCRIPT_DIR}/ferredis.tmpfiles"
fi

for b in ferredis-server ferredis-cli; do
  if [[ ! -x "${BIN_DIR}/${b}" && ! -f "${BIN_DIR}/${b}" ]]; then
    echo "Binaire manquant: ${BIN_DIR}/${b}" >&2
    echo "Build: cargo build --release -p ferredis-server -p ferredis-cli" >&2
    exit 1
  fi
done

install -d "${PREFIX}/bin"
install -Dm755 "${BIN_DIR}/ferredis-server" "${PREFIX}/bin/ferredis-server"
install -Dm755 "${BIN_DIR}/ferredis-cli" "${PREFIX}/bin/ferredis-cli"
if [[ -f "${BIN_DIR}/ferredis-gui" ]]; then
  install -Dm755 "${BIN_DIR}/ferredis-gui" "${PREFIX}/bin/ferredis-gui"
fi

# Utilisateur système
if command -v systemd-sysusers >/dev/null 2>&1 && [[ -f "$SYSUSERS_SRC" ]]; then
  install -Dm644 "$SYSUSERS_SRC" /usr/lib/sysusers.d/ferredis.conf
  systemd-sysusers ferredis.conf 2>/dev/null || systemd-sysusers || true
fi
if ! id ferredis >/dev/null 2>&1; then
  if command -v useradd >/dev/null 2>&1; then
    useradd --system --home-dir /var/lib/ferredis --shell /usr/bin/nologin \
      --user-group ferredis 2>/dev/null \
      || useradd --system -d /var/lib/ferredis -s /sbin/nologin -r ferredis
  fi
fi

install -d -m 0750 -o ferredis -g ferredis /var/lib/ferredis
install -d /etc/ferredis
if [[ ! -f /etc/ferredis/ferredis.conf ]]; then
  install -Dm644 "$CONF_SRC" /etc/ferredis/ferredis.conf
else
  echo "Conserve /etc/ferredis/ferredis.conf existant"
fi

if command -v systemd-tmpfiles >/dev/null 2>&1 && [[ -f "$TMPFILES_SRC" ]]; then
  install -Dm644 "$TMPFILES_SRC" /usr/lib/tmpfiles.d/ferredis.conf
  systemd-tmpfiles --create ferredis.conf 2>/dev/null || true
fi

# Unit systemd (chemin standard multi-distro)
UNIT_DST="/usr/lib/systemd/system/ferredis.service"
if [[ ! -d /usr/lib/systemd/system ]]; then
  UNIT_DST="/etc/systemd/system/ferredis.service"
fi
# Ajuste ExecStart si PREFIX ≠ /usr
if [[ "$PREFIX" != "/usr" ]]; then
  sed "s|/usr/bin/ferredis-server|${PREFIX}/bin/ferredis-server|g" \
    "$UNIT_SRC" > /tmp/ferredis.service.$$
  install -Dm644 /tmp/ferredis.service.$$ "$UNIT_DST"
  rm -f /tmp/ferredis.service.$$
else
  install -Dm644 "$UNIT_SRC" "$UNIT_DST"
fi

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload
  if [[ "$ENABLE" -eq 1 ]]; then
    systemctl enable ferredis.service
  fi
  if [[ "$START" -eq 1 ]]; then
    systemctl restart ferredis.service || systemctl start ferredis.service
  fi
  echo
  echo "Service installé. Commandes :"
  echo "  sudo systemctl enable --now ferredis"
  echo "  sudo systemctl status ferredis"
  echo "  sudo systemctl stop ferredis"
else
  echo "systemd absent — binaires installés dans ${PREFIX}/bin (pas de service)."
fi
