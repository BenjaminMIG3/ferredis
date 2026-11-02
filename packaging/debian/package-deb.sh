#!/usr/bin/env bash
# Construit un paquet .deb amd64 (Ubuntu/Debian).
# Réutilise dist/linux si présent, sinon lance packaging/package.sh.
# Usage : bash packaging/debian/package-deb.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DIST="${ROOT}/dist"
LINUX_OUT="${DIST}/linux"
STAGE="${DIST}/deb-root"
PKG_DIR="${STAGE}/DEBIAN"

VER="$(grep -m1 '^version' "${ROOT}/Cargo.toml" | sed -E 's/.*"([^"]+)".*/\1/')"
PKGREL="${PKGREL:-1}"
ARCH=amd64

if [[ ! -x "${LINUX_OUT}/ferredis-server" ]]; then
  echo "Binaires absents — build via packaging/package.sh"
  bash "${ROOT}/packaging/package.sh"
fi

rm -rf "$STAGE"
mkdir -p \
  "${STAGE}/usr/bin" \
  "${STAGE}/usr/lib/systemd/system" \
  "${STAGE}/usr/lib/sysusers.d" \
  "${STAGE}/usr/lib/tmpfiles.d" \
  "${STAGE}/usr/share/doc/ferredis" \
  "${STAGE}/etc/ferredis" \
  "${STAGE}/var/lib/ferredis" \
  "$PKG_DIR"

install -Dm755 "${LINUX_OUT}/ferredis-server" "${STAGE}/usr/bin/ferredis-server"
install -Dm755 "${LINUX_OUT}/ferredis-cli" "${STAGE}/usr/bin/ferredis-cli"
if [[ -f "${LINUX_OUT}/ferredis-gui" ]]; then
  install -Dm755 "${LINUX_OUT}/ferredis-gui" "${STAGE}/usr/bin/ferredis-gui"
fi
install -Dm644 "${ROOT}/packaging/linux/ferredis.service" \
  "${STAGE}/usr/lib/systemd/system/ferredis.service"
install -Dm644 "${ROOT}/packaging/linux/ferredis.conf" \
  "${STAGE}/etc/ferredis/ferredis.conf"
install -Dm644 "${ROOT}/packaging/linux/ferredis.sysusers" \
  "${STAGE}/usr/lib/sysusers.d/ferredis.conf"
install -Dm644 "${ROOT}/packaging/linux/ferredis.tmpfiles" \
  "${STAGE}/usr/lib/tmpfiles.d/ferredis.conf"

if [[ -f "${ROOT}/LICENSE" ]]; then
  install -Dm644 "${ROOT}/LICENSE" "${STAGE}/usr/share/doc/ferredis/copyright"
fi
cat > "${STAGE}/usr/share/doc/ferredis/README.Debian" <<EOF
Ferredis — serveur compatible Redis (Rust).

  sudo systemctl enable --now ferredis
  ferredis-cli PING

Config : /etc/ferredis/ferredis.conf
Données : /var/lib/ferredis
EOF

# --- métadonnées Debian ---
SIZE_KB="$(du -sk "$STAGE" | awk '{print $1}')"
cat > "${PKG_DIR}/control" <<EOF
Package: ferredis
Version: ${VER}-${PKGREL}
Section: database
Priority: optional
Architecture: ${ARCH}
Maintainer: Ferredis Contributors <noreply@users.noreply.github.com>
Homepage: https://github.com/BenjaminMIG3/ferredis
Depends: libc6, libgcc-s1 | libgcc1, libxkbcommon0, libxcb1
Installed-Size: ${SIZE_KB}
Description: Redis-compatible in-memory server written in Rust
 Ferredis is an open-source Redis OSS 8.0-compatible server (RESP,
 RDB/AOF, replication, Cluster) with systemd integration.
EOF

cat > "${PKG_DIR}/conffiles" <<EOF
/etc/ferredis/ferredis.conf
EOF

cat > "${PKG_DIR}/postinst" <<'EOF'
#!/bin/sh
set -e
if command -v systemd-sysusers >/dev/null 2>&1; then
  systemd-sysusers ferredis.conf >/dev/null 2>&1 || true
fi
if ! id ferredis >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/ferredis --shell /usr/sbin/nologin \
    --user-group ferredis 2>/dev/null \
    || useradd --system -d /var/lib/ferredis -s /usr/sbin/nologin -r -U ferredis
fi
install -d -m 0750 -o ferredis -g ferredis /var/lib/ferredis
if command -v systemd-tmpfiles >/dev/null 2>&1; then
  systemd-tmpfiles --create ferredis.conf >/dev/null 2>&1 || true
fi
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  systemctl daemon-reload || true
fi
echo "Ferredis installé. Démarrez avec : sudo systemctl enable --now ferredis"
EOF

cat > "${PKG_DIR}/prerm" <<'EOF'
#!/bin/sh
set -e
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  if [ "$1" = "remove" ] || [ "$1" = "deconfigure" ]; then
    systemctl stop ferredis.service >/dev/null 2>&1 || true
    systemctl disable ferredis.service >/dev/null 2>&1 || true
  fi
fi
EOF

cat > "${PKG_DIR}/postrm" <<'EOF'
#!/bin/sh
set -e
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  systemctl daemon-reload || true
fi
EOF

chmod 755 "${PKG_DIR}/postinst" "${PKG_DIR}/prerm" "${PKG_DIR}/postrm"

DEB_NAME="ferredis_${VER}-${PKGREL}_${ARCH}.deb"
mkdir -p "$DIST"
dpkg-deb --root-owner-group --build "$STAGE" "${DIST}/${DEB_NAME}"
# Alias stable pour scripts / release globs
cp -f "${DIST}/${DEB_NAME}" "${DIST}/ferredis_amd64.deb"
echo "Artefact: ${DIST}/${DEB_NAME}"
ls -la "${DIST}/${DEB_NAME}"
