#!/usr/bin/env bash
# Produit un paquet pacman (.pkg.tar.zst) depuis la racine du repo (CI ou machine Arch).
# Usage : depuis la racine → ./packaging/archlinux/package-arch.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ARCH_DIR="${ROOT}/packaging/archlinux"
OUT="${ROOT}/dist"
mkdir -p "$OUT"

# Version depuis workspace Cargo.toml
VER="$(grep -m1 '^version' "${ROOT}/Cargo.toml" | sed -E 's/.*"([^"]+)".*/\1/')"
sed -i "s/^pkgver=.*/pkgver=${VER}/" "${ARCH_DIR}/PKGBUILD"

# Source locale : archive le workspace sans target/
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
STAGE="${TMP}/ferredis-${VER}"
mkdir -p "$STAGE"
# Copie minimale pour makepkg (évite target/ et dist/)
rsync -a --exclude target --exclude dist --exclude .git \
  --exclude 'ferredis-compat/appendonly.aof' --exclude 'ferredis-compat/ferredis-cold' \
  "${ROOT}/" "${STAGE}/"
tar -C "$TMP" -czf "${ARCH_DIR}/ferredis-${VER}.tar.gz" "ferredis-${VER}"

# PKGBUILD en mode source locale (inclut systemd)
cat > "${ARCH_DIR}/PKGBUILD.local" <<EOF
pkgname=ferredis
pkgver=${VER}
pkgrel=1
pkgdesc="Redis-compatible in-memory server written in Rust"
arch=('x86_64' 'aarch64')
url="https://github.com/BenjaminMIG3/ferredis"
license=('MIT')
depends=('gcc-libs' 'glibc' 'libxkbcommon' 'libxcb')
makedepends=('cargo' 'rust' 'clang' 'pkgconf' 'libxkbcommon' 'libxcb')
backup=('etc/ferredis/ferredis.conf')
install=ferredis.install
options=('!lto')
source=("ferredis-\${pkgver}.tar.gz")
sha256sums=('SKIP')

prepare() {
  cd "\${srcdir}/ferredis-\${pkgver}"
  cargo fetch --target "\$CARCH-unknown-linux-gnu" || true
}

build() {
  cd "\${srcdir}/ferredis-\${pkgver}"
  export CARGO_TARGET_DIR=target
  cargo build --release -p ferredis-server -p ferredis-cli -p ferredis-gui
}

package() {
  cd "\${srcdir}/ferredis-\${pkgver}"
  install -Dm755 target/release/ferredis-server "\${pkgdir}/usr/bin/ferredis-server"
  install -Dm755 target/release/ferredis-cli "\${pkgdir}/usr/bin/ferredis-cli"
  install -Dm755 target/release/ferredis-gui "\${pkgdir}/usr/bin/ferredis-gui"
  install -Dm644 packaging/linux/ferredis.service "\${pkgdir}/usr/lib/systemd/system/ferredis.service"
  install -Dm644 packaging/linux/ferredis.conf "\${pkgdir}/etc/ferredis/ferredis.conf"
  install -Dm644 packaging/linux/ferredis.sysusers "\${pkgdir}/usr/lib/sysusers.d/ferredis.conf"
  install -Dm644 packaging/linux/ferredis.tmpfiles "\${pkgdir}/usr/lib/tmpfiles.d/ferredis.conf"
  [[ -f LICENSE ]] && install -Dm644 LICENSE "\${pkgdir}/usr/share/licenses/\${pkgname}/LICENSE"
}
EOF

cd "$ARCH_DIR"
cp -f PKGBUILD.local PKGBUILD.build
cp -f ferredis.install "${ARCH_DIR}/" 2>/dev/null || true
# makepkg nécessite un environnement Arch ; en CI on utilise le conteneur archlinux
if ! command -v makepkg >/dev/null 2>&1; then
  echo "makepkg introuvable — génération de l'archive source uniquement."
  cp -f "ferredis-${VER}.tar.gz" "$OUT/ferredis-${VER}-arch-src.tar.gz"
  cp -f PKGBUILD "$OUT/PKGBUILD"
  cp -f ferredis.install "$OUT/" 2>/dev/null || true
  echo "Artefacts source: $OUT/ferredis-${VER}-arch-src.tar.gz et $OUT/PKGBUILD"
  exit 0
fi

makepkg -f --noconfirm -p PKGBUILD.build
shopt -s nullglob
for pkg in ferredis-*.pkg.tar.*; do
  cp -f "$pkg" "$OUT/"
  echo "Artefact: $OUT/$pkg"
done
