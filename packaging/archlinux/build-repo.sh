#!/usr/bin/env bash
# Construit un dépôt pacman local à partir des .pkg.tar.* dans dist/
# Usage : ./packaging/archlinux/build-repo.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DIST="${ROOT}/dist"
REPO_OUT="${DIST}/archlinux/x86_64"
mkdir -p "$REPO_OUT"

shopt -s nullglob
pkgs=("$DIST"/ferredis-*.pkg.tar.zst "$DIST"/ferredis-*.pkg.tar.xz)
if [[ ${#pkgs[@]} -eq 0 ]]; then
  echo "Aucun paquet dans dist/ — lancez d'abord package-arch.sh" >&2
  exit 1
fi

cp -f "${pkgs[@]}" "$REPO_OUT/"
cd "$REPO_OUT"
rm -f ferredis.db* ferredis.files*
repo-add ferredis.db.tar.gz ferredis-*.pkg.tar.*
# Symlinks attendus par pacman
ln -sf ferredis.db.tar.gz ferredis.db
ln -sf ferredis.files.tar.gz ferredis.files 2>/dev/null || true
echo "Dépôt prêt : $REPO_OUT"
ls -la
