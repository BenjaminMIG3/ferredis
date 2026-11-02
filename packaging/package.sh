#!/usr/bin/env bash
# Packaging Linux natif — même séquence que package.ps1 : server + cli + gui, copie, archive.
# Deps GUI (Ubuntu/Debian) : libxkbcommon-dev libxcb-render0-dev libxcb-shape0-dev libxcb-xfixes0-dev
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${ROOT}/dist/linux"
mkdir -p "$OUT"
cd "$ROOT"

if command -v apt-get >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    libxkbcommon-dev \
    libxcb-render0-dev \
    libxcb-shape0-dev \
    libxcb-xfixes0-dev
fi

cargo build --release -p ferredis-server -p ferredis-cli -p ferredis-gui
cp -f target/release/ferredis-server "$OUT/"
cp -f target/release/ferredis-cli "$OUT/"
cp -f target/release/ferredis-gui "$OUT/"
cp -f LICENSE "$OUT/" 2>/dev/null || true
(
  cd "$OUT"
  tar -czf ../ferredis-linux-x86_64.tar.gz .
)
echo "Artefact: ${ROOT}/dist/ferredis-linux-x86_64.tar.gz"
