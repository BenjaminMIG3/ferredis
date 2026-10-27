#!/usr/bin/env bash
# Packaging Linux natif (même features Win) — server / cli / gui.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${ROOT}/dist/linux"
mkdir -p "$OUT"
cd "$ROOT"
cargo build --release -p ferredis-server -p ferredis-cli
cargo build --release -p ferredis-gui || true
cp -f target/release/ferredis-server "$OUT/"
cp -f target/release/ferredis-cli "$OUT/"
if [[ -f target/release/ferredis-gui ]]; then
  cp -f target/release/ferredis-gui "$OUT/"
fi
cp -f LICENSE "$OUT/" 2>/dev/null || true
(
  cd "$OUT"
  tar -czf ../ferredis-linux-x86_64.tar.gz .
)
echo "Artefact: ${ROOT}/dist/ferredis-linux-x86_64.tar.gz"
