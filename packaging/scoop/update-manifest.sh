#!/usr/bin/env bash
# Met à jour bucket/ferredis.json (version + hash SHA256 du zip Windows).
# Usage: bash packaging/scoop/update-manifest.sh 0.1.0 dist/ferredis-windows-x86_64.zip
set -euo pipefail

VERSION="${1:?version requise (ex: 0.1.0)}"
ZIP="${2:?chemin zip requis}"
REPO="${3:-BenjaminMIG3/ferredis}"

VERSION="${VERSION#v}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
MANIFEST="$ROOT/bucket/ferredis.json"

if [[ ! -f "$ZIP" ]]; then
  echo "Zip introuvable: $ZIP" >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  HASH="$(sha256sum "$ZIP" | awk '{print $1}')"
else
  HASH="$(shasum -a 256 "$ZIP" | awk '{print $1}')"
fi

URL="https://github.com/${REPO}/releases/download/v${VERSION}/ferredis-windows-x86_64.zip"

python3 - "$MANIFEST" "$VERSION" "$HASH" "$URL" <<'PY'
import json, sys
path, version, hash_, url = sys.argv[1:5]
with open(path, encoding="utf-8") as f:
    data = json.load(f)
data["version"] = version
data.setdefault("architecture", {}).setdefault("64bit", {})
data["architecture"]["64bit"]["url"] = url
data["architecture"]["64bit"]["hash"] = hash_
with open(path, "w", encoding="utf-8", newline="\n") as f:
    json.dump(data, f, indent=4, ensure_ascii=False)
    f.write("\n")
print(f"Scoop manifest mis à jour: {path}")
print(f"  version={version}")
print(f"  hash={hash_}")
print(f"  url={url}")
PY
