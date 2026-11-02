#!/usr/bin/env bash
# Construit un dépôt apt (unsigned, trusted=yes côté client) sous dist/apt/.
# Usage : bash packaging/debian/build-repo.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DIST="${ROOT}/dist"
APT_ROOT="${DIST}/apt"
POOL="${APT_ROOT}/pool/main/f/ferredis"
BIN="${APT_ROOT}/dists/stable/main/binary-amd64"

shopt -s nullglob
debs=("$DIST"/ferredis_*.deb)
# Ignore l'alias ferredis_amd64.deb pour éviter le doublon dans le pool
filtered=()
for d in "${debs[@]}"; do
  base="$(basename "$d")"
  if [[ "$base" == "ferredis_amd64.deb" ]]; then
    continue
  fi
  filtered+=("$d")
done
if [[ ${#filtered[@]} -eq 0 ]]; then
  echo "Aucun .deb versionné dans dist/ — lancez package-deb.sh" >&2
  exit 1
fi

rm -rf "$APT_ROOT"
mkdir -p "$POOL" "$BIN"
cp -f "${filtered[@]}" "$POOL/"

# Packages index (chemins relatifs à APT_ROOT)
(
  cd "$APT_ROOT"
  dpkg-scanpackages --arch amd64 pool /dev/null > dists/stable/main/binary-amd64/Packages
  gzip -9c dists/stable/main/binary-amd64/Packages > dists/stable/main/binary-amd64/Packages.gz
)

# Release (non signé — clients : [trusted=yes])
apt-ftparchive -o APT::FTPArchive::Release::Origin="Ferredis" \
  -o APT::FTPArchive::Release::Label="Ferredis" \
  -o APT::FTPArchive::Release::Suite="stable" \
  -o APT::FTPArchive::Release::Codename="stable" \
  -o APT::FTPArchive::Release::Architectures="amd64" \
  -o APT::FTPArchive::Release::Components="main" \
  release "${APT_ROOT}/dists/stable" > "${APT_ROOT}/dists/stable/Release"

# Index HTML minimal pour navigation Pages
cat > "${APT_ROOT}/index.html" <<'HTML'
<!DOCTYPE html>
<html lang="fr"><head><meta charset="utf-8"><title>Ferredis apt</title></head>
<body>
<h1>Dépôt apt Ferredis</h1>
<pre>deb [arch=amd64 trusted=yes] https://benjaminmig3.github.io/ferredis/apt stable main</pre>
<p>Puis : <code>sudo apt-get update && sudo apt-get install ferredis</code></p>
</body></html>
HTML

echo "Dépôt apt prêt : $APT_ROOT"
find "$APT_ROOT" -type f | sort
