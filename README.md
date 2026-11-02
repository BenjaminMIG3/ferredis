# Ferredis

Serveur compatible Redis Open Source **8.0**, écrit en Rust. Parité protocole RESP2/RESP3, persistance RDB/AOF, réplication, Sentinel, Cluster, TLS, Lua, et extensions Stack (JSON, TimeSeries, Search, structures probabilistes, Vector set).

Compatible Linux et Windows.

## Téléchargement

| Plateforme | Lien |
|---|---|
| **Releases GitHub** | [Releases](https://github.com/BenjaminMIG3/ferredis/releases) (Linux `.tar.gz`, Windows `.zip`, Arch `.pkg.tar.zst`) |
| **Site artefacts / dépôt pacman** | [GitHub Pages](https://benjaminmig3.github.io/ferredis/) |
| **Windows (Scoop)** | `scoop bucket add ferredis https://github.com/BenjaminMIG3/ferredis` puis `scoop install ferredis` |

### Arch Linux — `pacman -S ferredis`

```bash
# 1) Activer le dépôt binaire (une fois)
curl -fsSL https://raw.githubusercontent.com/BenjaminMIG3/ferredis/main/packaging/archlinux/enable-repo.sh | sudo bash
# 2) Installer / mettre à jour
sudo pacman -Syu ferredis
sudo systemctl enable --now ferredis
```

Ou manuellement : copier [`packaging/archlinux/pacman-ferredis.conf`](packaging/archlinux/pacman-ferredis.conf) vers `/etc/pacman.d/ferredis.conf` et `Include` dans `/etc/pacman.conf`.

**AUR** (après publication) : `yay -S ferredis` / `paru -S ferredis` — sources dans `packaging/aur/`.

### Linux (toutes distros systemd)

```bash
# Depuis une release : ferredis-linux-x86_64.tar.gz
tar xzf ferredis-linux-x86_64.tar.gz
sudo ./install.sh --from-dir . --enable --start
sudo systemctl status ferredis
```

### Windows — Scoop (recommandé, sans admin)

```powershell
# Prérequis : https://scoop.sh
scoop bucket add ferredis https://github.com/BenjaminMIG3/ferredis
scoop install ferredis
ferredis-server
ferredis-cli PING
# Mise à jour
scoop update ferredis
```

### Windows — zip / service

```powershell
# Depuis ferredis-windows-x86_64.zip (PowerShell Admin pour le service)
.\install-service.ps1 -Start
Get-Service Ferredis
```

Publier une release : `git tag v0.1.0 && git push origin v0.1.0` (workflow `release` + GitHub Pages).

## Crates

| Crate | Rôle |
|---|---|
| `ferredis-core` | Moteur in-memory, commandes, persistance, réseau |
| `ferredis-server` | Binaire serveur |
| `ferredis-cli` | CLI interactive (style `redis-cli`) |
| `ferredis-gui` | Interface desktop (egui) |
| `ferredis-compat` | Helpers / tests de compatibilité |

## Prérequis

- [Rust](https://rustup.rs/) stable (edition 2021)

## Build

```bash
cargo build --release -p ferredis-server -p ferredis-cli
cargo build --release -p ferredis-gui   # optionnel
```

## Démarrage rapide

```bash
cargo run -p ferredis-server -- --bind 127.0.0.1 --port 6379
cargo run -p ferredis-cli -- -p 6379 PING
cargo run -p ferredis-gui
```

Clients Redis standards : `REDIS_URL=redis://127.0.0.1:6379`.

### Options serveur utiles

| Option | Description |
|---|---|
| `--port` / `--bind` | Écoute TCP |
| `--requirepass` | Mot de passe AUTH |
| `--config` | Fichier type `redis.conf` (défaut : `ferredis.conf`) |
| `--dir` / `--dbfilename` | Persistance RDB |
| `--appendonly` | Active AOF |
| `--tls-cert` / `--tls-key` / `--tls-port` | TLS |
| `--unixsocket` | Socket Unix (Linux) / named pipe (Windows) |
| `--sentinel` | Mode Sentinel |
| `--cluster-enabled` | Mode Cluster |

## Tests

```bash
cargo test --workspace --exclude ferredis-gui
```

## Packaging local

```bash
./packaging/package.sh              # Linux tarball + systemd
./packaging/archlinux/package-arch.sh
.\packaging\package.ps1             # Windows zip + service
```

Config : `/etc/ferredis/ferredis.conf` (Linux) · `%ProgramFiles%\Ferredis\ferredis.conf` (Windows).

## Licence

MIT — voir [LICENSE](LICENSE).
