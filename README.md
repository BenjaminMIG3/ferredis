# Ferredis

Serveur compatible Redis Open Source **8.0**, écrit en Rust. Parité protocole RESP2/RESP3, persistance RDB/AOF, réplication, Sentinel, Cluster, TLS, Lua, et extensions Stack (JSON, TimeSeries, Search, structures probabilistes, Vector set).

Compatible Linux et Windows.

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
# Serveur (port 6379 par défaut)
cargo run -p ferredis-server -- --bind 127.0.0.1 --port 6379

# CLI
cargo run -p ferredis-cli -- -p 6379 PING
cargo run -p ferredis-cli -- -p 6379   # mode interactif

# GUI
cargo run -p ferredis-gui
```

Clients Redis standards (`redis-cli`, `redis-py`, etc.) fonctionnent via `REDIS_URL=redis://127.0.0.1:6379`.

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

## Packaging

```bash
# Linux
./packaging/package.sh

# Windows (PowerShell)
.\packaging\package.ps1
```

Artefacts dans `dist/`.

## Licence

MIT — voir [LICENSE](LICENSE).
