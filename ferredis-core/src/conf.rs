//! Parsing / réécriture fichier de configuration style redis.conf.

use crate::command::ServerConfig;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Clone)]
pub struct ConfFile {
    pub path: Option<PathBuf>,
    pub lines: Vec<String>,
}

impl ConfFile {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = fs::read_to_string(path)?;
        let lines: Vec<String> = text.lines().map(String::from).collect();
        Ok(Self {
            path: Some(path.to_path_buf()),
            lines,
        })
    }

    pub fn apply_to_config(&self, config: &mut ServerConfig) {
        for line in &self.lines {
            let t = line.trim();
            if t.is_empty() || t.starts_with('#') {
                continue;
            }
            let Some((k, v)) = t.split_once(char::is_whitespace) else {
                continue;
            };
            let key = k.trim().to_ascii_lowercase();
            let val = v.trim().trim_matches('"');
            match key.as_str() {
                "port" => {
                    if let Ok(p) = val.parse::<u16>() {
                        config.port = p;
                    }
                }
                "bind" => {
                    config.bind = val.split_whitespace().map(String::from).collect();
                }
                "requirepass" => {
                    config.requirepass = Some(val.to_string());
                }
                "protected-mode" => {
                    config.protected_mode = matches!(val.to_ascii_lowercase().as_str(), "yes" | "1" | "true");
                }
                "dir" => config.persist.dir = PathBuf::from(val),
                "dbfilename" => config.persist.dbfilename = val.to_string(),
                "appendonly" => {
                    config.persist.appendonly =
                        matches!(val.to_ascii_lowercase().as_str(), "yes" | "1" | "true");
                }
                "appendfilename" => config.persist.appendfilename = val.to_string(),
                "appendfsync" => config.persist.appendfsync = val.to_string(),
                "aof-use-rdb-preamble" => {
                    config.persist.aof_use_rdb_preamble =
                        matches!(val.to_ascii_lowercase().as_str(), "yes" | "1" | "true");
                }
                "unixsocket" => config.unixsocket = Some(val.to_string()),
                "unixsocketperm" => config.unixsocketperm = val.to_string(),
                "tls-port" => {
                    if let Ok(p) = val.parse() {
                        config.tls_port = p;
                    }
                }
                "tls-cert-file" => config.tls_cert_file = Some(PathBuf::from(val)),
                "tls-key-file" => config.tls_key_file = Some(PathBuf::from(val)),
                "tls-ca-cert-file" => config.tls_ca_cert_file = Some(PathBuf::from(val)),
                "tls-auth-clients" => config.tls_auth_clients = val.to_string(),
                "io-threads" => {
                    if let Ok(n) = val.parse::<u64>() {
                        config.io_threads = n.max(1);
                    }
                }
                "maxclients" => {
                    if let Ok(n) = val.parse() {
                        config.maxclients = n;
                    }
                }
                "timeout" => {
                    if let Ok(n) = val.parse() {
                        config.timeout = n;
                    }
                }
                "tcp-keepalive" => {
                    if let Ok(n) = val.parse() {
                        config.tcp_keepalive = n;
                    }
                }
                "slowlog-max-len" => {
                    if let Ok(n) = val.parse() {
                        config.slowlog.set_max_len(n);
                    }
                }
                "slowlog-log-slower-than" => {
                    if let Ok(n) = val.parse::<i64>() {
                        let us = if n < 0 { u64::MAX } else { n as u64 };
                        config.slowlog.set_slower_than_us(us);
                    }
                }
                "latency-monitor-threshold" => {
                    if let Ok(n) = val.parse() {
                        config.latency.set_threshold_ms(n);
                    }
                }
                "notify-keyspace-events" => {
                    *config.notify_keyspace_events.lock() = val.to_string();
                }
                "hz" => {
                    if let Ok(n) = val.parse() {
                        config.hz = n;
                    }
                }
                "maxmemory" => {
                    if let Ok(n) = val.parse() {
                        config.maxmemory = n;
                    }
                }
                "maxmemory-policy" => config.maxmemory_policy = val.to_string(),
                "save" => config.save_lines.push(val.to_string()),
                "masterauth" => {
                    // Appliqué après création du CommandContext (voir serve).
                    let _ = val;
                }
                "replica-read-only" | "slave-read-only" => {
                    let _ = val;
                }
                "replicaof" | "slaveof" => {
                    let _ = val;
                }
                _ => {}
            }
        }
    }

    pub fn rewrite(&self, config: &ServerConfig) -> anyhow::Result<()> {
        let path = self
            .path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no config file path"))?;
        let mut out = String::new();
        out.push_str("# Generated by CONFIG REWRITE (Ferredis)\n");
        writeln!(out, "port {}", config.port)?;
        if !config.bind.is_empty() {
            writeln!(out, "bind {}", config.bind.join(" "))?;
        }
        writeln!(
            out,
            "protected-mode {}",
            if config.protected_mode { "yes" } else { "no" }
        )?;
        if let Some(p) = &config.requirepass {
            writeln!(out, "requirepass \"{}\"", p.replace('"', "\\\""))?;
        }
        writeln!(out, "dir {}", config.persist.dir.display())?;
        writeln!(out, "dbfilename {}", config.persist.dbfilename)?;
        writeln!(
            out,
            "appendonly {}",
            if config.persist.appendonly { "yes" } else { "no" }
        )?;
        writeln!(out, "appendfilename {}", config.persist.appendfilename)?;
        writeln!(out, "appendfsync {}", config.persist.appendfsync)?;
        writeln!(
            out,
            "aof-use-rdb-preamble {}",
            if config.persist.aof_use_rdb_preamble {
                "yes"
            } else {
                "no"
            }
        )?;
        writeln!(out, "maxclients {}", config.maxclients)?;
        writeln!(out, "timeout {}", config.timeout)?;
        writeln!(out, "tcp-keepalive {}", config.tcp_keepalive)?;
        writeln!(out, "slowlog-max-len {}", config.slowlog.max_len())?;
        writeln!(
            out,
            "slowlog-log-slower-than {}",
            config.slowlog.slower_than_us() as i64
        )?;
        writeln!(
            out,
            "latency-monitor-threshold {}",
            config.latency.threshold_ms()
        )?;
        writeln!(
            out,
            "notify-keyspace-events {}",
            config.notify_keyspace_events.lock()
        )?;
        writeln!(out, "hz {}", config.hz)?;
        writeln!(out, "maxmemory {}", config.maxmemory)?;
        writeln!(out, "maxmemory-policy {}", config.maxmemory_policy)?;
        if let Some(u) = &config.unixsocket {
            writeln!(out, "unixsocket {}", u)?;
            writeln!(out, "unixsocketperm {}", config.unixsocketperm)?;
        }
        writeln!(out, "io-threads {}", config.io_threads)?;
        writeln!(out, "tls-port {}", config.tls_port)?;
        if let Some(p) = &config.tls_cert_file {
            writeln!(out, "tls-cert-file {}", p.display())?;
        }
        if let Some(p) = &config.tls_key_file {
            writeln!(out, "tls-key-file {}", p.display())?;
        }
        if let Some(p) = &config.tls_ca_cert_file {
            writeln!(out, "tls-ca-cert-file {}", p.display())?;
        }
        writeln!(out, "tls-auth-clients {}", config.tls_auth_clients)?;
        fs::write(path, out)?;
        Ok(())
    }
}

/// Applique un fichier conf optionnel au démarrage.
pub fn load_optional_conf(path: Option<PathBuf>, config: &mut ServerConfig) -> ConfFile {
    let mut cf = ConfFile::default();
    if let Some(p) = path {
        if p.exists() {
            if let Ok(loaded) = ConfFile::load(&p) {
                loaded.apply_to_config(config);
                config.conf_path = Some(p);
                cf = loaded;
            }
        } else {
            config.conf_path = Some(p);
        }
    }
    cf
}
