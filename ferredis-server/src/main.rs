use clap::Parser;
use ferredis_core::conf::load_optional_conf;
use ferredis_core::server::resolve_listen_addrs;
use ferredis_core::{FerredisServer, PersistConfig, ServerConfig};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "ferredis-server",
    about = "Redis-compatible open-source server"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,

    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    #[arg(long)]
    requirepass: Option<String>,

    #[arg(long, default_value = ".")]
    dir: PathBuf,

    #[arg(long, default_value = "dump.rdb")]
    dbfilename: String,

    #[arg(long, default_value_t = false)]
    appendonly: bool,

    #[arg(long, default_value = "appendonly.aof")]
    appendfilename: String,

    /// Fichier de configuration style redis.conf (CLI surcharge après chargement).
    #[arg(long, default_value = "ferredis.conf")]
    config: PathBuf,

    #[arg(long, default_value_t = true)]
    protected_mode: bool,

    /// Certificat TLS PEM (avec --tls-key et --tls-port).
    #[arg(long)]
    tls_cert: Option<PathBuf>,

    #[arg(long)]
    tls_key: Option<PathBuf>,

    /// Port TLS dédié (dual plain+TLS). 0 = pas de listener TLS.
    #[arg(long, default_value_t = 0)]
    tls_port: u16,

    #[arg(long)]
    tls_ca_cert: Option<PathBuf>,

    /// no | optional | yes
    #[arg(long, default_value = "no")]
    tls_auth_clients: String,

    /// Chemin unixsocket (Linux) / named pipe (Windows).
    #[arg(long)]
    unixsocket: Option<String>,

    /// Mode Redis Sentinel (commandes SENTINEL *, pas de data store applicatif).
    #[arg(long, default_value_t = false)]
    sentinel: bool,

    /// Active Redis Cluster (MOVED/ASK + commandes CLUSTER).
    #[arg(long, default_value_t = false)]
    cluster_enabled: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let cli_requirepass = args.requirepass.clone();
    let mut config = ServerConfig {
        requirepass: args.requirepass,
        persist: PersistConfig {
            dir: args.dir.clone(),
            dbfilename: args.dbfilename.clone(),
            appendonly: args.appendonly,
            appendfilename: args.appendfilename.clone(),
            ..PersistConfig::default()
        },
        protected_mode: args.protected_mode,
        port: args.port,
        bind: args.bind.split_whitespace().map(String::from).collect(),
        ..ServerConfig::default()
    };

    let conf_path = if args.config.as_os_str().is_empty() {
        None
    } else {
        Some(args.config.clone())
    };
    load_optional_conf(conf_path, &mut config);

    if cli_requirepass.is_some() {
        config.requirepass = cli_requirepass;
    }
    config.port = args.port;
    config.bind = args.bind.split_whitespace().map(String::from).collect();
    config.protected_mode = args.protected_mode;
    config.persist.dir = args.dir;
    config.persist.dbfilename = args.dbfilename;
    config.persist.appendonly = args.appendonly;
    config.persist.appendfilename = args.appendfilename;

    if let Some(c) = args.tls_cert {
        config.tls_cert_file = Some(c);
    }
    if let Some(k) = args.tls_key {
        config.tls_key_file = Some(k);
    }
    if args.tls_port > 0 {
        config.tls_port = args.tls_port;
    }
    if let Some(ca) = args.tls_ca_cert {
        config.tls_ca_cert_file = Some(ca);
    }
    if args.tls_auth_clients != "no" {
        config.tls_auth_clients = args.tls_auth_clients;
    }
    if let Some(u) = args.unixsocket {
        config.unixsocket = Some(u);
    }
    if args.sentinel {
        config.sentinel_mode = true;
    }
    if args.cluster_enabled {
        config.cluster_enabled = true;
    }

    if config.tls_cert_file.is_some() ^ config.tls_key_file.is_some() {
        anyhow::bail!("tls-cert and tls-key must be provided together");
    }
    if config.tls_cert_file.is_some() && config.tls_port == 0 {
        tracing::warn!("TLS cert set but tls-port=0 — no TLS listener (set --tls-port)");
    }

    let addrs = resolve_listen_addrs(&config).map_err(anyhow::Error::msg)?;
    let server = FerredisServer::new(config);
    let ctrl = server.context().config.read().control.clone();
    tokio::spawn(async move {
        // Ctrl+C / SIGINT → SHUTDOWN avec persistance (parité redis-server).
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("signal: initiating SHUTDOWN SAVE");
            ctrl.request_shutdown(ferredis_core::control::ShutdownRequest {
                save: true,
                ..Default::default()
            });
        }
    });
    server.serve_addrs(&addrs).await?;
    Ok(())
}
