//! Redis command dispatcher.
//!
//! Architecture :
//! * [`dispatch`] — point d'entrée asynchrone d'une connexion : authentification, mode
//!   abonné, file MULTI, commandes bloquantes ;
//! * [`execute`] — exécution synchrone d'une commande (aucun verrou global pris) ; réutilisée
//!   par EXEC et par les scripts Lua ;
//! * `exec_lock` — verrou lecteurs/écrivain : les commandes simples le prennent en lecture,
//!   EXEC et EVAL en écriture, ce qui rend transactions et scripts atomiques.

use crate::acl::{self, shared_acl, SharedAcl};
use crate::async_drop::SharedAsyncDrop;
use crate::blocking;
use crate::clients::{ClientInfo, ClientRegistry, SharedClients};
use crate::cmd_hashset as hashset;
use crate::cmd_key as key;
use crate::cmd_list as list;
pub use crate::cmd_pubsub::format_pubsub_event;
use crate::cmd_server as server;
use crate::cmd_string as string;
use crate::command_matrix::deferred_error;
use crate::command_registry;
use crate::commands_extra as extra;
use crate::control::SharedControl;
use crate::functions::{shared_functions, SharedFunctions};
use crate::latency::SharedLatency;
use crate::monitor::{format_monitor_line, SharedMonitor};
use crate::opts::{trailing_sync_mode, SyncMode};
use crate::persist::{AofLogger, PersistConfig, SharedAof};
use crate::pubsub::{shared_pubsub, SharedPubSub};
use crate::resp::RespValue;
use crate::script::{shared_scripts, SharedScripts};
use crate::search::{shared_search, SharedSearch};
use crate::slowlog::SharedSlowLog;
use crate::stats::SharedStats;
use crate::store::{shared_db, ListWaiters, SharedDb, SharedWaiters};
use crate::tracking::{shared_tracking, SharedTracking};
use crate::util::ERR_INT;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

/// Noms de commandes (source unique `command_registry`).
pub fn supported_commands() -> &'static [&'static str] {
    command_registry::supported_command_names()
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub requirepass: Option<String>,
    pub started_at: Instant,
    pub persist: PersistConfig,
    /// Redis-compatible notify-keyspace-events flags (default "AKE").
    pub notify_keyspace_events: Arc<Mutex<String>>,
    /// Identifiant d'instance (INFO `run_id`).
    pub run_id: String,
    /// Adresse d'écoute effective (renseignée par `serve`).
    pub listen_addr: Arc<Mutex<String>>,
    pub port: u16,
    pub bind: Vec<String>,
    pub protected_mode: bool,
    pub unixsocket: Option<String>,
    pub unixsocketperm: String,
    pub maxclients: u64,
    pub timeout: u64,
    pub tcp_keepalive: u64,
    pub hz: u64,
    pub maxmemory: u64,
    /// Soft limit : déclenche compression / demote avant OOM (0 = désactivé).
    pub maxmemory_soft: u64,
    pub maxmemory_policy: String,
    /// Seuil octets pour compression LZ4 transparente (0 = off).
    pub compress_threshold: u64,
    /// Active defrag périodique.
    pub activedefrag: bool,
    /// `memory` | `hybrid`.
    pub storage_mode: String,
    /// Durée group-commit AOF (ms, 0 = immédiat selon appendfsync).
    pub aof_group_commit_ms: u64,
    /// `ap` | `cp` (Raft quorum writes).
    pub cluster_consistency: String,
    pub save_lines: Vec<String>,
    pub conf_path: Option<PathBuf>,
    pub stats: SharedStats,
    pub slowlog: SharedSlowLog,
    pub latency: SharedLatency,
    pub monitor: SharedMonitor,
    pub control: SharedControl,
    pub last_save: Arc<AtomicI64>,
    /// Port TLS (0 = désactivé). Dual listener si `port` et `tls_port` > 0.
    pub tls_port: u16,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
    pub tls_ca_cert_file: Option<PathBuf>,
    /// `no` | `optional` | `yes` (mTLS).
    pub tls_auth_clients: String,
    /// Préparation P12 : pool parse/écriture (1 = chemin actuel).
    pub io_threads: u64,
    /// Mode Redis Sentinel (`--sentinel`).
    pub sentinel_mode: bool,
    /// Active le support Cluster (redirections MOVED/ASK).
    pub cluster_enabled: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let run_id: Vec<u8> = (0..20).map(|_| rng.gen()).collect();
        Self {
            requirepass: None,
            started_at: Instant::now(),
            persist: PersistConfig::default(),
            notify_keyspace_events: Arc::new(Mutex::new("AKE".into())),
            run_id: hex::encode(run_id),
            listen_addr: Arc::new(Mutex::new(String::new())),
            port: 6379,
            bind: vec!["127.0.0.1".into()],
            protected_mode: true,
            unixsocket: None,
            unixsocketperm: "770".into(),
            maxclients: 10_000,
            timeout: 0,
            tcp_keepalive: 300,
            hz: 10,
            maxmemory: 0,
            maxmemory_soft: 0,
            maxmemory_policy: "noeviction".into(),
            compress_threshold: 128,
            activedefrag: false,
            storage_mode: "memory".into(),
            aof_group_commit_ms: 0,
            cluster_consistency: "ap".into(),
            save_lines: Vec::new(),
            conf_path: None,
            stats: crate::stats::shared_stats(),
            slowlog: crate::slowlog::shared_slowlog(),
            latency: crate::latency::shared_latency(),
            monitor: crate::monitor::shared_monitor(),
            control: crate::control::shared_control(),
            last_save: Arc::new(AtomicI64::new(-1)),
            tls_port: 0,
            tls_cert_file: None,
            tls_key_file: None,
            tls_ca_cert_file: None,
            tls_auth_clients: "no".into(),
            io_threads: 1,
            sentinel_mode: false,
            cluster_enabled: false,
        }
    }
}

/// Clé surveillée par WATCH : identité d'écriture au moment du WATCH.
///
/// On stocke uniquement `version` (pas l'`Entry` complète) : Redis invalide WATCH
/// sur **écriture**, pas sur lecture LRU/LFU. Comparer l'`Entry` entière faisait
/// échouer EXEC après un simple `get_fresh` (mutation de `last_access`).
#[derive(Debug, Clone)]
pub struct WatchedKey {
    pub db: usize,
    pub key: Bytes,
    /// `None` si la clé n'existait pas ; sinon [`Entry::version`] au WATCH.
    pub version: Option<u64>,
}

/// Mode CLIENT REPLY.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplyMode {
    #[default]
    On,
    Off,
    Skip,
}

pub struct ClientState {
    pub db_index: usize,
    pub authenticated: bool,
    /// Utilisateur ACL courant (`default` par défaut).
    pub acl_user: String,
    pub pubsub_id: Option<u64>,
    pub name: Option<String>,
    pub in_multi: bool,
    /// Erreur à la mise en file (commande inconnue, arité) : EXEC renverra EXECABORT.
    pub multi_error: bool,
    /// Exécution dans EXEC : les commandes bloquantes n'attendent pas, SSUBSCRIBE est refusé.
    pub deny_blocking: bool,
    pub multi_queue: Vec<Vec<Bytes>>,
    pub watched: Vec<WatchedKey>,
    /// Fiche partagée avec le registre de clients (None hors connexion TCP).
    pub info: Option<Arc<ClientInfo>>,
    pub monitor_rx: Option<mpsc::UnboundedReceiver<Bytes>>,
    pub reply_mode: ReplyMode,
    pub no_evict: bool,
    pub no_touch: bool,
    /// RESP 2 (défaut) ou 3 après `HELLO`.
    pub resp_version: u8,
    /// Flux à écrire vers un réplica (après PSYNC/SYNC).
    pub replica_feed: Option<mpsc::UnboundedReceiver<Bytes>>,
    /// Connexion côté master servant un réplica.
    pub is_replica_conn: bool,
    /// Commandes appliquées depuis le flux master (bypass read-only).
    pub from_master: bool,
    /// Flag CLIENT READONLY (cluster / compat).
    pub readonly: bool,
    /// Port annoncé via REPLCONF listening-port.
    pub repl_listening_port: u16,
    /// Capacités annoncées (capa).
    pub repl_capa: Vec<String>,
    /// Flag ASKING (une commande, cluster migration).
    pub asking: bool,
    /// CLIENT DURABILITY : none | everysec | always | quorum.
    pub durability: DurabilityMode,
}

/// Niveau de durabilité demandé par le client (phase F3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DurabilityMode {
    #[default]
    None,
    Everysec,
    Always,
    Quorum,
}

impl DurabilityMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "none" => Some(Self::None),
            "everysec" => Some(Self::Everysec),
            "always" => Some(Self::Always),
            "quorum" => Some(Self::Quorum),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Everysec => "everysec",
            Self::Always => "always",
            Self::Quorum => "quorum",
        }
    }
}

impl Default for ClientState {
    fn default() -> Self {
        Self {
            db_index: 0,
            authenticated: false,
            acl_user: "default".into(),
            pubsub_id: None,
            name: None,
            in_multi: false,
            multi_error: false,
            deny_blocking: false,
            multi_queue: Vec::new(),
            watched: Vec::new(),
            info: None,
            monitor_rx: None,
            reply_mode: ReplyMode::On,
            no_evict: false,
            no_touch: false,
            resp_version: 2,
            replica_feed: None,
            is_replica_conn: false,
            from_master: false,
            readonly: false,
            repl_listening_port: 0,
            repl_capa: Vec::new(),
            asking: false,
            durability: DurabilityMode::None,
        }
    }
}

/// Pause globale CLIENT PAUSE (deadline unix-ms, write_only).
#[derive(Default)]
pub struct ClientPause {
    pub until_ms: AtomicU64,
    pub write_only: AtomicBool,
}

pub type SharedPause = Arc<ClientPause>;

#[derive(Clone)]
pub struct CommandContext {
    pub db: SharedDb,
    pub pubsub: SharedPubSub,
    pub waiters: SharedWaiters,
    pub config: Arc<RwLock<ServerConfig>>,
    pub scripts: SharedScripts,
    pub functions: SharedFunctions,
    pub search: SharedSearch,
    pub acl: SharedAcl,
    pub aof: SharedAof,
    pub clients: SharedClients,
    pub pause: SharedPause,
    /// Atomicité : lecture pour les commandes simples, écriture pour EXEC / EVAL.
    pub exec_lock: Arc<RwLock<()>>,
    pub async_drop: SharedAsyncDrop,
    pub tracking: SharedTracking,
    pub repl: crate::repl::SharedRepl,
    pub sentinel: crate::sentinel::SharedSentinel,
    pub cluster: crate::cluster::SharedCluster,
    pub query: crate::query::SharedQuery,
    pub storage: crate::storage::SharedStorage,
    pub raft: crate::raft::SharedRaft,
    pub defrag: std::sync::Arc<crate::mem::DefragStats>,
}

impl CommandContext {
    /// Contexte neuf (base vide, aucun client) pour la configuration donnée.
    pub fn new(config: ServerConfig) -> Self {
        let db = shared_db();
        let async_drop = crate::async_drop::shared_async_drop(Arc::clone(&db));
        let acl = shared_acl();
        acl.sync_requirepass(config.requirepass.as_deref());
        let io_threads = config.io_threads.clamp(1, 255) as u8;
        let tracking = shared_tracking();
        tracking
            .io_threads_hint
            .store(io_threads, std::sync::atomic::Ordering::Relaxed);
        Self {
            db,
            pubsub: shared_pubsub(),
            waiters: Arc::new(ListWaiters::new()),
            aof: Arc::new(AofLogger::new(&config.persist)),
            scripts: shared_scripts(),
            functions: shared_functions(),
            search: shared_search(),
            acl,
            clients: Arc::new(ClientRegistry::new()),
            pause: Arc::new(ClientPause::default()),
            exec_lock: Arc::new(RwLock::new(())),
            async_drop,
            tracking,
            repl: crate::repl::shared_repl(),
            sentinel: crate::sentinel::shared_sentinel(),
            cluster: crate::cluster::shared_cluster(),
            query: crate::query::shared_query(),
            storage: {
                let s = crate::storage::StorageEngine::new();
                s.set_mode(crate::storage::StorageMode::parse(&config.storage_mode));
                s.compress_threshold.store(
                    config.compress_threshold,
                    std::sync::atomic::Ordering::Relaxed,
                );
                s
            },
            raft: {
                let r = crate::raft::shared_raft(config.run_id.clone());
                r.set_consistency(crate::raft::ClusterConsistency::parse(
                    &config.cluster_consistency,
                ));
                r
            },
            defrag: std::sync::Arc::new(crate::mem::DefragStats::default()),
            config: Arc::new(RwLock::new(config)),
        }
    }

    /// Propage une écriture vers AOF + réplicas (DRY avec les sites d'append).
    pub fn propagate(&self, args: &[Bytes]) {
        if self.raft.is_cp() {
            let _ = self.raft.commit_write(std::time::Duration::from_millis(50));
        }
        self.aof.append_command(args);
        if !self.repl.is_replica() {
            self.repl.feed_command(args);
        }
    }

    /// Applique la durabilité client après une écriture (CLIENT DURABILITY).
    pub fn apply_durability(&self, state: &ClientState) {
        match state.durability {
            DurabilityMode::None => {}
            DurabilityMode::Everysec => {
                // everysec : déjà couvert par la politique AOF ; rien de synchrone.
            }
            DurabilityMode::Always => {
                let _ = self.aof.fsync_now();
            }
            DurabilityMode::Quorum => {
                let _ = self.raft.commit_write(std::time::Duration::from_millis(200));
                let _ = self.aof.fsync_now();
            }
        }
    }

    /// Publish Redis keyspace / keyevent notifications for live clients.
    pub fn notify_key(&self, db: usize, key: &Bytes, event: &str) {
        let flags = self.config.read().notify_keyspace_events.lock().clone();
        if flags.is_empty() {
            return;
        }
        let keyspace = flags.contains('K') || flags.contains('A');
        let keyevent = flags.contains('E') || flags.contains('A');
        if keyspace {
            let ch = Bytes::from(format!(
                "__keyspace@{db}__:{}",
                String::from_utf8_lossy(key)
            ));
            self.pubsub.publish(&ch, Bytes::from(event.to_string()));
        }
        if keyevent {
            let ch = Bytes::from(format!("__keyevent@{db}__:{event}"));
            self.pubsub.publish(&ch, key.clone());
        }
    }
}

pub(crate) fn arg_str(args: &[Bytes], i: usize) -> Option<&str> {
    args.get(i).and_then(|b| std::str::from_utf8(b).ok())
}

pub(crate) fn wrong_arity(cmd: &str) -> RespValue {
    RespValue::err(format!("ERR wrong number of arguments for '{cmd}' command"))
}

fn need_auth() -> RespValue {
    RespValue::err("NOAUTH Authentication required.")
}

fn in_subscribe_mode(ctx: &CommandContext, state: &ClientState) -> bool {
    state
        .pubsub_id
        .is_some_and(|id| ctx.pubsub.is_subscribed(id))
}

/// Contrôles d'admission : authentification, ACL, mode abonné.
fn admission(ctx: &CommandContext, state: &ClientState, cmd: &str) -> Option<RespValue> {
    let needs_auth = !state.authenticated
        && !matches!(cmd, "AUTH" | "HELLO" | "QUIT")
        && (ctx.config.read().requirepass.is_some() || acl_requires_auth(ctx));
    if needs_auth {
        return Some(need_auth());
    }
    if in_subscribe_mode(ctx, state)
        && !matches!(
            cmd,
            "SUBSCRIBE"
                | "PSUBSCRIBE"
                | "SSUBSCRIBE"
                | "UNSUBSCRIBE"
                | "PUNSUBSCRIBE"
                | "SUNSUBSCRIBE"
                | "PING"
                | "QUIT"
                | "RESET"
        )
    {
        return Some(RespValue::err(format!(
            "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
            cmd.to_ascii_lowercase()
        )));
    }
    if state.monitor_rx.is_some() && !matches!(cmd, "PING" | "QUIT") {
        return Some(RespValue::err(
            "ERR MONITOR mode: only PING and QUIT are allowed",
        ));
    }
    None
}

fn acl_requires_auth(ctx: &CommandContext) -> bool {
    // Si default n'a pas nopass, AUTH est requis.
    ctx.acl.authenticate("default", "").is_err()
}

/// Dispatch a command. Returns response(s). For subscribe commands may return multiple.
pub async fn dispatch(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
) -> Vec<RespValue> {
    let Some(first) = args.first() else {
        return vec![RespValue::err("ERR empty command")];
    };
    let Ok(name) = std::str::from_utf8(first) else {
        return vec![RespValue::err("ERR invalid command name")];
    };
    let cmd = name.to_ascii_uppercase();

    if let Some(reject) = admission(ctx, state, &cmd) {
        return vec![reject];
    }
    // ACL permissions (après AUTH).
    if !matches!(cmd.as_str(), "AUTH" | "HELLO" | "QUIT" | "ACL") {
        let keys = acl::keys_for_command(&cmd, args);
        let chans = acl::channels_for_command(&cmd, args);
        let addr = state.info.as_ref().map(|i| i.addr.as_str()).unwrap_or("?");
        if let Err(e) = ctx
            .acl
            .check_command(&state.acl_user, &cmd, &keys, &chans, addr)
        {
            return vec![RespValue::err(e)];
        }
    }
    // CLIENT PAUSE
    maybe_wait_pause(ctx, &cmd);

    if let Some(reject) = crate::repl::reject_replica_write(ctx, state, &cmd) {
        return vec![reject];
    }
    if (ctx.config.read().sentinel_mode || ctx.sentinel.is_enabled())
        && !crate::sentinel::sentinel_command_allowed(&cmd)
    {
        return vec![RespValue::err(
            "ERR unknown or unsupported command in sentinel mode",
        )];
    }
    if let Some(redir) = crate::cluster::redirect_if_needed(ctx, state, args) {
        return vec![redir];
    }

    ctx.clients.count_command();
    ctx.config.read().stats.record_command(&cmd);
    let started = Instant::now();
    if let Some(info) = &state.info {
        info.touch(&cmd, state.db_index);
        info.set_user(&state.acl_user);
    }

    // En mode abonné RESP2, PING répond par un tableau `["pong", message]`.
    if cmd == "PING" && in_subscribe_mode(ctx, state) {
        let payload = args.get(1).cloned().unwrap_or_default();
        return vec![RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(b"pong")),
            RespValue::bulk(payload),
        ])];
    }

    // Dans MULTI, tout sauf les commandes de contrôle est mis en file.
    if state.in_multi
        && !matches!(
            cmd.as_str(),
            "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH" | "AUTH" | "QUIT" | "RESET"
        )
    {
        if let Some(reject) = queue_check(&cmd, args) {
            state.multi_error = true;
            return vec![reject];
        }
        state.multi_queue.push(args.to_vec());
        return vec![RespValue::simple(Bytes::from_static(b"QUEUED"))];
    }

    if blocking::is_blocking(&cmd) {
        let r = blocking::run(ctx, state, &cmd, args).await;
        record_post_exec(ctx, state, args, started);
        return vec![r];
    }
    let out = run_locked(ctx, state, &cmd, args);
    // ASKING ne dure qu'une commande (Redis).
    if cmd != "ASKING" {
        state.asking = false;
    }
    record_post_exec(ctx, state, args, started);
    apply_reply_mode(state, out)
}

fn maybe_wait_pause(ctx: &CommandContext, cmd: &str) {
    let until = ctx.pause.until_ms.load(Ordering::SeqCst);
    if until == 0 {
        return;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    if now >= until {
        ctx.pause.until_ms.store(0, Ordering::SeqCst);
        return;
    }
    let write_only = ctx.pause.write_only.load(Ordering::SeqCst);
    let is_write = command_registry::lookup(cmd).is_some_and(|s| s.has_flag("write"));
    if write_only && !is_write {
        return;
    }
    let wait = Duration::from_millis(until.saturating_sub(now));
    std::thread::sleep(wait.min(Duration::from_secs(1)));
    // Boucle courte si pause longue (évite de bloquer indéfiniment d'un coup).
    let deadline = Instant::now() + Duration::from_millis(until.saturating_sub(now));
    while Instant::now() < deadline {
        let u = ctx.pause.until_ms.load(Ordering::SeqCst);
        if u == 0 {
            break;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if now >= u {
            ctx.pause.until_ms.store(0, Ordering::SeqCst);
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn apply_reply_mode(state: &mut ClientState, out: Vec<RespValue>) -> Vec<RespValue> {
    match state.reply_mode {
        ReplyMode::On => out,
        ReplyMode::Off => Vec::new(),
        ReplyMode::Skip => {
            state.reply_mode = ReplyMode::On;
            Vec::new()
        }
    }
}

fn record_post_exec(ctx: &CommandContext, state: &ClientState, args: &[Bytes], started: Instant) {
    let dur = started.elapsed();
    let us = dur.as_micros() as u64;
    let addr = state.info.as_ref().map(|i| i.addr.as_str()).unwrap_or("?");
    let name = state.info.as_ref().map(|i| i.name()).unwrap_or_default();
    let cmd_name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    let cfg = ctx.config.read();
    cfg.slowlog.maybe_record(us, args, addr, &name);
    if let Some(spec) = command_registry::spec_by_name(&cmd_name) {
        cfg.latency
            .record_command(&spec.name.to_ascii_lowercase(), dur, spec.has_flag("fast"));
        // Client tracking : enregistre les clés lues.
        if spec.has_flag("readonly") {
            if let Some(info) = &state.info {
                let keys = acl::keys_for_command(&cmd_name, args);
                ctx.tracking.track_reads(info.id, &keys);
            }
        }
        // Mutations : invalidation (complète notify_key déjà appelé par les handlers).
        if spec.has_flag("write") {
            if let Some(info) = &state.info {
                let keys = acl::keys_for_command(&cmd_name, args);
                if !keys.is_empty() {
                    ctx.tracking.invalidate_keys(Some(info.id), &keys);
                }
            }
        }
    }
    if !matches!(cmd_name.as_str(), "MONITOR" | "AUTH" | "HELLO") {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let line = format_monitor_line(ts, state.db_index, addr, args);
        cfg.monitor.fanout(line);
    }
}

/// Exécution synchrone sous le bon mode de verrou (jamais tenu à travers un `await`).
fn run_locked(
    ctx: &CommandContext,
    state: &mut ClientState,
    cmd: &str,
    args: &[Bytes],
) -> Vec<RespValue> {
    // Gate maxmemory unique (DRY) avant toute commande denyoom.
    if let Err(e) = crate::eviction::prepare_write(ctx, state, cmd) {
        return vec![e];
    }
    match cmd {
        "EXEC" => vec![exec_transaction(ctx, state)],
        // PSYNC/SYNC prennent déjà l'exclusivité ici : `full_resync` ne doit pas
        // ré-acquérir `exec_lock` (deadlock parking_lot read→write même thread).
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO" | "FUNCTION"
        | "PSYNC" | "SYNC" => {
            let _exclusive = ctx.exec_lock.write();
            execute(ctx, state, cmd, args)
        }
        _ => {
            let _shared = ctx.exec_lock.read();
            execute(ctx, state, cmd, args)
        }
    }
}

/// EXEC : exécute la file sous le verrou exclusif, après contrôle des clés surveillées.
fn exec_transaction(ctx: &CommandContext, state: &mut ClientState) -> RespValue {
    if !state.in_multi {
        return RespValue::err("ERR EXEC without MULTI");
    }
    state.in_multi = false;
    let queued = std::mem::take(&mut state.multi_queue);
    let watched = std::mem::take(&mut state.watched);
    if std::mem::take(&mut state.multi_error) {
        return RespValue::err("EXECABORT Transaction discarded because of previous errors.");
    }

    let _exclusive = ctx.exec_lock.write();
    if extra::watched_keys_changed(ctx, &watched) {
        return RespValue::null_array();
    }
    let mut items = Vec::with_capacity(queued.len());
    state.deny_blocking = true;
    for q in queued {
        let name = String::from_utf8_lossy(&q[0]).to_ascii_uppercase();
        let reply = execute(ctx, state, &name, &q)
            .into_iter()
            .next()
            .unwrap_or_else(RespValue::null_bulk);
        items.push(reply);
    }
    state.deny_blocking = false;
    RespValue::array(items)
}

/// Erreur Redis d'une commande inconnue.
fn unknown_command(cmd: &str, args: &[Bytes]) -> RespValue {
    let name: String = String::from_utf8_lossy(&args[0])
        .chars()
        .take(128)
        .collect();
    let mut tail = String::new();
    for a in args.iter().skip(1) {
        if tail.len() >= 128 {
            break;
        }
        let room = 128 - tail.len();
        let s = String::from_utf8_lossy(a);
        let cut = s
            .char_indices()
            .map(|(i, _)| i)
            .chain([s.len()])
            .take_while(|&i| i <= room)
            .last()
            .unwrap_or(0);
        tail.push_str(&format!("'{}' ", &s[..cut]));
    }
    let msg = format!("ERR unknown command '{name}', with args beginning with: {tail}");
    match deferred_error(cmd) {
        Some(m) => RespValue::err(m),
        None => RespValue::err(msg),
    }
}

/// Contrôle à la mise en file MULTI : commande connue et arité correcte.
fn queue_check(cmd: &str, args: &[Bytes]) -> Option<RespValue> {
    match command_registry::lookup(cmd) {
        None => Some(unknown_command(cmd, args)),
        Some(spec) if !spec.arity_ok(args.len()) => Some(wrong_arity(&cmd.to_ascii_lowercase())),
        Some(_) => None,
    }
}

/// Exécute une commande déjà normalisée (`cmd` en majuscules). Aucun verrou global n'est pris :
/// l'appelant en est responsable (voir [`run_locked`], EXEC et les scripts).
pub(crate) fn execute(
    ctx: &CommandContext,
    state: &mut ClientState,
    cmd: &str,
    args: &[Bytes],
) -> Vec<RespValue> {
    if let Some(spec) = command_registry::lookup(cmd) {
        if let Some(exec) = spec.exec {
            if !spec.arity_ok(args.len()) {
                return vec![wrong_arity(&cmd.to_ascii_lowercase())];
            }
            return match exec {
                command_registry::Exec::One(f) => vec![f(ctx, state, args)],
                command_registry::Exec::Many(f) => f(ctx, state, args),
            };
        }
    }
    match cmd {
        // -- transactions --
        "MULTI" => {
            if state.in_multi {
                return vec![RespValue::err("ERR MULTI calls can not be nested")];
            }
            state.in_multi = true;
            state.multi_error = false;
            state.multi_queue.clear();
            vec![RespValue::ok()]
        }
        "DISCARD" => {
            if !state.in_multi {
                return vec![RespValue::err("ERR DISCARD without MULTI")];
            }
            state.in_multi = false;
            state.multi_error = false;
            state.multi_queue.clear();
            state.watched.clear();
            vec![RespValue::ok()]
        }
        "EXEC" => vec![RespValue::err("ERR EXEC is not allowed in this context")],
        "WATCH" => vec![extra::cmd_watch(ctx, state, args)],
        "UNWATCH" => vec![extra::cmd_unwatch(state)],

        // -- connexion / serveur --
        "PING" => vec![cmd_ping(args)],
        "ECHO" => vec![cmd_echo(args)],
        "AUTH" => vec![cmd_auth(ctx, state, args)],
        "SELECT" => vec![cmd_select(ctx, state, args)],
        "QUIT" => vec![RespValue::ok()],
        "HELLO" => vec![cmd_hello(ctx, state, args)],
        "RESET" => {
            state.in_multi = false;
            state.multi_error = false;
            state.multi_queue.clear();
            state.watched.clear();
            state.db_index = 0;
            state.name = None;
            vec![RespValue::simple(Bytes::from_static(b"RESET"))]
        }
        "COMMAND" => vec![server::cmd_command(args)],
        "INFO" => vec![server::cmd_info(ctx, args)],
        "CLIENT" => vec![server::cmd_client(ctx, state, args)],
        "CONFIG" => vec![server::cmd_config(ctx, args)],
        "SLOWLOG" => vec![server::cmd_slowlog(ctx, args)],
        "MONITOR" => vec![server::cmd_monitor(ctx, state)],
        "SHUTDOWN" => vec![server::cmd_shutdown(ctx, args)],
        "LOLWUT" => vec![server::cmd_lolwut(args)],
        "MEMORY" => vec![server::cmd_memory(ctx, state, args)],
        "FQ.SELECT" | "FQ.INDEX" | "FQ.DROPINDEX" | "FQ.LIST" => {
            vec![crate::cmd_query::cmd_fq(ctx, state, args)]
        }
        "INDEX" => vec![crate::cmd_query::cmd_index(ctx, state, args)],
        "FERREDIS" => vec![server::cmd_ferredis(ctx, args)],
        "DATABASES" => vec![server::cmd_databases(ctx, state, args)],
        "TIME" => vec![cmd_time()],
        "ACL" => vec![acl::cmd_acl(ctx, state, args)],
        "DBSIZE" => vec![RespValue::Integer(ctx.db.dbsize(state.db_index))],
        "FLUSHDB" => vec![cmd_flushdb(ctx, state, args)],
        "FLUSHALL" => vec![cmd_flushall(ctx, state, args)],
        "SAVE" => vec![extra::cmd_save(ctx)],
        "BGSAVE" => vec![extra::cmd_bgsave(ctx)],
        "LASTSAVE" => vec![extra::cmd_lastsave(ctx)],
        "BGREWRITEAOF" => vec![extra::cmd_bgrewriteaof(ctx)],

        // -- scripts / functions --
        "EVAL" => vec![extra::cmd_eval(ctx, state, args, false, false)],
        "EVAL_RO" => vec![extra::cmd_eval(ctx, state, args, false, true)],
        "EVALSHA" => vec![extra::cmd_eval(ctx, state, args, true, false)],
        "EVALSHA_RO" => vec![extra::cmd_eval(ctx, state, args, true, true)],
        "SCRIPT" => vec![extra::cmd_script(ctx, state, args)],
        "FUNCTION" => vec![crate::functions::cmd_function(ctx, state, args)],
        "FCALL" => vec![crate::functions::cmd_fcall(ctx, state, args, false)],
        "FCALL_RO" => vec![crate::functions::cmd_fcall(ctx, state, args, true)],

        // -- clés --
        "DEL" => {
            let r = cmd_del(ctx, state, args);
            if matches!(r, RespValue::Integer(n) if n > 0) {
                ctx.propagate(args);
                for k in args.iter().skip(1) {
                    ctx.notify_key(state.db_index, k, "del");
                }
            }
            vec![r]
        }
        "UNLINK" => vec![cmd_unlink(ctx, state, args)],
        "EXISTS" => vec![cmd_exists(ctx, state, args)],
        "TYPE" => vec![cmd_type(ctx, state, args)],
        "TTL" => vec![cmd_ttl(ctx, state, args)],
        "PTTL" => vec![cmd_pttl(ctx, state, args)],
        "EXPIRE" => vec![key::cmd_expire(
            ctx,
            state,
            args,
            key::ExpireMode {
                millis: false,
                absolute: false,
            },
        )],
        "PEXPIRE" => vec![key::cmd_expire(
            ctx,
            state,
            args,
            key::ExpireMode {
                millis: true,
                absolute: false,
            },
        )],
        "EXPIREAT" => vec![key::cmd_expire(
            ctx,
            state,
            args,
            key::ExpireMode {
                millis: false,
                absolute: true,
            },
        )],
        "PEXPIREAT" => vec![key::cmd_expire(
            ctx,
            state,
            args,
            key::ExpireMode {
                millis: true,
                absolute: true,
            },
        )],
        "EXPIRETIME" => vec![key::cmd_expiretime(ctx, state, args, false)],
        "PEXPIRETIME" => vec![key::cmd_expiretime(ctx, state, args, true)],
        "PERSIST" => vec![cmd_persist(ctx, state, args)],
        "RENAME" => vec![key::cmd_rename(ctx, state, args, false)],
        "RENAMENX" => vec![key::cmd_rename(ctx, state, args, true)],
        "COPY" => vec![key::cmd_copy(ctx, state, args)],
        "MOVE" => vec![key::cmd_move(ctx, state, args)],
        "RANDOMKEY" => vec![key::cmd_randomkey(ctx, state)],
        "TOUCH" => vec![key::cmd_touch(ctx, state, args)],
        "OBJECT" => vec![key::cmd_object(ctx, state, args)],
        "SCAN" => vec![cmd_scan(ctx, state, args)],
        "KEYS" => vec![cmd_keys(ctx, state, args)],

        // -- chaînes --
        "GET" => vec![cmd_get(ctx, state, args)],
        "SET" => vec![string::cmd_set(ctx, state, args)],
        "SETEX" => vec![string::cmd_setex(ctx, state, args, false)],
        "PSETEX" => vec![string::cmd_setex(ctx, state, args, true)],
        "SETNX" => vec![cmd_setnx(ctx, state, args)],
        "GETSET" => vec![cmd_getset(ctx, state, args)],
        "GETDEL" => vec![string::cmd_getdel(ctx, state, args)],
        "GETEX" => vec![string::cmd_getex(ctx, state, args)],
        "MGET" => vec![cmd_mget(ctx, state, args)],
        "MSET" => vec![cmd_mset(ctx, state, args)],
        "MSETNX" => vec![string::cmd_msetnx(ctx, state, args)],
        "APPEND" => vec![string::cmd_append(ctx, state, args)],
        "STRLEN" => vec![cmd_strlen(ctx, state, args)],
        "INCR" => vec![cmd_incr(ctx, state, args, 1)],
        "DECR" => vec![cmd_incr(ctx, state, args, -1)],
        "INCRBY" => vec![cmd_incrby(ctx, state, args)],
        "DECRBY" => vec![cmd_decrby(ctx, state, args)],
        "INCRBYFLOAT" => vec![string::cmd_incrbyfloat(ctx, state, args)],
        "SETRANGE" => vec![string::cmd_setrange(ctx, state, args)],
        "GETRANGE" | "SUBSTR" => vec![string::cmd_getrange(ctx, state, args)],

        // -- listes --
        "LPUSH" => vec![cmd_lpush(ctx, state, args)],
        "RPUSH" => vec![cmd_rpush(ctx, state, args)],
        "LPUSHX" => vec![list::cmd_pushx(ctx, state, args, true)],
        "RPUSHX" => vec![list::cmd_pushx(ctx, state, args, false)],
        "LPOP" => vec![cmd_lpop(ctx, state, args)],
        "RPOP" => vec![cmd_rpop(ctx, state, args)],
        "LLEN" => vec![cmd_llen(ctx, state, args)],
        "LRANGE" => vec![cmd_lrange(ctx, state, args)],
        "LINDEX" => vec![list::cmd_lindex(ctx, state, args)],
        "LSET" => vec![list::cmd_lset(ctx, state, args)],
        "LREM" => vec![list::cmd_lrem(ctx, state, args)],
        "LTRIM" => vec![list::cmd_ltrim(ctx, state, args)],
        "LINSERT" => vec![list::cmd_linsert(ctx, state, args)],
        "LPOS" => vec![list::cmd_lpos(ctx, state, args)],
        "RPOPLPUSH" => vec![list::cmd_lmove(ctx, state, args, true)],
        "LMOVE" => vec![list::cmd_lmove(ctx, state, args, false)],
        c if blocking::is_blocking(c) => vec![blocking::run_once(ctx, state, c, args)],

        // -- hashes --
        "HSET" => vec![extra::cmd_hset(ctx, state, args)],
        "HGET" => vec![extra::cmd_hget(ctx, state, args)],
        "HDEL" => vec![extra::cmd_hdel(ctx, state, args)],
        "HGETALL" => vec![extra::cmd_hgetall(ctx, state, args)],
        "HEXISTS" => vec![extra::cmd_hexists(ctx, state, args)],
        "HLEN" => vec![extra::cmd_hlen(ctx, state, args)],
        "HINCRBY" => vec![extra::cmd_hincrby(ctx, state, args)],
        "HMGET" => vec![extra::cmd_hmget(ctx, state, args)],
        "HMSET" => vec![extra::cmd_hmset(ctx, state, args)],
        "HKEYS" => vec![extra::cmd_hkeys(ctx, state, args)],
        "HVALS" => vec![extra::cmd_hvals(ctx, state, args)],
        "HSETNX" => vec![hashset::cmd_hsetnx(ctx, state, args)],
        "HINCRBYFLOAT" => vec![hashset::cmd_hincrbyfloat(ctx, state, args)],
        "HSTRLEN" => vec![hashset::cmd_hstrlen(ctx, state, args)],
        "HSCAN" => vec![hashset::cmd_hscan(ctx, state, args)],

        // -- sets --
        "SADD" => vec![extra::cmd_sadd(ctx, state, args)],
        "SREM" => vec![extra::cmd_srem(ctx, state, args)],
        "SMEMBERS" => vec![extra::cmd_smembers(ctx, state, args)],
        "SISMEMBER" => vec![extra::cmd_sismember(ctx, state, args)],
        "SCARD" => vec![extra::cmd_scard(ctx, state, args)],
        "SINTER" => vec![extra::cmd_sinter(ctx, state, args)],
        "SUNION" => vec![extra::cmd_sunion(ctx, state, args)],
        "SDIFF" => vec![extra::cmd_sdiff(ctx, state, args)],
        "SSCAN" => vec![hashset::cmd_sscan(ctx, state, args)],
        "SPOP" => vec![hashset::cmd_spop(ctx, state, args)],
        "SMOVE" => vec![hashset::cmd_smove(ctx, state, args)],
        "SMISMEMBER" => vec![hashset::cmd_smismember(ctx, state, args)],
        "SINTERSTORE" => vec![hashset::cmd_setstore(
            ctx,
            state,
            args,
            hashset::SetOp::Inter,
        )],
        "SUNIONSTORE" => vec![hashset::cmd_setstore(
            ctx,
            state,
            args,
            hashset::SetOp::Union,
        )],
        "SDIFFSTORE" => vec![hashset::cmd_setstore(
            ctx,
            state,
            args,
            hashset::SetOp::Diff,
        )],

        _ => vec![unknown_command(cmd, args)],
    }
}

fn cmd_flushdb(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    let mode = match trailing_sync_mode(args, 1) {
        Ok(m) => m.0,
        Err("arity") => return wrong_arity("flushdb"),
        Err(_) => return RespValue::err("ERR syntax error"),
    };
    match mode {
        SyncMode::Async => {
            ctx.async_drop.flushdb(state.db_index);
        }
        _ => {
            ctx.db.flushdb(state.db_index);
        }
    }
    ctx.notify_key(state.db_index, &Bytes::from_static(b"*"), "flushdb");
    RespValue::ok()
}

fn cmd_flushall(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    let mode = match trailing_sync_mode(args, 1) {
        Ok(m) => m.0,
        Err("arity") => return wrong_arity("flushall"),
        Err(_) => return RespValue::err("ERR syntax error"),
    };
    match mode {
        SyncMode::Async => ctx.async_drop.flushall(),
        _ => ctx.db.flushall(),
    }
    ctx.notify_key(state.db_index, &Bytes::from_static(b"*"), "flushall");
    RespValue::ok()
}

fn cmd_unlink(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("unlink");
    }
    let keys: Vec<Bytes> = args[1..].to_vec();
    let count = keys.len() as i64;
    ctx.async_drop.unlink(state.db_index, keys);
    RespValue::Integer(count)
}

fn cmd_ping(args: &[Bytes]) -> RespValue {
    if args.len() == 1 {
        RespValue::pong()
    } else if args.len() == 2 {
        RespValue::bulk(args[1].clone())
    } else {
        wrong_arity("ping")
    }
}

fn cmd_echo(args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("echo");
    }
    RespValue::bulk(args[1].clone())
}

fn cmd_auth(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 && args.len() != 3 {
        return wrong_arity("auth");
    }
    let (user, password) = if args.len() == 2 {
        ("default", String::from_utf8_lossy(&args[1]).into_owned())
    } else {
        (
            arg_str(args, 1).unwrap_or("default"),
            String::from_utf8_lossy(&args[2]).into_owned(),
        )
    };
    match ctx.acl.authenticate(user, &password) {
        Ok(()) => {
            state.authenticated = true;
            state.acl_user = user.to_string();
            if let Some(info) = &state.info {
                info.set_user(user);
            }
            RespValue::ok()
        }
        Err(e) => {
            server::record_auth_failure(ctx, state);
            RespValue::err(e)
        }
    }
}

fn cmd_select(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("select");
    }
    let idx: usize = match arg_str(args, 1).and_then(|s| s.parse().ok()) {
        Some(i) => i,
        None => return RespValue::err("ERR invalid DB index"),
    };
    if idx >= ctx.db.db_count() {
        return RespValue::err("ERR DB index is out of range");
    }
    state.db_index = idx;
    RespValue::ok()
}

fn cmd_get(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("get");
    }
    match ctx.db.get_string(state.db_index, &args[1]) {
        Ok(Some(v)) => {
            ctx.config.read().stats.record_keyspace_hit();
            RespValue::bulk(v)
        }
        Ok(None) => {
            ctx.config.read().stats.record_keyspace_miss();
            RespValue::null_bulk()
        }
        Err(e) => RespValue::err(e),
    }
}

fn cmd_del(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("del");
    }
    RespValue::Integer(ctx.db.del(state.db_index, &args[1..]))
}

fn cmd_exists(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("exists");
    }
    RespValue::Integer(ctx.db.exists(state.db_index, &args[1..]))
}

fn cmd_type(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("type");
    }
    RespValue::simple(Bytes::from(
        ctx.db.key_type(state.db_index, &args[1]).to_string(),
    ))
}

fn cmd_ttl(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("ttl");
    }
    RespValue::Integer(ctx.db.ttl(state.db_index, &args[1]))
}

fn cmd_pttl(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("pttl");
    }
    RespValue::Integer(ctx.db.pttl(state.db_index, &args[1]))
}

fn cmd_persist(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("persist");
    }
    RespValue::Integer(ctx.db.persist(state.db_index, &args[1]))
}

fn cmd_incr(ctx: &CommandContext, state: &ClientState, args: &[Bytes], delta: i64) -> RespValue {
    if args.len() != 2 {
        return wrong_arity(if delta > 0 { "incr" } else { "decr" });
    }
    match ctx.db.incr_by(state.db_index, &args[1], delta) {
        Ok(n) => {
            ctx.notify_key(
                state.db_index,
                &args[1],
                if delta > 0 { "incr" } else { "decr" },
            );
            RespValue::Integer(n)
        }
        Err(e) => RespValue::err(e),
    }
}

fn cmd_incrby(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("incrby");
    }
    let delta: i64 = match arg_str(args, 2).and_then(|s| s.parse().ok()) {
        Some(s) => s,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    match ctx.db.incr_by(state.db_index, &args[1], delta) {
        Ok(n) => RespValue::Integer(n),
        Err(e) => RespValue::err(e),
    }
}

fn cmd_decrby(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("decrby");
    }
    let delta: i64 = match arg_str(args, 2).and_then(|s| s.parse().ok()) {
        Some(s) => s,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    match ctx.db.incr_by(state.db_index, &args[1], -delta) {
        Ok(n) => RespValue::Integer(n),
        Err(e) => RespValue::err(e),
    }
}

fn cmd_mget(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("mget");
    }
    let mut items = Vec::with_capacity(args.len() - 1);
    for key in &args[1..] {
        match ctx.db.get_string(state.db_index, key) {
            Ok(Some(v)) => items.push(RespValue::bulk(v)),
            _ => items.push(RespValue::null_bulk()),
        }
    }
    RespValue::array(items)
}

fn cmd_mset(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 || args.len() % 2 == 0 {
        return wrong_arity("mset");
    }
    let mut i = 1;
    while i + 1 < args.len() {
        let _ = ctx.db.set_string(
            state.db_index,
            args[i].clone(),
            args[i + 1].clone(),
            false,
            false,
            None,
            None,
        );
        i += 2;
    }
    RespValue::ok()
}

fn cmd_setnx(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("setnx");
    }
    match ctx.db.set_string(
        state.db_index,
        args[1].clone(),
        args[2].clone(),
        true,
        false,
        None,
        None,
    ) {
        Ok(true) => RespValue::Integer(1),
        Ok(false) => RespValue::Integer(0),
        Err(e) => RespValue::err(e),
    }
}

fn cmd_getset(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("getset");
    }
    let old = ctx.db.get_string(state.db_index, &args[1]);
    let _ = ctx.db.set_string(
        state.db_index,
        args[1].clone(),
        args[2].clone(),
        false,
        false,
        None,
        None,
    );
    match old {
        Ok(Some(v)) => RespValue::bulk(v),
        Ok(None) => RespValue::null_bulk(),
        Err(e) => RespValue::err(e),
    }
}

fn cmd_strlen(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("strlen");
    }
    match ctx.db.get_string(state.db_index, &args[1]) {
        Ok(Some(v)) => RespValue::Integer(v.len() as i64),
        Ok(None) => RespValue::Integer(0),
        Err(e) => RespValue::err(e),
    }
}

fn cmd_lpush(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("lpush");
    }
    match ctx.db.list_push(state.db_index, &args[1], &args[2..], true) {
        Ok(n) => {
            ctx.waiters.notify(state.db_index, &args[1]);
            ctx.notify_key(state.db_index, &args[1], "lpush");
            RespValue::Integer(n)
        }
        Err(e) => RespValue::err(e),
    }
}

fn cmd_rpush(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("rpush");
    }
    match ctx
        .db
        .list_push(state.db_index, &args[1], &args[2..], false)
    {
        Ok(n) => {
            ctx.waiters.notify(state.db_index, &args[1]);
            ctx.notify_key(state.db_index, &args[1], "rpush");
            RespValue::Integer(n)
        }
        Err(e) => RespValue::err(e),
    }
}

fn cmd_lpop(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 || args.len() > 3 {
        return wrong_arity("lpop");
    }
    let count = if args.len() == 3 {
        match arg_str(args, 2).and_then(|s| s.parse::<usize>().ok()) {
            Some(c) => Some(c),
            None => return RespValue::err("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };
    match ctx.db.list_pop(state.db_index, &args[1], true, count) {
        Ok(None) if count.is_some() => RespValue::null_array(),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some(vals)) if count.is_none() => RespValue::bulk(vals.into_iter().next().unwrap()),
        Ok(Some(vals)) => RespValue::array(vals.into_iter().map(RespValue::bulk).collect()),
        Err(e) => RespValue::err(e),
    }
}

fn cmd_rpop(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 || args.len() > 3 {
        return wrong_arity("rpop");
    }
    let count = if args.len() == 3 {
        match arg_str(args, 2).and_then(|s| s.parse::<usize>().ok()) {
            Some(c) => Some(c),
            None => return RespValue::err("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };
    match ctx.db.list_pop(state.db_index, &args[1], false, count) {
        Ok(None) if count.is_some() => RespValue::null_array(),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some(vals)) if count.is_none() => RespValue::bulk(vals.into_iter().next().unwrap()),
        Ok(Some(vals)) => RespValue::array(vals.into_iter().map(RespValue::bulk).collect()),
        Err(e) => RespValue::err(e),
    }
}

fn cmd_llen(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("llen");
    }
    match ctx.db.list_len(state.db_index, &args[1]) {
        Ok(n) => RespValue::Integer(n),
        Err(e) => RespValue::err(e),
    }
}

fn cmd_lrange(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("lrange");
    }
    let start: i64 = match arg_str(args, 2).and_then(|s| s.parse().ok()) {
        Some(s) => s,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    let stop: i64 = match arg_str(args, 3).and_then(|s| s.parse().ok()) {
        Some(s) => s,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    match ctx.db.list_range(state.db_index, &args[1], start, stop) {
        Ok(vals) => RespValue::array(vals.into_iter().map(RespValue::bulk).collect()),
        Err(e) => RespValue::err(e),
    }
}

fn cmd_scan(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("scan");
    }
    let cursor: u64 = match arg_str(args, 1).and_then(|s| s.parse().ok()) {
        Some(c) => c,
        None => return RespValue::err("ERR invalid cursor"),
    };
    let mut pattern = None;
    let mut type_filter = None;
    let mut count = 10usize;
    let mut i = 2;
    while i < args.len() {
        let opt = arg_str(args, i).unwrap_or("").to_ascii_uppercase();
        let Some(value) = arg_str(args, i + 1) else {
            return RespValue::err("ERR syntax error");
        };
        match opt.as_str() {
            "MATCH" => pattern = Some(value.to_string()),
            "COUNT" => match value.parse::<usize>() {
                Ok(n) if n >= 1 => count = n,
                Ok(_) => return RespValue::err("ERR syntax error"),
                Err(_) => return RespValue::err(ERR_INT),
            },
            "TYPE" => type_filter = Some(value.to_ascii_lowercase()),
            _ => return RespValue::err("ERR syntax error"),
        }
        i += 2;
    }
    let (next, keys) = ctx.db.scan(
        state.db_index,
        cursor,
        pattern.as_deref(),
        count,
        type_filter.as_deref(),
    );
    RespValue::array(vec![
        RespValue::bulk(Bytes::from(next.to_string())),
        RespValue::array(keys.into_iter().map(RespValue::bulk).collect()),
    ])
}

fn cmd_keys(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("keys");
    }
    let pattern = arg_str(args, 1).unwrap_or("*");
    let keys = ctx.db.keys(state.db_index, pattern);
    RespValue::array(keys.into_iter().map(RespValue::bulk).collect())
}

fn cmd_time() -> RespValue {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    RespValue::array(vec![
        RespValue::bulk(Bytes::from(dur.as_secs().to_string())),
        RespValue::bulk(Bytes::from(dur.subsec_micros().to_string())),
    ])
}

fn cmd_hello(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    // HELLO [protover [AUTH user pass] [SETNAME name]]
    let mut proto: u8 = state.resp_version;
    let mut i = 1;
    if let Some(p) = arg_str(args, 1) {
        match p.parse::<i64>() {
            Ok(2) => {
                proto = 2;
                i = 2;
            }
            Ok(3) => {
                proto = 3;
                i = 2;
            }
            Ok(_) => {
                return RespValue::err("NOPROTO sorry, this protocol version is not supported.")
            }
            Err(_) => {
                return RespValue::err("ERR Protocol version is not an integer or out of range")
            }
        }
    }
    while i < args.len() {
        let opt = arg_str(args, i).unwrap_or("").to_ascii_uppercase();
        if opt == "AUTH" && i + 2 < args.len() {
            let user = arg_str(args, i + 1).unwrap_or("default");
            let pass = String::from_utf8_lossy(&args[i + 2]).into_owned();
            match ctx.acl.authenticate(user, &pass) {
                Ok(()) => {
                    state.authenticated = true;
                    state.acl_user = user.to_string();
                    if let Some(info) = &state.info {
                        info.set_user(user);
                    }
                }
                Err(e) => {
                    server::record_auth_failure(ctx, state);
                    return RespValue::err(e);
                }
            }
            i += 3;
        } else if opt == "SETNAME" && i + 1 < args.len() {
            let name = String::from_utf8_lossy(&args[i + 1]).to_string();
            if let Some(info) = &state.info {
                info.set_name(&name);
            }
            state.name = Some(name);
            i += 2;
        } else {
            return RespValue::err("ERR syntax error");
        }
    }
    if (ctx.config.read().requirepass.is_some() || acl_requires_auth(ctx)) && !state.authenticated {
        return RespValue::err("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");
    }
    state.resp_version = proto;
    if let Some(info) = &state.info {
        info.set_resp(proto);
    }
    let id = state.info.as_ref().map_or(0, |c| c.id as i64);
    let entries = vec![
        (
            RespValue::bulk(Bytes::from_static(b"server")),
            RespValue::bulk(Bytes::from_static(b"ferredis")),
        ),
        (
            RespValue::bulk(Bytes::from_static(b"version")),
            RespValue::bulk(Bytes::from(server::REDIS_COMPAT_VERSION)),
        ),
        (
            RespValue::bulk(Bytes::from_static(b"proto")),
            RespValue::Integer(proto as i64),
        ),
        (
            RespValue::bulk(Bytes::from_static(b"id")),
            RespValue::Integer(id),
        ),
        (
            RespValue::bulk(Bytes::from_static(b"mode")),
            RespValue::bulk(Bytes::from_static(b"standalone")),
        ),
        (
            RespValue::bulk(Bytes::from_static(b"role")),
            RespValue::bulk(Bytes::from_static(b"master")),
        ),
    ];
    if proto >= 3 {
        RespValue::map(entries)
    } else {
        let mut flat = Vec::with_capacity(entries.len() * 2);
        for (k, v) in entries {
            flat.push(k);
            flat.push(v);
        }
        RespValue::array(flat)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx() -> (CommandContext, ClientState) {
        let ctx = CommandContext::new(ServerConfig::default());
        let state = ClientState {
            authenticated: true,
            ..Default::default()
        };
        (ctx, state)
    }

    #[tokio::test]
    async fn set_get_del() {
        let (ctx, mut state) = test_ctx();
        let r = dispatch(
            &ctx,
            &mut state,
            &[
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"k"),
                Bytes::from_static(b"v"),
            ],
        )
        .await;
        assert_eq!(r[0], RespValue::ok());
        let r = dispatch(
            &ctx,
            &mut state,
            &[Bytes::from_static(b"GET"), Bytes::from_static(b"k")],
        )
        .await;
        assert_eq!(r[0], RespValue::bulk(Bytes::from_static(b"v")));
    }

    #[tokio::test]
    async fn incr_and_expire() {
        let (ctx, mut state) = test_ctx();
        let r = dispatch(
            &ctx,
            &mut state,
            &[Bytes::from_static(b"INCR"), Bytes::from_static(b"c")],
        )
        .await;
        assert_eq!(r[0], RespValue::Integer(1));
        let _ = dispatch(
            &ctx,
            &mut state,
            &[
                Bytes::from_static(b"EXPIRE"),
                Bytes::from_static(b"c"),
                Bytes::from_static(b"60"),
            ],
        )
        .await;
        let r = dispatch(
            &ctx,
            &mut state,
            &[Bytes::from_static(b"TTL"), Bytes::from_static(b"c")],
        )
        .await;
        match &r[0] {
            RespValue::Integer(n) => assert!(*n > 0 && *n <= 60),
            _ => panic!("expected integer"),
        }
    }

    #[tokio::test]
    async fn list_ops() {
        let (ctx, mut state) = test_ctx();
        let r = dispatch(
            &ctx,
            &mut state,
            &[
                Bytes::from_static(b"LPUSH"),
                Bytes::from_static(b"l"),
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ],
        )
        .await;
        assert_eq!(r[0], RespValue::Integer(2));
        let r = dispatch(
            &ctx,
            &mut state,
            &[
                Bytes::from_static(b"LRANGE"),
                Bytes::from_static(b"l"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"-1"),
            ],
        )
        .await;
        assert!(matches!(r[0], RespValue::Array(Some(_))));
    }

    #[tokio::test]
    async fn set_nx() {
        let (ctx, mut state) = test_ctx();
        let r = dispatch(
            &ctx,
            &mut state,
            &[
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"lock"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"NX"),
                Bytes::from_static(b"EX"),
                Bytes::from_static(b"10"),
            ],
        )
        .await;
        assert_eq!(r[0], RespValue::ok());
        let r = dispatch(
            &ctx,
            &mut state,
            &[
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"lock"),
                Bytes::from_static(b"2"),
                Bytes::from_static(b"NX"),
            ],
        )
        .await;
        assert_eq!(r[0], RespValue::null_bulk());
    }
}
