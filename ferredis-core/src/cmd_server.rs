//! Commandes d'administration : INFO, CLIENT, CONFIG, COMMAND, SLOWLOG, LATENCY, FERREDIS…

use crate::command::{wrong_arity, ClientState, CommandContext};
use crate::command_registry::{self, command_info_array};
use crate::conf::ConfFile;
use crate::control::ShutdownRequest;
use crate::latency::doctor_report;
use crate::persist::save_rdb;
use crate::pubsub::SubKind;
use crate::resp::RespValue;
use crate::slowlog::SlowLogEntry;
use crate::util::*;
use bytes::Bytes;
use std::fmt::Write as _;
use std::sync::atomic::Ordering;

pub const REDIS_COMPAT_VERSION: &str = "8.0.0";

fn text(s: impl Into<String>) -> RespValue {
    RespValue::bulk(Bytes::from(s.into()))
}

// ───────────────────────────── INFO ─────────────────────────────

/// Construit les sections `INFO` (nom, corps) dans l'ordre standard de Redis.
fn info_sections(ctx: &CommandContext) -> Vec<(&'static str, String)> {
    let cfg = ctx.config.read();
    let uptime = cfg.started_at.elapsed().as_secs();
    let port = cfg.port;
    let mem = ctx.db.approx_memory();
    let clients = &ctx.clients;

    let mut keyspace = String::new();
    for db in 0..ctx.db.db_count() {
        let (keys, expires) = ctx.db.key_stats(db);
        if keys > 0 {
            let _ = write!(
                keyspace,
                "db{db}:keys={keys},expires={expires},avg_ttl=0\r\n"
            );
        }
    }

    vec![
        (
            "Server",
            format!(
                "redis_version:{REDIS_COMPAT_VERSION}\r\nferredis_version:{}\r\nredis_mode:standalone\r\nos:{}\r\narch_bits:{}\r\nprocess_id:{}\r\nrun_id:{}\r\ntcp_port:{port}\r\nuptime_in_seconds:{uptime}\r\nuptime_in_days:{}\r\n",
                env!("CARGO_PKG_VERSION"),
                std::env::consts::OS,
                usize::BITS,
                std::process::id(),
                cfg.run_id,
                uptime / 86_400,
            ),
        ),
        (
            "Clients",
            format!(
                "connected_clients:{}\r\nblocked_clients:{}\r\nmaxclients:10000\r\n",
                clients.connected(),
                ctx.waiters.blocked_count(),
            ),
        ),
        (
            "Memory",
            {
                ctx.storage.mem.set_used(mem as u64);
                let frag = ctx.storage.mem.fragmentation_ratio();
                let (_, _, defrag_running) = ctx.defrag.snapshot();
                format!(
                    "used_memory:{mem}\r\nused_memory_human:{}\r\nused_memory_rss:{}\r\nmaxmemory:{}\r\nmaxmemory_soft:{}\r\nmaxmemory_policy:{}\r\nevicted_keys:{}\r\nmem_fragmentation_ratio:{frag:.2}\r\nactive_defrag_running:{}\r\ncompress_threshold:{}\r\ncompress_bytes_saved:{}\r\n{}\
io-threads:{}\r\nio_threaded_reads_processed:{}\r\nio_threaded_writes_processed:{}\r\n",
                    human_bytes(mem),
                    ctx.storage.mem.rss_bytes(),
                    cfg.maxmemory,
                    cfg.maxmemory_soft,
                    cfg.maxmemory_policy,
                    cfg.stats.evicted_keys(),
                    defrag_running as u8,
                    cfg.compress_threshold,
                    ctx.storage.mem.compress_saves(),
                    ctx.storage.info_lines(),
                    cfg.io_threads,
                    crate::io_threads::global()
                        .map(|p| p.stats().reads_processed.load(std::sync::atomic::Ordering::Relaxed))
                        .unwrap_or(0),
                    crate::io_threads::global()
                        .map(|p| p.stats().writes_processed.load(std::sync::atomic::Ordering::Relaxed))
                        .unwrap_or(0),
                )
            },
        ),
        (
            "Persistence",
            format!(
                "loading:0\r\nrdb_changes_since_last_save:{}\r\naof_enabled:{}\r\naof_group_commit_ms:{}\r\n",
                ctx.db.dirty.load(std::sync::atomic::Ordering::Relaxed),
                ctx.aof.enabled() as u8,
                cfg.aof_group_commit_ms,
            ),
        ),
        (
            "Cluster",
            format!(
                "cluster_enabled:{}\r\n{}",
                cfg.cluster_enabled as u8,
                ctx.raft.info_lines(),
            ),
        ),
        (
            "Stats",
            format!(
                "total_connections_received:{}\r\ntotal_commands_processed:{}\r\nrejected_connections:{}\r\nauth_failures:{}\r\nkeyspace_hits:{}\r\nkeyspace_misses:{}\r\ninstantaneous_ops_per_sec:{}\r\npubsub_channels:{}\r\npubsub_patterns:{}\r\npubsubshard_channels:{}\r\n{}\r\n{}",
                clients.total_connections(),
                clients.total_commands(),
                clients.rejected(),
                clients.auth_failures(),
                cfg.stats.keyspace_hits(),
                cfg.stats.keyspace_misses(),
                cfg.stats.instantaneous_ops(),
                ctx.pubsub.count(SubKind::Channel),
                ctx.pubsub.count(SubKind::Pattern),
                ctx.pubsub.count(SubKind::Shard),
                cfg.stats.commandstats_section(),
                cfg.stats.errorstats_section(),
            ),
        ),
        ("Replication", ctx.repl.info_section()),
        ("Cluster", "cluster_enabled:0\r\n".into()),
        ("Keyspace", keyspace),
    ]
}

fn human_bytes(n: usize) -> String {
    const UNITS: [&str; 4] = ["B", "K", "M", "G"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n}B")
    } else {
        format!("{v:.2}{}", UNITS[u])
    }
}

/// INFO [section …].
pub(crate) fn cmd_info(ctx: &CommandContext, args: &[Bytes]) -> RespValue {
    let wanted: Vec<String> = args[1..]
        .iter()
        .map(|a| upper(a).to_ascii_lowercase())
        .collect();
    let everything = wanted.is_empty()
        || wanted
            .iter()
            .any(|w| matches!(w.as_str(), "all" | "everything" | "default"));
    let mut out = String::new();
    for (name, body) in info_sections(ctx) {
        if everything || wanted.iter().any(|w| w == &name.to_ascii_lowercase()) {
            if !out.is_empty() {
                out.push_str("\r\n");
            }
            let _ = write!(out, "# {name}\r\n{body}");
        }
    }
    text(out)
}

// ───────────────────────────── CLIENT ─────────────────────────────

fn client_line(ctx: &CommandContext, info: &crate::clients::ClientInfo, multi: bool) -> String {
    info.list_line(
        ctx.pubsub.counts(info.pubsub_id()),
        multi,
        ctx.repl.has_replica_client(info.id),
    )
}

/// CLIENT ID|GETNAME|SETNAME|SETINFO|LIST|INFO|KILL|PAUSE|…
pub(crate) fn cmd_client(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("client");
    }
    match upper(&args[1]).as_str() {
        "ID" => RespValue::Integer(state.info.as_ref().map_or(0, |i| i.id as i64)),
        "SETNAME" => {
            if args.len() != 3 {
                return wrong_arity("client|setname");
            }
            if args[2].iter().any(|b| *b <= b' ' || *b > b'~') {
                return err(
                    "ERR Client names cannot contain spaces, newlines or special characters.",
                );
            }
            let name = String::from_utf8_lossy(&args[2]).into_owned();
            if let Some(info) = &state.info {
                info.set_name(&name);
            }
            state.name = (!name.is_empty()).then_some(name);
            RespValue::ok()
        }
        "GETNAME" => match &state.name {
            Some(n) => text(n.clone()),
            None => RespValue::null_bulk(),
        },
        "SETINFO" => {
            if args.len() != 4 {
                return wrong_arity("client|setinfo");
            }
            if let Some(info) = &state.info {
                info.set_lib(&upper(&args[2]), &String::from_utf8_lossy(&args[3]));
            }
            RespValue::ok()
        }
        "LIST" => {
            let mut out = String::new();
            for c in ctx.clients.snapshot() {
                let multi = state.in_multi && state.info.as_ref().is_some_and(|i| i.id == c.id);
                let _ = write!(out, "{}\n", client_line(ctx, &c, multi));
            }
            text(out)
        }
        "INFO" => match &state.info {
            Some(i) => text(format!("{}\n", client_line(ctx, i, state.in_multi))),
            None => text(""),
        },
        "KILL" => client_kill(ctx, state, &args[2..]),
        "NO-EVICT" => client_flag_on_off(state, &args[2..], |s, on| {
            s.no_evict = on;
            if let Some(i) = &s.info {
                i.set_no_evict(on);
            }
        }),
        "NO-TOUCH" => client_flag_on_off(state, &args[2..], |s, on| {
            s.no_touch = on;
            if let Some(i) = &s.info {
                i.set_no_touch(on);
            }
        }),
        "REPLY" => {
            if args.len() != 3 {
                return wrong_arity("client|reply");
            }
            match upper(&args[2]).as_str() {
                "ON" => state.reply_mode = crate::command::ReplyMode::On,
                "OFF" => state.reply_mode = crate::command::ReplyMode::Off,
                "SKIP" => state.reply_mode = crate::command::ReplyMode::Skip,
                _ => return err(ERR_SYNTAX),
            }
            RespValue::ok()
        }
        "UNBLOCK" => client_unblock(ctx, &args[2..]),
        "PAUSE" => client_pause(ctx, &args[2..]),
        "UNPAUSE" => {
            ctx.pause
                .until_ms
                .store(0, std::sync::atomic::Ordering::SeqCst);
            RespValue::ok()
        }
        "TRACKING" => client_tracking(ctx, state, &args[2..]),
        "CACHING" => client_caching(ctx, state, &args[2..]),
        "GETREDIR" => {
            let id = state.info.as_ref().map(|i| i.id).unwrap_or(0);
            RespValue::Integer(ctx.tracking.session(id).redirect_id())
        }
        "TRACKINGINFO" => client_trackinginfo(ctx, state),
        "DURABILITY" => {
            if args.len() == 2 {
                return text(state.durability.as_str());
            }
            if args.len() != 3 {
                return wrong_arity("client|durability");
            }
            match crate::command::DurabilityMode::parse(&String::from_utf8_lossy(&args[2])) {
                Some(m) => {
                    state.durability = m;
                    RespValue::ok()
                }
                None => err("ERR syntax error — use none|everysec|always|quorum"),
            }
        }
        _ => err("ERR unknown subcommand or wrong number of arguments for 'CLIENT'"),
    }
}

fn client_flag_on_off(
    state: &mut ClientState,
    args: &[Bytes],
    set: impl FnOnce(&mut ClientState, bool),
) -> RespValue {
    if args.len() != 1 {
        return wrong_arity("client");
    }
    match upper(&args[0]).as_str() {
        "ON" => {
            set(state, true);
            RespValue::ok()
        }
        "OFF" => {
            set(state, false);
            RespValue::ok()
        }
        _ => err(ERR_SYNTAX),
    }
}

fn client_pause(ctx: &CommandContext, args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return wrong_arity("client|pause");
    }
    let ms = match parse_i64(&args[0]) {
        Some(n) if n >= 0 => n as u64,
        _ => return err("ERR timeout is not an integer or out of range"),
    };
    let write_only = match args.get(1).map(|a| upper(a)).as_deref() {
        None | Some("ALL") => false,
        Some("WRITE") => true,
        _ => return err(ERR_SYNTAX),
    };
    let until = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
        + ms;
    ctx.pause
        .write_only
        .store(write_only, std::sync::atomic::Ordering::SeqCst);
    ctx.pause
        .until_ms
        .store(until, std::sync::atomic::Ordering::SeqCst);
    RespValue::ok()
}

fn client_unblock(ctx: &CommandContext, args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return wrong_arity("client|unblock");
    }
    let id = match parse_i64(&args[0]) {
        Some(n) if n > 0 => n as u64,
        _ => return err("ERR client-id should be greater than 0"),
    };
    let mode = match args.get(1).map(|a| upper(a)).as_deref() {
        None | Some("TIMEOUT") => 1u64,
        Some("ERROR") => 2,
        _ => return err(ERR_SYNTAX),
    };
    match ctx.clients.get(id) {
        Some(c) => {
            c.request_unblock(mode);
            // Réveille aussi les waiters liés (tentative).
            RespValue::Integer(1)
        }
        None => RespValue::Integer(0),
    }
}

fn client_tracking(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return wrong_arity("client|tracking");
    }
    let Some(info) = state.info.as_ref() else {
        return err("ERR CLIENT TRACKING requires a connected client");
    };
    match upper(&args[0]).as_str() {
        "ON" => {
            let mut flags = crate::tracking::TrackingFlags::default();
            let mut redirect: i64 = -1;
            let mut prefixes = Vec::new();
            let mut i = 1;
            while i < args.len() {
                match upper(&args[i]).as_str() {
                    "OPTIN" => {
                        flags.optin = true;
                        i += 1;
                    }
                    "OPTOUT" => {
                        flags.optout = true;
                        i += 1;
                    }
                    "BCAST" => {
                        flags.bcast = true;
                        i += 1;
                    }
                    "NOLOOP" => {
                        flags.noloop = true;
                        i += 1;
                    }
                    "REDIRECT" => {
                        let id = args
                            .get(i + 1)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse().ok());
                        let Some(id) = id else {
                            return err("ERR Invalid redirected client id");
                        };
                        redirect = id;
                        i += 2;
                    }
                    "PREFIX" => {
                        let Some(p) = args.get(i + 1) else {
                            return err(ERR_SYNTAX);
                        };
                        prefixes.push(p.clone());
                        i += 2;
                    }
                    _ => return err(ERR_SYNTAX),
                }
            }
            if flags.optin && flags.optout {
                return err("ERR OPTIN and OPTOUT are mutually exclusive");
            }
            ctx.tracking.enable(info.id, flags, redirect, prefixes);
            info.set_tracking(true);
            info.set_redir(redirect);
            RespValue::ok()
        }
        "OFF" => {
            ctx.tracking.disable(info.id);
            info.set_tracking(false);
            info.set_redir(-1);
            RespValue::ok()
        }
        _ => err(ERR_SYNTAX),
    }
}

fn client_caching(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 1 {
        return wrong_arity("client|caching");
    }
    let Some(info) = state.info.as_ref() else {
        return err("ERR CLIENT CACHING requires a connected client");
    };
    let sess = ctx.tracking.session(info.id);
    if !sess.is_enabled() {
        return err("ERR CLIENT CACHING can be used only when tracking is enabled");
    }
    let flags = *sess.flags.lock();
    if !flags.optin && !flags.optout {
        return err(
            "ERR CLIENT CACHING can be used only when the client is in OPTIN or OPTOUT mode",
        );
    }
    match upper(&args[0]).as_str() {
        "YES" => {
            *sess.caching.lock() = Some(true);
            RespValue::ok()
        }
        "NO" => {
            *sess.caching.lock() = Some(false);
            RespValue::ok()
        }
        _ => err(ERR_SYNTAX),
    }
}

fn client_trackinginfo(ctx: &CommandContext, state: &ClientState) -> RespValue {
    let id = state.info.as_ref().map(|i| i.id).unwrap_or(0);
    let sess = ctx.tracking.session(id);
    let flags = *sess.flags.lock();
    let mut flag_list = Vec::new();
    if sess.is_enabled() {
        flag_list.push(text("on"));
    } else {
        flag_list.push(text("off"));
    }
    if flags.optin {
        flag_list.push(text("optin"));
    }
    if flags.optout {
        flag_list.push(text("optout"));
    }
    if flags.bcast {
        flag_list.push(text("bcast"));
    }
    if flags.noloop {
        flag_list.push(text("noloop"));
    }
    let prefixes = sess
        .prefixes
        .lock()
        .iter()
        .cloned()
        .map(RespValue::bulk)
        .collect();
    RespValue::array(vec![
        text("flags"),
        RespValue::array(flag_list),
        text("redirect"),
        RespValue::Integer(sess.redirect_id()),
        text("prefixes"),
        RespValue::array(prefixes),
    ])
}

/// CLIENT KILL addr | CLIENT KILL [ID id] [ADDR addr] [SKIPME yes|no] …
fn client_kill(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return wrong_arity("client|kill");
    }
    let me = state.info.as_ref().map(|i| i.id);
    if args.len() == 1 {
        let addr = String::from_utf8_lossy(&args[0]);
        return match ctx.clients.snapshot().into_iter().find(|c| c.addr == addr) {
            Some(c) => {
                c.kill();
                RespValue::ok()
            }
            None => err("ERR No such client"),
        };
    }
    let (mut id, mut addr, mut laddr, mut user, mut maxage, mut skipme) = (
        None::<u64>,
        None::<String>,
        None::<String>,
        None::<String>,
        None::<u64>,
        true,
    );
    let mut typ = None::<String>;
    for pair in args.chunks(2) {
        let [k, v] = pair else {
            return err(ERR_SYNTAX);
        };
        match upper(k).as_str() {
            "ID" => id = parse_i64(v).map(|n| n as u64),
            "ADDR" => addr = Some(String::from_utf8_lossy(v).into_owned()),
            "LADDR" => laddr = Some(String::from_utf8_lossy(v).into_owned()),
            "USER" => user = Some(String::from_utf8_lossy(v).into_owned()),
            "TYPE" => typ = Some(upper(v)),
            "MAXAGE" => maxage = parse_i64(v).map(|n| n as u64),
            "SKIPME" => skipme = upper(v) != "NO",
            _ => return err(ERR_SYNTAX),
        }
    }
    let mut killed = 0;
    for c in ctx.clients.snapshot() {
        if id.is_some_and(|i| i != c.id) {
            continue;
        }
        if addr.as_ref().is_some_and(|a| *a != c.addr) {
            continue;
        }
        if laddr.as_ref().is_some_and(|a| *a != c.laddr) {
            continue;
        }
        if user.as_ref().is_some_and(|u| *u != c.user()) {
            continue;
        }
        if let Some(age) = maxage {
            if c.connected_at.elapsed().as_secs() < age {
                continue;
            }
        }
        if let Some(ref t) = typ {
            let (subs, psubs, ssubs) = ctx.pubsub.counts(c.pubsub_id());
            let is_pubsub = subs + psubs + ssubs > 0;
            let match_type = match t.as_str() {
                "NORMAL" => !is_pubsub,
                "MASTER" | "SLAVE" | "REPLICA" => false,
                "PUBSUB" => is_pubsub,
                _ => true,
            };
            if !match_type {
                continue;
            }
        }
        if skipme && Some(c.id) == me {
            continue;
        }
        c.kill();
        killed += 1;
    }
    RespValue::Integer(killed)
}

// ───────────────────────────── CONFIG ─────────────────────────────

/// Valeur d'un paramètre de configuration connu.
fn config_value(ctx: &CommandContext, name: &str) -> Option<String> {
    let cfg = ctx.config.read();
    Some(match name {
        "notify-keyspace-events" => cfg.notify_keyspace_events.lock().clone(),
        "port" => cfg.port.to_string(),
        "bind" => cfg.bind.join(" "),
        "databases" => ctx.db.db_count().to_string(),
        "dir" => cfg.persist.dir.to_string_lossy().into_owned(),
        "dbfilename" => cfg.persist.dbfilename.clone(),
        "appendonly" => if cfg.persist.appendonly { "yes" } else { "no" }.into(),
        "appendfilename" => cfg.persist.appendfilename.clone(),
        "appendfsync" => cfg.persist.appendfsync.clone(),
        "aof-use-rdb-preamble" => {
            if cfg.persist.aof_use_rdb_preamble {
                "yes".into()
            } else {
                "no".into()
            }
        }
        "requirepass" => cfg.requirepass.clone().unwrap_or_default(),
        "masterauth" => ctx.repl.masterauth().unwrap_or_default(),
        "timeout" => cfg.timeout.to_string(),
        "maxmemory" => cfg.maxmemory.to_string(),
        "maxmemory-soft" => cfg.maxmemory_soft.to_string(),
        "compress-threshold" => cfg.compress_threshold.to_string(),
        "activedefrag" => if cfg.activedefrag { "yes" } else { "no" }.into(),
        "storage-mode" => cfg.storage_mode.clone(),
        "aof-group-commit-ms" => cfg.aof_group_commit_ms.to_string(),
        "cluster-mode" => cfg.cluster_consistency.clone(),
        "slowlog-max-len" => cfg.slowlog.max_len().to_string(),
        "maxclients" => cfg.maxclients.to_string(),
        "maxmemory-policy" => cfg.maxmemory_policy.clone(),
        "save" => cfg.save_lines.join(" "),
        "hz" => cfg.hz.to_string(),
        "tcp-keepalive" => cfg.tcp_keepalive.to_string(),
        "slowlog-log-slower-than" => cfg.slowlog.slower_than_us().to_string(),
        "latency-monitor-threshold" => cfg.latency.threshold_ms().to_string(),
        "protected-mode" => if cfg.protected_mode { "yes" } else { "no" }.into(),
        "unixsocket" => cfg.unixsocket.clone().unwrap_or_default(),
        "unixsocketperm" => cfg.unixsocketperm.clone(),
        "io-threads" => cfg.io_threads.to_string(),
        "cluster-enabled" => "no".into(),
        "replica-read-only" => {
            if ctx.repl.replica_read_only() {
                "yes".into()
            } else {
                "no".into()
            }
        }
        "tls-port" => cfg.tls_port.to_string(),
        "tls-cert-file" => cfg
            .tls_cert_file
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default(),
        "tls-key-file" => cfg
            .tls_key_file
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default(),
        "tls-ca-cert-file" => cfg
            .tls_ca_cert_file
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default(),
        "tls-auth-clients" => cfg.tls_auth_clients.clone(),
        _ => return None,
    })
}

const CONFIG_KEYS: &[&str] = &[
    "notify-keyspace-events",
    "port",
    "bind",
    "databases",
    "dir",
    "dbfilename",
    "appendonly",
    "appendfilename",
    "appendfsync",
    "aof-use-rdb-preamble",
    "requirepass",
    "masterauth",
    "timeout",
    "maxmemory",
    "maxmemory-soft",
    "compress-threshold",
    "activedefrag",
    "storage-mode",
    "aof-group-commit-ms",
    "cluster-mode",
    "slowlog-max-len",
    "maxclients",
    "maxmemory-policy",
    "save",
    "hz",
    "tcp-keepalive",
    "slowlog-log-slower-than",
    "latency-monitor-threshold",
    "protected-mode",
    "unixsocket",
    "unixsocketperm",
    "io-threads",
    "cluster-enabled",
    "replica-read-only",
    "tls-port",
    "tls-cert-file",
    "tls-key-file",
    "tls-ca-cert-file",
    "tls-auth-clients",
];

pub(crate) fn cmd_config(ctx: &CommandContext, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("config");
    }
    match upper(&args[1]).as_str() {
        "GET" => {
            if args.len() < 3 {
                return wrong_arity("config|get");
            }
            let mut items = Vec::new();
            for pat in &args[2..] {
                let pat = String::from_utf8_lossy(pat).to_ascii_lowercase();
                for key in CONFIG_KEYS {
                    if crate::store::match_glob(&pat, key) {
                        if let Some(v) = config_value(ctx, key) {
                            items.push(text(*key));
                            items.push(text(v));
                        }
                    }
                }
            }
            RespValue::array(items)
        }
        "SET" => {
            if args.len() < 4 || args.len() % 2 != 0 {
                return wrong_arity("config|set");
            }
            for pair in args[2..].chunks(2) {
                let name = String::from_utf8_lossy(&pair[0]).to_ascii_lowercase();
                if config_value(ctx, &name).is_none() {
                    return err(&format!(
                        "ERR Unknown option or number of arguments for CONFIG SET - '{name}'"
                    ));
                }
                if let Err(e) = apply_config_set(ctx, &name, &pair[1]) {
                    return err(&e);
                }
            }
            RespValue::ok()
        }
        "RESETSTAT" => {
            ctx.config.read().stats.reset();
            RespValue::ok()
        }
        "REWRITE" => {
            let cf = ConfFile {
                path: ctx.config.read().conf_path.clone(),
                lines: vec![],
            };
            match cf.rewrite(&ctx.config.read()) {
                Ok(()) => RespValue::ok(),
                Err(e) => err(&format!("ERR CONFIG REWRITE failed: {e}")),
            }
        }
        _ => err("ERR unknown subcommand or wrong number of arguments for 'CONFIG'"),
    }
}

fn apply_config_set(ctx: &CommandContext, name: &str, value: &[u8]) -> Result<(), String> {
    let val = String::from_utf8_lossy(value);
    let mut sync_pass: Option<Option<String>> = None;
    {
        let mut cfg = ctx.config.write();
        match name {
            "notify-keyspace-events" => {
                *cfg.notify_keyspace_events.lock() = val.into_owned();
            }
            "requirepass" => {
                let v = val.into_owned();
                if v.is_empty() {
                    cfg.requirepass = None;
                    sync_pass = Some(None);
                } else {
                    cfg.requirepass = Some(v.clone());
                    sync_pass = Some(Some(v));
                }
            }
            "appendonly" => {
                cfg.persist.appendonly =
                    matches!(val.to_ascii_lowercase().as_str(), "yes" | "1" | "true");
            }
            "appendfsync" => {
                cfg.persist.appendfsync = val.clone().into_owned();
                ctx.aof.set_fsync_policy(val.as_ref());
            }
            "aof-use-rdb-preamble" => {
                cfg.persist.aof_use_rdb_preamble =
                    matches!(val.to_ascii_lowercase().as_str(), "yes" | "1" | "true");
            }
            "dir" => cfg.persist.dir = val.into_owned().into(),
            "dbfilename" => cfg.persist.dbfilename = val.into_owned(),
            "appendfilename" => cfg.persist.appendfilename = val.into_owned(),
            "protected-mode" => {
                cfg.protected_mode =
                    matches!(val.to_ascii_lowercase().as_str(), "yes" | "1" | "true");
            }
            "maxmemory-policy" => {
                if crate::eviction::MaxMemoryPolicy::parse(val.as_ref()).is_none() {
                    return Err("ERR Invalid argument 'maxmemory-policy'".into());
                }
                cfg.maxmemory_policy = val.into_owned();
            }
            "activedefrag" => {
                cfg.activedefrag =
                    matches!(val.to_ascii_lowercase().as_str(), "yes" | "1" | "true");
            }
            "storage-mode" => {
                let mode = crate::storage::StorageMode::parse(val.as_ref());
                cfg.storage_mode = mode.as_str().into();
                ctx.storage.set_mode(mode);
            }
            "cluster-mode" => {
                let c = crate::raft::ClusterConsistency::parse(val.as_ref());
                cfg.cluster_consistency = c.as_str().into();
                ctx.raft.set_consistency(c);
            }
            "compress-threshold" | "maxmemory-soft" | "aof-group-commit-ms" => {
                if let Ok(n) = val.parse::<u64>() {
                    match name {
                        "compress-threshold" => {
                            cfg.compress_threshold = n;
                            ctx.storage
                                .compress_threshold
                                .store(n, std::sync::atomic::Ordering::Relaxed);
                        }
                        "maxmemory-soft" => cfg.maxmemory_soft = n,
                        "aof-group-commit-ms" => {
                            cfg.aof_group_commit_ms = n;
                            ctx.aof.set_group_commit_ms(n);
                        }
                        _ => {}
                    }
                }
            }
            "unixsocket" => {
                cfg.unixsocket = if val.is_empty() {
                    None
                } else {
                    Some(val.into_owned())
                };
            }
            "unixsocketperm" => cfg.unixsocketperm = val.into_owned(),
            "io-threads" => {
                if let Ok(n) = val.parse::<u64>() {
                    cfg.io_threads = n.max(1);
                    ctx.tracking.io_threads_hint.store(
                        cfg.io_threads.clamp(1, 255) as u8,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    if let Some(pool) = crate::io_threads::global() {
                        pool.configure(cfg.io_threads as usize);
                    }
                }
            }
            "tls-port" => {
                if let Ok(n) = val.parse() {
                    cfg.tls_port = n;
                }
            }
            "tls-cert-file" => {
                cfg.tls_cert_file = if val.is_empty() {
                    None
                } else {
                    Some(val.into_owned().into())
                };
            }
            "tls-key-file" => {
                cfg.tls_key_file = if val.is_empty() {
                    None
                } else {
                    Some(val.into_owned().into())
                };
            }
            "tls-ca-cert-file" => {
                cfg.tls_ca_cert_file = if val.is_empty() {
                    None
                } else {
                    Some(val.into_owned().into())
                };
            }
            "tls-auth-clients" => cfg.tls_auth_clients = val.into_owned(),
            "masterauth" => {
                let v = val.into_owned();
                if v.is_empty() {
                    ctx.repl.set_masterauth(None);
                } else {
                    ctx.repl.set_masterauth(Some(v));
                }
            }
            "replica-read-only" => {
                ctx.repl.set_replica_read_only(matches!(
                    val.to_ascii_lowercase().as_str(),
                    "yes" | "1" | "true"
                ));
            }
            "slowlog-max-len" => {
                if let Ok(n) = val.parse() {
                    cfg.slowlog.set_max_len(n);
                }
            }
            "slowlog-log-slower-than" => {
                if let Ok(n) = val.parse::<i64>() {
                    let us = if n < 0 { u64::MAX } else { n as u64 };
                    cfg.slowlog.set_slower_than_us(us);
                }
            }
            "latency-monitor-threshold" => {
                if let Ok(n) = val.parse() {
                    cfg.latency.set_threshold_ms(n);
                }
            }
            "maxclients" => {
                if let Ok(n) = val.parse() {
                    cfg.maxclients = n;
                }
            }
            "timeout" | "tcp-keepalive" | "hz" | "maxmemory" | "port" => {
                if let Ok(n) = val.parse() {
                    match name {
                        "timeout" => cfg.timeout = n,
                        "tcp-keepalive" => cfg.tcp_keepalive = n,
                        "hz" => cfg.hz = n,
                        "maxmemory" => cfg.maxmemory = n,
                        "port" => cfg.port = n as u16,
                        _ => {}
                    }
                }
            }
            "bind" => {
                cfg.bind = val.split_whitespace().map(String::from).collect();
            }
            _ => {}
        }
    } // drop cfg write lock
    if let Some(p) = sync_pass {
        ctx.acl.sync_requirepass(p.as_deref());
    }
    Ok(())
}

// ───────────────────────────── divers ─────────────────────────────

/// COMMAND [COUNT|DOCS|INFO|LIST|GETKEYS|GETKEYSANDFLAGS] — source `command_registry`.
pub(crate) fn cmd_command(args: &[Bytes]) -> RespValue {
    match args.get(1).map(|a| upper(a)).as_deref() {
        Some("COUNT") => RespValue::Integer(command_registry::command_count() as i64),
        Some("LIST") => RespValue::array(
            command_registry::supported_command_names()
                .iter()
                .map(|c| text(c.to_ascii_lowercase()))
                .collect(),
        ),
        Some("INFO") => {
            let names: Vec<String> = if args.len() <= 2 {
                command_registry::supported_command_names()
                    .iter()
                    .map(|c| c.to_string())
                    .collect()
            } else {
                args[2..].iter().map(|b| upper(b)).collect()
            };
            let mut out = Vec::new();
            for n in names {
                if let Some(spec) = command_registry::spec_by_name(&n) {
                    out.push(command_info_array(spec));
                }
            }
            RespValue::array(out)
        }
        Some("DOCS") => {
            let mut out = Vec::new();
            for spec in command_registry::COMMAND_SPECS {
                out.push(RespValue::array(vec![
                    text(spec.name.to_ascii_lowercase()),
                    RespValue::array(vec![RespValue::array(vec![
                        text("summary"),
                        text(format!("Redis-compatible {} command (Ferredis)", spec.name)),
                        text("since"),
                        text("7.2.0"),
                        text("group"),
                        text("server"),
                    ])]),
                ]));
            }
            RespValue::array(out)
        }
        Some("GETKEYS") | Some("GETKEYSANDFLAGS") => cmd_command_getkeys(args),
        _ => RespValue::array(vec![]),
    }
}

fn cmd_command_getkeys(args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("command");
    }
    let cmd = upper(&args[2]);
    let numkeys = match cmd.as_str() {
        "MGET" | "DEL" | "UNLINK" | "EXISTS" | "TOUCH" => args.len().saturating_sub(3),
        "MSET" | "MSETNX" => (args.len().saturating_sub(3)) / 2,
        "GET" | "SET" | "HGET" | "HSET" => 1,
        _ => 0,
    };
    if numkeys == 0 {
        return RespValue::array(vec![]);
    }
    let keys: Vec<RespValue> = args[3..3 + numkeys]
        .iter()
        .map(|b| RespValue::bulk(b.clone()))
        .collect();
    if upper(&args[1]).as_str() == "GETKEYSANDFLAGS" {
        return RespValue::array(
            keys.into_iter()
                .map(|k| RespValue::array(vec![k, RespValue::array(vec![])]))
                .collect(),
        );
    }
    RespValue::array(keys)
}

fn slowlog_entry_resp(e: &SlowLogEntry) -> RespValue {
    let mut argv: Vec<RespValue> = e.args.iter().map(|a| RespValue::bulk(a.clone())).collect();
    RespValue::array(vec![
        RespValue::Integer(e.id as i64),
        RespValue::Integer(e.unix_time),
        RespValue::Integer(e.duration_us as i64),
        RespValue::array(argv.drain(..).collect()),
        text(if e.client_addr.is_empty() {
            "0.0.0.0:0".into()
        } else {
            e.client_addr.clone()
        }),
        text(e.client_name.clone()),
    ])
}

/// SLOWLOG GET|LEN|RESET
pub(crate) fn cmd_slowlog(ctx: &CommandContext, args: &[Bytes]) -> RespValue {
    match args.get(1).map(|a| upper(a)).as_deref() {
        Some("GET") => {
            let n = args
                .get(2)
                .and_then(|b| parse_i64(b))
                .filter(|v| *v >= 0)
                .map_or(10, |v| v as usize);
            RespValue::array(
                ctx.config
                    .read()
                    .slowlog
                    .get(n)
                    .into_iter()
                    .map(|e| slowlog_entry_resp(&e))
                    .collect(),
            )
        }
        Some("LEN") => RespValue::Integer(ctx.config.read().slowlog.len() as i64),
        Some("RESET") => {
            ctx.config.read().slowlog.reset();
            RespValue::ok()
        }
        _ => err("ERR unknown subcommand or wrong number of arguments for 'SLOWLOG'"),
    }
}

/// LATENCY LATEST | HISTORY | RESET | GRAPH | DOCTOR | HISTOGRAM | HELP.
pub(crate) fn cmd_latency(ctx: &CommandContext, _: &mut ClientState, args: &[Bytes]) -> RespValue {
    let sub = upper(&args[1]);
    let arity: i64 = match sub.as_str() {
        "LATEST" | "DOCTOR" | "HELP" => 2,
        "HISTORY" | "GRAPH" => 3,
        "RESET" | "HISTOGRAM" => -2,
        _ => {
            return err(&format!(
                "ERR unknown subcommand '{}'. Try LATENCY HELP.",
                String::from_utf8_lossy(&args[1])
            ))
        }
    };
    let n = args.len() as i64;
    if (arity > 0 && n != arity) || (arity < 0 && n < -arity) {
        return wrong_arity(&format!("latency|{}", sub.to_ascii_lowercase()));
    }
    let names = || {
        args[2..]
            .iter()
            .map(|a| String::from_utf8_lossy(a).into_owned())
            .collect::<Vec<_>>()
    };
    let int = |v: u64| RespValue::Integer(v as i64);
    let cfg = ctx.config.read();
    let lat = &cfg.latency;
    match sub.as_str() {
        "LATEST" => RespValue::array(
            lat.latest()
                .into_iter()
                .map(|(ev, ts, last, max)| RespValue::array(vec![text(ev), int(ts), int(last), int(max)]))
                .collect(),
        ),
        "HISTORY" => RespValue::array(
            lat.history_for(&String::from_utf8_lossy(&args[2]))
                .into_iter()
                .map(|(ts, ms)| RespValue::array(vec![int(ts), int(ms)]))
                .collect(),
        ),
        "RESET" => int(lat.reset(&names()) as u64),
        "DOCTOR" => text(doctor_report(lat)),
        "GRAPH" => {
            let ev = String::from_utf8_lossy(&args[2]);
            match lat.graph(&ev) {
                Some(g) => text(g),
                None => err(&format!("ERR No samples available for event '{ev}'")),
            }
        }
        "HISTOGRAM" => {
            let wanted: Vec<String> = names().into_iter().map(|n| n.to_ascii_lowercase()).collect();
            let mut out = Vec::new();
            for (name, calls, points) in lat.histograms(&wanted) {
                out.push(text(name));
                out.push(RespValue::array(vec![
                    text("calls"),
                    int(calls),
                    text("histogram_usec"),
                    RespValue::array(points.into_iter().flat_map(|(us, c)| [int(us), int(c)]).collect()),
                ]));
            }
            RespValue::array(out)
        }
        _ => RespValue::array(
            [
                "LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "DOCTOR",
                "    Return a human readable latency analysis report.",
                "GRAPH <event>",
                "    Return an ASCII latency graph for the <event> class.",
                "HISTORY <event>",
                "    Return time-latency samples for the <event> class.",
                "LATEST",
                "    Return the latest latency samples for all events.",
                "RESET [<event> ...]",
                "    Reset latency data of one or more <event> classes.",
                "    (default: reset all data for all event classes)",
                "HISTOGRAM [COMMAND ...]",
                "    Return a cumulative distribution of latencies in the format of a histogram for the specified command names.",
                "    If no commands are specified then all histograms are replied.",
                "HELP",
                "    Print this help.",
            ]
            .into_iter()
            .map(|l| RespValue::simple(Bytes::from_static(l.as_bytes())))
            .collect(),
        ),
    }
}

pub(crate) fn cmd_monitor(ctx: &CommandContext, state: &mut ClientState) -> RespValue {
    if state.monitor_rx.is_some() {
        return err("ERR MONITOR already active");
    }
    let (id, rx) = ctx.config.read().monitor.subscribe();
    state.monitor_rx = Some(rx);
    let _ = id;
    RespValue::ok()
}

pub(crate) fn cmd_shutdown(ctx: &CommandContext, args: &[Bytes]) -> RespValue {
    let mut req = ShutdownRequest::default();
    for a in args.iter().skip(1) {
        match upper(a).as_str() {
            "NOSAVE" => req.nosave = true,
            "SAVE" => req.save = true,
            "NOW" => req.now = true,
            "FORCE" => req.force = true,
            "ABORT" => req.abort = true,
            _ => return err("ERR syntax error"),
        }
    }
    if req.abort {
        ctx.config.read().control.request_shutdown(req);
        return err("ERR Shutdown aborted");
    }
    // Redis : SAVE forcé / défaut = dump ; NOSAVE = skip. On flush ici pour que
    // le client reçoive SHUTDOWN après la persistance (comme redis-server).
    if req.should_persist() {
        let path = {
            let cfg = ctx.config.read();
            cfg.persist.dir.join(&cfg.persist.dbfilename)
        };
        match save_rdb(&path, &ctx.db) {
            Ok(()) => mark_last_save(&ctx.config.read()),
            Err(e) => tracing::warn!("SHUTDOWN SAVE failed: {e}"),
        }
        // AOF : fsync final si actif.
        if ctx.aof.enabled() {
            let _ = ctx.aof.fsync_now();
        }
    }
    ctx.config.read().control.request_shutdown(req);
    RespValue::simple(Bytes::from_static(b"SHUTDOWN"))
}

pub(crate) fn cmd_lolwut(args: &[Bytes]) -> RespValue {
    let ver = if args.len() >= 3 && upper(&args[1]) == "VERSION" {
        String::from_utf8_lossy(&args[2]).into_owned()
    } else {
        "6".into()
    };
    text(format!(
        " Ferredis LOLWUT v{ver} — parité Redis 8 en cours (voir PLAN.md)\n  ◢◣\n ◢◣◢◣\n"
    ))
}

pub(crate) fn mark_last_save(cfg: &crate::command::ServerConfig) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    cfg.last_save.store(t, Ordering::Relaxed);
}

/// MEMORY USAGE|STATS|DOCTOR|PURGE|MALLOC-STATS|HELP.
pub(crate) fn cmd_memory(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    match args.get(1).map(|a| upper(a)).as_deref() {
        Some("USAGE") if args.len() >= 3 => {
            let mut samples = 5usize;
            let mut i = 3;
            while i + 1 < args.len() {
                if upper(&args[i]) == "SAMPLES" {
                    samples = parse_i64(&args[i + 1]).unwrap_or(5).max(0) as usize;
                    i += 2;
                } else {
                    return err(ERR_SYNTAX);
                }
            }
            let _ = samples;
            let size = ctx.db.with_db(state.db_index, |d| {
                d.get_fresh(&args[2])
                    .map(|e| args[2].len() + e.value.approx_bytes() + 48)
            });
            size.map_or_else(RespValue::null_bulk, |n| RespValue::Integer(n as i64))
        }
        Some("STATS") => {
            let used = ctx.db.approx_memory();
            let (keys, expires) = (0..ctx.db.db_count()).fold((0usize, 0usize), |(k, e), db| {
                let (a, b) = ctx.db.key_stats(db);
                (k + a, e + b)
            });
            RespValue::array(vec![
                text("peak.allocated"),
                RespValue::Integer(used as i64),
                text("total.allocated"),
                RespValue::Integer(used as i64),
                text("startup.allocated"),
                RespValue::Integer(1024 * 1024),
                text("replication.backlog"),
                RespValue::Integer(0),
                text("clients.slaves"),
                RespValue::Integer(0),
                text("clients.normal"),
                RespValue::Integer(ctx.clients.connected() as i64),
                text("aof.buffer"),
                RespValue::Integer(0),
                text("lua.caches"),
                RespValue::Integer(0),
                text("db.bytes"),
                RespValue::Integer(used as i64),
                text("db.keys"),
                RespValue::Integer(keys as i64),
                text("db.expires"),
                RespValue::Integer(expires as i64),
                text("overhead.total"),
                RespValue::Integer((keys * 64) as i64),
                text("keys.count"),
                RespValue::Integer(keys as i64),
                text("keys.bytes-per-key"),
                RespValue::Integer(if keys == 0 { 0 } else { (used / keys) as i64 }),
                text("dataset.bytes"),
                RespValue::Integer(used as i64),
                text("dataset.percentage"),
                text("100.00"),
                text("peak.percentage"),
                text("100.00"),
                text("allocator.allocated"),
                RespValue::Integer(used as i64),
                text("allocator.active"),
                RespValue::Integer(used as i64),
                text("allocator.resident"),
                RespValue::Integer(used as i64),
                text("allocator-fragmentation.ratio"),
                text(format!("{:.2}", ctx.storage.mem.fragmentation_ratio())),
            ])
        }
        Some("DOCTOR") => {
            let used = ctx.db.approx_memory();
            ctx.storage.mem.set_used(used as u64);
            let (keys, expires) = (0..ctx.db.db_count()).fold((0usize, 0usize), |(k, e), db| {
                let (a, b) = ctx.db.key_stats(db);
                (k + a, e + b)
            });
            let mut tips = Vec::new();
            if keys > 0 && expires * 100 / keys > 50 {
                tips.push("Many keys have an expire set; consider volatile eviction policies.");
            }
            if used > 100 * 1024 * 1024 {
                tips.push("Dataset is large; monitor maxmemory and fragmentation.");
            }
            if ctx.storage.mem.fragmentation_ratio() > 1.5 {
                tips.push("High fragmentation ratio; enable activedefrag or run MEMORY PURGE.");
            }
            if tips.is_empty() {
                text("Sam, I detect no memory issues at this time.")
            } else {
                text(tips.join("\n"))
            }
        }
        Some("PURGE") => {
            ctx.defrag.running.store(1, Ordering::Relaxed);
            let moved = crate::mem::active_defrag_pass(&ctx.db, 256);
            ctx.defrag.keys_moved.fetch_add(moved as u64, Ordering::Relaxed);
            ctx.defrag.runs.fetch_add(1, Ordering::Relaxed);
            ctx.defrag.running.store(0, Ordering::Relaxed);
            ctx.storage.mem.record_defrag(moved as u64);
            RespValue::ok()
        }
        Some("MALLOC-STATS") => {
            let used = ctx.db.approx_memory();
            text(format!(
                "___ Begin jemalloc statistics ___\n\
                 allocated: {used}\n\
                 active: {used}\n\
                 resident: {used}\n\
                 ___ End jemalloc statistics ___"
            ))
        }
        Some("HELP") => RespValue::array(
            [
                "MEMORY DOCTOR",
                "MEMORY MALLOC-STATS",
                "MEMORY PURGE",
                "MEMORY STATS",
                "MEMORY USAGE key [SAMPLES count]",
                "MEMORY HELP",
            ]
            .iter()
            .map(|s| text((*s).to_string()))
            .collect(),
        ),
        _ => err("ERR unknown subcommand or wrong number of arguments for 'MEMORY'"),
    }
}

/// DEBUG HELP | OBJECT key | RELOAD — sous-ensemble Redis (admin).
///
/// Les sous-commandes destructives (`SEGFAULT`, `PANIC`, …) sont refusées
/// explicitement plutôt que d'être silencieusement absentes.
pub(crate) fn cmd_debug(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("debug");
    }
    match upper(&args[1]).as_str() {
        "HELP" => RespValue::array(
            [
                "DEBUG <subcommand> [<arg> ...]. Subcommands are:",
                "HELP",
                "    Shows this help.",
                "OBJECT <key>",
                "    Show low-level info about `key` (encoding, idle, freq).",
                "RELOAD",
                "    Reload the RDB file from disk (Ferredis).",
            ]
            .iter()
            .map(|s| RespValue::bulk(Bytes::from_static(s.as_bytes())))
            .collect(),
        ),
        "OBJECT" => {
            if args.len() != 3 {
                return wrong_arity("debug|object");
            }
            let key = &args[2];
            ctx.db.with_db(state.db_index, |d| {
                match d.get_fresh_notouch(key) {
                    None => err("ERR no such key"),
                    Some(e) => {
                        // Format proche de Redis DEBUG OBJECT (champs utiles, pas d'adresse C).
                        let s = format!(
                            "Value at:ferredis refcount:1 encoding:{} serializedlength:{} lru_seconds_idle:{} lfu_freq:{}",
                            e.object_encoding(),
                            e.value.approx_bytes(),
                            e.idle_secs(),
                            e.lfu_freq,
                        );
                        RespValue::bulk(Bytes::from(s))
                    }
                }
            })
        }
        "RELOAD" => {
            let path = {
                let cfg = ctx.config.read();
                cfg.persist.dir.join(&cfg.persist.dbfilename)
            };
            match crate::persist::load_rdb(&path, &ctx.db) {
                Ok(()) => RespValue::ok(),
                Err(e) => RespValue::err(format!("ERR {e}")),
            }
        }
        "SEGFAULT" | "PANIC" | "OOM" | "ASSERT" => {
            err("ERR DEBUG subcommand disabled for safety in Ferredis")
        }
        other => RespValue::err(format!("ERR Unknown DEBUG subcommand '{other}'")),
    }
}

/// FERREDIS EVENTS | VERSION | DATABASES — extensions propres à Ferredis.
///
/// `EVENTS` → `[temps_unix_ms, id_client, adresse, type, détail]` (récent → ancien).
/// `DATABASES` → bases non vides : `[db, N, keys, K, expires, E]` par entrée.
pub(crate) fn cmd_ferredis(ctx: &CommandContext, args: &[Bytes]) -> RespValue {
    match args.get(1).map(|a| upper(a)).as_deref() {
        Some("EVENTS") => {
            let limit = args
                .get(2)
                .and_then(|b| parse_i64(b))
                .filter(|n| *n > 0)
                .map_or(100, |n| n as usize);
            RespValue::array(
                ctx.clients
                    .recent_events(limit)
                    .into_iter()
                    .map(|e| {
                        RespValue::array(vec![
                            RespValue::Integer(e.at_unix_ms as i64),
                            RespValue::Integer(e.client_id as i64),
                            text(e.addr),
                            text(e.kind.as_str()),
                            text(e.detail),
                        ])
                    })
                    .collect(),
            )
        }
        Some("VERSION") => text(env!("CARGO_PKG_VERSION")),
        Some("DATABASES") | Some("DBS") => databases_reply(ctx),
        Some("HELP") => RespValue::array(
            [
                "FERREDIS EVENTS [count]",
                "FERREDIS VERSION",
                "FERREDIS DATABASES | DBS",
                "FERREDIS HELP",
            ]
            .iter()
            .map(|s| RespValue::bulk(Bytes::from_static(s.as_bytes())))
            .collect(),
        ),
        _ => err("ERR unknown subcommand. Try FERREDIS HELP | EVENTS | VERSION | DATABASES."),
    }
}

/// Liste les bases avec au moins une clé.
///
/// Format CLI clair (une entrée = une base) :
/// `[["db", 0, "keys", 39, "expires", 34], …]`
///
/// Aussi exposé en commande top-level `DATABASES` (raccourci CLI).
pub(crate) fn cmd_databases(
    ctx: &CommandContext,
    _state: &mut ClientState,
    _args: &[Bytes],
) -> RespValue {
    databases_reply(ctx)
}

fn databases_reply(ctx: &CommandContext) -> RespValue {
    let mut out = Vec::new();
    for db in 0..ctx.db.db_count() {
        let (keys, expires) = ctx.db.key_stats(db);
        if keys == 0 {
            continue;
        }
        out.push(RespValue::array(vec![
            text("db"),
            RespValue::Integer(db as i64),
            text("keys"),
            RespValue::Integer(keys as i64),
            text("expires"),
            RespValue::Integer(expires as i64),
        ]));
    }
    RespValue::array(out)
}

/// Enregistre un échec AUTH (appelé par `cmd_auth`).
pub(crate) fn record_auth_failure(ctx: &CommandContext, state: &ClientState) {
    ctx.clients.record_auth_failure(state.info.as_deref());
}
