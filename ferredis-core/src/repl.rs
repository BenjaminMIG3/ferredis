//! Réplication Redis 7.2 : master/replica, PSYNC, backlog, WAIT, ROLE, FAILOVER.

use crate::command::{ClientState, CommandContext};
use crate::rdb::{decode_rdb, encode_rdb_bytes};
use crate::resp::{parse, RespError, RespValue};
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Notify};

const DEFAULT_BACKLOG: usize = 1 << 20; // 1 MiB
const REPLID_LEN: usize = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplRole {
    Master,
    Replica,
}

struct Backlog {
    /// Octets du flux (ring).
    buf: VecDeque<u8>,
    cap: usize,
    /// Offset du premier octet encore présent.
    start_offset: i64,
}

impl Backlog {
    fn new(cap: usize) -> Self {
        Self {
            buf: VecDeque::with_capacity(cap.min(64 * 1024)),
            cap: cap.max(1024),
            start_offset: 0,
        }
    }

    fn append(&mut self, data: &[u8]) {
        if data.len() >= self.cap {
            self.buf.clear();
            self.buf.extend(data[data.len() - self.cap..].iter().copied());
            return;
        }
        while self.buf.len() + data.len() > self.cap {
            self.buf.pop_front();
            self.start_offset += 1;
        }
        self.buf.extend(data.iter().copied());
    }

    fn oldest(&self) -> i64 {
        self.start_offset
    }

    fn newest(&self) -> i64 {
        self.start_offset + self.buf.len() as i64
    }

    /// Tranche `[from, end)` relative aux offsets de réplication.
    fn since(&self, from: i64) -> Option<Vec<u8>> {
        if from < self.start_offset || from > self.newest() {
            return None;
        }
        let skip = (from - self.start_offset) as usize;
        Some(self.buf.iter().skip(skip).copied().collect())
    }
}

struct ReplicaLink {
    tx: mpsc::UnboundedSender<Bytes>,
    ack_offset: AtomicI64,
    addr: String,
    port: AtomicU64,
    capa_eof: AtomicBool,
    capa_psync2: AtomicBool,
}

/// État de réplication partagé (master + côté réplica).
pub struct ReplicationState {
    role: Mutex<ReplRole>,
    master_replid: Mutex<String>,
    master_replid2: Mutex<String>,
    second_repl_offset: AtomicI64,
    master_repl_offset: AtomicI64,
    backlog: Mutex<Backlog>,
    replicas: DashMap<u64, Arc<ReplicaLink>>,
    wait_notify: Notify,
    /// Côté réplica : hôte/port du master (`None` = master autonome).
    master_host: Mutex<Option<(String, u16)>>,
    master_link_up: AtomicBool,
    replica_offset: AtomicI64,
    replica_read_only: AtomicBool,
    masterauth: Mutex<Option<String>>,
    /// Génération de tâche REPLICAOF (annule l'ancienne).
    replica_task_gen: AtomicU64,
    /// FAILOVER en cours (deadline unix-ms, 0 = idle).
    failover_deadline_ms: AtomicU64,
    failover_abort: AtomicBool,
}

pub type SharedRepl = Arc<ReplicationState>;

fn random_replid() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..20).map(|_| rng.gen()).collect();
    hex::encode(bytes)
}

impl ReplicationState {
    pub fn new() -> Self {
        Self {
            role: Mutex::new(ReplRole::Master),
            master_replid: Mutex::new(random_replid()),
            master_replid2: Mutex::new("0".repeat(REPLID_LEN)),
            second_repl_offset: AtomicI64::new(-1),
            master_repl_offset: AtomicI64::new(0),
            backlog: Mutex::new(Backlog::new(DEFAULT_BACKLOG)),
            replicas: DashMap::new(),
            wait_notify: Notify::new(),
            master_host: Mutex::new(None),
            master_link_up: AtomicBool::new(false),
            replica_offset: AtomicI64::new(0),
            replica_read_only: AtomicBool::new(true),
            masterauth: Mutex::new(None),
            replica_task_gen: AtomicU64::new(0),
            failover_deadline_ms: AtomicU64::new(0),
            failover_abort: AtomicBool::new(false),
        }
    }

    pub fn role(&self) -> ReplRole {
        *self.role.lock()
    }

    pub fn is_replica(&self) -> bool {
        matches!(self.role(), ReplRole::Replica)
    }

    pub fn is_master(&self) -> bool {
        matches!(self.role(), ReplRole::Master)
    }

    pub fn master_replid(&self) -> String {
        self.master_replid.lock().clone()
    }

    pub fn master_offset(&self) -> i64 {
        self.master_repl_offset.load(Ordering::Acquire)
    }

    pub fn replica_offset(&self) -> i64 {
        self.replica_offset.load(Ordering::Acquire)
    }

    pub fn replica_read_only(&self) -> bool {
        self.replica_read_only.load(Ordering::Relaxed)
    }

    pub fn set_replica_read_only(&self, v: bool) {
        self.replica_read_only.store(v, Ordering::Relaxed);
    }

    pub fn set_masterauth(&self, v: Option<String>) {
        *self.masterauth.lock() = v;
    }

    pub fn masterauth(&self) -> Option<String> {
        self.masterauth.lock().clone()
    }

    pub fn connected_replicas(&self) -> usize {
        self.replicas.len()
    }

    pub fn has_replica_client(&self, client_id: u64) -> bool {
        self.replicas.contains_key(&client_id)
    }

    pub fn master_host(&self) -> Option<(String, u16)> {
        self.master_host.lock().clone()
    }

    pub fn master_link_up(&self) -> bool {
        self.master_link_up.load(Ordering::Relaxed)
    }

    /// Encode RESP array et propage vers backlog + réplicas (master uniquement).
    pub fn feed_command(&self, args: &[Bytes]) {
        if self.is_replica() {
            return;
        }
        let encoded = encode_resp_command(args);
        let len = encoded.len() as i64;
        {
            let mut bl = self.backlog.lock();
            bl.append(&encoded);
        }
        self.master_repl_offset.fetch_add(len, Ordering::AcqRel);
        let bytes = Bytes::from(encoded);
        self.replicas.retain(|_, link| link.tx.send(bytes.clone()).is_ok());
        self.wait_notify.notify_waiters();
    }

    pub fn remove_replica(&self, client_id: u64) {
        self.replicas.remove(&client_id);
        self.wait_notify.notify_waiters();
    }

    pub fn update_ack(&self, client_id: u64, offset: i64) {
        if let Some(link) = self.replicas.get(&client_id) {
            link.ack_offset.store(offset, Ordering::Release);
            self.wait_notify.notify_waiters();
        }
    }

    pub fn set_replica_port(&self, client_id: u64, port: u64) {
        if let Some(link) = self.replicas.get(&client_id) {
            link.port.store(port, Ordering::Relaxed);
        }
    }

    pub fn set_capa(&self, client_id: u64, capa: &str) {
        if let Some(link) = self.replicas.get(&client_id) {
            match capa.to_ascii_lowercase().as_str() {
                "eof" => link.capa_eof.store(true, Ordering::Relaxed),
                "psync2" => link.capa_psync2.store(true, Ordering::Relaxed),
                _ => {}
            }
        }
    }

    /// Nombre de réplicas ayant ack >= `offset`.
    pub fn replicas_acked(&self, offset: i64) -> i64 {
        self.replicas
            .iter()
            .filter(|e| e.value().ack_offset.load(Ordering::Acquire) >= offset)
            .count() as i64
    }

    /// Attend jusqu'à `numreplicas` acks pour `offset`, ou timeout (ms). Retourne le compte.
    /// Ne bloque pas le runtime Tokio (`block_in_place` + parking_lot Notify).
    pub fn wait_replicas(&self, numreplicas: i64, offset: i64, timeout_ms: u64) -> i64 {
        if numreplicas <= 0 {
            return self.replicas_acked(offset);
        }
        if timeout_ms == 0 {
            return self.replicas_acked(offset);
        }
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            let n = self.replicas_acked(offset);
            if n >= numreplicas {
                return n;
            }
            if Instant::now() >= deadline {
                return n;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait = remaining.min(Duration::from_millis(50));
            // Libère le worker Tokio pendant l'attente (les ACK arrivent sur d'autres tâches).
            tokio::task::block_in_place(|| {
                let _ = self.wait_notify;
                std::thread::sleep(wait);
            });
        }
    }

    fn become_master_locked(&self) {
        *self.role.lock() = ReplRole::Master;
        *self.master_host.lock() = None;
        self.master_link_up.store(false, Ordering::Relaxed);
        // Nouveau cycle de réplication : ancien id → replid2.
        let old = self.master_replid.lock().clone();
        let off = self.master_repl_offset.load(Ordering::Acquire);
        *self.master_replid2.lock() = old;
        self.second_repl_offset.store(off + 1, Ordering::Release);
        *self.master_replid.lock() = random_replid();
        self.replica_task_gen.fetch_add(1, Ordering::AcqRel);
        self.failover_deadline_ms.store(0, Ordering::Relaxed);
        self.failover_abort.store(false, Ordering::Relaxed);
    }

    pub fn promote_to_master(&self) {
        self.become_master_locked();
    }

    pub fn become_replica(&self, host: String, port: u16) {
        *self.role.lock() = ReplRole::Replica;
        *self.master_host.lock() = Some((host, port));
        self.master_link_up.store(false, Ordering::Relaxed);
        // Déconnecter les réplicas éventuels.
        self.replicas.clear();
        self.replica_task_gen.fetch_add(1, Ordering::AcqRel);
    }

    pub fn info_section(&self) -> String {
        match self.role() {
            ReplRole::Master => {
                // Un seul lock backlog : `format!` garde les temporaires jusqu'à la fin
                // → plusieurs `backlog.lock()` = deadlock (Mutex non-réentrant).
                let (bl_cap, bl_oldest, bl_len) = {
                    let bl = self.backlog.lock();
                    (bl.cap, bl.oldest(), bl.buf.len())
                };
                let replid2 = self.master_replid2.lock().clone();
                let mut s = format!(
                    "role:master\r\nconnected_slaves:{}\r\nmaster_failover_state:no-failover\r\nmaster_replid:{}\r\nmaster_replid2:{}\r\nmaster_repl_offset:{}\r\nsecond_repl_offset:{}\r\nrepl_backlog_active:1\r\nrepl_backlog_size:{bl_cap}\r\nrepl_backlog_first_byte_offset:{bl_oldest}\r\nrepl_backlog_histlen:{bl_len}\r\n",
                    self.replicas.len(),
                    self.master_replid(),
                    replid2,
                    self.master_offset(),
                    self.second_repl_offset.load(Ordering::Relaxed),
                );
                for (i, entry) in self.replicas.iter().enumerate() {
                    let link = entry.value();
                    let port = link.port.load(Ordering::Relaxed);
                    let ack = link.ack_offset.load(Ordering::Relaxed);
                    let lag = (self.master_offset() - ack).max(0);
                    s.push_str(&format!(
                        "slave{i}:ip={},port={port},state=online,offset={ack},lag={lag}\r\n",
                        link.addr.split(':').next().unwrap_or(&link.addr),
                    ));
                }
                s
            }
            ReplRole::Replica => {
                let (host, port) = self
                    .master_host()
                    .unwrap_or_else(|| ("?".into(), 0));
                let link = if self.master_link_up() {
                    "up"
                } else {
                    "down"
                };
                format!(
                    "role:slave\r\nmaster_host:{host}\r\nmaster_port:{port}\r\nmaster_link_status:{link}\r\nmaster_last_io_seconds_ago:-1\r\nmaster_sync_in_progress:0\r\nslave_read_repl_offset:{}\r\nslave_repl_offset:{}\r\nslave_priority:100\r\nslave_read_only:{}\r\nconnected_slaves:0\r\nmaster_failover_state:no-failover\r\nmaster_replid:{}\r\nmaster_replid2:{}\r\nmaster_repl_offset:{}\r\n",
                    self.replica_offset(),
                    self.replica_offset(),
                    if self.replica_read_only() { "1" } else { "0" },
                    self.master_replid(),
                    self.master_replid2.lock(),
                    self.master_offset(),
                )
            }
        }
    }
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn shared_repl() -> SharedRepl {
    Arc::new(ReplicationState::new())
}

/// Encode une commande en tableau RESP2 (format AOF / flux réplica).
pub fn encode_resp_command(args: &[Bytes]) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(64 + args.iter().map(|a| a.len() + 16).sum::<usize>());
    buf.put_u8(b'*');
    buf.extend_from_slice(args.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    for a in args {
        buf.put_u8(b'$');
        buf.extend_from_slice(a.len().to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
    buf.to_vec()
}

fn text(s: impl Into<String>) -> RespValue {
    RespValue::bulk(Bytes::from(s.into()))
}

fn err(msg: &str) -> RespValue {
    RespValue::err(msg)
}

fn upper(b: &Bytes) -> String {
    String::from_utf8_lossy(b).to_ascii_uppercase()
}

fn arg_str(args: &[Bytes], i: usize) -> Option<&str> {
    args.get(i).and_then(|b| std::str::from_utf8(b).ok())
}

fn parse_i64(b: &Bytes) -> Option<i64> {
    std::str::from_utf8(b).ok().and_then(|s| s.parse().ok())
}

// ───────────────────────────── Commandes ─────────────────────────────

pub fn cmd_role(ctx: &CommandContext, _state: &mut ClientState, _args: &[Bytes]) -> RespValue {
    match ctx.repl.role() {
        ReplRole::Master => {
            let offset = ctx.repl.master_offset();
            let mut slaves = Vec::new();
            for entry in ctx.repl.replicas.iter() {
                let link = entry.value();
                let ip = link.addr.split(':').next().unwrap_or("?").to_string();
                let port = link.port.load(Ordering::Relaxed);
                let ack = link.ack_offset.load(Ordering::Relaxed);
                slaves.push(RespValue::array(vec![
                    text(ip),
                    text(port.to_string()),
                    text(ack.to_string()),
                ]));
            }
            RespValue::array(vec![
                text("master"),
                RespValue::Integer(offset),
                RespValue::array(slaves),
            ])
        }
        ReplRole::Replica => {
            let (host, port) = ctx
                .repl
                .master_host()
                .unwrap_or_else(|| ("?".into(), 0));
            let state = if ctx.repl.master_link_up() {
                "connected"
            } else {
                "connect"
            };
            RespValue::array(vec![
                text("slave"),
                text(host),
                RespValue::Integer(port as i64),
                text(state),
                RespValue::Integer(ctx.repl.replica_offset()),
            ])
        }
    }
}

pub fn cmd_wait(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return crate::command::wrong_arity("wait");
    }
    let numreplicas = match parse_i64(&args[1]) {
        Some(n) if n >= 0 => n,
        _ => return err("ERR numreplicas must be >= 0"),
    };
    let timeout = match parse_i64(&args[2]) {
        Some(n) if n >= 0 => n as u64,
        _ => return err("ERR timeout is not an integer or out of range"),
    };
    // Demander un GETACK aux réplicas.
    let getack = Bytes::from(encode_resp_command(&[
        Bytes::from_static(b"REPLCONF"),
        Bytes::from_static(b"GETACK"),
        Bytes::from_static(b"*"),
    ]));
    for entry in ctx.repl.replicas.iter() {
        let _ = entry.value().tx.send(getack.clone());
    }
    let offset = ctx.repl.master_offset();
    let n = ctx.repl.wait_replicas(numreplicas, offset, timeout);
    RespValue::Integer(n)
}

pub fn cmd_readonly(_ctx: &CommandContext, state: &mut ClientState, _args: &[Bytes]) -> RespValue {
    state.readonly = true;
    RespValue::ok()
}

pub fn cmd_readwrite(_ctx: &CommandContext, state: &mut ClientState, _args: &[Bytes]) -> RespValue {
    state.readonly = false;
    RespValue::ok()
}

pub fn cmd_replicaof(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return crate::command::wrong_arity("replicaof");
    }
    let host = match arg_str(args, 1) {
        Some(h) => h,
        None => return err("ERR syntax error"),
    };
    if host.eq_ignore_ascii_case("NO") {
        let port = arg_str(args, 2).unwrap_or("");
        if !port.eq_ignore_ascii_case("ONE") {
            return err("ERR syntax error");
        }
        ctx.repl.promote_to_master();
        return RespValue::ok();
    }
    let port: u16 = match arg_str(args, 2).and_then(|s| s.parse().ok()) {
        Some(p) => p,
        None => return err("ERR port should be a number"),
    };
    // Ne pas se répliquer soi-même.
    let self_port = ctx.config.read().port;
    if (host == "127.0.0.1" || host == "localhost" || host == "::1") && port == self_port {
        return err("ERR can't replicate itself");
    }
    ctx.repl.become_replica(host.to_string(), port);
    spawn_replica_client(ctx.clone(), state.info.as_ref().map(|i| i.id));
    RespValue::ok()
}

pub fn cmd_slaveof(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    cmd_replicaof(ctx, state, args)
}

pub fn cmd_replconf_multi(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
) -> Vec<RespValue> {
    if args.len() < 2 {
        return vec![crate::command::wrong_arity("replconf")];
    }
    let sub = upper(&args[1]);
    match sub.as_str() {
        "LISTENING-PORT" => {
            if args.len() < 3 {
                return vec![err("ERR wrong number of arguments for 'replconf' command")];
            }
            let port = parse_i64(&args[2]).unwrap_or(0).max(0) as u64;
            state.repl_listening_port = port as u16;
            if let Some(info) = &state.info {
                ctx.repl.set_replica_port(info.id, port);
            }
            vec![RespValue::ok()]
        }
        "IP-ADDRESS" => vec![RespValue::ok()],
        "CAPA" => {
            for c in args.iter().skip(2) {
                let capa = String::from_utf8_lossy(c);
                if let Some(info) = &state.info {
                    ctx.repl.set_capa(info.id, &capa);
                }
                state.repl_capa.push(capa.to_ascii_lowercase());
            }
            vec![RespValue::ok()]
        }
        "ACK" => {
            if args.len() < 3 {
                return vec![err("ERR wrong number of arguments for 'replconf' command")];
            }
            let off = parse_i64(&args[2]).unwrap_or(0);
            if let Some(info) = &state.info {
                ctx.repl.update_ack(info.id, off);
            }
            // Redis n'envoie pas de réponse à ACK en mode réplica.
            if state.is_replica_conn {
                Vec::new()
            } else {
                vec![RespValue::ok()]
            }
        }
        "GETACK" => {
            let off = if ctx.repl.is_replica() {
                ctx.repl.replica_offset()
            } else {
                ctx.repl.master_offset()
            };
            vec![RespValue::array(vec![
                text("REPLCONF"),
                text("ACK"),
                text(off.to_string()),
            ])]
        }
        "WORKDIR" | "RDB-CHANNEL" | "RDB-ONLY" => vec![RespValue::ok()],
        _ => vec![err(&format!(
            "ERR Unknown REPLCONF subcommand '{}'",
            String::from_utf8_lossy(&args[1])
        ))],
    }
}

/// PSYNC / SYNC → réponses initiales ; active `state.replica_feed`.
pub fn cmd_psync(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> Vec<RespValue> {
    if ctx.repl.is_replica() {
        return vec![err("ERR PSYNC from replica not supported as master")];
    }
    let (req_id, req_off) = if args.len() >= 3 {
        (
            String::from_utf8_lossy(&args[1]).into_owned(),
            parse_i64(&args[2]).unwrap_or(-1),
        )
    } else {
        ("?".into(), -1)
    };

    // Partial resync ?
    if req_id != "?" && req_off >= 0 {
        let id = ctx.repl.master_replid();
        let id2 = ctx.repl.master_replid2.lock().clone();
        let second_off = ctx.repl.second_repl_offset.load(Ordering::Relaxed);
        let id_ok = req_id == id || (req_id == id2 && req_off >= second_off && second_off >= 0);
        if id_ok {
            let chunk = {
                let bl = ctx.repl.backlog.lock();
                bl.since(req_off)
            };
            if let Some(data) = chunk {
                let (tx, rx) = mpsc::unbounded_channel();
                if !data.is_empty() {
                    let _ = tx.send(Bytes::from(data));
                }
                let addr = state
                    .info
                    .as_ref()
                    .map(|i| i.addr.clone())
                    .unwrap_or_default();
                let client_id = state.info.as_ref().map(|i| i.id).unwrap_or(0);
                let link = Arc::new(ReplicaLink {
                    tx,
                    ack_offset: AtomicI64::new(req_off),
                    addr,
                    port: AtomicU64::new(u64::from(state.repl_listening_port)),
                    capa_eof: AtomicBool::new(false),
                    capa_psync2: AtomicBool::new(true),
                });
                ctx.repl.replicas.insert(client_id, link);
                state.replica_feed = Some(rx);
                state.is_replica_conn = true;
                return vec![RespValue::simple(Bytes::from_static(b"CONTINUE"))];
            }
        }
    }

    full_resync(ctx, state)
}

pub fn cmd_sync(ctx: &CommandContext, state: &mut ClientState, _args: &[Bytes]) -> Vec<RespValue> {
    if ctx.repl.is_replica() {
        return vec![err("ERR SYNC from replica not supported as master")];
    }
    full_resync(ctx, state)
}

fn full_resync(ctx: &CommandContext, state: &mut ClientState) -> Vec<RespValue> {
    // Appelé sous `exec_lock` write (voir `run_locked` PSYNC/SYNC) — ne pas re-locker.
    let dump = ctx.db.dump_all();
    let fn_codes: Vec<Bytes> = ctx
        .functions
        .list_libs()
        .into_iter()
        .map(|lib| Bytes::from(lib.code))
        .collect();
    let rdb = match encode_rdb_bytes(&dump, &fn_codes) {
        Ok(b) => b,
        Err(e) => return vec![err(&format!("ERR RDB encode failed: {e}"))],
    };
    let offset = ctx.repl.master_offset();
    let replid = ctx.repl.master_replid();
    let (tx, rx) = mpsc::unbounded_channel();
    let addr = state
        .info
        .as_ref()
        .map(|i| i.addr.clone())
        .unwrap_or_default();
    let client_id = state.info.as_ref().map(|i| i.id).unwrap_or(0);
    // Bulk RDB Redis-repl : `$len\r\n` + octets, **sans** CRLF final (quirk Redis).
    let mut raw = Vec::with_capacity(rdb.len() + 32);
    raw.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
    raw.extend_from_slice(&rdb);
    let _ = tx.send(Bytes::from(raw));

    let link = Arc::new(ReplicaLink {
        tx,
        ack_offset: AtomicI64::new(offset),
        addr,
        port: AtomicU64::new(u64::from(state.repl_listening_port)),
        capa_eof: AtomicBool::new(false),
        capa_psync2: AtomicBool::new(true),
    });
    ctx.repl.replicas.insert(client_id, link);

    state.replica_feed = Some(rx);
    state.is_replica_conn = true;
    vec![RespValue::simple(Bytes::from(format!(
        "FULLRESYNC {replid} {offset}"
    )))]
}

pub fn cmd_failover(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let mut i = 1;
    let mut abort = false;
    let mut timeout_ms: u64 = 0;
    let mut to: Option<(String, u16)> = None;
    let mut force = false;
    while i < args.len() {
        let t = upper(&args[i]);
        match t.as_str() {
            "ABORT" => {
                abort = true;
                i += 1;
            }
            "FORCE" => {
                force = true;
                i += 1;
            }
            "TIMEOUT" => {
                if i + 1 >= args.len() {
                    return err("ERR syntax error");
                }
                timeout_ms = parse_i64(&args[i + 1]).unwrap_or(0).max(0) as u64;
                i += 2;
            }
            "TO" => {
                if i + 2 >= args.len() {
                    return err("ERR syntax error");
                }
                let host = String::from_utf8_lossy(&args[i + 1]).into_owned();
                let port: u16 = match parse_i64(&args[i + 2]) {
                    Some(p) if p > 0 && p <= 65535 => p as u16,
                    _ => return err("ERR Invalid port"),
                };
                to = Some((host, port));
                i += 3;
                if i < args.len() && upper(&args[i]) == "FORCE" {
                    force = true;
                    i += 1;
                }
            }
            _ => return err("ERR syntax error"),
        }
    }

    if abort {
        ctx.repl.failover_abort.store(true, Ordering::Relaxed);
        ctx.repl.failover_deadline_ms.store(0, Ordering::Relaxed);
        return RespValue::ok();
    }

    if ctx.repl.is_replica() {
        // Promotion locale après rattrapage optionnel.
        if timeout_ms > 0 {
            let deadline = Instant::now() + Duration::from_millis(timeout_ms);
            while Instant::now() < deadline {
                if ctx.repl.failover_abort.load(Ordering::Relaxed) {
                    return err("ERR FAILOVER aborted");
                }
                if ctx.repl.master_link_up() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        ctx.repl.promote_to_master();
        return RespValue::ok();
    }

    // Master : attendre que les réplicas rattrapent, puis OK (orchestration Sentinel → P7).
    let _ = (to, force);
    let target = ctx.repl.master_offset();
    if timeout_ms == 0 {
        timeout_ms = 300_000;
    }
    let deadline_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
        + timeout_ms;
    ctx.repl
        .failover_deadline_ms
        .store(deadline_ms, Ordering::Relaxed);
    ctx.repl.failover_abort.store(false, Ordering::Relaxed);

    let n = ctx.repl.wait_replicas(1, target, timeout_ms);
    if ctx.repl.failover_abort.load(Ordering::Relaxed) {
        return err("ERR FAILOVER aborted");
    }
    if n < 1 && ctx.repl.connected_replicas() > 0 && !force {
        return err("ERR FAILOVER timed out waiting for replicas");
    }
    ctx.repl.failover_deadline_ms.store(0, Ordering::Relaxed);
    RespValue::ok()
}

/// Refus d'écriture sur réplica read-only (hors flux master).
pub fn reject_replica_write(ctx: &CommandContext, state: &ClientState, cmd: &str) -> Option<RespValue> {
    if !ctx.repl.is_replica() || state.from_master {
        return None;
    }
    if !ctx.repl.replica_read_only() {
        return None;
    }
    let write = crate::command_registry::lookup(cmd).is_some_and(|s| s.has_flag("write"));
    if write {
        Some(err(
            "READONLY You can't write against a read only replica.",
        ))
    } else {
        None
    }
}

fn spawn_replica_client(ctx: CommandContext, _client_id: Option<u64>) {
    let gen = ctx.repl.replica_task_gen.load(Ordering::Acquire);
    let ctx2 = ctx.clone();
    tokio::spawn(async move {
        replica_client_loop(ctx2, gen).await;
    });
}

async fn replica_client_loop(ctx: CommandContext, gen: u64) {
    loop {
        if ctx.repl.replica_task_gen.load(Ordering::Acquire) != gen {
            return;
        }
        if !ctx.repl.is_replica() {
            return;
        }
        let Some((host, port)) = ctx.repl.master_host() else {
            return;
        };
        match run_replica_sync(&ctx, &host, port, gen).await {
            Ok(()) => {
                // Sync terminé proprement (master fermé) → reconnect.
                ctx.repl.master_link_up.store(false, Ordering::Relaxed);
            }
            Err(e) => {
                tracing::debug!("replica sync error: {e}");
                ctx.repl.master_link_up.store(false, Ordering::Relaxed);
            }
        }
        if ctx.repl.replica_task_gen.load(Ordering::Acquire) != gen {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn run_replica_sync(
    ctx: &CommandContext,
    host: &str,
    port: u16,
    gen: u64,
) -> anyhow::Result<()> {
    let addr = format!("{host}:{port}");
    tracing::info!(%addr, "replica connecting to master");
    let mut sock = TcpStream::connect(&addr).await?;
    if ctx.repl.replica_task_gen.load(Ordering::Acquire) != gen {
        return Ok(());
    }

    // AUTH si masterauth.
    if let Some(pass) = ctx.repl.masterauth() {
        write_cmd(&mut sock, &[b"AUTH", pass.as_bytes()]).await?;
        let auth = read_one(&mut sock).await?;
        if matches!(auth, RespValue::Error(_)) {
            anyhow::bail!("AUTH failed: {auth:?}");
        }
    }

    write_cmd(&mut sock, &[b"PING"]).await?;
    let pong = read_one(&mut sock).await?;
    if matches!(pong, RespValue::Error(_)) {
        anyhow::bail!("PING failed: {pong:?}");
    }

    // Annoncer le vrai port d'écoute (éphémère en tests).
    let listen_port = ctx
        .config
        .read()
        .listen_addr
        .lock()
        .rsplit(':')
        .next()
        .unwrap_or("0")
        .to_string();
    write_cmd(
        &mut sock,
        &[b"REPLCONF", b"listening-port", listen_port.as_bytes()],
    )
    .await?;
    let rc1 = read_one(&mut sock).await?;
    if matches!(rc1, RespValue::Error(_)) {
        anyhow::bail!("REPLCONF listening-port failed: {rc1:?}");
    }

    write_cmd(&mut sock, &[b"REPLCONF", b"capa", b"psync2"]).await?;
    let rc2 = read_one(&mut sock).await?;
    if matches!(rc2, RespValue::Error(_)) {
        anyhow::bail!("REPLCONF capa failed: {rc2:?}");
    }

    let psync_id = ctx.repl.master_replid();
    let psync_off = ctx.repl.replica_offset();
    let (id_arg, off_arg) = if psync_off > 0 && psync_id.len() == REPLID_LEN {
        (psync_id, psync_off.to_string())
    } else {
        ("?".into(), "-1".into())
    };
    write_cmd(
        &mut sock,
        &[b"PSYNC", id_arg.as_bytes(), off_arg.as_bytes()],
    )
    .await?;

    let reply = read_simple_or_err(&mut sock).await?;
    let reply = reply.trim_start_matches('+');
    tracing::info!(%reply, "PSYNC reply");
    if reply.starts_with("CONTINUE") {
        ctx.repl.master_link_up.store(true, Ordering::Relaxed);
        return apply_command_stream(ctx, &mut sock, gen).await;
    }
    if !reply.starts_with("FULLRESYNC") {
        anyhow::bail!("unexpected PSYNC reply: {reply}");
    }
    let parts: Vec<&str> = reply.split_whitespace().collect();
    if parts.len() >= 3 {
        *ctx.repl.master_replid.lock() = parts[1].to_string();
        if let Ok(off) = parts[2].parse::<i64>() {
            ctx.repl.replica_offset.store(off, Ordering::Release);
        }
    }

    // Lire le bulk RDB : $<len>\r\n + bytes (sans CRLF final Redis).
    let rdb = read_rdb_bulk(&mut sock).await?;
    tracing::info!(rdb_len = rdb.len(), "RDB received");
    {
        let snap = decode_rdb(&rdb).map_err(|e| anyhow::anyhow!("RDB decode: {e}"))?;
        let _guard = ctx.exec_lock.write();
        ctx.db.restore_all(snap.dbs);
        for code in &snap.function_codes {
            if let Ok(s) = std::str::from_utf8(code) {
                let _ = ctx.functions.load(s, true);
            }
        }
    }
    ctx.repl.master_link_up.store(true, Ordering::Relaxed);
    tracing::info!("FULLRESYNC applied, entering command stream");
    apply_command_stream(ctx, &mut sock, gen).await
}

async fn apply_command_stream(
    ctx: &CommandContext,
    sock: &mut TcpStream,
    gen: u64,
) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(4096);
    let mut tmp = [0u8; 8192];
    let mut state = ClientState {
        authenticated: true,
        from_master: true,
        ..Default::default()
    };
    loop {
        if ctx.repl.replica_task_gen.load(Ordering::Acquire) != gen || !ctx.repl.is_replica() {
            return Ok(());
        }
        // ACK périodique.
        let ack = encode_resp_command(&[
            Bytes::from_static(b"REPLCONF"),
            Bytes::from_static(b"ACK"),
            Bytes::from(ctx.repl.replica_offset().to_string()),
        ]);
        sock.write_all(&ack).await?;

        let read_fut = sock.read(&mut tmp);
        let n = tokio::time::timeout(Duration::from_secs(1), read_fut).await;
        match n {
            Ok(Ok(0)) => anyhow::bail!("master closed connection"),
            Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => continue, // timeout → nouvel ACK
        }

        while !buf.is_empty() {
            let before = buf.len();
            let args = match parse_command_array(&mut buf) {
                Ok(a) => a,
                Err(RespError::Incomplete) => break,
                Err(e) => anyhow::bail!("protocol: {e}"),
            };
            if args.is_empty() {
                continue;
            }
            let consumed = (before - buf.len()) as i64;
            let _ = crate::command::dispatch(ctx, &mut state, &args).await;
            ctx.repl
                .replica_offset
                .fetch_add(consumed, Ordering::AcqRel);
        }
    }
}

fn parse_command_array(buf: &mut BytesMut) -> Result<Vec<Bytes>, RespError> {
    crate::resp::parse_command(buf)
}

async fn write_cmd(sock: &mut TcpStream, args: &[&[u8]]) -> std::io::Result<()> {
    let mut out = BytesMut::new();
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    sock.write_all(&out).await
}

async fn read_one(sock: &mut TcpStream) -> anyhow::Result<RespValue> {
    let mut buf = BytesMut::new();
    let mut tmp = [0u8; 4096];
    loop {
        match parse(&mut buf) {
            Ok(v) => return Ok(v),
            Err(RespError::Incomplete) => {
                let n = sock.read(&mut tmp).await?;
                if n == 0 {
                    anyhow::bail!("connection closed");
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            Err(e) => anyhow::bail!("{e}"),
        }
    }
}

/// Lit une simple string brute (+...) ou erreur, avant le bulk RDB.
async fn read_simple_or_err(sock: &mut TcpStream) -> anyhow::Result<String> {
    let mut line = Vec::new();
    let mut b = [0u8; 1];
    loop {
        sock.read_exact(&mut b).await?;
        if b[0] == b'\n' {
            break;
        }
        if b[0] != b'\r' {
            line.push(b[0]);
        }
    }
    let s = String::from_utf8_lossy(&line).into_owned();
    if s.starts_with('-') {
        anyhow::bail!("{s}");
    }
    Ok(s)
}

async fn read_rdb_bulk(sock: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    // Attendre '$'
    let mut b = [0u8; 1];
    loop {
        sock.read_exact(&mut b).await?;
        if b[0] == b'$' {
            break;
        }
        if b[0] == b'\n' || b[0] == b'\r' {
            continue;
        }
        // Ignorer jusqu'à bulk (certains chemins).
        if b[0] == b'+' || b[0] == b'-' {
            let mut line = vec![b[0]];
            loop {
                sock.read_exact(&mut b).await?;
                if b[0] == b'\n' {
                    break;
                }
                if b[0] != b'\r' {
                    line.push(b[0]);
                }
            }
            continue;
        }
        anyhow::bail!("expected RDB bulk, got {}", b[0] as char);
    }
    let mut len_buf = Vec::new();
    loop {
        sock.read_exact(&mut b).await?;
        if b[0] == b'\n' {
            break;
        }
        if b[0] != b'\r' {
            len_buf.push(b[0]);
        }
    }
    let len: usize = std::str::from_utf8(&len_buf)?.parse()?;
    let mut data = vec![0u8; len];
    sock.read_exact(&mut data).await?;
    Ok(data)
}

/// Helpers exposés pour WAITAOF.
pub fn wait_replica_acks(ctx: &CommandContext, numreplicas: i64, timeout_ms: u64) -> i64 {
    if numreplicas <= 0 {
        return 0;
    }
    let getack = Bytes::from(encode_resp_command(&[
        Bytes::from_static(b"REPLCONF"),
        Bytes::from_static(b"GETACK"),
        Bytes::from_static(b"*"),
    ]));
    for entry in ctx.repl.replicas.iter() {
        let _ = entry.value().tx.send(getack.clone());
    }
    let offset = ctx.repl.master_offset();
    ctx.repl.wait_replicas(numreplicas, offset, timeout_ms)
}
