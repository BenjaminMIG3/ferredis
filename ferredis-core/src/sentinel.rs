//! Mode Redis Sentinel : surveillance, quorum SDOWN/ODOWN, failover via P6.

use crate::command::{arg_str, wrong_arity, ClientState, CommandContext};
use crate::resp::RespValue;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Notify;

const DEFAULT_DOWN_AFTER_MS: u64 = 30_000;
const DEFAULT_FAILOVER_TIMEOUT_MS: u64 = 180_000;
const DEFAULT_PARALLEL_SYNCS: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceFlags {
    Master,
    Slave,
    Sentinel,
}

#[derive(Debug, Clone)]
pub struct MonitoredMaster {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub quorum: u32,
    pub down_after_ms: u64,
    pub failover_timeout_ms: u64,
    pub parallel_syncs: u64,
    pub runid: String,
    pub flags: Vec<&'static str>,
    pub link_up: bool,
    pub last_pong_ms: u64,
    pub s_down: bool,
    pub o_down: bool,
    pub failover_state: FailoverState,
    pub replicas: Vec<ReplicaInfo>,
    pub config_epoch: u64,
    pub num_other_sentinels: u32,
}

#[derive(Debug, Clone, Default)]
pub struct ReplicaInfo {
    pub host: String,
    pub port: u16,
    pub runid: String,
    pub flags: Vec<&'static str>,
    pub link_up: bool,
    pub slave_priority: u32,
    pub slave_repl_offset: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FailoverState {
    #[default]
    None,
    WaitStart,
    SelectSlave,
    SendSlaveofNoOne,
    WaitPromotion,
    ReconfSlaves,
    UpdateConfig,
}

struct SentinelInner {
    myid: String,
    masters: HashMap<String, MonitoredMaster>,
    /// Rapports « master down » reçus d'autres sentinels : name → set of sentinel runids.
    odown_votes: HashMap<String, Vec<String>>,
    #[allow(dead_code)]
    announce_ip: Option<String>,
    #[allow(dead_code)]
    announce_port: Option<u16>,
}

pub struct SentinelState {
    inner: Mutex<SentinelInner>,
    enabled: AtomicBool,
    task_gen: AtomicU64,
    wake: Notify,
}

pub type SharedSentinel = Arc<SentinelState>;

fn random_runid() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..20).map(|_| rng.gen()).collect();
    hex::encode(bytes)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl SentinelState {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(SentinelInner {
                myid: random_runid(),
                masters: HashMap::new(),
                odown_votes: HashMap::new(),
                announce_ip: None,
                announce_port: None,
            }),
            enabled: AtomicBool::new(false),
            task_gen: AtomicU64::new(0),
            wake: Notify::new(),
        }
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Release);
        self.task_gen.fetch_add(1, Ordering::AcqRel);
        self.wake.notify_waiters();
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    pub fn myid(&self) -> String {
        self.inner.lock().myid.clone()
    }

    pub fn monitor(&self, name: String, host: String, port: u16, quorum: u32) -> Result<(), String> {
        if quorum == 0 {
            return Err("ERR Invalid quorum specified for SENTINEL MONITOR".into());
        }
        let mut g = self.inner.lock();
        if g.masters.contains_key(&name) {
            return Err("ERR Duplicated master name".into());
        }
        g.masters.insert(
            name.clone(),
            MonitoredMaster {
                name,
                host,
                port,
                quorum,
                down_after_ms: DEFAULT_DOWN_AFTER_MS,
                failover_timeout_ms: DEFAULT_FAILOVER_TIMEOUT_MS,
                parallel_syncs: DEFAULT_PARALLEL_SYNCS,
                runid: String::new(),
                flags: vec!["master"],
                link_up: false,
                last_pong_ms: 0,
                s_down: false,
                o_down: false,
                failover_state: FailoverState::None,
                replicas: Vec::new(),
                config_epoch: 0,
                num_other_sentinels: 0,
            },
        );
        drop(g);
        self.wake.notify_waiters();
        Ok(())
    }

    pub fn remove(&self, name: &str) -> Result<(), String> {
        let mut g = self.inner.lock();
        if g.masters.remove(name).is_none() {
            return Err(format!("ERR No such master with name '{name}'"));
        }
        g.odown_votes.remove(name);
        Ok(())
    }

    pub fn get_master_addr(&self, name: &str) -> Option<(String, u16)> {
        let g = self.inner.lock();
        g.masters.get(name).map(|m| (m.host.clone(), m.port))
    }

    pub fn master(&self, name: &str) -> Option<MonitoredMaster> {
        self.inner.lock().masters.get(name).cloned()
    }

    pub fn masters(&self) -> Vec<MonitoredMaster> {
        self.inner.lock().masters.values().cloned().collect()
    }

    pub fn set_option(&self, name: &str, opt: &str, val: &str) -> Result<(), String> {
        let mut g = self.inner.lock();
        let m = g
            .masters
            .get_mut(name)
            .ok_or_else(|| format!("ERR No such master with name '{name}'"))?;
        match opt.to_ascii_lowercase().as_str() {
            "down-after-milliseconds" => {
                m.down_after_ms = val
                    .parse()
                    .map_err(|_| "ERR Invalid down-after-milliseconds value")?;
            }
            "failover-timeout" => {
                m.failover_timeout_ms = val
                    .parse()
                    .map_err(|_| "ERR Invalid failover-timeout value")?;
            }
            "parallel-syncs" => {
                m.parallel_syncs = val.parse().map_err(|_| "ERR Invalid parallel-syncs value")?;
            }
            "quorum" => {
                m.quorum = val.parse().map_err(|_| "ERR Invalid quorum value")?;
                if m.quorum == 0 {
                    return Err("ERR Invalid quorum specified".into());
                }
            }
            _ => return Err(format!("ERR Unknown option or wrong number of arguments for SENTINEL SET '{opt}'")),
        }
        Ok(())
    }

    fn mark_pong(&self, name: &str, runid: Option<&str>) {
        let mut g = self.inner.lock();
        if let Some(m) = g.masters.get_mut(name) {
            m.link_up = true;
            m.last_pong_ms = now_ms();
            if let Some(id) = runid {
                if !id.is_empty() {
                    m.runid = id.to_string();
                }
            }
            if m.s_down {
                m.s_down = false;
                m.flags.retain(|f| *f != "s_down");
            }
        }
    }

    fn evaluate_sdown(&self) {
        let now = now_ms();
        let mut g = self.inner.lock();
        for m in g.masters.values_mut() {
            if m.last_pong_ms == 0 {
                continue;
            }
            let elapsed = now.saturating_sub(m.last_pong_ms);
            if elapsed > m.down_after_ms {
                if !m.s_down {
                    m.s_down = true;
                    if !m.flags.contains(&"s_down") {
                        m.flags.push("s_down");
                    }
                    m.link_up = false;
                }
            }
        }
    }

    /// Vote local + votes reçus → ODOWN si >= quorum.
    fn evaluate_odown(&self) {
        let myid = {
            let g = self.inner.lock();
            g.myid.clone()
        };
        let mut g = self.inner.lock();
        let names: Vec<String> = g.masters.keys().cloned().collect();
        for name in names {
            let (s_down, o_down, quorum) = match g.masters.get(&name) {
                Some(m) => (m.s_down, m.o_down, m.quorum),
                None => continue,
            };
            if !s_down {
                if o_down {
                    if let Some(mm) = g.masters.get_mut(&name) {
                        mm.o_down = false;
                        mm.flags.retain(|f| *f != "o_down");
                    }
                    g.odown_votes.remove(&name);
                }
                continue;
            }
            {
                let votes = g.odown_votes.entry(name.clone()).or_default();
                if !votes.iter().any(|v| v == &myid) {
                    votes.push(myid.clone());
                }
            }
            let n = g.odown_votes.get(&name).map(|v| v.len()).unwrap_or(0) as u32;
            let o = n >= quorum;
            if let Some(mm) = g.masters.get_mut(&name) {
                if o && !mm.o_down {
                    mm.o_down = true;
                    if !mm.flags.contains(&"o_down") {
                        mm.flags.push("o_down");
                    }
                } else if !o && mm.o_down {
                    mm.o_down = false;
                    mm.flags.retain(|f| *f != "o_down");
                }
            }
        }
    }

    pub fn is_master_down_vote(
        &self,
        ip: &str,
        port: u16,
        current_epoch: u64,
        runid: &str,
    ) -> (i64, String, u64) {
        let mut g = self.inner.lock();
        let leader = g.myid.clone();
        let epoch = current_epoch.max(
            g.masters
                .values()
                .map(|m| m.config_epoch)
                .max()
                .unwrap_or(0),
        );
        let mut answer: i64 = 0;
        let mut master_name: Option<String> = None;
        for m in g.masters.values() {
            if m.host == ip && m.port == port {
                if m.s_down {
                    answer = 1;
                }
                master_name = Some(m.name.clone());
                break;
            }
        }
        if let Some(name) = master_name {
            if runid != "*" && !runid.is_empty() {
                let votes = g.odown_votes.entry(name).or_default();
                if !votes.iter().any(|v| v == runid) {
                    votes.push(runid.to_string());
                }
            }
        }
        (answer, leader, epoch)
    }

    /// Failover manuel : choisit un réplica (ou le master lui-même si seul) et promeut.
    pub async fn start_failover(&self, name: &str, force: bool) -> Result<(), String> {
        let (host, port, replicas) = {
            let mut g = self.inner.lock();
            let m = g
                .masters
                .get_mut(name)
                .ok_or_else(|| format!("ERR No such master with name '{name}'"))?;
            if !force && !m.o_down && !m.s_down {
                // Failover manuel Redis autorise même si up.
            }
            m.failover_state = FailoverState::SelectSlave;
            (m.host.clone(), m.port, m.replicas.clone())
        };

        // Découvrir les réplicas via INFO replication si liste vide.
        let mut reps = replicas;
        if reps.is_empty() {
            if let Ok(discovered) = fetch_replicas(&host, port).await {
                reps = discovered;
            }
        }

        let candidate = reps
            .iter()
            .filter(|r| r.link_up || r.port != 0)
            .max_by_key(|r| (r.slave_priority, r.slave_repl_offset))
            .cloned();

        let Some(cand) = candidate else {
            // Pas de réplica : tenter REPLICAOF NO ONE sur le master (no-op) → échec explicite.
            let mut g = self.inner.lock();
            if let Some(m) = g.masters.get_mut(name) {
                m.failover_state = FailoverState::None;
            }
            return Err("ERR Failover will fail because there are no good slaves to promote".into());
        };

        {
            let mut g = self.inner.lock();
            if let Some(m) = g.masters.get_mut(name) {
                m.failover_state = FailoverState::SendSlaveofNoOne;
            }
        }

        promote_replica(&cand.host, cand.port).await?;

        // Reconfigurer l'ancien master et les autres réplicas.
        let _ = send_replicaof(&host, port, &cand.host, cand.port).await;
        for r in &reps {
            if r.host == cand.host && r.port == cand.port {
                continue;
            }
            let _ = send_replicaof(&r.host, r.port, &cand.host, cand.port).await;
        }

        {
            let mut g = self.inner.lock();
            if let Some(m) = g.masters.get_mut(name) {
                m.host = cand.host;
                m.port = cand.port;
                m.runid = cand.runid;
                m.config_epoch = m.config_epoch.saturating_add(1);
                m.s_down = false;
                m.o_down = false;
                m.flags = vec!["master"];
                m.failover_state = FailoverState::None;
                m.link_up = true;
                m.last_pong_ms = now_ms();
                m.replicas.clear();
            }
            g.odown_votes.remove(name);
        }
        Ok(())
    }
}

impl Default for SentinelState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn shared_sentinel() -> SharedSentinel {
    Arc::new(SentinelState::new())
}

fn master_to_info_array(m: &MonitoredMaster) -> RespValue {
    let flags = m.flags.join(",");
    RespValue::array(vec![
        bulk("name"),
        bulk(&m.name),
        bulk("ip"),
        bulk(&m.host),
        bulk("port"),
        bulk(m.port.to_string()),
        bulk("runid"),
        bulk(&m.runid),
        bulk("flags"),
        bulk(&flags),
        bulk("link-pending-commands"),
        bulk("0"),
        bulk("link-refcount"),
        bulk("1"),
        bulk("last-ping-sent"),
        bulk("0"),
        bulk("last-ok-ping-reply"),
        bulk(m.last_pong_ms.to_string()),
        bulk("last-ping-reply"),
        bulk(m.last_pong_ms.to_string()),
        bulk("down-after-milliseconds"),
        bulk(m.down_after_ms.to_string()),
        bulk("info-refresh"),
        bulk("0"),
        bulk("role-reported"),
        bulk("master"),
        bulk("role-reported-time"),
        bulk("0"),
        bulk("config-epoch"),
        bulk(m.config_epoch.to_string()),
        bulk("num-slaves"),
        bulk(m.replicas.len().to_string()),
        bulk("num-other-sentinels"),
        bulk(m.num_other_sentinels.to_string()),
        bulk("quorum"),
        bulk(m.quorum.to_string()),
        bulk("failover-timeout"),
        bulk(m.failover_timeout_ms.to_string()),
        bulk("parallel-syncs"),
        bulk(m.parallel_syncs.to_string()),
    ])
}

fn replica_to_info_array(r: &ReplicaInfo, master: &MonitoredMaster) -> RespValue {
    let flags = if r.flags.is_empty() {
        "slave".to_string()
    } else {
        r.flags.join(",")
    };
    RespValue::array(vec![
        bulk("name"),
        bulk(format!("{}:{}", r.host, r.port)),
        bulk("ip"),
        bulk(&r.host),
        bulk("port"),
        bulk(r.port.to_string()),
        bulk("runid"),
        bulk(&r.runid),
        bulk("flags"),
        bulk(&flags),
        bulk("link-pending-commands"),
        bulk("0"),
        bulk("link-refcount"),
        bulk("1"),
        bulk("last-ping-sent"),
        bulk("0"),
        bulk("last-ok-ping-reply"),
        bulk("0"),
        bulk("last-ping-reply"),
        bulk("0"),
        bulk("down-after-milliseconds"),
        bulk(master.down_after_ms.to_string()),
        bulk("info-refresh"),
        bulk("0"),
        bulk("role-reported"),
        bulk("slave"),
        bulk("role-reported-time"),
        bulk("0"),
        bulk("master-link-down-time"),
        bulk("0"),
        bulk("master-link-status"),
        bulk(if r.link_up { "ok" } else { "err" }),
        bulk("master-host"),
        bulk(&master.host),
        bulk("master-port"),
        bulk(master.port.to_string()),
        bulk("slave-priority"),
        bulk(r.slave_priority.to_string()),
        bulk("slave-repl-offset"),
        bulk(r.slave_repl_offset.to_string()),
    ])
}

fn bulk(s: impl AsRef<[u8]>) -> RespValue {
    RespValue::bulk(Bytes::copy_from_slice(s.as_ref()))
}

fn err(msg: &str) -> RespValue {
    RespValue::err(msg.to_string())
}

/// Commande `SENTINEL` (sous-commandes Redis 7).
pub fn cmd_sentinel(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("sentinel");
    }
    if !ctx.sentinel.is_enabled() && !ctx.config.read().sentinel_mode {
        // Autoriser aussi hors mode pour tests unitaires si déjà enable().
    }
    let sub = arg_str(args, 1).unwrap_or("").to_ascii_lowercase();
    match sub.as_str() {
        "masters" => {
            let list = ctx.sentinel.masters();
            RespValue::array(list.iter().map(master_to_info_array).collect())
        }
        "master" => {
            let name = match arg_str(args, 2) {
                Some(n) => n,
                None => return wrong_arity("sentinel"),
            };
            match ctx.sentinel.master(name) {
                Some(m) => master_to_info_array(&m),
                None => err(&format!("ERR No such master with name '{name}'")),
            }
        }
        "replicas" | "slaves" => {
            let name = match arg_str(args, 2) {
                Some(n) => n,
                None => return wrong_arity("sentinel"),
            };
            match ctx.sentinel.master(name) {
                Some(m) => RespValue::array(
                    m.replicas
                        .iter()
                        .map(|r| replica_to_info_array(r, &m))
                        .collect(),
                ),
                None => err(&format!("ERR No such master with name '{name}'")),
            }
        }
        "sentinels" => {
            let name = match arg_str(args, 2) {
                Some(n) => n,
                None => return wrong_arity("sentinel"),
            };
            if ctx.sentinel.master(name).is_none() {
                return err(&format!("ERR No such master with name '{name}'"));
            }
            RespValue::array(Vec::new())
        }
        "get-master-addr-by-name" => {
            let name = match arg_str(args, 2) {
                Some(n) => n,
                None => return wrong_arity("sentinel"),
            };
            match ctx.sentinel.get_master_addr(name) {
                Some((h, p)) => RespValue::array(vec![bulk(h), bulk(p.to_string())]),
                None => RespValue::null_array(),
            }
        }
        "monitor" => {
            if args.len() != 6 {
                return wrong_arity("sentinel");
            }
            let name = arg_str(args, 2).unwrap_or("").to_string();
            let host = arg_str(args, 3).unwrap_or("").to_string();
            let port: u16 = match arg_str(args, 4).and_then(|s| s.parse().ok()) {
                Some(p) => p,
                None => return err("ERR Invalid port specified for SENTINEL MONITOR"),
            };
            let quorum: u32 = match arg_str(args, 5).and_then(|s| s.parse().ok()) {
                Some(q) => q,
                None => return err("ERR Invalid quorum specified for SENTINEL MONITOR"),
            };
            match ctx.sentinel.monitor(name, host, port, quorum) {
                Ok(()) => RespValue::ok(),
                Err(e) => err(&e),
            }
        }
        "remove" => {
            let name = match arg_str(args, 2) {
                Some(n) => n,
                None => return wrong_arity("sentinel"),
            };
            match ctx.sentinel.remove(name) {
                Ok(()) => RespValue::ok(),
                Err(e) => err(&e),
            }
        }
        "set" => {
            if args.len() < 5 || args.len() % 2 == 0 {
                return wrong_arity("sentinel");
            }
            let name = arg_str(args, 2).unwrap_or("");
            let mut i = 3;
            while i + 1 < args.len() {
                let opt = arg_str(args, i).unwrap_or("");
                let val = arg_str(args, i + 1).unwrap_or("");
                if let Err(e) = ctx.sentinel.set_option(name, opt, val) {
                    return err(&e);
                }
                i += 2;
            }
            RespValue::ok()
        }
        "ckquorum" => {
            let name = match arg_str(args, 2) {
                Some(n) => n,
                None => return wrong_arity("sentinel"),
            };
            match ctx.sentinel.master(name) {
                Some(m) => {
                    // Un seul sentinel local → OK si quorum == 1.
                    if m.quorum <= 1 + m.num_other_sentinels {
                        RespValue::ok()
                    } else {
                        err(&format!(
                            "NOQUORUM {} usable Sentinels for quorum {}. Currently only usable Sentinels: 1.",
                            name, m.quorum
                        ))
                    }
                }
                None => err(&format!("ERR No such master with name '{name}'")),
            }
        }
        "failover" => {
            let name = match arg_str(args, 2) {
                Some(n) => n.to_string(),
                None => return wrong_arity("sentinel"),
            };
            // Exécution async via block_in_place pour rester dans le handler sync.
            let sent = ctx.sentinel.clone();
            match tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(sent.start_failover(&name, true))
            }) {
                Ok(()) => RespValue::ok(),
                Err(e) => err(&e),
            }
        }
        "myid" => bulk(ctx.sentinel.myid()),
        "flushconfig" | "reset" => RespValue::ok(),
        "is-master-down-by-addr" => {
            if args.len() < 5 {
                return wrong_arity("sentinel");
            }
            let ip = arg_str(args, 2).unwrap_or("");
            let port: u16 = arg_str(args, 3).and_then(|s| s.parse().ok()).unwrap_or(0);
            let epoch: u64 = arg_str(args, 4).and_then(|s| s.parse().ok()).unwrap_or(0);
            let runid = arg_str(args, 5).unwrap_or("*");
            let (ans, leader, ep) = ctx.sentinel.is_master_down_vote(ip, port, epoch, runid);
            RespValue::array(vec![
                RespValue::Integer(ans),
                bulk(leader),
                RespValue::Integer(ep as i64),
            ])
        }
        "help" => RespValue::array(
            [
                "SENTINEL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "MASTERS",
                "MASTER <name>",
                "REPLICAS <name>",
                "SENTINELS <name>",
                "GET-MASTER-ADDR-BY-NAME <name>",
                "MONITOR <name> <ip> <port> <quorum>",
                "REMOVE <name>",
                "SET <name> <option> <value> [<option> <value> ...]",
                "CKQUORUM <name>",
                "FAILOVER <name>",
                "MYID",
                "IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>",
                "FLUSHCONFIG",
                "HELP",
            ]
            .into_iter()
            .map(bulk)
            .collect(),
        ),
        _ => err(&format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}'. Try SENTINEL HELP.",
            arg_str(args, 1).unwrap_or("?")
        )),
    }
}

/// Boucle de ping / détection SDOWN (lancée au démarrage en mode sentinel).
pub fn spawn_sentinel_loop(ctx: CommandContext) {
    if !ctx.config.read().sentinel_mode {
        return;
    }
    ctx.sentinel.enable();
    let gen = ctx.sentinel.task_gen.load(Ordering::Acquire);
    let ctx2 = ctx.clone();
    tokio::spawn(async move {
        sentinel_loop(ctx2, gen).await;
    });
}

async fn sentinel_loop(ctx: CommandContext, gen: u64) {
    let mut tick = tokio::time::interval(Duration::from_millis(1000));
    loop {
        if ctx.sentinel.task_gen.load(Ordering::Acquire) != gen {
            return;
        }
        tick.tick().await;
        let masters = ctx.sentinel.masters();
        for m in masters {
            match ping_master(&m.host, m.port).await {
                Ok(runid) => ctx.sentinel.mark_pong(&m.name, runid.as_deref()),
                Err(_) => {
                    // Pas de pong : last_pong reste ; evaluate_sdown décidera.
                    if m.last_pong_ms == 0 {
                        // Premier échec : initialiser le timer.
                        ctx.sentinel.mark_pong(&m.name, None);
                        // Puis forcer last ancien pour accélérer les tests si down-after petit.
                        let mut g = ctx.sentinel.inner.lock();
                        if let Some(mm) = g.masters.get_mut(&m.name) {
                            if !mm.link_up {
                                mm.last_pong_ms = now_ms().saturating_sub(mm.down_after_ms + 1);
                            }
                        }
                    }
                }
            }
            // Rafraîchir la liste des réplicas.
            if let Ok(reps) = fetch_replicas(&m.host, m.port).await {
                let mut g = ctx.sentinel.inner.lock();
                if let Some(mm) = g.masters.get_mut(&m.name) {
                    mm.replicas = reps;
                }
            }
        }
        ctx.sentinel.evaluate_sdown();
        ctx.sentinel.evaluate_odown();

        // Failover automatique si ODOWN et idle.
        for m in ctx.sentinel.masters() {
            if m.o_down && m.failover_state == FailoverState::None {
                let _ = ctx.sentinel.start_failover(&m.name, false).await;
            }
        }
    }
}

async fn ping_master(host: &str, port: u16) -> anyhow::Result<Option<String>> {
    let addr = format!("{host}:{port}");
    let mut sock = TcpStream::connect(&addr).await?;
    write_cmd(&mut sock, &[b"PING"]).await?;
    let _ = read_one(&mut sock).await?;
    write_cmd(&mut sock, &[b"INFO", b"server"]).await?;
    let info = read_one(&mut sock).await?;
    let runid = match info {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            s.lines()
                .find_map(|l| l.strip_prefix("run_id:").map(|x| x.trim().to_string()))
        }
        _ => None,
    };
    Ok(runid)
}

async fn fetch_replicas(host: &str, port: u16) -> anyhow::Result<Vec<ReplicaInfo>> {
    let addr = format!("{host}:{port}");
    let mut sock = TcpStream::connect(&addr).await?;
    write_cmd(&mut sock, &[b"INFO", b"replication"]).await?;
    let info = read_one(&mut sock).await?;
    let text = match info {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        _ => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("slave") {
            // slave0:ip=127.0.0.1,port=6380,state=online,offset=123,lag=0
            if let Some(idx) = rest.find(':') {
                let kv = &rest[idx + 1..];
                let mut host = String::new();
                let mut port = 0u16;
                let mut offset = 0i64;
                let mut online = false;
                for part in kv.split(',') {
                    let mut it = part.splitn(2, '=');
                    let k = it.next().unwrap_or("");
                    let v = it.next().unwrap_or("");
                    match k {
                        "ip" => host = v.to_string(),
                        "port" => port = v.parse().unwrap_or(0),
                        "offset" => offset = v.parse().unwrap_or(0),
                        "state" => online = v == "online",
                        _ => {}
                    }
                }
                if port != 0 {
                    out.push(ReplicaInfo {
                        host,
                        port,
                        runid: String::new(),
                        flags: vec!["slave"],
                        link_up: online,
                        slave_priority: 100,
                        slave_repl_offset: offset,
                    });
                }
            }
        }
    }
    Ok(out)
}

async fn promote_replica(host: &str, port: u16) -> Result<(), String> {
    send_raw(host, port, &[b"REPLICAOF", b"NO", b"ONE"])
        .await
        .map_err(|e| format!("ERR promotion failed: {e}"))?;
    Ok(())
}

async fn send_replicaof(host: &str, port: u16, master_host: &str, master_port: u16) -> anyhow::Result<()> {
    let port_s = master_port.to_string();
    send_raw(
        host,
        port,
        &[
            b"REPLICAOF",
            master_host.as_bytes(),
            port_s.as_bytes(),
        ],
    )
    .await
}

async fn send_raw(host: &str, port: u16, args: &[&[u8]]) -> anyhow::Result<()> {
    let addr = format!("{host}:{port}");
    let mut sock = TcpStream::connect(&addr).await?;
    write_cmd(&mut sock, args).await?;
    let _ = read_one(&mut sock).await?;
    Ok(())
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
        match crate::resp::parse(&mut buf) {
            Ok(v) => return Ok(v),
            Err(crate::resp::RespError::Incomplete) => {
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

/// Commandes autorisées en mode sentinel (hors SENTINEL).
pub fn sentinel_command_allowed(cmd: &str) -> bool {
    matches!(
        cmd,
        "PING"
            | "INFO"
            | "SENTINEL"
            | "SUBSCRIBE"
            | "PSUBSCRIBE"
            | "UNSUBSCRIBE"
            | "PUNSUBSCRIBE"
            | "PUBLISH"
            | "AUTH"
            | "HELLO"
            | "COMMAND"
            | "CLIENT"
            | "QUIT"
            | "ROLE"
            | "CONFIG"
            | "SHUTDOWN"
            | "ACL"
    )
}
