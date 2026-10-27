//! Mode Redis Cluster : slots 0–16383, gossip minimal, redirections -MOVED/-ASK.

use crate::command::{arg_str, wrong_arity, ClientState, CommandContext};
use crate::resp::RespValue;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

pub const CLUSTER_SLOTS: u16 = 16384;

#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub cport: u16,
    pub flags: HashSet<&'static str>,
    pub master_id: Option<String>,
    pub ping_sent: u64,
    pub pong_recv: u64,
    pub config_epoch: u64,
    pub link: &'static str,
    pub slots: Vec<(u16, u16)>,
}

struct ClusterInner {
    enabled: bool,
    myself_id: String,
    nodes: HashMap<String, ClusterNode>,
    /// slot → node id
    slots: Vec<Option<String>>,
    current_epoch: u64,
    migrating: HashMap<u16, String>, // slot → destination node id
    importing: HashSet<u16>,
}

pub struct ClusterState {
    inner: Mutex<ClusterInner>,
}

pub type SharedCluster = Arc<ClusterState>;

fn random_node_id() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..20).map(|_| rng.gen()).collect();
    hex::encode(bytes)
}

/// CRC16-CCITT XMODEM (Redis cluster key hash).
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= u16::from(b) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// Hash tag `{...}` Redis.
pub fn key_slot(key: &[u8]) -> u16 {
    let start = key.iter().position(|&c| c == b'{');
    let hash_key = if let Some(s) = start {
        if let Some(e) = key[s + 1..].iter().position(|&c| c == b'}') {
            let inner = &key[s + 1..s + 1 + e];
            if !inner.is_empty() {
                inner
            } else {
                key
            }
        } else {
            key
        }
    } else {
        key
    };
    crc16(hash_key) % CLUSTER_SLOTS
}

impl ClusterState {
    pub fn new() -> Self {
        let id = random_node_id();
        let mut nodes = HashMap::new();
        nodes.insert(
            id.clone(),
            ClusterNode {
                id: id.clone(),
                host: "127.0.0.1".into(),
                port: 6379,
                cport: 16379,
                flags: HashSet::from(["myself", "master"]),
                master_id: None,
                ping_sent: 0,
                pong_recv: 0,
                config_epoch: 0,
                link: "connected",
                slots: Vec::new(),
            },
        );
        Self {
            inner: Mutex::new(ClusterInner {
                enabled: false,
                myself_id: id,
                nodes,
                slots: vec![None; CLUSTER_SLOTS as usize],
                current_epoch: 0,
                migrating: HashMap::new(),
                importing: HashSet::new(),
            }),
        }
    }

    pub fn enable(&self, host: &str, port: u16) {
        let mut g = self.inner.lock();
        g.enabled = true;
        let me = g.myself_id.clone();
        if let Some(n) = g.nodes.get_mut(&me) {
            n.host = host.to_string();
            n.port = port;
            n.cport = port.saturating_add(10000);
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.lock().enabled
    }

    pub fn myself_id(&self) -> String {
        self.inner.lock().myself_id.clone()
    }

    pub fn slot_owner(&self, slot: u16) -> Option<String> {
        let g = self.inner.lock();
        g.slots.get(slot as usize).and_then(|o| o.clone())
    }

    pub fn is_slot_mine(&self, slot: u16) -> bool {
        let g = self.inner.lock();
        matches!(g.slots.get(slot as usize), Some(Some(id)) if *id == g.myself_id)
            || g.importing.contains(&slot)
    }

    pub fn is_migrating(&self, slot: u16) -> Option<String> {
        self.inner.lock().migrating.get(&slot).cloned()
    }

    pub fn add_slots(&self, slots: impl IntoIterator<Item = u16>) -> Result<(), String> {
        let mut g = self.inner.lock();
        let me = g.myself_id.clone();
        for s in slots {
            if s >= CLUSTER_SLOTS {
                return Err("ERR Invalid or out of range slot".into());
            }
            if let Some(Some(owner)) = g.slots.get(s as usize) {
                if owner != &me {
                    return Err(format!("ERR Slot {s} is already busy"));
                }
            }
            g.slots[s as usize] = Some(me.clone());
        }
        rebuild_slot_ranges(&mut g);
        Ok(())
    }

    pub fn del_slots(&self, slots: impl IntoIterator<Item = u16>) -> Result<(), String> {
        let mut g = self.inner.lock();
        let me = g.myself_id.clone();
        for s in slots {
            if s >= CLUSTER_SLOTS {
                return Err("ERR Invalid or out of range slot".into());
            }
            match g.slots.get(s as usize) {
                Some(Some(owner)) if owner == &me => g.slots[s as usize] = None,
                Some(Some(_)) => return Err(format!("ERR Slot {s} is already busy")),
                _ => {}
            }
            g.migrating.remove(&s);
            g.importing.remove(&s);
        }
        rebuild_slot_ranges(&mut g);
        Ok(())
    }

    pub fn meet(&self, host: String, port: u16) -> Result<(), String> {
        let mut g = self.inner.lock();
        let id = random_node_id();
        g.nodes.insert(
            id.clone(),
            ClusterNode {
                id,
                host,
                port,
                cport: port.saturating_add(10000),
                flags: HashSet::from(["master", "handshake"]),
                master_id: None,
                ping_sent: 0,
                pong_recv: 0,
                config_epoch: 0,
                link: "connected",
                slots: Vec::new(),
            },
        );
        g.current_epoch = g.current_epoch.saturating_add(1);
        Ok(())
    }

    pub fn nodes_line(&self) -> String {
        let g = self.inner.lock();
        let mut lines = Vec::new();
        for n in g.nodes.values() {
            let flags: String = {
                let mut f: Vec<&str> = n.flags.iter().copied().collect();
                f.sort_unstable();
                f.join(",")
            };
            let master = n.master_id.as_deref().unwrap_or("-");
            let slots = slot_ranges_str(&n.slots);
            lines.push(format!(
                "{} {}:{}@{} {} {} {} {} {} {}",
                n.id, n.host, n.port, n.cport, flags, master, n.ping_sent, n.pong_recv, n.config_epoch, n.link
            ) + &if slots.is_empty() {
                String::new()
            } else {
                format!(" {slots}")
            });
        }
        lines.join("\n") + "\n"
    }

    pub fn slots_reply(&self) -> RespValue {
        let g = self.inner.lock();
        // Grouper plages par owner.
        let mut by_owner: BTreeMap<String, Vec<(u16, u16)>> = BTreeMap::new();
        let mut start: Option<(u16, String)> = None;
        for s in 0..CLUSTER_SLOTS {
            let owner = g.slots[s as usize].clone();
            match (start.take(), owner) {
                (None, Some(id)) => start = Some((s, id)),
                (Some((st, id)), Some(oid)) if id == oid => start = Some((st, id)),
                (Some((st, id)), other) => {
                    by_owner.entry(id).or_default().push((st, s - 1));
                    start = other.map(|oid| (s, oid));
                }
                (None, None) => {}
            }
        }
        if let Some((st, id)) = start {
            by_owner
                .entry(id)
                .or_default()
                .push((st, CLUSTER_SLOTS - 1));
        }
        let mut out = Vec::new();
        for (id, ranges) in by_owner {
            let Some(node) = g.nodes.get(&id) else { continue };
            for (a, b) in ranges {
                out.push(RespValue::array(vec![
                    RespValue::Integer(i64::from(a)),
                    RespValue::Integer(i64::from(b)),
                    RespValue::array(vec![
                        bulk(&node.host),
                        RespValue::Integer(i64::from(node.port)),
                        bulk(&node.id),
                    ]),
                ]));
            }
        }
        RespValue::array(out)
    }

    pub fn setslot(
        &self,
        slot: u16,
        sub: &str,
        node_id: Option<&str>,
    ) -> Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err("ERR Invalid or out of range slot".into());
        }
        let mut g = self.inner.lock();
        let me = g.myself_id.clone();
        match sub.to_ascii_lowercase().as_str() {
            "migrating" => {
                let dest = node_id.ok_or("ERR syntax error")?;
                if !g.nodes.contains_key(dest) {
                    return Err("ERR Unknown node id".into());
                }
                g.migrating.insert(slot, dest.to_string());
            }
            "importing" => {
                let _src = node_id.ok_or("ERR syntax error")?;
                g.importing.insert(slot);
            }
            "stable" => {
                g.migrating.remove(&slot);
                g.importing.remove(&slot);
            }
            "node" => {
                let id = node_id.ok_or("ERR syntax error")?.to_string();
                if !g.nodes.contains_key(&id) && id != me {
                    return Err("ERR Unknown node id".into());
                }
                g.slots[slot as usize] = Some(id);
                g.migrating.remove(&slot);
                g.importing.remove(&slot);
                rebuild_slot_ranges(&mut g);
            }
            _ => return Err("ERR syntax error".into()),
        }
        Ok(())
    }

    pub fn bump_epoch(&self) -> u64 {
        let mut g = self.inner.lock();
        g.current_epoch = g.current_epoch.saturating_add(1);
        let epoch = g.current_epoch;
        let me = g.myself_id.clone();
        if let Some(n) = g.nodes.get_mut(&me) {
            n.config_epoch = epoch;
        }
        epoch
    }

    pub fn forget(&self, node_id: &str) -> Result<(), String> {
        let mut g = self.inner.lock();
        if node_id == g.myself_id {
            return Err("ERR I tried hard but I can't forget myself".into());
        }
        if g.nodes.remove(node_id).is_none() {
            return Err("ERR Unknown node id".into());
        }
        for slot in g.slots.iter_mut() {
            if slot.as_deref() == Some(node_id) {
                *slot = None;
            }
        }
        rebuild_slot_ranges(&mut g);
        Ok(())
    }

    pub fn reset(&self, soft: bool) {
        let mut g = self.inner.lock();
        let me = g.myself_id.clone();
        if !soft {
            for s in g.slots.iter_mut() {
                *s = None;
            }
            g.nodes.retain(|id, _| id == &me);
            g.migrating.clear();
            g.importing.clear();
            if let Some(n) = g.nodes.get_mut(&me) {
                n.slots.clear();
            }
        }
        g.current_epoch = 0;
    }

    pub fn count_keys_in_slot(ctx: &CommandContext, slot: u16) -> i64 {
        let mut n = 0i64;
        ctx.db.with_db(0, |d| {
            for k in d.map.keys() {
                if key_slot(k) == slot {
                    n += 1;
                }
            }
        });
        n
    }

    pub fn get_keys_in_slot(ctx: &CommandContext, slot: u16, count: usize) -> Vec<Bytes> {
        let mut out = Vec::new();
        ctx.db.with_db(0, |d| {
            for k in d.map.keys() {
                if key_slot(k) == slot {
                    out.push(k.clone());
                    if out.len() >= count {
                        break;
                    }
                }
            }
        });
        out
    }

    pub fn info(&self) -> String {
        let g = self.inner.lock();
        let assigned = g.slots.iter().filter(|s| s.is_some()).count();
        let ok = if g.enabled { "1" } else { "0" };
        format!(
            "cluster_state:{}\r\ncluster_slots_assigned:{assigned}\r\ncluster_slots_ok:{assigned}\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:{}\r\ncluster_size:{}\r\ncluster_current_epoch:{}\r\ncluster_my_epoch:{}\r\ncluster_stats_messages_ping_sent:0\r\ncluster_stats_messages_pong_sent:0\r\ncluster_stats_messages_sent:0\r\ncluster_stats_messages_ping_received:0\r\ncluster_stats_messages_pong_received:0\r\ncluster_stats_messages_received:0\r\n",
            if ok == "1" && assigned > 0 { "ok" } else { "fail" },
            g.nodes.len(),
            g.nodes.values().filter(|n| n.flags.contains("master")).count(),
            g.current_epoch,
            g.nodes.get(&g.myself_id).map(|n| n.config_epoch).unwrap_or(0),
        )
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn shared_cluster() -> SharedCluster {
    Arc::new(ClusterState::new())
}

fn rebuild_slot_ranges(g: &mut ClusterInner) {
    let me = g.myself_id.clone();
    for n in g.nodes.values_mut() {
        n.slots.clear();
    }
    let mut start: Option<(u16, String)> = None;
    for s in 0..CLUSTER_SLOTS {
        let owner = g.slots[s as usize].clone();
        match (start.take(), owner) {
            (None, Some(id)) => start = Some((s, id)),
            (Some((st, id)), Some(oid)) if id == oid => start = Some((st, id)),
            (Some((st, id)), other) => {
                if let Some(n) = g.nodes.get_mut(&id) {
                    n.slots.push((st, s - 1));
                }
                start = other.map(|oid| (s, oid));
            }
            (None, None) => {}
        }
    }
    if let Some((st, id)) = start {
        if let Some(n) = g.nodes.get_mut(&id) {
            n.slots.push((st, CLUSTER_SLOTS - 1));
        }
    }
    let _ = me;
}

fn slot_ranges_str(ranges: &[(u16, u16)]) -> String {
    ranges
        .iter()
        .map(|(a, b)| {
            if a == b {
                a.to_string()
            } else {
                format!("{a}-{b}")
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn bulk(s: impl AsRef<[u8]>) -> RespValue {
    RespValue::bulk(Bytes::copy_from_slice(s.as_ref()))
}

fn err(msg: &str) -> RespValue {
    RespValue::err(msg.to_string())
}

fn parse_slot_list(args: &[Bytes], start: usize) -> Result<Vec<u16>, String> {
    let mut out = Vec::new();
    for a in args.iter().skip(start) {
        let s = std::str::from_utf8(a).map_err(|_| "ERR Invalid or out of range slot")?;
        let slot: u16 = s
            .parse()
            .map_err(|_| "ERR Invalid or out of range slot")?;
        if slot >= CLUSTER_SLOTS {
            return Err("ERR Invalid or out of range slot".into());
        }
        out.push(slot);
    }
    if out.is_empty() {
        return Err("ERR syntax error".into());
    }
    Ok(out)
}

fn parse_slot_ranges(args: &[Bytes], start: usize) -> Result<Vec<u16>, String> {
    let mut out = Vec::new();
    let mut i = start;
    while i + 1 < args.len() {
        let a: u16 = arg_str(args, i)
            .and_then(|s| s.parse().ok())
            .ok_or("ERR Invalid or out of range slot")?;
        let b: u16 = arg_str(args, i + 1)
            .and_then(|s| s.parse().ok())
            .ok_or("ERR Invalid or out of range slot")?;
        if a > b || b >= CLUSTER_SLOTS {
            return Err("ERR Invalid or out of range slot".into());
        }
        for s in a..=b {
            out.push(s);
        }
        i += 2;
    }
    if out.is_empty() {
        return Err("ERR syntax error".into());
    }
    Ok(out)
}

pub fn cmd_cluster(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("cluster");
    }
    if !ctx.cluster.is_enabled() && !ctx.config.read().cluster_enabled {
        // Activer à la volée si config cluster-enabled.
        if ctx.config.read().cluster_enabled {
            let (host, port) = {
                let cfg = ctx.config.read();
                let host = cfg.bind.first().cloned().unwrap_or_else(|| "127.0.0.1".into());
                (host, cfg.port)
            };
            ctx.cluster.enable(&host, port);
        } else {
            return err("ERR This instance has cluster support disabled");
        }
    }
    let sub = arg_str(args, 1).unwrap_or("").to_ascii_lowercase();
    match sub.as_str() {
        "help" => RespValue::array(
            [
                "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "ADDSLOTS <slot> [<slot> ...]",
                "ADDSLOTSRANGE <start> <end> [<start> <end> ...]",
                "BUMPEPOCH",
                "COUNTKEYSINSLOT <slot>",
                "DELSLOTS <slot> [<slot> ...]",
                "DELSLOTSRANGE <start> <end> [<start> <end> ...]",
                "FAILOVER [FORCE|TAKEOVER]",
                "FLUSHSLOTS",
                "FORGET <node-id>",
                "GETKEYSINSLOT <slot> <count>",
                "INFO",
                "KEYSLOT <key>",
                "MEET <ip> <port> [<bus-port>]",
                "MYID",
                "MYSHARDID",
                "NODES",
                "REPLICAS <node-id>",
                "REPLICATE <node-id>",
                "RESET [HARD|SOFT]",
                "SAVECONFIG",
                "SET-CONFIG-EPOCH <epoch>",
                "SETSLOT <slot> <IMPORTING|MIGRATING|NODE|STABLE> [<node-id>]",
                "SHARDS",
                "SLAVES <node-id>",
                "SLOTS",
                "LINKS",
                "COUNT-FAILURE-REPORTS <node-id>",
                "HELP",
            ]
            .into_iter()
            .map(bulk)
            .collect(),
        ),
        "keyslot" => {
            let key = args.get(2).cloned().unwrap_or_default();
            RespValue::Integer(i64::from(key_slot(&key)))
        }
        "myid" => bulk(ctx.cluster.myself_id()),
        "myshardid" => bulk(ctx.cluster.myself_id()),
        "nodes" => RespValue::bulk(Bytes::from(ctx.cluster.nodes_line())),
        "slots" => ctx.cluster.slots_reply(),
        "info" => RespValue::bulk(Bytes::from(ctx.cluster.info())),
        "addslots" => match parse_slot_list(args, 2) {
            Ok(slots) => match ctx.cluster.add_slots(slots) {
                Ok(()) => RespValue::ok(),
                Err(e) => err(&e),
            },
            Err(e) => err(&e),
        },
        "addslotsrange" => match parse_slot_ranges(args, 2) {
            Ok(slots) => match ctx.cluster.add_slots(slots) {
                Ok(()) => RespValue::ok(),
                Err(e) => err(&e),
            },
            Err(e) => err(&e),
        },
        "delslots" => match parse_slot_list(args, 2) {
            Ok(slots) => match ctx.cluster.del_slots(slots) {
                Ok(()) => RespValue::ok(),
                Err(e) => err(&e),
            },
            Err(e) => err(&e),
        },
        "delslotsrange" => match parse_slot_ranges(args, 2) {
            Ok(slots) => match ctx.cluster.del_slots(slots) {
                Ok(()) => RespValue::ok(),
                Err(e) => err(&e),
            },
            Err(e) => err(&e),
        },
        "flushslots" => {
            ctx.cluster.reset(false);
            RespValue::ok()
        }
        "meet" => {
            if args.len() < 4 {
                return wrong_arity("cluster");
            }
            let host = arg_str(args, 2).unwrap_or("").to_string();
            let port: u16 = match arg_str(args, 3).and_then(|s| s.parse().ok()) {
                Some(p) => p,
                None => return err("ERR Invalid TCP port"),
            };
            match ctx.cluster.meet(host, port) {
                Ok(()) => RespValue::ok(),
                Err(e) => err(&e),
            }
        }
        "bumpepoch" => {
            let e = ctx.cluster.bump_epoch();
            RespValue::array(vec![bulk("BUMPED"), RespValue::Integer(e as i64)])
        }
        "forget" => {
            let id = match arg_str(args, 2) {
                Some(i) => i,
                None => return wrong_arity("cluster"),
            };
            match ctx.cluster.forget(id) {
                Ok(()) => RespValue::ok(),
                Err(e) => err(&e),
            }
        }
        "reset" => {
            let soft = arg_str(args, 2)
                .map(|s| s.eq_ignore_ascii_case("soft"))
                .unwrap_or(false);
            ctx.cluster.reset(soft);
            RespValue::ok()
        }
        "setslot" => {
            if args.len() < 4 {
                return wrong_arity("cluster");
            }
            let slot: u16 = match arg_str(args, 2).and_then(|s| s.parse().ok()) {
                Some(s) => s,
                None => return err("ERR Invalid or out of range slot"),
            };
            let sub2 = arg_str(args, 3).unwrap_or("");
            let node = arg_str(args, 4);
            match ctx.cluster.setslot(slot, sub2, node) {
                Ok(()) => RespValue::ok(),
                Err(e) => err(&e),
            }
        },
        "countkeysinslot" => {
            let slot: u16 = match arg_str(args, 2).and_then(|s| s.parse().ok()) {
                Some(s) => s,
                None => return err("ERR Invalid or out of range slot"),
            };
            RespValue::Integer(ClusterState::count_keys_in_slot(ctx, slot))
        }
        "getkeysinslot" => {
            if args.len() < 4 {
                return wrong_arity("cluster");
            }
            let slot: u16 = match arg_str(args, 2).and_then(|s| s.parse().ok()) {
                Some(s) => s,
                None => return err("ERR Invalid or out of range slot"),
            };
            let count: usize = arg_str(args, 3).and_then(|s| s.parse().ok()).unwrap_or(0);
            let keys = ClusterState::get_keys_in_slot(ctx, slot, count);
            RespValue::array(keys.into_iter().map(RespValue::bulk).collect())
        }
        "saveconfig" => RespValue::ok(),
        "set-config-epoch" => {
            let epoch: u64 = arg_str(args, 2).and_then(|s| s.parse().ok()).unwrap_or(0);
            let mut g = ctx.cluster.inner.lock();
            g.current_epoch = epoch;
            let me = g.myself_id.clone();
            if let Some(n) = g.nodes.get_mut(&me) {
                n.config_epoch = epoch;
            }
            RespValue::ok()
        }
        "replicas" | "slaves" => RespValue::array(Vec::new()),
        "replicate" => RespValue::ok(),
        "failover" => RespValue::ok(),
        "shards" => {
            // Approximation : un shard = ce nœud + ses slots.
            let g = ctx.cluster.inner.lock();
            let me = g.nodes.get(&g.myself_id);
            let slots: Vec<RespValue> = me
                .map(|n| {
                    n.slots
                        .iter()
                        .flat_map(|(a, b)| {
                            vec![
                                RespValue::Integer(i64::from(*a)),
                                RespValue::Integer(i64::from(*b)),
                            ]
                        })
                        .collect()
                })
                .unwrap_or_default();
            let node = me.map(|n| {
                RespValue::array(vec![
                    bulk("id"),
                    bulk(&n.id),
                    bulk("port"),
                    RespValue::Integer(i64::from(n.port)),
                    bulk("ip"),
                    bulk(&n.host),
                    bulk("endpoint"),
                    bulk(&n.host),
                    bulk("role"),
                    bulk("master"),
                    bulk("replication-offset"),
                    RespValue::Integer(0),
                    bulk("health"),
                    bulk("online"),
                ])
            });
            RespValue::array(vec![RespValue::array(vec![
                bulk("slots"),
                RespValue::array(slots),
                bulk("nodes"),
                RespValue::array(node.into_iter().collect()),
            ])])
        }
        "links" => RespValue::array(Vec::new()),
        "count-failure-reports" => RespValue::Integer(0),
        "asking" => {
            // ASKING est une commande séparée ; ici no-op de compat.
            state.asking = true;
            RespValue::ok()
        }
        _ => err(&format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}'. Try CLUSTER HELP.",
            arg_str(args, 1).unwrap_or("?")
        )),
    }
}

pub fn cmd_asking(_ctx: &CommandContext, state: &mut ClientState, _args: &[Bytes]) -> RespValue {
    state.asking = true;
    RespValue::ok()
}

/// Si cluster activé et clé hors slot local → erreur MOVED/ASK.
pub fn redirect_if_needed(
    ctx: &CommandContext,
    state: &ClientState,
    args: &[Bytes],
) -> Option<RespValue> {
    if !ctx.cluster.is_enabled() && !ctx.config.read().cluster_enabled {
        return None;
    }
    let cmd = String::from_utf8_lossy(args.first()?).to_ascii_uppercase();
    if matches!(
        cmd.as_str(),
        "CLUSTER"
            | "ASKING"
            | "AUTH"
            | "HELLO"
            | "PING"
            | "QUIT"
            | "COMMAND"
            | "INFO"
            | "CLIENT"
            | "CONFIG"
            | "READONLY"
            | "READWRITE"
            | "REPLICAOF"
            | "SLAVEOF"
            | "SCRIPT"
            | "FUNCTION"
            | "MODULE"
            | "ACL"
            | "SENTINEL"
    ) {
        return None;
    }
    if !ctx.cluster.is_enabled() {
        let cfg = ctx.config.read();
        let host = cfg
            .bind
            .first()
            .cloned()
            .unwrap_or_else(|| "127.0.0.1".into());
        ctx.cluster.enable(&host, cfg.port);
    }
    let keys = crate::acl::keys_for_command(&cmd, args);
    let key = keys.first()?;
    let slot = key_slot(key);
    if ctx.cluster.is_slot_mine(slot) {
        if let Some(dest) = ctx.cluster.is_migrating(slot) {
            let present = ctx.db.with_db(state.db_index, |d| d.map.contains_key(key));
            if !present && !state.asking {
                let g = ctx.cluster.inner.lock();
                if let Some(n) = g.nodes.get(&dest) {
                    return Some(RespValue::err(format!("ASK {slot} {}:{}", n.host, n.port)));
                }
            }
        }
        return None;
    }
    let owner = ctx.cluster.slot_owner(slot);
    let g = ctx.cluster.inner.lock();
    if let Some(id) = owner {
        if let Some(n) = g.nodes.get(&id) {
            return Some(RespValue::err(format!("MOVED {slot} {}:{}", n.host, n.port)));
        }
    }
    Some(RespValue::err("CLUSTERDOWN Hash slot not served"))
}
