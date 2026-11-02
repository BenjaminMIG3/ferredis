//! Couche réseau du GUI : une connexion persistante gérée par un thread de travail.
//!
//! L'interface n'effectue jamais d'E/S : elle envoie des [`Request`] et reçoit des [`Reply`].
//! Une actualisation complète ([`Request::Snapshot`]) tient en deux allers-retours pipelinés
//! (statistiques + clients + événements, puis TYPE/TTL de toutes les clés), au lieu d'une
//! connexion TCP par commande.

use bytes::BytesMut;
use eframe::egui;
use ferredis_core::resp::{parse, RespError, RespValue};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Nombre maximal de clés listées par actualisation.
pub const KEY_LIMIT: usize = 500;
/// Nombre d'événements de connexion demandés au serveur.
const EVENT_LIMIT: usize = 200;
/// Capacité du journal temps réel (événements keyspace).
const LIVE_CAPACITY: usize = 300;

// ───────────────────────────── Modèles ─────────────────────────────

#[derive(Clone, Debug)]
pub struct KeyRow {
    pub name: String,
    pub ty: String,
    pub ttl: i64,
}

#[derive(Clone, Debug, Default)]
pub struct ClientRow {
    pub id: String,
    pub addr: String,
    pub name: String,
    pub age: u64,
    pub idle: u64,
    pub db: String,
    pub cmd: String,
    pub flags: String,
}

#[derive(Clone, Debug)]
pub struct EventRow {
    pub at_ms: u64,
    pub client_id: i64,
    pub addr: String,
    pub kind: String,
    pub detail: String,
}

/// Base non vide (ou sélectionnée) exposée au sélecteur GUI.
#[derive(Clone, Debug, Default)]
pub struct DbInfo {
    pub index: u16,
    pub keys: i64,
    pub expires: i64,
}

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub info: Vec<(String, String)>,
    /// Index de la base SELECT'ée pour ce snapshot.
    pub db: u16,
    pub dbsize: i64,
    /// Bases actives (au moins une clé), plus la base courante si vide.
    pub databases: Vec<DbInfo>,
    pub keys: Vec<KeyRow>,
    pub truncated: bool,
    pub clients: Vec<ClientRow>,
    pub events: Vec<EventRow>,
    pub rtt: Duration,
}

impl Snapshot {
    /// Valeur d'un champ INFO.
    pub fn info(&self, key: &str) -> Option<&str> {
        self.info
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

#[derive(Clone, Debug, Default)]
pub struct KeyDetail {
    pub key: String,
    pub ty: String,
    pub ttl: i64,
    pub len: i64,
    pub rows: Vec<(String, String)>,
    pub truncated: bool,
}

pub enum Request {
    /// Change de serveur cible (ferme la connexion courante).
    Connect {
        host: String,
        port: u16,
    },
    /// Ferme la connexion TCP sans en ouvrir une nouvelle.
    Disconnect,
    Snapshot {
        pattern: String,
        db: u16,
    },
    Exec(String),
    Inspect(String),
    Delete(String),
    KillClient(String),
}

pub enum Reply {
    Snapshot(Snapshot),
    Console {
        input: String,
        output: String,
        is_err: bool,
        elapsed: Duration,
    },
    Detail(KeyDetail),
    Done(String),
    Down(String),
}

/// Poignée côté interface : envoi des requêtes, réception des réponses.
pub struct Backend {
    tx: Sender<Request>,
    pub rx: Receiver<Reply>,
}

impl Backend {
    pub fn send(&self, req: Request) {
        let _ = self.tx.send(req);
    }
}

// ───────────────────────────── Connexion ─────────────────────────────

pub struct Conn {
    stream: TcpStream,
    buf: BytesMut,
}

impl Conn {
    pub fn open(host: &str, port: u16, name: &str, read_timeout: Duration) -> Result<Self, String> {
        let addr = (host, port)
            .to_socket_addrs()
            .map_err(|e| e.to_string())?
            .next()
            .ok_or("adresse introuvable")?;
        let stream =
            TcpStream::connect_timeout(&addr, Duration::from_secs(2)).map_err(|e| e.to_string())?;
        stream.set_read_timeout(Some(read_timeout)).ok();
        stream.set_nodelay(true).ok();
        let mut conn = Self {
            stream,
            buf: BytesMut::with_capacity(8192),
        };
        // Rend la connexion identifiable dans CLIENT LIST (erreur ignorée).
        let _ = conn.call(&["CLIENT", "SETNAME", name]);
        Ok(conn)
    }

    pub fn call(&mut self, args: &[&str]) -> Result<RespValue, String> {
        let cmd: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        Ok(self.pipeline(&[cmd])?.remove(0))
    }

    /// Envoie toutes les commandes d'un coup puis lit autant de réponses.
    pub fn pipeline(&mut self, cmds: &[Vec<String>]) -> Result<Vec<RespValue>, String> {
        let mut out = Vec::with_capacity(cmds.len() * 32);
        for cmd in cmds {
            out.extend_from_slice(format!("*{}\r\n", cmd.len()).as_bytes());
            for a in cmd {
                out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                out.extend_from_slice(a.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
        }
        self.stream.write_all(&out).map_err(|e| e.to_string())?;
        (0..cmds.len()).map(|_| self.read_reply()).collect()
    }

    fn read_reply(&mut self) -> Result<RespValue, String> {
        let mut tmp = [0u8; 16 * 1024];
        loop {
            match parse(&mut self.buf) {
                Ok(v) => return Ok(v),
                Err(RespError::Incomplete) => {
                    let n = self.stream.read(&mut tmp).map_err(|e| e.to_string())?;
                    if n == 0 {
                        return Err("connexion fermée par le serveur".into());
                    }
                    self.buf.extend_from_slice(&tmp[..n]);
                }
                Err(e) => return Err(e.to_string()),
            }
        }
    }
}

// ───────────────────────────── Thread de travail ─────────────────────────────

pub fn spawn_worker(host: String, port: u16, ctx: egui::Context) -> Backend {
    let (tx, req_rx) = mpsc::channel::<Request>();
    let (rep_tx, rx) = mpsc::channel::<Reply>();

    thread::spawn(move || {
        let mut target = (host, port);
        let mut conn: Option<Conn> = None;
        let mut current_db: u16 = 0;

        for req in req_rx {
            match req {
                Request::Connect { host, port } => {
                    target = (host, port);
                    conn = None;
                    current_db = 0;
                    continue;
                }
                Request::Disconnect => {
                    conn = None;
                    current_db = 0;
                    continue;
                }
                _ => {}
            }
            let reply = match run_request(&mut conn, &target, &mut current_db, req) {
                Ok(r) => r,
                Err(e) => {
                    conn = None; // reconnexion à la prochaine requête
                    current_db = 0;
                    Reply::Down(e)
                }
            };
            if rep_tx.send(reply).is_err() {
                break;
            }
            ctx.request_repaint();
        }
    });

    Backend { tx, rx }
}

fn run_request(
    conn: &mut Option<Conn>,
    target: &(String, u16),
    current_db: &mut u16,
    req: Request,
) -> Result<Reply, String> {
    if conn.is_none() {
        *conn = Some(Conn::open(
            &target.0,
            target.1,
            "ferredis-gui",
            Duration::from_secs(5),
        )?);
        *current_db = 0;
    }
    let c = conn.as_mut().expect("connexion établie ci-dessus");

    match req {
        Request::Connect { .. } | Request::Disconnect => {
            unreachable!("traité par la boucle worker")
        }
        Request::Snapshot { pattern, db } => {
            select_db(c, current_db, db)?;
            snapshot(c, &pattern, db).map(Reply::Snapshot)
        }
        Request::Inspect(key) => inspect(c, &key).map(Reply::Detail),
        Request::Delete(key) => {
            c.call(&["DEL", &key])?;
            Ok(Reply::Done(format!("Clé supprimée : {key}")))
        }
        Request::KillClient(id) => {
            let r = c.call(&["CLIENT", "KILL", "ID", &id])?;
            Ok(Reply::Done(format!("CLIENT KILL ID {id} → {}", render(&r))))
        }
        Request::Exec(line) => {
            let started = Instant::now();
            let (output, is_err) = match split_args(&line) {
                Err(e) => (e, true),
                Ok(args) if args.is_empty() => return Ok(Reply::Done(String::new())),
                Ok(args) => match rejected_in_console(&args[0]) {
                    Some(why) => (why, true),
                    None => {
                        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
                        let r = c.call(&refs)?;
                        let is_err = matches!(r, RespValue::Error(_));
                        (render(&r), is_err)
                    }
                },
            };
            Ok(Reply::Console {
                input: line,
                output,
                is_err,
                elapsed: started.elapsed(),
            })
        }
    }
}

fn select_db(c: &mut Conn, current: &mut u16, db: u16) -> Result<(), String> {
    if *current == db {
        return Ok(());
    }
    let idx = db.to_string();
    match c.call(&["SELECT", &idx])? {
        RespValue::Error(e) => Err(crate::textutil::from_redis_bytes(&e)),
        _ => {
            *current = db;
            Ok(())
        }
    }
}

/// Commandes qui ne peuvent pas fonctionner sur la connexion partagée de la console.
fn rejected_in_console(cmd: &str) -> Option<String> {
    const STREAMING: [&str; 5] = ["SUBSCRIBE", "PSUBSCRIBE", "MONITOR", "SYNC", "PSYNC"];
    const BLOCKING: [&str; 6] = [
        "BLPOP",
        "BRPOP",
        "BZPOPMIN",
        "BZPOPMAX",
        "BRPOPLPUSH",
        "BLMOVE",
    ];
    let up = cmd.to_ascii_uppercase();
    if up == "SELECT" {
        Some(
            "SELECT : utilise le sélecteur de base de l'onglet Clés (évite de désynchroniser la session)."
                .into(),
        )
    } else if STREAMING.contains(&up.as_str()) {
        Some(format!(
            "{up} n'est pas disponible dans la console (utilise l'onglet Live)."
        ))
    } else if BLOCKING.contains(&up.as_str()) {
        Some(format!(
            "{up} bloquerait l'interface : utilise ferredis-cli."
        ))
    } else {
        None
    }
}

fn snapshot(c: &mut Conn, pattern: &str, db: u16) -> Result<Snapshot, String> {
    let started = Instant::now();
    let first = c.pipeline(&[
        vec!["PING".into()],
        vec!["INFO".into()],
        vec!["DBSIZE".into()],
        vec!["CLIENT".into(), "LIST".into()],
        vec!["FERREDIS".into(), "EVENTS".into(), EVENT_LIMIT.to_string()],
        vec!["DATABASES".into()],
    ])?;
    let rtt = started.elapsed();

    let info = as_text(&first[1])
        .map(|t| parse_info(&t))
        .unwrap_or_default();
    let dbsize = match first[2] {
        RespValue::Integer(n) => n,
        _ => 0,
    };
    let clients = as_text(&first[3])
        .map(|t| t.lines().filter_map(parse_client_line).collect())
        .unwrap_or_default();
    let events = parse_events(&first[4]);
    let mut databases = parse_databases(&first[5]);
    if databases.is_empty() {
        databases = databases_from_info(&info);
    }
    // Toujours exposer la base courante, même vide (sinon le sélecteur disparaît).
    if !databases.iter().any(|d| d.index == db) {
        databases.push(DbInfo {
            index: db,
            keys: dbsize,
            expires: 0,
        });
        databases.sort_by_key(|d| d.index);
    }

    // Parcours SCAN jusqu'à la limite.
    let pattern = if pattern.trim().is_empty() {
        "*"
    } else {
        pattern.trim()
    };
    let mut names: Vec<String> = Vec::new();
    let mut cursor = "0".to_string();
    let mut truncated = false;
    loop {
        let r = c.call(&["SCAN", &cursor, "MATCH", pattern, "COUNT", "500"])?;
        let RespValue::Array(Some(parts)) = r else {
            return Err(format!("SCAN : {}", render(&r)));
        };
        cursor = parts
            .first()
            .and_then(as_text)
            .ok_or("curseur SCAN invalide")?;
        if let Some(RespValue::Array(Some(keys))) = parts.get(1) {
            names.extend(keys.iter().filter_map(as_text));
        }
        if names.len() >= KEY_LIMIT {
            truncated = cursor != "0" || names.len() > KEY_LIMIT;
            names.truncate(KEY_LIMIT);
            break;
        }
        if cursor == "0" {
            break;
        }
    }
    names.sort_unstable();
    names.dedup();

    // TYPE + TTL de toutes les clés en un seul pipeline.
    let mut cmds = Vec::with_capacity(names.len() * 2);
    for n in &names {
        cmds.push(vec!["TYPE".to_string(), n.clone()]);
        cmds.push(vec!["TTL".to_string(), n.clone()]);
    }
    let replies = if cmds.is_empty() {
        Vec::new()
    } else {
        c.pipeline(&cmds)?
    };
    let keys = names
        .into_iter()
        .zip(replies.chunks(2))
        .map(|(name, pair)| KeyRow {
            name,
            ty: as_text(&pair[0]).unwrap_or_else(|| "none".into()),
            ttl: match pair[1] {
                RespValue::Integer(n) => n,
                _ => -1,
            },
        })
        .collect();

    Ok(Snapshot {
        info,
        db,
        dbsize,
        databases,
        keys,
        truncated,
        clients,
        events,
        rtt,
    })
}

/// Parse `DATABASES` : `[["db", N, "keys", K, "expires", E], …]` ou ancien `[[N, K, E], …]`.
fn parse_databases(v: &RespValue) -> Vec<DbInfo> {
    let RespValue::Array(Some(items)) = v else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let RespValue::Array(Some(fields)) = item else {
            continue;
        };
        if let Some(db) = parse_db_entry(fields) {
            out.push(db);
        }
    }
    out.sort_by_key(|d| d.index);
    out
}

fn parse_db_entry(fields: &[RespValue]) -> Option<DbInfo> {
    // Ancien format : trois entiers [db, keys, expires].
    if fields.len() == 3 && fields.iter().all(|f| matches!(f, RespValue::Integer(_))) {
        return Some(DbInfo {
            index: as_i64(&fields[0])? as u16,
            keys: as_i64(&fields[1])?,
            expires: as_i64(&fields[2])?,
        });
    }
    // Format étiqueté : db / keys / expires.
    let mut index = None;
    let mut keys = None;
    let mut expires = None;
    let mut i = 0;
    while i + 1 < fields.len() {
        let label = as_text(&fields[i])?.to_ascii_lowercase();
        let val = as_i64(&fields[i + 1])?;
        match label.as_str() {
            "db" => index = Some(val as u16),
            "keys" => keys = Some(val),
            "expires" => expires = Some(val),
            _ => {}
        }
        i += 2;
    }
    Some(DbInfo {
        index: index?,
        keys: keys.unwrap_or(0),
        expires: expires.unwrap_or(0),
    })
}

fn as_i64(v: &RespValue) -> Option<i64> {
    match v {
        RespValue::Integer(n) => Some(*n),
        RespValue::BulkString(Some(b)) | RespValue::SimpleString(b) => {
            crate::textutil::from_redis_bytes(b).parse().ok()
        }
        _ => None,
    }
}

/// Repli : lignes `dbN:keys=…,expires=…` de INFO keyspace.
fn databases_from_info(info: &[(String, String)]) -> Vec<DbInfo> {
    let mut out = Vec::new();
    for (k, v) in info {
        let Some(rest) = k.strip_prefix("db") else {
            continue;
        };
        if !rest.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let Ok(index) = rest.parse::<u16>() else {
            continue;
        };
        let mut keys = 0i64;
        let mut expires = 0i64;
        for part in v.split(',') {
            if let Some((n, val)) = part.split_once('=') {
                match n {
                    "keys" => keys = val.parse().unwrap_or(0),
                    "expires" => expires = val.parse().unwrap_or(0),
                    _ => {}
                }
            }
        }
        out.push(DbInfo {
            index,
            keys,
            expires,
        });
    }
    out.sort_by_key(|d| d.index);
    out
}

fn inspect(c: &mut Conn, key: &str) -> Result<KeyDetail, String> {
    let head = c.pipeline(&[
        vec!["TYPE".into(), key.into()],
        vec!["TTL".into(), key.into()],
    ])?;
    let ty = as_text(&head[0]).unwrap_or_else(|| "none".into());
    let ttl = match head[1] {
        RespValue::Integer(n) => n,
        _ => -1,
    };
    let k = key.to_string();
    let (len_cmd, data_cmd): (Vec<&str>, Vec<&str>) = match ty.as_str() {
        "string" => (vec!["STRLEN", &k], vec!["GET", &k]),
        "list" => (vec!["LLEN", &k], vec!["LRANGE", &k, "0", "199"]),
        "hash" => (vec!["HLEN", &k], vec!["HSCAN", &k, "0", "COUNT", "200"]),
        "set" => (vec!["SCARD", &k], vec!["SSCAN", &k, "0", "COUNT", "200"]),
        "zset" => (
            vec!["ZCARD", &k],
            vec!["ZRANGE", &k, "0", "199", "WITHSCORES"],
        ),
        "stream" => (
            vec!["XLEN", &k],
            vec!["XRANGE", &k, "-", "+", "COUNT", "100"],
        ),
        _ => {
            return Ok(KeyDetail {
                key: k,
                ty,
                ttl,
                ..Default::default()
            })
        }
    };
    let to_owned = |v: Vec<&str>| v.into_iter().map(String::from).collect::<Vec<_>>();
    let body = c.pipeline(&[to_owned(len_cmd), to_owned(data_cmd)])?;
    let len = match body[0] {
        RespValue::Integer(n) => n,
        _ => 0,
    };

    let rows: Vec<(String, String)> = match (ty.as_str(), &body[1]) {
        ("string", v) => vec![("valeur".into(), as_text(v).unwrap_or_default())],
        ("list", RespValue::Array(Some(items))) => items
            .iter()
            .enumerate()
            .map(|(i, v)| (i.to_string(), as_text(v).unwrap_or_default()))
            .collect(),
        ("hash" | "set", RespValue::Array(Some(parts))) => match parts.get(1) {
            Some(RespValue::Array(Some(items))) if ty == "hash" => items
                .chunks(2)
                .map(|p| {
                    (
                        as_text(&p[0]).unwrap_or_default(),
                        p.get(1).and_then(as_text).unwrap_or_default(),
                    )
                })
                .collect(),
            Some(RespValue::Array(Some(items))) => items
                .iter()
                .map(|v| ("membre".to_string(), as_text(v).unwrap_or_default()))
                .collect(),
            _ => Vec::new(),
        },
        ("zset", RespValue::Array(Some(items))) => items
            .chunks(2)
            .map(|p| {
                (
                    as_text(&p[0]).unwrap_or_default(),
                    p.get(1).and_then(as_text).unwrap_or_default(),
                )
            })
            .collect(),
        ("stream", RespValue::Array(Some(entries))) => entries
            .iter()
            .filter_map(|e| match e {
                RespValue::Array(Some(p)) if p.len() == 2 => {
                    let fields = match &p[1] {
                        RespValue::Array(Some(f)) => f
                            .chunks(2)
                            .map(|kv| {
                                format!(
                                    "{}={}",
                                    as_text(&kv[0]).unwrap_or_default(),
                                    kv.get(1).and_then(as_text).unwrap_or_default()
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("  "),
                        _ => String::new(),
                    };
                    Some((as_text(&p[0]).unwrap_or_default(), fields))
                }
                _ => None,
            })
            .collect(),
        (_, other) => vec![("réponse".into(), render(other))],
    };

    let shown = rows.len() as i64;
    Ok(KeyDetail {
        key: k,
        ty: ty.clone(),
        ttl,
        len,
        truncated: ty != "string" && shown < len,
        rows,
    })
}

// ───────────────────────────── Flux temps réel ─────────────────────────────

#[derive(Clone, Debug)]
pub struct LiveEvent {
    pub at_ms: u64,
    pub db: String,
    pub event: String,
    pub key: String,
}

/// Journal partagé alimenté par le thread d'abonnement.
#[derive(Default)]
pub struct LiveFeed {
    pub events: Mutex<VecDeque<LiveEvent>>,
    /// Passe à `true` à chaque nouvel événement (consommé par l'interface).
    pub dirty: AtomicBool,
    /// Message d'état de la connexion d'abonnement.
    pub status: Mutex<String>,
}

impl LiveFeed {
    fn push(&self, ev: LiveEvent) {
        let mut q = self.events.lock().unwrap_or_else(|e| e.into_inner());
        if q.len() == LIVE_CAPACITY {
            q.pop_front();
        }
        q.push_back(ev);
        self.dirty.store(true, Ordering::SeqCst);
    }

    fn set_status(&self, s: impl Into<String>) {
        *self.status.lock().unwrap_or_else(|e| e.into_inner()) = s.into();
    }
}

/// Lance l'abonnement aux notifications keyspace ; s'arrête quand `stop` passe à `true`.
pub fn spawn_live(
    host: String,
    port: u16,
    feed: Arc<LiveFeed>,
    stop: Arc<AtomicBool>,
    ctx: egui::Context,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !stop.load(Ordering::SeqCst) {
            match live_session(&host, port, &feed, &stop, &ctx) {
                Ok(()) => {}
                Err(e) => feed.set_status(format!("abonnement interrompu : {e}")),
            }
            ctx.request_repaint();
            // Attente interruptible avant la reconnexion.
            for _ in 0..10 {
                if stop.load(Ordering::SeqCst) {
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
    })
}

fn live_session(
    host: &str,
    port: u16,
    feed: &LiveFeed,
    stop: &AtomicBool,
    ctx: &egui::Context,
) -> Result<(), String> {
    let mut conn = Conn::open(host, port, "ferredis-gui-live", Duration::from_millis(400))?;
    // Active les notifications si le serveur ne le fait pas (équivalent de redis-cli).
    let _ = conn.call(&["CONFIG", "SET", "notify-keyspace-events", "AKE"]);
    let ack = conn.call(&["PSUBSCRIBE", "__keyevent@*__:*"])?;
    if let RespValue::Error(e) = ack {
        return Err(crate::textutil::from_redis_bytes(&e));
    }
    feed.set_status("abonné aux événements");
    ctx.request_repaint();

    let mut tmp = [0u8; 16 * 1024];
    while !stop.load(Ordering::SeqCst) {
        match parse(&mut conn.buf) {
            Ok(msg) => {
                if let Some(ev) = live_event(&msg) {
                    feed.push(ev);
                    ctx.request_repaint();
                }
            }
            Err(RespError::Incomplete) => match conn.stream.read(&mut tmp) {
                Ok(0) => return Err("connexion fermée".into()),
                Ok(n) => conn.buf.extend_from_slice(&tmp[..n]),
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(e) => return Err(e.to_string()),
            },
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

/// `pmessage <pattern> __keyevent@0__:set <key>` → événement structuré.
fn live_event(v: &RespValue) -> Option<LiveEvent> {
    let RespValue::Array(Some(items)) = v else {
        return None;
    };
    if items.len() < 4 || items[0].as_bytes()? != b"pmessage" {
        return None;
    }
    let channel = as_text(&items[2])?;
    let key = as_text(&items[3])?;
    let rest = channel.strip_prefix("__keyevent@")?;
    let (db, event) = rest.split_once("__:")?;
    Some(LiveEvent {
        at_ms: now_ms(),
        db: db.to_string(),
        event: event.to_string(),
        key,
    })
}

// ───────────────────────────── Utilitaires ─────────────────────────────

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// `HH:MM:SS` (UTC) d'un horodatage en millisecondes.
pub fn clock(ms: u64) -> String {
    let s = ms / 1000 % 86_400;
    format!("{:02}:{:02}:{:02}", s / 3600, s / 60 % 60, s % 60)
}

/// Durée compacte : `3d 4h`, `5m 12s`, `42s`.
pub fn human_duration(secs: u64) -> String {
    match secs {
        s if s >= 86_400 => format!("{}j {}h", s / 86_400, s % 86_400 / 3600),
        s if s >= 3600 => format!("{}h {}m", s / 3600, s % 3600 / 60),
        s if s >= 60 => format!("{}m {}s", s / 60, s % 60),
        s => format!("{s}s"),
    }
}

fn as_text(v: &RespValue) -> Option<String> {
    match v {
        RespValue::BulkString(Some(b)) | RespValue::SimpleString(b) => {
            Some(crate::textutil::from_redis_bytes(b))
        }
        RespValue::Integer(n) => Some(n.to_string()),
        _ => None,
    }
}

fn parse_info(text: &str) -> Vec<(String, String)> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.to_string(), v.trim().to_string()))
        .collect()
}

fn parse_client_line(line: &str) -> Option<ClientRow> {
    let mut row = ClientRow::default();
    let mut any = false;
    for part in line.split_whitespace() {
        let Some((k, v)) = part.split_once('=') else {
            continue;
        };
        any = true;
        match k {
            "id" => row.id = v.into(),
            "addr" => row.addr = v.into(),
            "name" => row.name = v.into(),
            "age" => row.age = v.parse().unwrap_or(0),
            "idle" => row.idle = v.parse().unwrap_or(0),
            "db" => row.db = v.into(),
            "cmd" => row.cmd = v.into(),
            "flags" => row.flags = v.into(),
            _ => {}
        }
    }
    any.then_some(row)
}

fn parse_events(v: &RespValue) -> Vec<EventRow> {
    let RespValue::Array(Some(items)) = v else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|e| {
            let RespValue::Array(Some(f)) = e else {
                return None;
            };
            if f.len() < 5 {
                return None;
            }
            let num = |v: &RespValue| match v {
                RespValue::Integer(n) => *n,
                other => as_text(other).and_then(|s| s.parse().ok()).unwrap_or(0),
            };
            Some(EventRow {
                at_ms: num(&f[0]).max(0) as u64,
                client_id: num(&f[1]),
                addr: as_text(&f[2]).unwrap_or_default(),
                kind: as_text(&f[3]).unwrap_or_default(),
                detail: as_text(&f[4]).unwrap_or_default(),
            })
        })
        .collect()
}

/// Découpe une ligne de commande façon shell (guillemets simples/doubles, échappements).
pub fn split_args(line: &str) -> Result<Vec<String>, String> {
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut in_arg = false;
    let mut quote: Option<char> = None;
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some('"'), '\\') => match chars.next() {
                Some('n') => cur.push('\n'),
                Some('t') => cur.push('\t'),
                Some('r') => cur.push('\r'),
                Some(o) => cur.push(o),
                None => return Err("échappement final invalide".into()),
            },
            (Some(_), c) => cur.push(c),
            (None, '"' | '\'') => {
                quote = Some(c);
                in_arg = true;
            }
            (None, c) if c.is_whitespace() => {
                if in_arg {
                    args.push(std::mem::take(&mut cur));
                    in_arg = false;
                }
            }
            (None, c) => {
                cur.push(c);
                in_arg = true;
            }
        }
    }
    if quote.is_some() {
        return Err("guillemet non fermé".into());
    }
    if in_arg {
        args.push(cur);
    }
    Ok(args)
}

/// Rendu façon `redis-cli`.
pub fn render(v: &RespValue) -> String {
    let mut out = String::new();
    render_into(v, 0, &mut out);
    out
}

fn render_into(v: &RespValue, indent: usize, out: &mut String) {
    use crate::textutil::from_redis_bytes;
    match v {
        RespValue::SimpleString(b) => out.push_str(&from_redis_bytes(b)),
        RespValue::Error(b) => {
            out.push_str("(error) ");
            out.push_str(&from_redis_bytes(b));
        }
        RespValue::Integer(n) => out.push_str(&format!("(integer) {n}")),
        RespValue::BulkString(None) | RespValue::Array(None) => out.push_str("(nil)"),
        RespValue::BulkString(Some(b)) => {
            out.push('"');
            out.push_str(&from_redis_bytes(b).replace('\n', "\\n"));
            out.push('"');
        }
        RespValue::Array(Some(items)) if items.is_empty() => out.push_str("(empty array)"),
        RespValue::Array(Some(items)) => {
            let width = items.len().to_string().len();
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push('\n');
                    out.push_str(&" ".repeat(indent));
                }
                let label = format!("{:>width$}) ", i + 1);
                out.push_str(&label);
                render_into(item, indent + label.len(), out);
            }
        }
        RespValue::Null => out.push_str("(nil)"),
        RespValue::Bool(b) => out.push_str(&format!("(bool) {b}")),
        RespValue::Double(n) => out.push_str(&format!("(double) {n}")),
        RespValue::BigNumber(b) => {
            out.push_str("(big number) ");
            out.push_str(&from_redis_bytes(b));
        }
        RespValue::Verbatim { format, data } => {
            out.push_str(&format!(
                "(verbatim) {}:{}",
                from_redis_bytes(format),
                from_redis_bytes(data)
            ));
        }
        RespValue::Map(entries) => {
            out.push_str(&format!("(map) {} pairs", entries.len()));
            for (i, (k, v)) in entries.iter().enumerate() {
                out.push('\n');
                out.push_str(&" ".repeat(indent));
                out.push_str(&format!("{} ) ", i + 1));
                render_into(k, indent + 4, out);
                out.push_str(" => ");
                render_into(v, indent + 4, out);
            }
        }
        RespValue::Set(items) | RespValue::Push(items) => {
            let kind = if matches!(v, RespValue::Push(_)) {
                "push"
            } else {
                "set"
            };
            out.push_str(&format!("({kind}) {} elems", items.len()));
            for (i, item) in items.iter().enumerate() {
                out.push('\n');
                out.push_str(&" ".repeat(indent));
                out.push_str(&format!("{} ) ", i + 1));
                render_into(item, indent + 4, out);
            }
        }
        RespValue::Attribute { value, .. } => render_into(value, indent, out),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_handles_quotes() {
        assert_eq!(
            split_args(r#"SET k "a b" 'c d'"#).unwrap(),
            vec!["SET", "k", "a b", "c d"]
        );
        assert_eq!(split_args(r#"SET k """#).unwrap(), vec!["SET", "k", ""]);
        assert!(split_args(r#"SET "k"#).is_err());
    }

    #[test]
    fn client_line_parsing() {
        let row = parse_client_line("id=3 addr=127.0.0.1:5000 name=web age=12 idle=1 db=0 cmd=get")
            .unwrap();
        assert_eq!(
            (row.id.as_str(), row.name.as_str(), row.age),
            ("3", "web", 12)
        );
    }

    #[test]
    fn durations() {
        assert_eq!(human_duration(42), "42s");
        assert_eq!(human_duration(90_000), "1j 1h");
        assert_eq!(clock(3_661_000), "01:01:01");
    }

    #[test]
    fn keyevent_parsing() {
        let msg = RespValue::Array(Some(vec![
            RespValue::bulk("pmessage"),
            RespValue::bulk("__keyevent@*__:*"),
            RespValue::bulk("__keyevent@0__:set"),
            RespValue::bulk("foo"),
        ]));
        let ev = live_event(&msg).unwrap();
        assert_eq!(
            (ev.db.as_str(), ev.event.as_str(), ev.key.as_str()),
            ("0", "set", "foo")
        );
    }
}
