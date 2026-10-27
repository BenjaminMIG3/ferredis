//! Registre des clients connectés et journal des tentatives de connexion.
//!
//! Alimente `CLIENT LIST|INFO|KILL`, la section `# Clients` de `INFO` et la
//! commande `FERREDIS EVENTS` consommée par l'interface graphique.

use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

/// Nombre maximal d'évènements conservés en mémoire.
pub const EVENT_LOG_CAPACITY: usize = 1000;

/// Nature d'un évènement de connexion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Connected,
    Disconnected,
    AuthFailed,
    ProtocolError,
    Killed,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "connect",
            Self::Disconnected => "disconnect",
            Self::AuthFailed => "auth_failed",
            Self::ProtocolError => "protocol_error",
            Self::Killed => "killed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientEvent {
    pub at_unix_ms: u64,
    pub client_id: u64,
    pub addr: String,
    pub kind: EventKind,
    pub detail: String,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Informations partagées sur un client (lues par `CLIENT LIST`, mises à jour par sa connexion).
pub struct ClientInfo {
    pub id: u64,
    pub addr: String,
    pub laddr: String,
    pub connected_at: Instant,
    last_active: Mutex<Instant>,
    cmd_count: AtomicU64,
    db: AtomicUsize,
    name: Mutex<String>,
    last_cmd: Mutex<String>,
    lib_name: Mutex<String>,
    lib_ver: Mutex<String>,
    user: Mutex<String>,
    pubsub_id: AtomicU64,
    killed: AtomicBool,
    kill_signal: Notify,
    /// CLIENT UNBLOCK : 0=none, 1=timeout, 2=error.
    unblock: AtomicU64,
    unblock_signal: Notify,
    no_evict: AtomicBool,
    no_touch: AtomicBool,
    tracking: AtomicBool,
    resp: AtomicU64,
    redir: AtomicI64,
}

impl ClientInfo {
    fn new(id: u64, addr: String, laddr: String) -> Self {
        let now = Instant::now();
        Self {
            id,
            addr,
            laddr,
            connected_at: now,
            last_active: Mutex::new(now),
            cmd_count: AtomicU64::new(0),
            db: AtomicUsize::new(0),
            name: Mutex::new(String::new()),
            last_cmd: Mutex::new(String::new()),
            lib_name: Mutex::new(String::new()),
            lib_ver: Mutex::new(String::new()),
            user: Mutex::new("default".into()),
            pubsub_id: AtomicU64::new(0),
            killed: AtomicBool::new(false),
            kill_signal: Notify::new(),
            unblock: AtomicU64::new(0),
            unblock_signal: Notify::new(),
            no_evict: AtomicBool::new(false),
            no_touch: AtomicBool::new(false),
            tracking: AtomicBool::new(false),
            resp: AtomicU64::new(2),
            redir: AtomicI64::new(-1),
        }
    }

    /// Enregistre l'exécution d'une commande (nom stocké en minuscules).
    pub fn touch(&self, cmd: &str, db: usize) {
        *self.last_active.lock() = Instant::now();
        self.cmd_count.fetch_add(1, Ordering::Relaxed);
        self.db.store(db, Ordering::Relaxed);
        let mut last = self.last_cmd.lock();
        last.clear();
        last.extend(cmd.chars().map(|c| c.to_ascii_lowercase()));
    }

    pub fn set_name(&self, name: &str) {
        *self.name.lock() = name.to_string();
    }

    pub fn name(&self) -> String {
        self.name.lock().clone()
    }

    pub fn set_lib(&self, attr: &str, value: &str) {
        match attr.to_ascii_lowercase().as_str() {
            "lib-name" => *self.lib_name.lock() = value.to_string(),
            "lib-ver" => *self.lib_ver.lock() = value.to_string(),
            _ => {}
        }
    }

    pub fn set_user(&self, user: &str) {
        *self.user.lock() = user.to_string();
    }

    pub fn user(&self) -> String {
        self.user.lock().clone()
    }

    pub fn set_no_evict(&self, on: bool) {
        self.no_evict.store(on, Ordering::Relaxed);
    }

    pub fn set_no_touch(&self, on: bool) {
        self.no_touch.store(on, Ordering::Relaxed);
    }

    pub fn set_tracking(&self, on: bool) {
        self.tracking.store(on, Ordering::Relaxed);
    }

    pub fn tracking(&self) -> bool {
        self.tracking.load(Ordering::Relaxed)
    }

    pub fn set_resp(&self, v: u8) {
        self.resp.store(u64::from(v), Ordering::Relaxed);
    }

    pub fn resp(&self) -> u8 {
        self.resp.load(Ordering::Relaxed) as u8
    }

    pub fn set_redir(&self, id: i64) {
        self.redir.store(id, Ordering::Relaxed);
    }

    pub fn redir(&self) -> i64 {
        self.redir.load(Ordering::Relaxed)
    }

    /// Demande CLIENT UNBLOCK (1=TIMEOUT, 2=ERROR).
    pub fn request_unblock(&self, mode: u64) {
        self.unblock.store(mode, Ordering::SeqCst);
        self.unblock_signal.notify_one();
    }

    pub fn take_unblock(&self) -> u64 {
        self.unblock.swap(0, Ordering::SeqCst)
    }

    pub async fn unblocked(&self) {
        self.unblock_signal.notified().await;
    }

    /// Identifiant d'abonnement pub/sub associé à la connexion (0 = aucun).
    pub fn set_pubsub_id(&self, id: u64) {
        self.pubsub_id.store(id, Ordering::Relaxed);
    }

    pub fn pubsub_id(&self) -> u64 {
        self.pubsub_id.load(Ordering::Relaxed)
    }

    pub fn commands_processed(&self) -> u64 {
        self.cmd_count.load(Ordering::Relaxed)
    }

    /// Demande la fermeture de la connexion (CLIENT KILL).
    pub fn kill(&self) {
        self.killed.store(true, Ordering::SeqCst);
        self.kill_signal.notify_one();
    }

    pub fn is_killed(&self) -> bool {
        self.killed.load(Ordering::SeqCst)
    }

    /// Future complétée lorsque le client est tué.
    pub async fn killed(&self) {
        if !self.is_killed() {
            self.kill_signal.notified().await;
        }
    }

    /// Ligne au format `CLIENT LIST`.
    pub fn list_line(
        &self,
        (subs, psubs, ssubs): (usize, usize, usize),
        multi: bool,
        replica: bool,
    ) -> String {
        let age = self.connected_at.elapsed().as_secs();
        let idle = self.last_active.lock().elapsed().as_secs();
        let flags = if replica {
            "S"
        } else if subs + psubs + ssubs > 0 {
            "P"
        } else if multi {
            "x"
        } else {
            "N"
        };
        format!(
            "id={} addr={} laddr={} fd={} name={} age={} idle={} flags={} db={} sub={} psub={} ssub={} multi={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={} user={} redir={} resp={} lib-name={} lib-ver={} io-thread=0 tot-net-in=0 tot-net-out=0 tot-cmds={}",
            self.id,
            self.addr,
            self.laddr,
            self.id + 7,
            self.name.lock(),
            age,
            idle,
            flags,
            self.db.load(Ordering::Relaxed),
            subs,
            psubs,
            ssubs,
            if multi { 0 } else { -1 },
            self.last_cmd.lock(),
            self.user.lock(),
            self.redir(),
            self.resp(),
            self.lib_name.lock(),
            self.lib_ver.lock(),
            self.commands_processed(),
        )
    }
}

/// Registre global : clients vivants, compteurs cumulés et journal d'évènements.
pub struct ClientRegistry {
    next_id: AtomicU64,
    clients: DashMap<u64, Arc<ClientInfo>>,
    events: Mutex<VecDeque<ClientEvent>>,
    total_connections: AtomicU64,
    total_commands: AtomicU64,
    auth_failures: AtomicU64,
    rejected: AtomicU64,
}

pub type SharedClients = Arc<ClientRegistry>;

impl Default for ClientRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientRegistry {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            clients: DashMap::new(),
            events: Mutex::new(VecDeque::with_capacity(EVENT_LOG_CAPACITY)),
            total_connections: AtomicU64::new(0),
            total_commands: AtomicU64::new(0),
            auth_failures: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        }
    }

    pub fn register(&self, addr: String, laddr: String) -> Arc<ClientInfo> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let info = Arc::new(ClientInfo::new(id, addr.clone(), laddr));
        self.clients.insert(id, Arc::clone(&info));
        self.total_connections.fetch_add(1, Ordering::Relaxed);
        self.push_event(id, addr, EventKind::Connected, String::new());
        info
    }

    pub fn unregister(&self, info: &ClientInfo, reason: &str) {
        if self.clients.remove(&info.id).is_some() {
            let kind = if info.is_killed() {
                EventKind::Killed
            } else {
                EventKind::Disconnected
            };
            self.push_event(info.id, info.addr.clone(), kind, reason.to_string());
        }
    }

    pub fn push_event(&self, client_id: u64, addr: String, kind: EventKind, detail: String) {
        let mut log = self.events.lock();
        if log.len() == EVENT_LOG_CAPACITY {
            log.pop_front();
        }
        log.push_back(ClientEvent {
            at_unix_ms: unix_ms(),
            client_id,
            addr,
            kind,
            detail,
        });
    }

    pub fn record_auth_failure(&self, info: Option<&ClientInfo>) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
        let (id, addr) = info
            .map(|i| (i.id, i.addr.clone()))
            .unwrap_or((0, "?".into()));
        self.push_event(id, addr, EventKind::AuthFailed, "invalid password".into());
    }

    pub fn record_rejected(&self, addr: String, why: &str) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        self.push_event(0, addr, EventKind::ProtocolError, why.to_string());
    }

    pub fn count_command(&self) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connected(&self) -> usize {
        self.clients.len()
    }

    pub fn total_connections(&self) -> u64 {
        self.total_connections.load(Ordering::Relaxed)
    }

    pub fn total_commands(&self) -> u64 {
        self.total_commands.load(Ordering::Relaxed)
    }

    pub fn auth_failures(&self) -> u64 {
        self.auth_failures.load(Ordering::Relaxed)
    }

    pub fn rejected(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    /// Clients triés par identifiant.
    pub fn snapshot(&self) -> Vec<Arc<ClientInfo>> {
        let mut v: Vec<_> = self.clients.iter().map(|e| Arc::clone(e.value())).collect();
        v.sort_by_key(|c| c.id);
        v
    }

    pub fn get(&self, id: u64) -> Option<Arc<ClientInfo>> {
        self.clients.get(&id).map(|e| Arc::clone(e.value()))
    }

    /// Les `limit` évènements les plus récents (du plus récent au plus ancien).
    pub fn recent_events(&self, limit: usize) -> Vec<ClientEvent> {
        self.events.lock().iter().rev().take(limit).cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_unregister_logs_events() {
        let reg = ClientRegistry::new();
        let c = reg.register("127.0.0.1:1".into(), "127.0.0.1:6379".into());
        assert_eq!(reg.connected(), 1);
        c.touch("get", 2);
        assert!(c.list_line((0, 0, 0), false, false).contains("db=2"));
        reg.unregister(&c, "eof");
        assert_eq!(reg.connected(), 0);
        let ev = reg.recent_events(10);
        assert_eq!(ev[0].kind, EventKind::Disconnected);
        assert_eq!(ev[1].kind, EventKind::Connected);
    }

    #[test]
    fn event_log_is_bounded() {
        let reg = ClientRegistry::new();
        for i in 0..(EVENT_LOG_CAPACITY + 50) {
            reg.push_event(i as u64, "x".into(), EventKind::Connected, String::new());
        }
        assert_eq!(reg.recent_events(usize::MAX).len(), EVENT_LOG_CAPACITY);
    }
}
