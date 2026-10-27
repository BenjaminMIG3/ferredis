//! MONITOR — fan-out des commandes vers les clients en mode surveillance.

use bytes::Bytes;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct MonitorHub {
    next_id: AtomicU64,
    subs: DashMap<u64, mpsc::UnboundedSender<Bytes>>,
}

impl MonitorHub {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            subs: DashMap::new(),
        }
    }

    pub fn subscribe(&self) -> (u64, mpsc::UnboundedReceiver<Bytes>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        self.subs.insert(id, tx);
        (id, rx)
    }

    pub fn unsubscribe(&self, id: u64) {
        self.subs.remove(&id);
    }

    pub fn fanout(&self, line: Bytes) {
        self.subs.retain(|_, tx| tx.send(line.clone()).is_ok());
    }

    pub fn count(&self) -> usize {
        self.subs.len()
    }
}

pub type SharedMonitor = Arc<MonitorHub>;

pub fn shared_monitor() -> SharedMonitor {
    Arc::new(MonitorHub::new())
}

/// Format Redis MONITOR : `"ts [db addr] \"cmd\" \"arg\"..."`.
pub fn format_monitor_line(
    unix_sec: f64,
    db: usize,
    addr: &str,
    args: &[Bytes],
) -> Bytes {
    let mut s = format!("{unix_sec:.6} [{db} {addr}]");
    for a in args {
        let escaped = String::from_utf8_lossy(a).replace('\\', "\\\\").replace('"', "\\\"");
        s.push_str(&format!(" \"{escaped}\""));
    }
    s.push_str("\r\n");
    Bytes::from(s)
}
