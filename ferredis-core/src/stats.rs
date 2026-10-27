//! Statistiques runtime (INFO, CONFIG RESETSTAT, futur COMMANDSTATS).

use dashmap::DashMap;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
pub struct ServerStats {
    keyspace_hits: AtomicU64,
    keyspace_misses: AtomicU64,
    evicted_keys: AtomicU64,
    errorstats: DashMap<String, AtomicU64>,
    per_command: DashMap<String, AtomicU64>,
    ops_window: Mutex<OpsWindow>,
}

#[derive(Debug)]
struct OpsWindow {
    start: Instant,
    count: u64,
}

impl Default for ServerStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerStats {
    pub fn new() -> Self {
        Self {
            keyspace_hits: AtomicU64::new(0),
            keyspace_misses: AtomicU64::new(0),
            evicted_keys: AtomicU64::new(0),
            errorstats: DashMap::new(),
            per_command: DashMap::new(),
            ops_window: Mutex::new(OpsWindow {
                start: Instant::now(),
                count: 0,
            }),
        }
    }

    pub fn record_evicted(&self) {
        self.evicted_keys.fetch_add(1, Ordering::Relaxed);
    }

    pub fn evicted_keys(&self) -> u64 {
        self.evicted_keys.load(Ordering::Relaxed)
    }

    pub fn record_keyspace_hit(&self) {
        self.keyspace_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_keyspace_miss(&self) {
        self.keyspace_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_command(&self, cmd: &str) {
        let key = cmd.to_ascii_lowercase();
        self.per_command
            .entry(key)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
        let mut w = self.ops_window.lock();
        w.count += 1;
    }

    pub fn record_error(&self, err_prefix: &str) {
        self.errorstats
            .entry(err_prefix.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn keyspace_hits(&self) -> u64 {
        self.keyspace_hits.load(Ordering::Relaxed)
    }

    pub fn keyspace_misses(&self) -> u64 {
        self.keyspace_misses.load(Ordering::Relaxed)
    }

    pub fn instantaneous_ops(&self) -> u64 {
        let w = self.ops_window.lock();
        let elapsed = w.start.elapsed().as_secs_f64().max(0.001);
        (w.count as f64 / elapsed) as u64
    }

    pub fn reset(&self) {
        self.keyspace_hits.store(0, Ordering::Relaxed);
        self.keyspace_misses.store(0, Ordering::Relaxed);
        self.evicted_keys.store(0, Ordering::Relaxed);
        self.errorstats.clear();
        self.per_command.clear();
        let mut w = self.ops_window.lock();
        w.start = Instant::now();
        w.count = 0;
    }

    pub fn commandstats_section(&self) -> String {
        let mut lines = String::new();
        let mut keys: Vec<_> = self
            .per_command
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        keys.sort_by(|a, b| a.0.cmp(&b.0));
        for (name, calls) in keys {
            use std::fmt::Write;
            let _ = write!(
                lines,
                "cmdstat_{name}:calls={calls},usec=0,usec_per_call=0.00\r\n"
            );
        }
        lines
    }

    pub fn errorstats_section(&self) -> String {
        let mut lines = String::new();
        for e in self.errorstats.iter() {
            use std::fmt::Write;
            let _ = write!(
                lines,
                "errorstat_{}:count={}\r\n",
                e.key(),
                e.value().load(Ordering::Relaxed)
            );
        }
        lines
    }
}

pub type SharedStats = Arc<ServerStats>;

pub fn shared_stats() -> SharedStats {
    Arc::new(ServerStats::new())
}
