//! Journal SLOWLOG (ring buffer).

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct SlowLogEntry {
    pub id: u64,
    pub unix_time: i64,
    pub duration_us: u64,
    pub args: Vec<Bytes>,
    pub client_addr: String,
    pub client_name: String,
}

#[derive(Debug)]
pub struct SlowLog {
    entries: Mutex<VecDeque<SlowLogEntry>>,
    next_id: AtomicU64,
    max_len: Mutex<usize>,
    slower_than_us: Mutex<u64>,
}

impl SlowLog {
    pub fn new(max_len: usize, slower_than_us: u64) -> Self {
        Self {
            entries: Mutex::new(VecDeque::new()),
            next_id: AtomicU64::new(1),
            max_len: Mutex::new(max_len),
            slower_than_us: Mutex::new(slower_than_us),
        }
    }

    pub fn set_max_len(&self, n: usize) {
        *self.max_len.lock() = n.max(1);
        let mut q = self.entries.lock();
        while q.len() > *self.max_len.lock() {
            q.pop_front();
        }
    }

    pub fn set_slower_than_us(&self, us: u64) {
        *self.slower_than_us.lock() = us;
    }

    pub fn max_len(&self) -> usize {
        *self.max_len.lock()
    }

    pub fn slower_than_us(&self) -> u64 {
        *self.slower_than_us.lock()
    }

    pub fn maybe_record(
        &self,
        duration_us: u64,
        args: &[Bytes],
        client_addr: &str,
        client_name: &str,
    ) {
        if duration_us < *self.slower_than_us.lock() {
            return;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let unix_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let entry = SlowLogEntry {
            id,
            unix_time,
            duration_us,
            args: args.to_vec(),
            client_addr: client_addr.to_string(),
            client_name: client_name.to_string(),
        };
        let mut q = self.entries.lock();
        let max = *self.max_len.lock();
        if q.len() >= max {
            q.pop_front();
        }
        q.push_back(entry);
    }

    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn reset(&self) {
        self.entries.lock().clear();
    }

    pub fn get(&self, count: usize) -> Vec<SlowLogEntry> {
        let q = self.entries.lock();
        q.iter().rev().take(count).cloned().collect()
    }
}

pub type SharedSlowLog = Arc<SlowLog>;

pub fn shared_slowlog() -> SharedSlowLog {
    Arc::new(SlowLog::new(128, 10_000))
}
