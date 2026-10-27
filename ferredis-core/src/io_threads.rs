//! Pool I/O configurable (`io-threads`) — SOLID : hors dispatch commandes.
//!
//! - `io_threads <= 1` : exécution inline sur le runtime Tokio.
//! - `io_threads > 1` : workers dédiés pour tâches auxiliaires.

use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Debug, Default)]
pub struct IoThreadStats {
    pub reads_processed: AtomicU64,
    pub writes_processed: AtomicU64,
}

struct QueueState {
    jobs: Vec<Box<dyn FnOnce() + Send>>,
    stop: bool,
}

struct Shared {
    state: Mutex<QueueState>,
    cv: Condvar,
}

pub struct IoThreadPool {
    n: AtomicUsize,
    stats: IoThreadStats,
    shared: Arc<Shared>,
    _workers: Vec<JoinHandle<()>>,
}

impl IoThreadPool {
    pub fn spawn(n: usize) -> Arc<Self> {
        let n = n.max(1);
        let shared = Arc::new(Shared {
            state: Mutex::new(QueueState {
                jobs: Vec::new(),
                stop: false,
            }),
            cv: Condvar::new(),
        });
        let mut workers = Vec::new();
        if n > 1 {
            for i in 0..n {
                let shared = Arc::clone(&shared);
                let stats = /* filled after */ ();
                let _ = stats;
                workers.push(
                    thread::Builder::new()
                        .name(format!("ferredis-io-{i}"))
                        .spawn(move || worker_loop(shared))
                        .expect("io thread"),
                );
            }
        }
        Arc::new(Self {
            n: AtomicUsize::new(n),
            stats: IoThreadStats::default(),
            shared,
            _workers: workers,
        })
    }

    pub fn configure(&self, n: usize) {
        self.n.store(n.max(1), Ordering::Relaxed);
    }

    pub fn threads(&self) -> usize {
        self.n.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> &IoThreadStats {
        &self.stats
    }

    pub fn record_read(&self) {
        self.stats.reads_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_write(&self) {
        self.stats.writes_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn submit(&self, job: Box<dyn FnOnce() + Send>) {
        if self.threads() <= 1 {
            job();
            self.record_write();
            return;
        }
        {
            let mut st = self.shared.state.lock();
            st.jobs.push(job);
        }
        self.shared.cv.notify_one();
    }

    pub fn run_blocking<R: Send + 'static>(&self, f: impl FnOnce() -> R + Send + 'static) -> R {
        if self.threads() <= 1 {
            self.record_read();
            return f();
        }
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.submit(Box::new(move || {
            let _ = tx.send(f());
        }));
        let r = rx.recv().expect("io job");
        self.record_write();
        r
    }
}

fn worker_loop(shared: Arc<Shared>) {
    loop {
        let job = {
            let mut st = shared.state.lock();
            loop {
                if st.stop {
                    return;
                }
                if let Some(j) = st.jobs.pop() {
                    break Some(j);
                }
                shared.cv.wait_for(&mut st, Duration::from_millis(200));
            }
        };
        if let Some(job) = job {
            job();
        }
    }
}

static GLOBAL: std::sync::OnceLock<Arc<IoThreadPool>> = std::sync::OnceLock::new();

pub fn init_global(n: usize) -> Arc<IoThreadPool> {
    let p = IoThreadPool::spawn(n);
    let _ = GLOBAL.set(Arc::clone(&p));
    p
}

pub fn global() -> Option<&'static Arc<IoThreadPool>> {
    GLOBAL.get()
}
