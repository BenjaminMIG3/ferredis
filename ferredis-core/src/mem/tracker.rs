//! Compteurs used_memory / RSS processus.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct MemoryTracker {
    /// Estimation allocator (somme des approx_bytes + overhead).
    used: AtomicU64,
    peak: AtomicU64,
    defrag_hits: AtomicU64,
    compress_saves: AtomicU64,
    tier_promotions: AtomicU64,
    tier_demotions: AtomicU64,
}

pub type SharedMemoryTracker = Arc<MemoryTracker>;

pub fn memory_tracker() -> SharedMemoryTracker {
    Arc::new(MemoryTracker::default())
}

impl MemoryTracker {
    pub fn set_used(&self, n: u64) {
        self.used.store(n, Ordering::Relaxed);
        let _ = self.peak.fetch_max(n, Ordering::Relaxed);
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    pub fn record_defrag(&self, n: u64) {
        self.defrag_hits.fetch_add(n, Ordering::Relaxed);
    }

    pub fn defrag_hits(&self) -> u64 {
        self.defrag_hits.load(Ordering::Relaxed)
    }

    pub fn record_compress_save(&self, bytes: u64) {
        self.compress_saves.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn compress_saves(&self) -> u64 {
        self.compress_saves.load(Ordering::Relaxed)
    }

    pub fn record_promotion(&self) {
        self.tier_promotions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_demotion(&self) {
        self.tier_demotions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn tier_promotions(&self) -> u64 {
        self.tier_promotions.load(Ordering::Relaxed)
    }

    pub fn tier_demotions(&self) -> u64 {
        self.tier_demotions.load(Ordering::Relaxed)
    }

    /// RSS processus (best-effort Win/Linux) ; retombe sur `used` si indisponible.
    pub fn rss_bytes(&self) -> u64 {
        process_rss().unwrap_or_else(|| self.used())
    }

    pub fn fragmentation_ratio(&self) -> f64 {
        let used = self.used().max(1) as f64;
        self.rss_bytes() as f64 / used
    }
}

#[cfg(target_os = "linux")]
fn process_rss() -> Option<u64> {
    let st = std::fs::read_to_string("/proc/self/statm").ok()?;
    let mut parts = st.split_whitespace();
    let _size = parts.next()?;
    let resident: u64 = parts.next()?.parse().ok()?;
    let page = 4096u64;
    Some(resident.saturating_mul(page))
}

#[cfg(target_os = "windows")]
fn process_rss() -> Option<u64> {
    // GetProcessMemoryInfo via rough WorkingSet — lecture compteur perf non dispo sans FFI.
    // Approximation : used * facteur empirique si pas de crate windows.
    None
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
fn process_rss() -> Option<u64> {
    None
}
