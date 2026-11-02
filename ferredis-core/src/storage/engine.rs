//! Orchestration hot (RAM) / warm (compressé) / cold (disque).

use crate::mem::{compress_string_bytes, decompress_string_bytes, memory_tracker, SharedMemoryTracker};
use crate::storage::ColdStore;
use bytes::Bytes;
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    Memory,
    Hybrid,
}

impl StorageMode {
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "hybrid" => Self::Hybrid,
            _ => Self::Memory,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Hybrid => "hybrid",
        }
    }
}

#[derive(Debug, Default)]
pub struct StorageStats {
    pub hot_bytes: AtomicU64,
    pub warm_bytes: AtomicU64,
    pub cold_bytes: AtomicU64,
    pub promotions: AtomicU64,
    pub demotions: AtomicU64,
}

pub struct StorageEngine {
    pub mode: RwLock<StorageMode>,
    pub compress_threshold: AtomicU64,
    pub cold_dir: RwLock<PathBuf>,
    pub cold: RwLock<Option<ColdStore>>,
    pub stats: StorageStats,
    pub mem: SharedMemoryTracker,
}

pub type SharedStorage = Arc<StorageEngine>;

impl StorageEngine {
    pub fn new() -> SharedStorage {
        Arc::new(Self {
            mode: RwLock::new(StorageMode::Memory),
            compress_threshold: AtomicU64::new(128),
            cold_dir: RwLock::new(PathBuf::from("./ferredis-cold")),
            cold: RwLock::new(None),
            stats: StorageStats::default(),
            mem: memory_tracker(),
        })
    }

    pub fn ensure_cold(&self) -> std::io::Result<()> {
        if self.cold.read().is_some() {
            return Ok(());
        }
        let dir = self.cold_dir.read().clone();
        let store = ColdStore::open(&dir)?;
        *self.cold.write() = Some(store);
        Ok(())
    }

    pub fn set_mode(&self, mode: StorageMode) {
        *self.mode.write() = mode;
        if mode == StorageMode::Hybrid {
            let _ = self.ensure_cold();
        }
    }

    pub fn mode(&self) -> StorageMode {
        *self.mode.read()
    }

    /// Compresse une string pour le tier warm si pertinent.
    pub fn maybe_compress(&self, raw: Bytes) -> Bytes {
        let th = self.compress_threshold.load(Ordering::Relaxed) as usize;
        match compress_string_bytes(&raw, th) {
            Some(c) => {
                let saved = raw.len().saturating_sub(c.len()) as u64;
                self.mem.record_compress_save(saved);
                self.stats
                    .warm_bytes
                    .fetch_add(c.len() as u64, Ordering::Relaxed);
                c
            }
            None => raw,
        }
    }

    pub fn expand_string(&self, stored: &Bytes) -> Bytes {
        decompress_string_bytes(stored)
    }

    /// Démote une valeur string vers le cold tier (clé logique = db:key).
    pub fn demote_string(&self, db: usize, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        if self.mode() != StorageMode::Hybrid {
            return Ok(());
        }
        self.ensure_cold()?;
        let mut full = Vec::with_capacity(8 + key.len());
        full.extend_from_slice(&(db as u32).to_le_bytes());
        full.extend_from_slice(key);
        if let Some(cold) = self.cold.read().as_ref() {
            cold.put(&full, value)?;
            self.stats.demotions.fetch_add(1, Ordering::Relaxed);
            self.mem.record_demotion();
            self.stats
                .cold_bytes
                .fetch_add(value.len() as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn promote_string(&self, db: usize, key: &[u8]) -> std::io::Result<Option<Bytes>> {
        if self.mode() != StorageMode::Hybrid {
            return Ok(None);
        }
        self.ensure_cold()?;
        let mut full = Vec::with_capacity(8 + key.len());
        full.extend_from_slice(&(db as u32).to_le_bytes());
        full.extend_from_slice(key);
        if let Some(cold) = self.cold.read().as_ref() {
            if let Some(v) = cold.get(&full)? {
                cold.remove(&full);
                self.stats.promotions.fetch_add(1, Ordering::Relaxed);
                self.mem.record_promotion();
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    pub fn info_lines(&self) -> String {
        format!(
            "storage_mode:{}\r\ncompress_threshold:{}\r\nhot_bytes:{}\r\nwarm_bytes:{}\r\ncold_bytes:{}\r\ntier_promotions:{}\r\ntier_demotions:{}\r\n",
            self.mode().as_str(),
            self.compress_threshold.load(Ordering::Relaxed),
            self.stats.hot_bytes.load(Ordering::Relaxed),
            self.stats.warm_bytes.load(Ordering::Relaxed),
            self.stats.cold_bytes.load(Ordering::Relaxed),
            self.stats.promotions.load(Ordering::Relaxed),
            self.stats.demotions.load(Ordering::Relaxed),
        )
    }
}
