//! Keyspace shardé (F2) — N shards par DB logique, locks indépendants.

use crate::store::DbInner;
use bytes::Bytes;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Nombre de shards par défaut (puissance de 2).
pub const DEFAULT_SHARDS: usize = 16;

/// Calcule le shard d'une clé (FNV-1a).
#[inline]
pub fn shard_index(key: &[u8], n: usize) -> usize {
    let n = n.next_power_of_two().max(1);
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in key {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    (h as usize) & (n - 1)
}

/// Une DB logique découpée en shards verrouillables séparément.
pub struct ShardedDb {
    shards: Vec<RwLock<DbInner>>,
    n: AtomicUsize,
}

impl ShardedDb {
    #[allow(dead_code)] // API F2 prête ; intégration progressive du store plat.
    pub fn new(n: usize) -> Self {
        let n = n.next_power_of_two().max(1);
        let mut shards = Vec::with_capacity(n);
        for _ in 0..n {
            shards.push(RwLock::new(DbInner::default()));
        }
        Self {
            shards,
            n: AtomicUsize::new(n),
        }
    }

    pub fn shard_count(&self) -> usize {
        self.n.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub(crate) fn read(&self, key: &[u8]) -> RwLockReadGuard<'_, DbInner> {
        let i = shard_index(key, self.shard_count());
        self.shards[i].read()
    }

    #[allow(dead_code)]
    pub(crate) fn write(&self, key: &[u8]) -> RwLockWriteGuard<'_, DbInner> {
        let i = shard_index(key, self.shard_count());
        self.shards[i].write()
    }

    /// Verrouille plusieurs shards dans l'ordre croissant (évite deadlock).
    #[allow(dead_code)]
    pub(crate) fn lock_keys_write(
        &self,
        keys: &[Bytes],
    ) -> Vec<(usize, RwLockWriteGuard<'_, DbInner>)> {
        let mut idxs: Vec<usize> = keys
            .iter()
            .map(|k| shard_index(k, self.shard_count()))
            .collect();
        idxs.sort_unstable();
        idxs.dedup();
        idxs.into_iter()
            .map(|i| (i, self.shards[i].write()))
            .collect()
    }

    pub fn approx_memory(&self) -> usize {
        self.shards
            .iter()
            .map(|s| {
                s.read()
                    .map
                    .iter()
                    .map(|(k, e)| k.len() + e.value.approx_bytes() + 64)
                    .sum::<usize>()
            })
            .sum()
    }
}

impl Default for ShardedDb {
    fn default() -> Self {
        Self::new(DEFAULT_SHARDS)
    }
}
