//! Active defrag : recopie des entrées pour réduire la fragmentation allocator.

use crate::store::SharedDb;
use crate::value::Entry;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct DefragStats {
    pub runs: AtomicU64,
    pub keys_moved: AtomicU64,
    pub running: AtomicU64,
}

impl DefragStats {
    pub fn snapshot(&self) -> (u64, u64, bool) {
        (
            self.runs.load(Ordering::Relaxed),
            self.keys_moved.load(Ordering::Relaxed),
            self.running.load(Ordering::Relaxed) > 0,
        )
    }
}

/// Une passe bornée : clone/reinsert jusqu'à `budget` clés.
pub fn active_defrag_pass(db: &SharedDb, budget: usize) -> usize {
    let mut moved = 0usize;
    let n = db.db_count();
    for di in 0..n {
        if moved >= budget {
            break;
        }
        let keys: Vec<_> = db.with_db(di, |inner| {
            inner
                .map
                .keys()
                .take(budget.saturating_sub(moved))
                .cloned()
                .collect()
        });
        for key in keys {
            if moved >= budget {
                break;
            }
            let Some(entry) = db.get_entry(di, &key) else {
                continue;
            };
            // Recopie profonde : force de nouvelles allocations.
            let fresh = Entry {
                value: clone_compact(&entry.value),
                expires_at: entry.expires_at,
                version: entry.version,
                last_access: entry.last_access,
                lfu_freq: entry.lfu_freq,
            };
            let _ = db.put_entry(di, key, fresh, true);
            moved += 1;
        }
    }
    moved
}

fn clone_compact(v: &crate::value::RedisValue) -> crate::value::RedisValue {
    // Clone standard suffit : les Bytes partagés sont re-matérialsés via copy_from_slice
    // pour les strings compressables.
    use crate::value::RedisValue;
    match v {
        RedisValue::String(s) => RedisValue::String(Bytes::copy_from_slice(s)),
        other => other.clone(),
    }
}

use bytes::Bytes;
