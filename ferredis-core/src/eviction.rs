//! Gestion `maxmemory` + politiques d'éviction (SOLID : module dédié, DRY : un seul gate).

use crate::command::{ClientState, CommandContext};
use crate::command_registry;
use crate::resp::RespValue;
use crate::util::err;
use bytes::Bytes;

pub const OOM_ERR: &str =
    "OOM command not allowed when used memory > 'maxmemory'.";

/// Policies Redis `maxmemory-policy` (validation unique).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxMemoryPolicy {
    NoEviction,
    AllKeysLru,
    VolatileLru,
    AllKeysLfu,
    VolatileLfu,
    AllKeysRandom,
    VolatileRandom,
    VolatileTtl,
    /// Ferredis : demote/compress avant éviction.
    AllKeysTiered,
    VolatileTiered,
}

impl MaxMemoryPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "noeviction" => Some(Self::NoEviction),
            "allkeys-lru" => Some(Self::AllKeysLru),
            "volatile-lru" => Some(Self::VolatileLru),
            "allkeys-lfu" => Some(Self::AllKeysLfu),
            "volatile-lfu" => Some(Self::VolatileLfu),
            "allkeys-random" => Some(Self::AllKeysRandom),
            "volatile-random" => Some(Self::VolatileRandom),
            "volatile-ttl" => Some(Self::VolatileTtl),
            "allkeys-tiered" => Some(Self::AllKeysTiered),
            "volatile-tiered" => Some(Self::VolatileTiered),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllKeysLru => "allkeys-lru",
            Self::VolatileLru => "volatile-lru",
            Self::AllKeysLfu => "allkeys-lfu",
            Self::VolatileLfu => "volatile-lfu",
            Self::AllKeysRandom => "allkeys-random",
            Self::VolatileRandom => "volatile-random",
            Self::VolatileTtl => "volatile-ttl",
            Self::AllKeysTiered => "allkeys-tiered",
            Self::VolatileTiered => "volatile-tiered",
        }
    }

    fn as_evict_base(self) -> Self {
        match self {
            Self::AllKeysTiered => Self::AllKeysLru,
            Self::VolatileTiered => Self::VolatileLru,
            other => other,
        }
    }
}

/// Avant une commande `write`+`denyoom` : évince si besoin, sinon OOM.
pub fn prepare_write(
    ctx: &CommandContext,
    state: &ClientState,
    cmd: &str,
) -> Result<(), RespValue> {
    let spec = match command_registry::lookup(cmd) {
        Some(s) if s.has_flag("write") && s.has_flag("denyoom") => s,
        _ => return Ok(()),
    };
    let _ = spec;
    let (maxmemory, soft, policy_s, compress_th) = {
        let cfg = ctx.config.read();
        (
            cfg.maxmemory as usize,
            cfg.maxmemory_soft as usize,
            cfg.maxmemory_policy.clone(),
            cfg.compress_threshold as usize,
        )
    };
    if maxmemory == 0 && soft == 0 {
        return Ok(());
    }
    let policy = MaxMemoryPolicy::parse(&policy_s).unwrap_or(MaxMemoryPolicy::NoEviction);
    let used = ctx.db.approx_memory();
    // Soft limit : compression / demote avant le plafond dur.
    let pressure = if soft > 0 { soft } else { maxmemory };
    if pressure > 0 && used >= pressure {
        let _ = relieve_pressure(ctx, compress_th, policy);
    }
    if maxmemory == 0 || used < maxmemory && ctx.db.approx_memory() < maxmemory {
        return Ok(());
    }
    if ctx.db.approx_memory() < maxmemory {
        return Ok(());
    }
    if state.no_evict || policy == MaxMemoryPolicy::NoEviction {
        return Err(err(OOM_ERR));
    }
    let evict_policy = policy.as_evict_base();
    let mut freed = 0usize;
    for _ in 0..16 {
        if ctx.db.approx_memory() < maxmemory {
            break;
        }
        match evict_one(ctx, evict_policy) {
            Some(n) => {
                freed += n;
                ctx.config.read().stats.record_evicted();
            }
            None => break,
        }
    }
    if ctx.db.approx_memory() >= maxmemory {
        let _ = freed;
        return Err(err(OOM_ERR));
    }
    Ok(())
}

/// expire déjà géré ailleurs ; ici compress + demote cold.
fn relieve_pressure(
    ctx: &CommandContext,
    compress_th: usize,
    policy: MaxMemoryPolicy,
) -> usize {
    let mut saved = 0usize;
    let samples = ctx.db.sample_keys_for_eviction(0, 8, policy.as_evict_base());
    for (key, _) in samples {
        if let Some(entry) = ctx.db.get_entry(0, &key) {
            if let crate::value::RedisValue::String(s) = &entry.value {
                if let Some(c) = crate::mem::compress_string_bytes(s, compress_th) {
                    saved += s.len().saturating_sub(c.len());
                    let mut e = entry.clone();
                    e.value = crate::value::RedisValue::String(c);
                    let _ = ctx.db.put_entry(0, key.clone(), e, true);
                } else if matches!(
                    policy,
                    MaxMemoryPolicy::AllKeysTiered | MaxMemoryPolicy::VolatileTiered
                ) {
                    let _ = ctx.storage.demote_string(0, &key, s);
                    // Remplace par stub froid (payload vide + marqueur).
                    let marker = Bytes::from_static(b"\0FRCL");
                    let mut e = entry.clone();
                    e.value = crate::value::RedisValue::String(marker);
                    let _ = ctx.db.put_entry(0, key, e, true);
                    saved += s.len();
                }
            }
        }
    }
    saved
}

fn evict_one(ctx: &CommandContext, policy: MaxMemoryPolicy) -> Option<usize> {
    let dbn = ctx.db.db_count();
    let mut best: Option<(usize, Bytes, i64)> = None; // (db, key, score) — plus petit score = évincer
    let samples = 5usize;
    for db in 0..dbn {
        let picks = ctx.db.sample_keys_for_eviction(db, samples, policy);
        for (key, score) in picks {
            match best {
                None => best = Some((db, key, score)),
                Some((_, _, s)) if score < s => best = Some((db, key, score)),
                _ => {}
            }
        }
    }
    let (db, key, _) = best?;
    let before = ctx.db.approx_memory();
    let _ = ctx.db.del(db, &[key]);
    let after = ctx.db.approx_memory();
    Some(before.saturating_sub(after))
}

/// Score d'éviction : LRU = idle secs (plus grand = meilleur candidat → on inverse en négatif),
/// LFU = -freq, TTL = -pttl, random = random.
pub(crate) fn eviction_score(
    policy: MaxMemoryPolicy,
    idle_secs: i64,
    freq: u8,
    pttl_ms: i64,
    rnd: i64,
) -> i64 {
    match policy {
        MaxMemoryPolicy::AllKeysLru | MaxMemoryPolicy::VolatileLru => -idle_secs,
        MaxMemoryPolicy::AllKeysLfu | MaxMemoryPolicy::VolatileLfu => -(freq as i64),
        MaxMemoryPolicy::VolatileTtl => {
            if pttl_ms < 0 {
                i64::MAX / 4
            } else {
                -pttl_ms
            }
        }
        MaxMemoryPolicy::AllKeysRandom | MaxMemoryPolicy::VolatileRandom => rnd,
        MaxMemoryPolicy::AllKeysTiered | MaxMemoryPolicy::VolatileTiered => -idle_secs,
        MaxMemoryPolicy::NoEviction => i64::MAX,
    }
}

