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
    let (maxmemory, policy_s) = {
        let cfg = ctx.config.read();
        (cfg.maxmemory as usize, cfg.maxmemory_policy.clone())
    };
    if maxmemory == 0 {
        return Ok(());
    }
    let policy = MaxMemoryPolicy::parse(&policy_s).unwrap_or(MaxMemoryPolicy::NoEviction);
    let used = ctx.db.approx_memory();
    if used < maxmemory {
        return Ok(());
    }
    if state.no_evict || policy == MaxMemoryPolicy::NoEviction {
        return Err(err(OOM_ERR));
    }
    // Éviction par échantillons jusqu'à sous le plafond (budget borné).
    let mut freed = 0usize;
    for _ in 0..16 {
        if ctx.db.approx_memory() < maxmemory {
            break;
        }
        match evict_one(ctx, policy) {
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
        MaxMemoryPolicy::NoEviction => i64::MAX,
    }
}

