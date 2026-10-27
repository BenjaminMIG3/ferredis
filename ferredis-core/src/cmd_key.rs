//! Commandes génériques sur l'espace de clés : EXPIRE*, RENAME*, COPY, MOVE, OBJECT…

use crate::command::{wrong_arity, ClientState, CommandContext};
use crate::resp::RespValue;
use crate::store::DbInner;
use crate::util::*;
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Variante d'EXPIRE : unité (s/ms) et mode (relatif/absolu).
#[derive(Clone, Copy)]
pub(crate) struct ExpireMode {
    pub millis: bool,
    pub absolute: bool,
}

/// EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT `key time [NX|XX|GT|LT]`.
pub(crate) fn cmd_expire(
    ctx: &CommandContext,
    state: &ClientState,
    args: &[Bytes],
    mode: ExpireMode,
) -> RespValue {
    let name = upper(&args[0]).to_ascii_lowercase();
    if args.len() < 3 {
        return wrong_arity(&name);
    }
    let Some(n) = parse_i64(&args[2]) else {
        return err(ERR_INT);
    };
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for opt in &args[3..] {
        match upper(opt).as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GT" => gt = true,
            "LT" => lt = true,
            other => return err(&format!("ERR Unsupported option {other}")),
        }
    }
    if nx && (xx || gt || lt) {
        return err("ERR NX and XX, GT or LT options at the same time are not compatible");
    }
    if gt && lt {
        return err("ERR GT and LT options at the same time are not compatible");
    }

    let invalid = || err(&format!("ERR invalid expire time in '{name}' command"));
    let Some(raw_ms) = (if mode.millis { Some(n) } else { n.checked_mul(1000) }) else {
        return invalid();
    };
    let rel_ms = if mode.absolute {
        match raw_ms.checked_sub(now_unix_ms()) {
            Some(v) => v,
            None => return invalid(),
        }
    } else {
        raw_ms
    };

    let key = &args[1];
    let changed = ctx.db.with_db(state.db_index, |d| {
        let Some(e) = d.get_fresh_mut(key) else {
            return false;
        };
        let cur = e.pttl_ms(); // -1 : pas de TTL
        let has_ttl = cur >= 0;
        if (nx && has_ttl) || (xx && !has_ttl) {
            return false;
        }
        // Sans TTL, l'expiration est « infinie » : GT échoue toujours, LT réussit toujours.
        if gt && (!has_ttl || rel_ms <= cur) {
            return false;
        }
        if lt && has_ttl && rel_ms >= cur {
            return false;
        }
        if rel_ms <= 0 {
            d.map.remove(key);
        } else {
            e.set_expire_ms(rel_ms);
        }
        true
    });
    if changed {
        ctx.db.touch();
        ctx.notify_key(state.db_index, key, if rel_ms <= 0 { "del" } else { "expire" });
        ctx.propagate(args);
    }
    RespValue::Integer(changed as i64)
}

/// EXPIRETIME / PEXPIRETIME.
pub(crate) fn cmd_expiretime(ctx: &CommandContext, state: &ClientState, args: &[Bytes], millis: bool) -> RespValue {
    if args.len() != 2 {
        return wrong_arity(&upper(&args[0]).to_ascii_lowercase());
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        d.get_fresh(&args[1]).map(|e| e.expire_at_unix_ms())
    });
    RespValue::Integer(match r {
        None => -2,
        Some(None) => -1,
        Some(Some(ms)) => {
            if millis {
                ms
            } else {
                ms / 1000
            }
        }
    })
}

/// RENAME / RENAMENX.
pub(crate) fn cmd_rename(ctx: &CommandContext, state: &ClientState, args: &[Bytes], nx: bool) -> RespValue {
    if args.len() != 3 {
        return wrong_arity(if nx { "renamenx" } else { "rename" });
    }
    let (src, dst) = (&args[1], &args[2]);
    let r: Result<bool, &'static str> = ctx.db.with_db(state.db_index, |d| {
        d.purge_if_expired(src);
        if !d.map.contains_key(src) {
            return Err(ERR_NO_SUCH_KEY);
        }
        if src == dst {
            return Ok(!nx);
        }
        d.purge_if_expired(dst);
        if nx && d.map.contains_key(dst) {
            return Ok(false);
        }
        let entry = d.map.remove(src).expect("présence vérifiée");
        d.map.insert(dst.clone(), entry);
        Ok(true)
    });
    match r {
        Err(e) => err(e),
        Ok(false) => RespValue::Integer(0),
        Ok(true) => {
            ctx.db.touch();
            ctx.waiters.notify(state.db_index, dst);
            ctx.notify_key(state.db_index, src, "rename_from");
            ctx.notify_key(state.db_index, dst, "rename_to");
            ctx.propagate(args);
            if nx {
                RespValue::Integer(1)
            } else {
                RespValue::ok()
            }
        }
    }
}

/// COPY src dst [DB n] [REPLACE].
pub(crate) fn cmd_copy(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("copy");
    }
    let mut dst_db = state.db_index;
    let mut replace = false;
    let mut i = 3;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "REPLACE" => replace = true,
            "DB" => {
                i += 1;
                match args.get(i).and_then(|b| parse_i64(b)) {
                    Some(n) if n >= 0 && (n as usize) < ctx.db.db_count() => dst_db = n as usize,
                    Some(_) => return err("ERR DB index is out of range"),
                    None => return err(ERR_INT),
                }
            }
            _ => return err(ERR_SYNTAX),
        }
        i += 1;
    }
    if state.db_index == dst_db && args[1] == args[2] {
        return err("ERR source and destination objects are the same");
    }
    let Some(entry) = ctx
        .db
        .with_db(state.db_index, |d| d.get_fresh(&args[1]).cloned())
    else {
        return RespValue::Integer(0);
    };
    let copied = ctx.db.with_db(dst_db, |d| {
        d.purge_if_expired(&args[2]);
        if !replace && d.map.contains_key(&args[2]) {
            return false;
        }
        d.map.insert(args[2].clone(), entry);
        true
    });
    if copied {
        ctx.db.touch();
        ctx.waiters.notify(dst_db, &args[2]);
        ctx.propagate(args);
    }
    RespValue::Integer(copied as i64)
}

/// MOVE key db.
pub(crate) fn cmd_move(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("move");
    }
    let Some(dst_db) = parse_i64(&args[2]) else {
        return err(ERR_INT);
    };
    if dst_db < 0 || dst_db as usize >= ctx.db.db_count() {
        return err("ERR DB index is out of range");
    }
    let dst_db = dst_db as usize;
    if dst_db == state.db_index {
        return err("ERR source and destination objects are the same");
    }
    let exists_dst = ctx
        .db
        .with_db(dst_db, |d| d.get_fresh(&args[1]).is_some());
    if exists_dst {
        return RespValue::Integer(0);
    }
    let Some(entry) = ctx.db.with_db(state.db_index, |d| {
        d.purge_if_expired(&args[1]);
        d.map.remove(&args[1])
    }) else {
        return RespValue::Integer(0);
    };
    ctx.db.with_db(dst_db, |d: &mut DbInner| {
        d.map.insert(args[1].clone(), entry);
    });
    ctx.db.touch();
    ctx.waiters.notify(dst_db, &args[1]);
    ctx.propagate(args);
    RespValue::Integer(1)
}

/// SWAPDB index1 index2.
pub(crate) fn cmd_swapdb(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let index = |raw: &[u8], msg: &str| {
        parse_i64(raw)
            .filter(|v| i32::try_from(*v).is_ok())
            .ok_or_else(|| err(msg))
    };
    let (a, b) = match (index(&args[1], "ERR invalid first DB index"), index(&args[2], "ERR invalid second DB index")) {
        (Err(e), _) | (_, Err(e)) => return e,
        (Ok(a), Ok(b)) => (a, b),
    };
    if a < 0 || b < 0 || !ctx.db.swap_dbs(a as usize, b as usize) {
        return err("ERR DB index is out of range");
    }
    ctx.waiters.notify_db(a as usize);
    ctx.waiters.notify_db(b as usize);
    ctx.propagate(args);
    RespValue::ok()
}

pub(crate) fn cmd_randomkey(ctx: &CommandContext, state: &ClientState) -> RespValue {
    match ctx.db.random_key(state.db_index) {
        Some(k) => RespValue::bulk(k),
        None => RespValue::null_bulk(),
    }
}

/// TOUCH key [key …] : nombre de clés existantes.
pub(crate) fn cmd_touch(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("touch");
    }
    RespValue::Integer(ctx.db.exists(state.db_index, &args[1..]))
}

/// OBJECT ENCODING|IDLETIME|FREQ|REFCOUNT key.
pub(crate) fn cmd_object(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("object");
    }
    let sub = upper(&args[1]);
    if sub == "HELP" {
        return RespValue::array(vec![RespValue::bulk(Bytes::from_static(
            b"OBJECT <subcommand> [<arg> ...]. Subcommands: ENCODING, FREQ, IDLETIME, REFCOUNT.",
        ))]);
    }
    if args.len() != 3 {
        return wrong_arity("object");
    }
    let key = &args[2];
    let meta = ctx.db.with_db(state.db_index, |d| {
        d.get_fresh_notouch(key).map(|e| {
            (
                e.object_encoding(),
                e.idle_secs(),
                e.lfu_freq as i64,
            )
        })
    });
    match sub.as_str() {
        "ENCODING" => match meta {
            Some((enc, _, _)) => RespValue::bulk(Bytes::from(enc)),
            None => RespValue::null_bulk(),
        },
        "IDLETIME" => match meta {
            Some((_, idle, _)) => RespValue::Integer(idle),
            None => RespValue::null_bulk(),
        },
        "FREQ" => match meta {
            Some((_, _, freq)) => RespValue::Integer(freq),
            None => RespValue::null_bulk(),
        },
        "REFCOUNT" => match meta {
            Some(_) => RespValue::Integer(1),
            None => RespValue::null_bulk(),
        },
        _ => err("ERR unknown subcommand for OBJECT"),
    }
}
