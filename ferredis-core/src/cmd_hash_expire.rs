//! Expiration par champ de hash (Redis 7.4 / 8) — module dédié (SOLID : S).
//!
//! Parseurs d'options partagés via [`crate::opts`] ; logique TTL dans [`HashObject`].

use crate::command::{wrong_arity, ClientState, CommandContext};
use crate::opts::{
    abs_expire_ms, parse_expire_cond, parse_fields_section, HashExpireWhen,
};
use crate::resp::RespValue;
use crate::store::WRONGTYPE;
use crate::util::*;
use crate::value::{Entry, HashExpireCond, HashObject, RedisValue};
use bytes::Bytes;

fn after_write(ctx: &CommandContext, state: &ClientState, key: &Bytes, args: &[Bytes]) {
    ctx.db.touch();
    ctx.notify_key(state.db_index, key, "hset");
    ctx.propagate(args);
}

#[derive(Debug)]
enum HashAccessErr {
    MissingKey,
    WrongType,
    Other(&'static str),
}

fn with_hash_mut<R>(
    ctx: &CommandContext,
    state: &ClientState,
    key: &Bytes,
    f: impl FnOnce(&mut HashObject) -> Result<R, &'static str>,
) -> Result<R, HashAccessErr> {
    ctx.db.with_db(state.db_index, |d| {
        d.purge_expired(key);
        match d.map.get_mut(key) {
            None => Err(HashAccessErr::MissingKey),
            Some(e) => match &mut e.value {
                RedisValue::Hash(h) => {
                    h.purge_expired(now_unix_ms());
                    f(h).map_err(HashAccessErr::Other)
                }
                _ => Err(HashAccessErr::WrongType),
            },
        }
    })
}

fn missing_int_array(fields: &[Bytes], code: i64) -> RespValue {
    RespValue::array(fields.iter().map(|_| RespValue::Integer(code)).collect())
}

fn missing_null_array(fields: &[Bytes]) -> RespValue {
    RespValue::array(fields.iter().map(|_| RespValue::Null).collect())
}

fn map_hash_err(e: HashAccessErr) -> RespValue {
    match e {
        HashAccessErr::MissingKey => err(ERR_NO_SUCH_KEY),
        HashAccessErr::WrongType => err(WRONGTYPE),
        HashAccessErr::Other(m) => err(m),
    }
}

/// HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT — moteur unique (DRY).
fn cmd_hexpire_generic(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    name: &str,
    when: impl FnOnce(i64) -> HashExpireWhen,
) -> RespValue {
    if args.len() < 5 {
        return wrong_arity(name);
    }
    let key = &args[1];
    let ttl_raw = match parse_i64(&args[2]) {
        Some(v) => v,
        None => return err(ERR_INT),
    };
    let mut i = 3;
    let cond = match parse_expire_cond(args, &mut i) {
        Ok(c) => c,
        Err(e) => return e,
    };
    let fields = match parse_fields_section(args, i) {
        Ok((f, _)) => f,
        Err(e) => return e,
    };
    let abs = when(ttl_raw).to_abs_ms();
    let now = now_unix_ms();
    let codes = match with_hash_mut(ctx, state, key, |h| {
        Ok(fields
            .iter()
            .map(|f| h.apply_expire(f, Some(abs), cond, now))
            .collect::<Vec<_>>())
    }) {
        Ok(c) => c,
        Err(HashAccessErr::MissingKey) => return missing_int_array(&fields, -2),
        Err(e) => return map_hash_err(e),
    };
    after_write(ctx, state, key, args);
    RespValue::array(codes.into_iter().map(RespValue::Integer).collect())
}

pub(crate) fn cmd_hexpire(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    cmd_hexpire_generic(ctx, state, args, "hexpire", HashExpireWhen::Seconds)
}

pub(crate) fn cmd_hpexpire(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    cmd_hexpire_generic(ctx, state, args, "hpexpire", HashExpireWhen::Millis)
}

pub(crate) fn cmd_hexpireat(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    cmd_hexpire_generic(ctx, state, args, "hexpireat", HashExpireWhen::AtSeconds)
}

pub(crate) fn cmd_hpexpireat(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
) -> RespValue {
    cmd_hexpire_generic(ctx, state, args, "hpexpireat", HashExpireWhen::AtMillis)
}

fn cmd_httl_generic(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    name: &str,
    as_seconds: bool,
    expire_time: bool,
) -> RespValue {
    if args.len() < 4 {
        return wrong_arity(name);
    }
    let key = &args[1];
    let fields = match parse_fields_section(args, 2) {
        Ok((f, _)) => f,
        Err(e) => return e,
    };
    let out = match with_hash_mut(ctx, state, key, |h| {
        Ok(fields
            .iter()
            .map(|f| {
                if expire_time {
                    let t = h.expire_time_ms(f);
                    if as_seconds && t > 0 {
                        t / 1000
                    } else {
                        t
                    }
                } else {
                    let ms = h.ttl_ms(f);
                    if ms < 0 {
                        ms
                    } else if as_seconds {
                        (ms + 999) / 1000
                    } else {
                        ms
                    }
                }
            })
            .collect::<Vec<_>>())
    }) {
        Ok(v) => v,
        Err(HashAccessErr::MissingKey) => return missing_int_array(&fields, -2),
        Err(e) => return map_hash_err(e),
    };
    RespValue::array(out.into_iter().map(RespValue::Integer).collect())
}

pub(crate) fn cmd_httl(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    cmd_httl_generic(ctx, state, args, "httl", true, false)
}

pub(crate) fn cmd_hpttl(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    cmd_httl_generic(ctx, state, args, "hpttl", false, false)
}

pub(crate) fn cmd_hexpiretime(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
) -> RespValue {
    cmd_httl_generic(ctx, state, args, "hexpiretime", true, true)
}

pub(crate) fn cmd_hpexpiretime(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
) -> RespValue {
    cmd_httl_generic(ctx, state, args, "hpexpiretime", false, true)
}

pub(crate) fn cmd_hpersist(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("hpersist");
    }
    let key = &args[1];
    let fields = match parse_fields_section(args, 2) {
        Ok((f, _)) => f,
        Err(e) => return e,
    };
    let codes = match with_hash_mut(ctx, state, key, |h| {
        Ok(fields.iter().map(|f| h.persist_field(f)).collect::<Vec<_>>())
    }) {
        Ok(c) => c,
        Err(HashAccessErr::MissingKey) => return missing_int_array(&fields, -2),
        Err(e) => return map_hash_err(e),
    };
    if codes.iter().any(|&c| c == 1) {
        after_write(ctx, state, key, args);
    }
    RespValue::array(codes.into_iter().map(RespValue::Integer).collect())
}

pub(crate) fn cmd_hgetdel(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("hgetdel");
    }
    let key = &args[1];
    let fields = match parse_fields_section(args, 2) {
        Ok((f, _)) => f,
        Err(e) => return e,
    };
    let values = match with_hash_mut(ctx, state, key, |h| {
        let mut out = Vec::with_capacity(fields.len());
        for f in &fields {
            out.push(h.remove(f));
        }
        Ok(out)
    }) {
        Ok(v) => v,
        Err(HashAccessErr::MissingKey) => return missing_null_array(&fields),
        Err(e) => return map_hash_err(e),
    };
    let _ = ctx.db.with_db(state.db_index, |d| {
        if let Some(e) = d.map.get(key) {
            if matches!(&e.value, RedisValue::Hash(h) if h.is_empty()) {
                d.map.remove(key);
            }
        }
    });
    after_write(ctx, state, key, args);
    RespValue::array(
        values
            .into_iter()
            .map(|o| o.map(RespValue::bulk).unwrap_or(RespValue::Null))
            .collect(),
    )
}

pub(crate) fn cmd_hgetex(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("hgetex");
    }
    let key = &args[1];
    let mut i = 2;
    let mut new_expire: Option<Option<i64>> = None;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "PERSIST" => {
                if new_expire.is_some() {
                    return err(ERR_SYNTAX);
                }
                new_expire = Some(None);
                i += 1;
            }
            "EX" | "PX" | "EXAT" | "PXAT" => {
                if new_expire.is_some() {
                    return err(ERR_SYNTAX);
                }
                let opt = upper(&args[i]);
                i += 1;
                let Some(raw) = args.get(i) else {
                    return err(ERR_SYNTAX);
                };
                match abs_expire_ms(&opt, raw, "hgetex") {
                    Ok(ms) => new_expire = Some(Some(ms)),
                    Err(e) => return e,
                }
                i += 1;
            }
            "FIELDS" => break,
            _ => return err(ERR_SYNTAX),
        }
    }
    let fields = match parse_fields_section(args, i) {
        Ok((f, _)) => f,
        Err(e) => return e,
    };
    let now = now_unix_ms();
    let values = match with_hash_mut(ctx, state, key, |h| {
        let mut out = Vec::with_capacity(fields.len());
        for f in &fields {
            let v = h.get(f).cloned();
            if v.is_some() {
                if let Some(exp) = new_expire {
                    let _ = h.apply_expire(f, exp, HashExpireCond::None, now);
                }
            }
            out.push(v);
        }
        Ok(out)
    }) {
        Ok(v) => v,
        Err(HashAccessErr::MissingKey) => return missing_null_array(&fields),
        Err(e) => return map_hash_err(e),
    };
    if new_expire.is_some() {
        after_write(ctx, state, key, args);
    }
    RespValue::array(
        values
            .into_iter()
            .map(|o| o.map(RespValue::bulk).unwrap_or(RespValue::Null))
            .collect(),
    )
}

pub(crate) fn cmd_hsetex(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 5 {
        return wrong_arity("hsetex");
    }
    let key = &args[1];
    let mut i = 2;
    let mut fnx = false;
    let mut fxx = false;
    let mut expire: Option<Option<i64>> = None;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "FNX" => {
                fnx = true;
                i += 1;
            }
            "FXX" => {
                fxx = true;
                i += 1;
            }
            "KEEPTTL" => {
                expire = Some(None);
                i += 1;
            }
            "EX" | "PX" | "EXAT" | "PXAT" => {
                if expire.is_some() {
                    return err(ERR_SYNTAX);
                }
                let opt = upper(&args[i]);
                i += 1;
                let Some(raw) = args.get(i) else {
                    return err(ERR_SYNTAX);
                };
                match abs_expire_ms(&opt, raw, "hsetex") {
                    Ok(ms) => expire = Some(Some(ms)),
                    Err(e) => return e,
                }
                i += 1;
            }
            "FIELDS" => break,
            _ => return err(ERR_SYNTAX),
        }
    }
    if fnx && fxx {
        return err(ERR_SYNTAX);
    }
    if i >= args.len() || upper(&args[i]) != "FIELDS" {
        return err(ERR_SYNTAX);
    }
    let n = match parse_i64(args.get(i + 1).map(|b| b.as_ref()).unwrap_or(b"")) {
        Some(v) if v > 0 => v as usize,
        _ => return err(ERR_INT),
    };
    let start = i + 2;
    if args.len() != start + n * 2 {
        return err(ERR_SYNTAX);
    }
    let pairs: Vec<(Bytes, Bytes)> = (0..n)
        .map(|k| (args[start + k * 2].clone(), args[start + k * 2 + 1].clone()))
        .collect();

    let r = ctx.db.with_db(state.db_index, |d| -> Result<i64, &'static str> {
        d.purge_expired(key);
        let h = match d.map.get_mut(key) {
            None => {
                if fxx {
                    return Ok(0);
                }
                d.map
                    .insert(key.clone(), Entry::new(RedisValue::Hash(HashObject::new())));
                match d.map.get_mut(key) {
                    Some(Entry {
                        value: RedisValue::Hash(h),
                        ..
                    }) => h,
                    _ => unreachable!(),
                }
            }
            Some(e) => match &mut e.value {
                RedisValue::Hash(h) => {
                    h.purge_expired(now_unix_ms());
                    h
                }
                _ => return Err(WRONGTYPE),
            },
        };
        let mut changed = 0i64;
        for (field, value) in pairs {
            let exists = h.contains(&field);
            if fnx && exists {
                continue;
            }
            if fxx && !exists {
                continue;
            }
            let old_ttl = h.get_field(&field).and_then(|f| f.expires_at_ms);
            let exp = match expire {
                None => None,
                Some(None) => old_ttl,
                Some(Some(ms)) => Some(ms),
            };
            h.insert_with_expire(field, value, exp);
            changed += 1;
        }
        Ok(changed)
    });
    match r {
        Err(e) => err(e),
        Ok(n) => {
            if n > 0 {
                after_write(ctx, state, key, args);
            }
            RespValue::Integer(n)
        }
    }
}
