//! Commandes sur les chaînes : SET (toutes options), SETEX, GETEX, GETDEL, INCRBYFLOAT…

use crate::command::{wrong_arity, ClientState, CommandContext};
use crate::resp::RespValue;
use crate::store::WRONGTYPE;
use crate::util::*;
use crate::value::{Entry, RedisValue};
use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_STRING_LEN: usize = 512 * 1024 * 1024;

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Option d'expiration d'un SET / GETEX.
#[derive(Clone, Copy, PartialEq)]
enum Expiry {
    None,
    Persist,
    KeepTtl,
    In(Duration),
}

/// Convertit `EX|PX|EXAT|PXAT value` en durée relative strictement positive.
fn expiry_from(opt: &str, raw: &[u8], cmd: &str) -> Result<Duration, RespValue> {
    let invalid = || err(&format!("ERR invalid expire time in '{cmd}' command"));
    let n = parse_i64(raw).ok_or_else(|| err(ERR_INT))?;
    let ms = match opt {
        "EX" => n.checked_mul(1000),
        "PX" => Some(n),
        "EXAT" => n.checked_mul(1000).and_then(|v| v.checked_sub(now_unix_ms())),
        "PXAT" => n.checked_sub(now_unix_ms()),
        _ => None,
    };
    match ms {
        Some(ms) if ms > 0 => Ok(Duration::from_millis(ms.min(3_153_600_000_000) as u64)),
        // Une échéance déjà passée est acceptée pour EXAT/PXAT (la clé expire aussitôt).
        Some(_) if opt.ends_with("AT") && n > 0 => Ok(Duration::from_millis(0)),
        _ => Err(invalid()),
    }
}

/// SET key value [NX|XX] [GET] [EX s|PX ms|EXAT ts|PXAT ts|KEEPTTL].
pub(crate) fn cmd_set(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("set");
    }
    let (key, value) = (&args[1], &args[2]);
    let (mut nx, mut xx, mut get) = (false, false, false);
    let mut expiry = Expiry::None;
    let mut i = 3;
    while i < args.len() {
        let opt = upper(&args[i]);
        match opt.as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GET" => get = true,
            "KEEPTTL" if expiry == Expiry::None => expiry = Expiry::KeepTtl,
            "EX" | "PX" | "EXAT" | "PXAT" if expiry == Expiry::None => {
                i += 1;
                let Some(raw) = args.get(i) else {
                    return err(ERR_SYNTAX);
                };
                match expiry_from(&opt, raw, "set") {
                    Ok(d) => expiry = Expiry::In(d),
                    Err(e) => return e,
                }
            }
            _ => return err(ERR_SYNTAX),
        }
        i += 1;
    }
    if nx && xx {
        return err(ERR_SYNTAX);
    }

    let outcome: Result<(bool, Option<Bytes>), &'static str> = ctx.db.with_db(state.db_index, |d| {
        let old = match d.get_fresh(key) {
            None => None,
            Some(e) => match &e.value {
                RedisValue::String(s) => Some((s.clone(), e.expires_at)),
                _ if get => return Err(WRONGTYPE),
                _ => None,
            },
        };
        let exists = d.map.contains_key(key);
        let old_value = old.as_ref().map(|(v, _)| v.clone());
        if (nx && exists) || (xx && !exists) {
            return Ok((false, old_value));
        }
        let mut entry = Entry::new(RedisValue::String(value.clone()));
        match expiry {
            Expiry::In(dur) => entry.expires_at = Some(Instant::now() + dur),
            Expiry::KeepTtl => entry.expires_at = old.and_then(|(_, ttl)| ttl),
            Expiry::None | Expiry::Persist => {}
        }
        d.map.insert(key.clone(), entry);
        Ok((true, old_value))
    });
    match outcome {
        Err(e) => err(e),
        Ok((written, old)) => {
            if written {
                ctx.db.touch();
                ctx.propagate(args);
                ctx.notify_key(state.db_index, key, "set");
            }
            if get {
                old.map_or_else(RespValue::null_bulk, RespValue::bulk)
            } else if written {
                RespValue::ok()
            } else {
                RespValue::null_bulk()
            }
        }
    }
}

/// SETEX key seconds value / PSETEX key ms value.
pub(crate) fn cmd_setex(ctx: &CommandContext, state: &ClientState, args: &[Bytes], millis: bool) -> RespValue {
    let name = if millis { "psetex" } else { "setex" };
    if args.len() != 4 {
        return wrong_arity(name);
    }
    let opt = if millis { "PX" } else { "EX" };
    let dur = match expiry_from(opt, &args[2], name) {
        Ok(d) => d,
        Err(e) => return e,
    };
    ctx.db.with_db(state.db_index, |d| {
        let mut entry = Entry::new(RedisValue::String(args[3].clone()));
        entry.expires_at = Some(Instant::now() + dur);
        d.map.insert(args[1].clone(), entry);
    });
    ctx.db.touch();
    ctx.propagate(args);
    ctx.notify_key(state.db_index, &args[1], "set");
    RespValue::ok()
}

/// GETDEL key.
pub(crate) fn cmd_getdel(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("getdel");
    }
    let r: Result<Option<Bytes>, &'static str> = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]) {
            None => Ok(None),
            Some(Entry { value: RedisValue::String(s), .. }) => {
                let s = s.clone();
                d.map.remove(&args[1]);
                Ok(Some(s))
            }
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some(v)) => {
            ctx.db.touch();
            ctx.propagate(&[Bytes::from_static(b"DEL"), args[1].clone()]);
            ctx.notify_key(state.db_index, &args[1], "del");
            RespValue::bulk(v)
        }
    }
}

/// GETEX key [EX s|PX ms|EXAT ts|PXAT ts|PERSIST].
pub(crate) fn cmd_getex(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("getex");
    }
    let mut expiry = Expiry::None;
    let mut i = 2;
    while i < args.len() {
        let opt = upper(&args[i]);
        match opt.as_str() {
            "PERSIST" if expiry == Expiry::None => expiry = Expiry::Persist,
            "EX" | "PX" | "EXAT" | "PXAT" if expiry == Expiry::None => {
                i += 1;
                let Some(raw) = args.get(i) else {
                    return err(ERR_SYNTAX);
                };
                match expiry_from(&opt, raw, "getex") {
                    Ok(d) => expiry = Expiry::In(d),
                    Err(e) => return e,
                }
            }
            _ => return err(ERR_SYNTAX),
        }
        i += 1;
    }
    let r: Result<Option<Bytes>, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(e) = d.get_fresh_mut(&args[1]) else {
            return Ok(None);
        };
        let RedisValue::String(s) = &e.value else {
            return Err(WRONGTYPE);
        };
        let v = s.clone();
        match expiry {
            Expiry::In(dur) => e.expires_at = Some(Instant::now() + dur),
            Expiry::Persist => e.expires_at = None,
            Expiry::None | Expiry::KeepTtl => {}
        }
        Ok(Some(v))
    });
    match r {
        Err(e) => err(e),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some(v)) => {
            if expiry != Expiry::None {
                ctx.db.touch();
            }
            RespValue::bulk(v)
        }
    }
}

/// MSETNX k v [k v …] : tout ou rien.
pub(crate) fn cmd_msetnx(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 || args.len() % 2 == 0 {
        return wrong_arity("msetnx");
    }
    let written = ctx.db.with_db(state.db_index, |d| {
        if args[1..].chunks(2).any(|kv| d.get_fresh(&kv[0]).is_some()) {
            return false;
        }
        for kv in args[1..].chunks(2) {
            d.map
                .insert(kv[0].clone(), Entry::new(RedisValue::String(kv[1].clone())));
        }
        true
    });
    if written {
        ctx.db.touch();
        ctx.propagate(args);
    }
    RespValue::Integer(written as i64)
}

/// APPEND key value (conserve le TTL existant).
pub(crate) fn cmd_append(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("append");
    }
    let r: Result<i64, &'static str> = ctx.db.with_db(state.db_index, |d| {
        d.purge_if_expired(&args[1]);
        match d.map.get_mut(&args[1]) {
            None => {
                d.map
                    .insert(args[1].clone(), Entry::new(RedisValue::String(args[2].clone())));
                Ok(args[2].len() as i64)
            }
            Some(Entry { value: RedisValue::String(s), .. }) => {
                if s.len() + args[2].len() > MAX_STRING_LEN {
                    return Err("ERR string exceeds maximum allowed size (proto-max-bulk-len)");
                }
                let mut buf = Vec::with_capacity(s.len() + args[2].len());
                buf.extend_from_slice(s);
                buf.extend_from_slice(&args[2]);
                let len = buf.len() as i64;
                *s = Bytes::from(buf);
                Ok(len)
            }
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(n) => {
            ctx.db.touch();
            ctx.propagate(args);
            ctx.notify_key(state.db_index, &args[1], "append");
            RespValue::Integer(n)
        }
    }
}

/// INCRBYFLOAT key increment.
pub(crate) fn cmd_incrbyfloat(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("incrbyfloat");
    }
    let Some(delta) = parse_f64(&args[2]).filter(|v| v.is_finite()) else {
        return err(ERR_FLOAT);
    };
    let r: Result<f64, &'static str> = ctx.db.with_db(state.db_index, |d| {
        d.purge_if_expired(&args[1]);
        let cur = match d.map.get(&args[1]) {
            None => 0.0,
            Some(Entry { value: RedisValue::String(s), .. }) => {
                parse_f64(s).ok_or(ERR_FLOAT)?
            }
            Some(_) => return Err(WRONGTYPE),
        };
        let next = cur + delta;
        if !next.is_finite() {
            return Err("ERR increment would produce NaN or Infinity");
        }
        let text = Bytes::from(fmt_f64(next));
        match d.map.get_mut(&args[1]) {
            Some(e) => e.value = RedisValue::String(text),
            None => {
                d.map
                    .insert(args[1].clone(), Entry::new(RedisValue::String(text)));
            }
        }
        Ok(next)
    });
    match r {
        Err(e) => err(e),
        Ok(v) => {
            ctx.db.touch();
            ctx.notify_key(state.db_index, &args[1], "incrbyfloat");
            // La réplication AOF rejoue la valeur finale (déterministe).
            ctx.propagate(&[
                Bytes::from_static(b"SET"),
                args[1].clone(),
                Bytes::from(fmt_f64(v)),
                Bytes::from_static(b"KEEPTTL"),
            ]);
            bulk_f64(v)
        }
    }
}

/// SETRANGE key offset value.
pub(crate) fn cmd_setrange(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("setrange");
    }
    let Some(offset) = parse_i64(&args[2]).filter(|o| *o >= 0) else {
        return err("ERR offset is out of range");
    };
    let offset = offset as usize;
    if offset.saturating_add(args[3].len()) > MAX_STRING_LEN {
        return err("ERR string exceeds maximum allowed size (proto-max-bulk-len)");
    }
    let r: Result<i64, &'static str> = ctx.db.with_db(state.db_index, |d| {
        d.purge_if_expired(&args[1]);
        let existing = match d.map.get(&args[1]) {
            None => Bytes::new(),
            Some(Entry { value: RedisValue::String(s), .. }) => s.clone(),
            Some(_) => return Err(WRONGTYPE),
        };
        if args[3].is_empty() {
            return Ok(existing.len() as i64);
        }
        let mut buf = existing.to_vec();
        if buf.len() < offset + args[3].len() {
            buf.resize(offset + args[3].len(), 0);
        }
        buf[offset..offset + args[3].len()].copy_from_slice(&args[3]);
        let len = buf.len() as i64;
        let value = RedisValue::String(Bytes::from(buf));
        match d.map.get_mut(&args[1]) {
            Some(e) => e.value = value,
            None => {
                d.map.insert(args[1].clone(), Entry::new(value));
            }
        }
        Ok(len)
    });
    match r {
        Err(e) => err(e),
        Ok(n) => {
            if !args[3].is_empty() {
                ctx.db.touch();
                ctx.propagate(args);
            }
            RespValue::Integer(n)
        }
    }
}

/// GETRANGE / SUBSTR key start end.
pub(crate) fn cmd_getrange(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("getrange");
    }
    let (Some(start), Some(end)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
        return err(ERR_INT);
    };
    match ctx.db.get_string(state.db_index, &args[1]) {
        Err(e) => err(e),
        Ok(None) => RespValue::bulk(Bytes::new()),
        Ok(Some(s)) => match normalize_range(start, end, s.len()) {
            Some((a, b)) => RespValue::bulk(s.slice(a..=b)),
            None => RespValue::bulk(Bytes::new()),
        },
    }
}

/// Longueur de la LCS sur deux lignes glissantes (mémoire O(|b|)).
fn lcs_len(a: &[u8], b: &[u8]) -> u32 {
    let mut prev = vec![0u32; b.len() + 1];
    let mut cur = vec![0u32; b.len() + 1];
    for &ca in a {
        for (j, &cb) in b.iter().enumerate() {
            cur[j + 1] = if ca == cb { prev[j] + 1 } else { prev[j + 1].max(cur[j]) };
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// Correspondance IDX : `(a_start, a_end, b_start, b_end)`.
type LcsMatch = (usize, usize, usize, usize);

/// Table complète puis remontée (`lcsCommand`) : chaîne LCS et plages correspondantes.
fn lcs_full(a: &[u8], b: &[u8], with_ranges: bool) -> (Vec<u8>, Vec<LcsMatch>, u32) {
    let w = b.len() + 1;
    let mut t = vec![0u32; (a.len() + 1) * w];
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            t[i * w + j] = if a[i - 1] == b[j - 1] {
                t[(i - 1) * w + j - 1] + 1
            } else {
                t[(i - 1) * w + j].max(t[i * w + j - 1])
            };
        }
    }
    let total = t[a.len() * w + b.len()];
    let mut out = vec![0u8; total as usize];
    let mut ranges = Vec::new();
    let (mut idx, mut i, mut j) = (total as usize, a.len(), b.len());
    let unset = a.len();
    let (mut as_, mut ae, mut bs, mut be) = (unset, 0, 0, 0);
    while i > 0 && j > 0 {
        let mut emit = false;
        if a[i - 1] == b[j - 1] {
            out[idx - 1] = a[i - 1];
            if as_ == unset {
                (as_, ae, bs, be) = (i - 1, i - 1, j - 1, j - 1);
            } else if as_ == i && bs == j {
                as_ -= 1;
                bs -= 1;
            } else {
                emit = true;
            }
            if as_ == 0 || bs == 0 {
                emit = true;
            }
            idx -= 1;
            i -= 1;
            j -= 1;
        } else {
            if t[(i - 1) * w + j] > t[i * w + j - 1] {
                i -= 1;
            } else {
                j -= 1;
            }
            if as_ != unset {
                emit = true;
            }
        }
        if emit {
            if with_ranges {
                ranges.push((as_, ae, bs, be));
            }
            as_ = unset;
        }
    }
    (out, ranges, total)
}

/// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN].
pub(crate) fn cmd_lcs(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let values = ctx.db.with_db(state.db_index, |d| {
        let mut get = |k: &Bytes| match d.get_fresh(k).map(|e| &e.value) {
            None => Ok(Bytes::new()),
            Some(RedisValue::String(s)) => Ok(s.clone()),
            Some(_) => Err(err("ERR The specified keys must contain string values")),
        };
        Ok::<_, RespValue>((get(&args[1])?, get(&args[2])?))
    });
    let (a, b) = match values {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (mut getlen, mut getidx, mut withmatchlen, mut minmatchlen) = (false, false, false, 0usize);
    let mut j = 3;
    while j < args.len() {
        match upper(&args[j]).as_str() {
            "IDX" => getidx = true,
            "LEN" => getlen = true,
            "WITHMATCHLEN" => withmatchlen = true,
            "MINMATCHLEN" if j + 1 < args.len() => {
                let Some(n) = parse_i64(&args[j + 1]) else {
                    return err(ERR_INT);
                };
                minmatchlen = n.max(0) as usize;
                j += 1;
            }
            _ => return err(ERR_SYNTAX),
        }
        j += 1;
    }
    if getidx && getlen {
        return err("ERR If you want both the length and indexes, please just use IDX.");
    }
    if getlen {
        return RespValue::Integer(lcs_len(&a, &b) as i64);
    }
    let cells = (a.len() as u128 + 1) * (b.len() as u128 + 1) * 4;
    if cells > MAX_STRING_LEN as u128 {
        return err("ERR Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len");
    }
    let (s, ranges, total) = lcs_full(&a, &b, getidx);
    if !getidx {
        return RespValue::bulk(Bytes::from(s));
    }
    let pair = |x: usize, y: usize| RespValue::array(vec![RespValue::Integer(x as i64), RespValue::Integer(y as i64)]);
    let matches = ranges
        .into_iter()
        .filter(|r| minmatchlen == 0 || r.1 - r.0 + 1 >= minmatchlen)
        .map(|(as_, ae, bs, be)| {
            let mut m = vec![pair(as_, ae), pair(bs, be)];
            if withmatchlen {
                m.push(RespValue::Integer((ae - as_ + 1) as i64));
            }
            RespValue::array(m)
        })
        .collect();
    RespValue::array(vec![
        RespValue::bulk(Bytes::from_static(b"matches")),
        RespValue::array(matches),
        RespValue::bulk(Bytes::from_static(b"len")),
        RespValue::Integer(total as i64),
    ])
}

#[cfg(test)]
mod lcs_tests {
    use super::*;

    #[test]
    fn lcs_matches_redis_doc() {
        let (a, b) = (b"ohmytext", b"mynewtext");
        assert_eq!(lcs_len(a, b), 6);
        let (s, ranges, total) = lcs_full(a, b, true);
        assert_eq!(s, b"mytext");
        assert_eq!(total, 6);
        assert_eq!(ranges, vec![(4, 7, 5, 8), (2, 3, 0, 1)]);
    }
}
