//! Compléments hash et set : HSETNX, HINCRBYFLOAT, HSTRLEN, HSCAN, SSCAN, SPOP,
//! SRANDMEMBER, SMOVE, SMISMEMBER, S{INTER,UNION,DIFF}STORE.

use crate::command::{wrong_arity, ClientState, CommandContext};
use crate::resp::RespValue;
use crate::store::{match_glob_bytes, paginate, WRONGTYPE};
use crate::util::*;
use crate::value::{Entry, RedisValue};
use bytes::Bytes;
use rand::seq::IteratorRandom;
use std::collections::HashSet;

/// Parse `[MATCH pattern] [COUNT n]` à partir de `args[from..]`.
pub(crate) fn parse_scan_opts(args: &[Bytes], from: usize) -> Result<(Option<String>, usize), RespValue> {
    let (mut pattern, mut count) = (None, 10usize);
    let mut i = from;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "MATCH" if i + 1 < args.len() => {
                pattern = Some(String::from_utf8_lossy(&args[i + 1]).into_owned());
            }
            "COUNT" if i + 1 < args.len() => match parse_i64(&args[i + 1]) {
                Some(n) if n >= 1 => count = n as usize,
                Some(_) => return Err(err(ERR_SYNTAX)),
                None => return Err(err(ERR_INT)),
            },
            "NOVALUES" => {
                i += 1;
                continue;
            }
            _ => return Err(err(ERR_SYNTAX)),
        }
        i += 2;
    }
    Ok((pattern, count))
}

pub(crate) fn cmd_hsetnx(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("hsetnx");
    }
    let r: Result<bool, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let h = d.hash_or_create(&args[1])?;
        h.purge_expired(crate::util::now_unix_ms());
        let inserted = if h.contains(&args[2]) {
            false
        } else {
            h.insert(args[2].clone(), args[3].clone());
            true
        };
        d.remove_if_empty(&args[1]);
        Ok(inserted)
    });
    match r {
        Err(e) => err(e),
        Ok(ins) => {
            if ins {
                ctx.db.touch();
                ctx.propagate(args);
                ctx.notify_key(state.db_index, &args[1], "hset");
            }
            RespValue::Integer(ins as i64)
        }
    }
}

pub(crate) fn cmd_hincrbyfloat(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("hincrbyfloat");
    }
    let Some(delta) = parse_f64(&args[3]).filter(|v| v.is_finite()) else {
        return err(ERR_FLOAT);
    };
    let r: Result<f64, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let h = d.hash_or_create(&args[1])?;
        let cur = match h.get(&args[2]) {
            None => 0.0,
            Some(v) => match parse_f64(v) {
                Some(f) => f,
                None => {
                    d.remove_if_empty(&args[1]);
                    return Err("ERR hash value is not a float");
                }
            },
        };
        let next = cur + delta;
        if !next.is_finite() {
            d.remove_if_empty(&args[1]);
            return Err("ERR increment would produce NaN or Infinity");
        }
        h.insert(args[2].clone(), Bytes::from(fmt_f64(next)));
        Ok(next)
    });
    match r {
        Err(e) => err(e),
        Ok(v) => {
            ctx.db.touch();
            ctx.propagate(&[
                Bytes::from_static(b"HSET"),
                args[1].clone(),
                args[2].clone(),
                Bytes::from(fmt_f64(v)),
            ]);
            bulk_f64(v)
        }
    }
}

pub(crate) fn cmd_hstrlen(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("hstrlen");
    }
    match ctx.db.hget(state.db_index, &args[1], &args[2]) {
        Ok(v) => RespValue::Integer(v.map_or(0, |b| b.len() as i64)),
        Err(e) => err(e),
    }
}

/// HSCAN key cursor [MATCH p] [COUNT n] [NOVALUES].
pub(crate) fn cmd_hscan(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("hscan");
    }
    let Some(cursor) = parse_i64(&args[2]).filter(|c| *c >= 0) else {
        return err("ERR invalid cursor");
    };
    let (pattern, count) = match parse_scan_opts(args, 3) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let novalues = args[3..].iter().any(|a| upper(a) == "NOVALUES");
    let r: Result<Vec<(Bytes, Bytes)>, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(h) = d.hash_mut(&args[1])? else {
            return Ok(Vec::new());
        };
        h.purge_expired(crate::util::now_unix_ms());
        let mut items: Vec<(Bytes, Bytes)> = h
            .iter_live()
            .filter(|(f, _)| pattern.as_deref().map_or(true, |p| match_glob_bytes(p.as_bytes(), f)))
            .map(|(f, v)| (f.clone(), v.clone()))
            .collect();
        items.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        Ok(items)
    });
    match r {
        Err(e) => err(e),
        Ok(items) => {
            let (next, page) = paginate(items, cursor as u64, count);
            let flat = page.into_iter().flat_map(|(f, v)| {
                if novalues {
                    vec![RespValue::bulk(f)]
                } else {
                    vec![RespValue::bulk(f), RespValue::bulk(v)]
                }
            });
            RespValue::array(vec![
                RespValue::bulk(Bytes::from(next.to_string())),
                RespValue::array(flat.collect()),
            ])
        }
    }
}

/// SSCAN key cursor [MATCH p] [COUNT n].
pub(crate) fn cmd_sscan(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("sscan");
    }
    let Some(cursor) = parse_i64(&args[2]).filter(|c| *c >= 0) else {
        return err("ERR invalid cursor");
    };
    let (pattern, count) = match parse_scan_opts(args, 3) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let r: Result<Vec<Bytes>, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(s) = d.set_mut(&args[1])? else {
            return Ok(Vec::new());
        };
        let mut items: Vec<Bytes> = s
            .iter()
            .filter(|m| pattern.as_deref().map_or(true, |p| match_glob_bytes(p.as_bytes(), m)))
            .cloned()
            .collect();
        items.sort_unstable();
        Ok(items)
    });
    match r {
        Err(e) => err(e),
        Ok(items) => {
            let (next, page) = paginate(items, cursor as u64, count);
            RespValue::array(vec![
                RespValue::bulk(Bytes::from(next.to_string())),
                bulk_array(page),
            ])
        }
    }
}

/// SPOP key [count].
pub(crate) fn cmd_spop(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 || args.len() > 3 {
        return wrong_arity("spop");
    }
    let count = match args.get(2) {
        None => None,
        Some(raw) => match parse_i64(raw) {
            Some(n) if n >= 0 => Some(n as usize),
            _ => return err("ERR value is out of range, must be positive"),
        },
    };
    let r: Result<Vec<Bytes>, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(s) = d.set_mut(&args[1])? else {
            return Ok(Vec::new());
        };
        let n = count.unwrap_or(1).min(s.len());
        let chosen: Vec<Bytes> = s.iter().cloned().choose_multiple(&mut rand::thread_rng(), n);
        for m in &chosen {
            s.remove(m);
        }
        d.remove_if_empty(&args[1]);
        Ok(chosen)
    });
    match r {
        Err(e) => err(e),
        Ok(items) => {
            if !items.is_empty() {
                ctx.db.touch();
                let mut aof = vec![Bytes::from_static(b"SREM"), args[1].clone()];
                aof.extend(items.iter().cloned());
                ctx.propagate(&aof);
            }
            match count {
                None => items
                    .into_iter()
                    .next()
                    .map_or_else(RespValue::null_bulk, RespValue::bulk),
                Some(_) => bulk_array(items),
            }
        }
    }
}

/// SRANDMEMBER key [count] (count < 0 : avec répétitions).
pub(crate) fn cmd_srandmember(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() > 3 {
        return err(ERR_SYNTAX);
    }
    let count = match args.get(2) {
        None => None,
        Some(raw) => match parse_i64(raw) {
            None => return err(ERR_INT),
            Some(i64::MIN) => {
                return err("ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807")
            }
            Some(n) => Some(n),
        },
    };
    let r: Result<RespValue, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let s = d.set_mut(&args[1])?;
        Ok(match (s, count) {
            (None, None) => RespValue::null_bulk(),
            (None, Some(_)) => RespValue::array(vec![]),
            (Some(s), None) => random_sample(s.iter(), s.len(), 1)
                .first()
                .map_or_else(RespValue::null_bulk, |m| RespValue::bulk((*m).clone())),
            (Some(s), Some(n)) => bulk_array(random_sample(s.iter(), s.len(), n).into_iter().cloned()),
        })
    });
    reply(r)
}

/// SINTERCARD numkeys key [key …] [LIMIT limit] : intersection comptée depuis le plus petit set,
/// arrêt anticipé dès que `limit` est atteint.
pub(crate) fn cmd_sintercard(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let numkeys = match crate::opts::ranged_long(&args[1], 1, i64::MAX, crate::opts::ERR_NUMKEYS_POSITIVE) {
        Ok(n) => n as usize,
        Err(e) => return e,
    };
    if numkeys > args.len() - 2 {
        return err("ERR Number of keys can't be greater than number of args");
    }
    let keys = &args[2..2 + numkeys];
    let limit = match crate::opts::parse_card_limit(args, 2 + numkeys) {
        Ok(l) => l,
        Err(e) => return e,
    };
    let r: Result<i64, &'static str> = ctx.db.with_db(state.db_index, |d| {
        for k in keys {
            d.purge_if_expired(k);
        }
        let mut sets: Vec<&HashSet<Bytes>> = Vec::with_capacity(numkeys);
        let mut missing = false;
        for k in keys {
            match d.map.get(k).map(|e| &e.value) {
                None => missing = true,
                Some(RedisValue::Set(s)) => sets.push(s),
                Some(_) => return Err(WRONGTYPE),
            }
        }
        if missing {
            return Ok(0);
        }
        sets.sort_by_key(|s| s.len());
        let (first, rest) = sets.split_first().expect("numkeys >= 1");
        let mut card = 0i64;
        for m in first.iter() {
            if rest.iter().all(|s| s.contains(m)) {
                card += 1;
                if limit > 0 && card as usize >= limit {
                    break;
                }
            }
        }
        Ok(card)
    });
    reply(r.map(RespValue::Integer))
}

/// HRANDFIELD key [count [WITHVALUES]].
pub(crate) fn cmd_hrandfield(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let opts = match crate::cmd_zset::parse_rand_count(args, "WITHVALUES") {
        Ok(o) => o,
        Err(e) => return e,
    };
    let r: Result<RespValue, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let h = d.hash_mut(&args[1])?;
        Ok(match (h, opts) {
            (None, None) => RespValue::null_bulk(),
            (None, Some(_)) => RespValue::array(vec![]),
            (Some(h), None) => {
                h.purge_expired(crate::util::now_unix_ms());
                let fields: Vec<Bytes> = h.iter_live().map(|(f, _)| f.clone()).collect();
                random_sample(fields.iter(), fields.len(), 1)
                    .first()
                    .map_or_else(RespValue::null_bulk, |f| RespValue::bulk((*f).clone()))
            }
            (Some(h), Some((count, with))) => {
                h.purge_expired(crate::util::now_unix_ms());
                let live: Vec<(Bytes, Bytes)> = h
                    .iter_live()
                    .map(|(f, v)| (f.clone(), v.clone()))
                    .collect();
                let picked = random_sample(live.iter(), live.len(), count);
                let mut out = Vec::with_capacity(picked.len() * if with { 2 } else { 1 });
                for (f, v) in picked {
                    out.push(RespValue::bulk(f.clone()));
                    if with {
                        out.push(RespValue::bulk(v.clone()));
                    }
                }
                RespValue::array(out)
            }
        })
    });
    reply(r)
}

/// SMOVE src dst member.
pub(crate) fn cmd_smove(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("smove");
    }
    let r: Result<bool, &'static str> = ctx.db.with_db(state.db_index, |d| {
        if let Some(e) = d.get_fresh(&args[2]) {
            if !matches!(e.value, RedisValue::Set(_)) {
                return Err(WRONGTYPE);
            }
        }
        let Some(src) = d.set_mut(&args[1])? else {
            return Ok(false);
        };
        if !src.remove(&args[3]) {
            return Ok(false);
        }
        d.remove_if_empty(&args[1]);
        d.set_or_create(&args[2])?.insert(args[3].clone());
        Ok(true)
    });
    match r {
        Err(e) => err(e),
        Ok(moved) => {
            if moved {
                ctx.db.touch();
                ctx.propagate(args);
            }
            RespValue::Integer(moved as i64)
        }
    }
}

/// SMISMEMBER key member [member …].
pub(crate) fn cmd_smismember(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("smismember");
    }
    let r: Result<Vec<i64>, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let s = d.set_mut(&args[1])?;
        Ok(args[2..]
            .iter()
            .map(|m| s.as_ref().map_or(0, |s| s.contains(m) as i64))
            .collect())
    });
    match r {
        Err(e) => err(e),
        Ok(v) => RespValue::array(v.into_iter().map(RespValue::Integer).collect()),
    }
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum SetOp {
    Inter,
    Union,
    Diff,
}

/// SINTERSTORE / SUNIONSTORE / SDIFFSTORE dst key [key …].
pub(crate) fn cmd_setstore(ctx: &CommandContext, state: &ClientState, args: &[Bytes], op: SetOp) -> RespValue {
    let name = match op {
        SetOp::Inter => "sinterstore",
        SetOp::Union => "sunionstore",
        SetOp::Diff => "sdiffstore",
    };
    if args.len() < 3 {
        return wrong_arity(name);
    }
    let r: Result<i64, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let mut sets: Vec<HashSet<Bytes>> = Vec::with_capacity(args.len() - 2);
        for key in &args[2..] {
            sets.push(match d.get_fresh(key) {
                None => HashSet::new(),
                Some(e) => match &e.value {
                    RedisValue::Set(s) => s.clone(),
                    _ => return Err(WRONGTYPE),
                },
            });
        }
        let result: HashSet<Bytes> = match op {
            SetOp::Union => sets.into_iter().flatten().collect(),
            SetOp::Inter => {
                // On part du plus petit ensemble.
                sets.sort_by_key(|s| s.len());
                let mut it = sets.into_iter();
                let mut acc = it.next().unwrap_or_default();
                for s in it {
                    acc.retain(|m| s.contains(m));
                }
                acc
            }
            SetOp::Diff => {
                let mut it = sets.into_iter();
                let mut acc = it.next().unwrap_or_default();
                for s in it {
                    acc.retain(|m| !s.contains(m));
                }
                acc
            }
        };
        let len = result.len() as i64;
        d.map.remove(&args[1]);
        if !result.is_empty() {
            d.map
                .insert(args[1].clone(), Entry::new(RedisValue::Set(result)));
        }
        Ok(len)
    });
    match r {
        Err(e) => err(e),
        Ok(n) => {
            ctx.db.touch();
            ctx.propagate(args);
            RespValue::Integer(n)
        }
    }
}
