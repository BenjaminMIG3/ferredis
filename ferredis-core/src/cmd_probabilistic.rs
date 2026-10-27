//! Commandes RedisBloom / Cuckoo / CMS / TopK / t-digest (`BF.*`, `CF.*`, …).

use crate::command::{wrong_arity, ClientState, CommandContext};
use crate::probabilistic::{BloomFilter, CountMinSketch, CuckooFilter, TDigest, TopK};
use crate::resp::RespValue;
use crate::store::{DbInner, WRONGTYPE};
use crate::util::*;
use crate::value::{Entry, RedisValue};
use bytes::Bytes;

fn after_write(ctx: &CommandContext, state: &ClientState, key: &Bytes, args: &[Bytes]) {
    ctx.db.touch();
    ctx.notify_key(state.db_index, key, "set");
    ctx.propagate(args);
}

fn info_array(pairs: &[(&'static str, String)]) -> RespValue {
    let mut out = Vec::with_capacity(pairs.len() * 2);
    for (k, v) in pairs {
        out.push(RespValue::bulk(Bytes::from_static(k.as_bytes())));
        out.push(RespValue::bulk(Bytes::from(v.clone())));
    }
    RespValue::array(out)
}

fn tdigest_min_max(td: &TDigest, min: bool) -> f64 {
    if td.count == 0.0 {
        f64::NAN
    } else if min {
        td.min
    } else {
        td.max
    }
}

fn bloom_mut<'a>(d: &'a mut DbInner, key: &Bytes) -> Result<&'a mut BloomFilter, &'static str> {
    match d.get_fresh_mut(key).map(|e| &mut e.value) {
        None => Err("ERR not found"),
        Some(RedisValue::Bloom(b)) => Ok(b),
        Some(_) => Err(WRONGTYPE),
    }
}

fn cuckoo_mut<'a>(d: &'a mut DbInner, key: &Bytes) -> Result<&'a mut CuckooFilter, &'static str> {
    match d.get_fresh_mut(key).map(|e| &mut e.value) {
        None => Err("ERR not found"),
        Some(RedisValue::Cuckoo(c)) => Ok(c),
        Some(_) => Err(WRONGTYPE),
    }
}

fn topk_mut<'a>(d: &'a mut DbInner, key: &Bytes) -> Result<&'a mut TopK, &'static str> {
    match d.get_fresh_mut(key).map(|e| &mut e.value) {
        None => Err("ERR TopK: key does not exist"),
        Some(RedisValue::TopK(t)) => Ok(t),
        Some(_) => Err(WRONGTYPE),
    }
}

fn tdigest_mut<'a>(d: &'a mut DbInner, key: &Bytes) -> Result<&'a mut TDigest, &'static str> {
    match d.get_fresh_mut(key).map(|e| &mut e.value) {
        None => Err("ERR T-Digest: key does not exist"),
        Some(RedisValue::TDigest(t)) => Ok(t),
        Some(_) => Err(WRONGTYPE),
    }
}

// ─── Bloom ───────────────────────────────────────────────────────────

pub(crate) fn cmd_bf_reserve(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("bf.reserve");
    }
    let key = &args[1];
    let err_rate = match parse_f64(&args[2]) {
        Some(v) if v > 0.0 && v < 1.0 => v,
        _ => return err("ERR error rate should be between 0 and 1"),
    };
    let capacity = match parse_i64(&args[3]) {
        Some(v) if v > 0 => v as u64,
        _ => return err("ERR capacity should be > 0"),
    };
    let mut expansion = 2u32;
    let mut i = 4;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "EXPANSION" => {
                i += 1;
                expansion = match args.get(i).and_then(|b| parse_i64(b)) {
                    Some(v) if v >= 0 => v as u32,
                    _ => return err(ERR_SYNTAX),
                };
            }
            "NONSCALING" => expansion = 0,
            _ => return err(ERR_SYNTAX),
        }
        i += 1;
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        if d.get_fresh(key).is_some() {
            return Err("ERR item exists");
        }
        d.map.insert(
            key.clone(),
            Entry::new(RedisValue::Bloom(BloomFilter::reserve(err_rate, capacity, expansion))),
        );
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_bf_add(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("bf.add");
    }
    let key = &args[1];
    let item = &args[2];
    let r = ctx.db.with_db(state.db_index, |d| {
        if d.get_fresh(key).is_none() {
            d.map.insert(
                key.clone(),
                Entry::new(RedisValue::Bloom(BloomFilter::reserve(0.01, 100, 2))),
            );
        }
        let bf = bloom_mut(d, key)?;
        Ok(bf.add(item) as i64)
    });
    match r {
        Err(e) => err(e),
        Ok(added) => {
            after_write(ctx, state, key, args);
            RespValue::Integer(added)
        }
    }
}

pub(crate) fn cmd_bf_madd(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("bf.madd");
    }
    let key = &args[1];
    let r = ctx.db.with_db(state.db_index, |d| {
        if d.get_fresh(key).is_none() {
            d.map.insert(
                key.clone(),
                Entry::new(RedisValue::Bloom(BloomFilter::reserve(0.01, 100, 2))),
            );
        }
        let bf = bloom_mut(d, key)?;
        let mut out = Vec::with_capacity(args.len() - 2);
        for item in &args[2..] {
            out.push(bf.add(item) as i64);
        }
        Ok(out)
    });
    match r {
        Err(e) => err(e),
        Ok(vals) => {
            after_write(ctx, state, key, args);
            RespValue::array(vals.into_iter().map(RespValue::Integer).collect())
        }
    }
}

pub(crate) fn cmd_bf_insert(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("bf.insert");
    }
    let key = &args[1];
    let mut capacity = 100u64;
    let mut error_rate = 0.01f64;
    let mut expansion = 2u32;
    let mut nocreate = false;
    let mut items_at = None;
    let mut i = 2;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "CAPACITY" => {
                i += 1;
                capacity = parse_i64(args.get(i).map(|b| b.as_ref()).unwrap_or(b"0"))
                    .unwrap_or(100)
                    .max(1) as u64;
            }
            "ERROR" => {
                i += 1;
                error_rate = parse_f64(args.get(i).map(|b| b.as_ref()).unwrap_or(b"0.01")).unwrap_or(0.01);
            }
            "EXPANSION" => {
                i += 1;
                expansion = parse_i64(args.get(i).map(|b| b.as_ref()).unwrap_or(b"2")).unwrap_or(2) as u32;
            }
            "NONSCALING" => expansion = 0,
            "NOCREATE" => nocreate = true,
            "ITEMS" => {
                items_at = Some(i + 1);
                break;
            }
            _ => return err(ERR_SYNTAX),
        }
        i += 1;
    }
    let Some(start) = items_at else {
        return err(ERR_SYNTAX);
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        if d.get_fresh(key).is_none() {
            if nocreate {
                return Err("ERR not found");
            }
            d.map.insert(
                key.clone(),
                Entry::new(RedisValue::Bloom(BloomFilter::reserve(
                    error_rate, capacity, expansion,
                ))),
            );
        }
        let bf = bloom_mut(d, key)?;
        let mut out = Vec::new();
        for item in &args[start..] {
            out.push(bf.add(item) as i64);
        }
        Ok(out)
    });
    match r {
        Err(e) => err(e),
        Ok(vals) => {
            after_write(ctx, state, key, args);
            RespValue::array(vals.into_iter().map(RespValue::Integer).collect())
        }
    }
}

pub(crate) fn cmd_bf_exists(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("bf.exists");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Ok(0i64),
            Some(RedisValue::Bloom(b)) => Ok(b.exists(&args[2]) as i64),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(n) => RespValue::Integer(n),
    }
}

pub(crate) fn cmd_bf_mexists(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("bf.mexists");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Ok(args[2..].iter().map(|_| 0i64).collect::<Vec<_>>()),
            Some(RedisValue::Bloom(b)) => Ok(args[2..]
                .iter()
                .map(|it| b.exists(it) as i64)
                .collect::<Vec<_>>()),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(vals) => RespValue::array(vals.into_iter().map(RespValue::Integer).collect()),
    }
}

pub(crate) fn cmd_bf_card(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("bf.card");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Ok(0i64),
            Some(RedisValue::Bloom(b)) => Ok(b.card() as i64),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(n) => RespValue::Integer(n),
    }
}

pub(crate) fn cmd_bf_info(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("bf.info");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Err("ERR not found"),
            Some(RedisValue::Bloom(b)) => Ok(b.info_pairs()),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(pairs) => info_array(&pairs),
    }
}

pub(crate) fn cmd_bf_scandump(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("bf.scandump");
    }
    let iter = match parse_i64(&args[2]) {
        Some(v) if v >= 0 => v,
        _ => return err(ERR_INT),
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Err("ERR not found"),
            Some(RedisValue::Bloom(b)) => {
                if iter == 0 {
                    Ok(RespValue::array(vec![
                        RespValue::Integer(1),
                        RespValue::bulk(Bytes::from(b.scandump())),
                    ]))
                } else {
                    Ok(RespValue::array(vec![RespValue::Integer(0), RespValue::Null]))
                }
            }
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(v) => v,
    }
}

pub(crate) fn cmd_bf_loadchunk(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("bf.loadchunk");
    }
    let key = &args[1];
    let data = &args[3];
    let Some(bf) = BloomFilter::loadchunk(data) else {
        return err("ERR invalid chunk");
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        d.map.insert(key.clone(), Entry::new(RedisValue::Bloom(bf)));
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_bf_debug(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("bf.debug");
    }
    cmd_bf_info(ctx, state, args)
}

// ─── Cuckoo ──────────────────────────────────────────────────────────

pub(crate) fn cmd_cf_reserve(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("cf.reserve");
    }
    let key = &args[1];
    let capacity = match parse_i64(&args[2]) {
        Some(v) if v > 0 => v as u64,
        _ => return err("ERR capacity should be > 0"),
    };
    let mut bucket_size = 2usize;
    let mut max_iterations = 20u32;
    let mut i = 3;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "BUCKETSIZE" => {
                i += 1;
                bucket_size = parse_i64(args.get(i).map(|b| b.as_ref()).unwrap_or(b"2"))
                    .unwrap_or(2)
                    .max(2) as usize;
            }
            "MAXITERATIONS" => {
                i += 1;
                max_iterations = parse_i64(args.get(i).map(|b| b.as_ref()).unwrap_or(b"20"))
                    .unwrap_or(20)
                    .max(1) as u32;
            }
            "EXPANSION" => {
                i += 1;
            }
            _ => return err(ERR_SYNTAX),
        }
        i += 1;
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        if d.get_fresh(key).is_some() {
            return Err("ERR item exists");
        }
        d.map.insert(
            key.clone(),
            Entry::new(RedisValue::Cuckoo(CuckooFilter::reserve(
                capacity,
                bucket_size,
                max_iterations,
            ))),
        );
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_cf_add(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    cf_add_inner(ctx, state, args, false, "cf.add")
}

pub(crate) fn cmd_cf_addnx(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    cf_add_inner(ctx, state, args, true, "cf.addnx")
}

fn cf_add_inner(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    nx: bool,
    name: &str,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arity(name);
    }
    let key = &args[1];
    let r = ctx.db.with_db(state.db_index, |d| {
        if d.get_fresh(key).is_none() {
            d.map.insert(
                key.clone(),
                Entry::new(RedisValue::Cuckoo(CuckooFilter::reserve(1000, 2, 20))),
            );
        }
        let cf = cuckoo_mut(d, key)?;
        if nx && cf.exists(&args[2]) {
            return Ok((0i64, false));
        }
        Ok((cf.add(&args[2]) as i64, true))
    });
    match r {
        Err(e) => err(e),
        Ok((v, wrote)) => {
            if wrote {
                after_write(ctx, state, key, args);
            }
            RespValue::Integer(v)
        }
    }
}

pub(crate) fn cmd_cf_insert(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    cf_insert_inner(ctx, state, args, false, "cf.insert")
}

pub(crate) fn cmd_cf_insertnx(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    cf_insert_inner(ctx, state, args, true, "cf.insertnx")
}

fn cf_insert_inner(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    nx: bool,
    name: &str,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arity(name);
    }
    let key = &args[1];
    let mut capacity = 1000u64;
    let mut nocreate = false;
    let mut items_at = None;
    let mut i = 2;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "CAPACITY" => {
                i += 1;
                capacity = parse_i64(args.get(i).map(|b| b.as_ref()).unwrap_or(b"1000"))
                    .unwrap_or(1000)
                    .max(1) as u64;
            }
            "NOCREATE" => nocreate = true,
            "ITEMS" => {
                items_at = Some(i + 1);
                break;
            }
            _ => return err(ERR_SYNTAX),
        }
        i += 1;
    }
    let Some(start) = items_at else {
        return err(ERR_SYNTAX);
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        if d.get_fresh(key).is_none() {
            if nocreate {
                return Err("ERR not found");
            }
            d.map.insert(
                key.clone(),
                Entry::new(RedisValue::Cuckoo(CuckooFilter::reserve(capacity, 2, 20))),
            );
        }
        let cf = cuckoo_mut(d, key)?;
        let mut out = Vec::new();
        for item in &args[start..] {
            if nx && cf.exists(item) {
                out.push(0i64);
            } else {
                out.push(cf.add(item) as i64);
            }
        }
        Ok(out)
    });
    match r {
        Err(e) => err(e),
        Ok(vals) => {
            after_write(ctx, state, key, args);
            RespValue::array(vals.into_iter().map(RespValue::Integer).collect())
        }
    }
}

pub(crate) fn cmd_cf_exists(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("cf.exists");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Ok(0i64),
            Some(RedisValue::Cuckoo(c)) => Ok(c.exists(&args[2]) as i64),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(n) => RespValue::Integer(n),
    }
}

pub(crate) fn cmd_cf_mexists(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("cf.mexists");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Ok(args[2..].iter().map(|_| 0i64).collect::<Vec<_>>()),
            Some(RedisValue::Cuckoo(c)) => Ok(args[2..]
                .iter()
                .map(|it| c.exists(it) as i64)
                .collect::<Vec<_>>()),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(vals) => RespValue::array(vals.into_iter().map(RespValue::Integer).collect()),
    }
}

pub(crate) fn cmd_cf_del(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("cf.del");
    }
    let key = &args[1];
    let r = ctx.db.with_db(state.db_index, |d| {
        let cf = cuckoo_mut(d, key)?;
        Ok(cf.del(&args[2]) as i64)
    });
    match r {
        Err(e) => err(e),
        Ok(ok) => {
            after_write(ctx, state, key, args);
            RespValue::Integer(ok)
        }
    }
}

pub(crate) fn cmd_cf_count(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("cf.count");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Ok(0i64),
            Some(RedisValue::Cuckoo(c)) => Ok(c.count(&args[2]) as i64),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(n) => RespValue::Integer(n),
    }
}

pub(crate) fn cmd_cf_info(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("cf.info");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Err("ERR not found"),
            Some(RedisValue::Cuckoo(c)) => Ok(c.info_pairs()),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(pairs) => info_array(&pairs),
    }
}

pub(crate) fn cmd_cf_scandump(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("cf.scandump");
    }
    let iter = match parse_i64(&args[2]) {
        Some(v) if v >= 0 => v,
        _ => return err(ERR_INT),
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Err("ERR not found"),
            Some(RedisValue::Cuckoo(c)) => {
                if iter == 0 {
                    Ok(RespValue::array(vec![
                        RespValue::Integer(1),
                        RespValue::bulk(Bytes::from(c.scandump())),
                    ]))
                } else {
                    Ok(RespValue::array(vec![RespValue::Integer(0), RespValue::Null]))
                }
            }
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(v) => v,
    }
}

pub(crate) fn cmd_cf_loadchunk(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("cf.loadchunk");
    }
    let key = &args[1];
    let Some(cf) = CuckooFilter::loadchunk(&args[3]) else {
        return err("ERR invalid chunk");
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        d.map.insert(key.clone(), Entry::new(RedisValue::Cuckoo(cf)));
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_cf_debug(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    cmd_cf_info(ctx, state, args)
}

// ─── CMS ─────────────────────────────────────────────────────────────

pub(crate) fn cmd_cms_initbydim(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("cms.initbydim");
    }
    let key = &args[1];
    let width = match parse_i64(&args[2]) {
        Some(v) if v > 0 => v as usize,
        _ => return err(ERR_INT),
    };
    let depth = match parse_i64(&args[3]) {
        Some(v) if v > 0 => v as usize,
        _ => return err(ERR_INT),
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        if d.get_fresh(key).is_some() {
            return Err("ERR item exists");
        }
        d.map.insert(
            key.clone(),
            Entry::new(RedisValue::Cms(CountMinSketch::init_by_dim(width, depth))),
        );
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_cms_initbyprob(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("cms.initbyprob");
    }
    let key = &args[1];
    let error = match parse_f64(&args[2]) {
        Some(v) if v > 0.0 && v < 1.0 => v,
        _ => return err("ERR invalid error"),
    };
    let prob = match parse_f64(&args[3]) {
        Some(v) if v > 0.0 && v < 1.0 => v,
        _ => return err("ERR invalid probability"),
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        if d.get_fresh(key).is_some() {
            return Err("ERR item exists");
        }
        d.map.insert(
            key.clone(),
            Entry::new(RedisValue::Cms(CountMinSketch::init_by_prob(error, prob))),
        );
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_cms_incrby(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 || args.len() % 2 != 0 {
        return wrong_arity("cms.incrby");
    }
    let key = &args[1];
    let r = ctx.db.with_db(state.db_index, |d| {
        let cms = match d.get_fresh_mut(key).map(|e| &mut e.value) {
            None => return Err("ERR not found"),
            Some(RedisValue::Cms(c)) => c,
            Some(_) => return Err(WRONGTYPE),
        };
        let mut out = Vec::new();
        let mut i = 2;
        while i + 1 < args.len() {
            let delta = match parse_i64(&args[i + 1]) {
                Some(v) if v >= 0 => v as u64,
                _ => return Err(ERR_INT),
            };
            cms.incrby(&args[i], delta);
            out.push(cms.query(&args[i]) as i64);
            i += 2;
        }
        Ok(out)
    });
    match r {
        Err(e) => err(e),
        Ok(vals) => {
            after_write(ctx, state, key, args);
            RespValue::array(vals.into_iter().map(RespValue::Integer).collect())
        }
    }
}

pub(crate) fn cmd_cms_query(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("cms.query");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Err("ERR not found"),
            Some(RedisValue::Cms(c)) => Ok(args[2..]
                .iter()
                .map(|it| c.query(it) as i64)
                .collect::<Vec<_>>()),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(vals) => RespValue::array(vals.into_iter().map(RespValue::Integer).collect()),
    }
}

pub(crate) fn cmd_cms_merge(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("cms.merge");
    }
    let dest = &args[1];
    let n = match parse_i64(&args[2]) {
        Some(v) if v > 0 => v as usize,
        _ => return err(ERR_INT),
    };
    if args.len() < 3 + n {
        return err(ERR_SYNTAX);
    }
    let src_keys: Vec<Bytes> = args[3..3 + n].to_vec();
    let mut weights = vec![1u64; n];
    if args.len() > 3 + n {
        if upper(&args[3 + n]) != "WEIGHTS" || args.len() != 4 + n + n {
            return err(ERR_SYNTAX);
        }
        for (i, w) in args[4 + n..].iter().enumerate() {
            weights[i] = parse_i64(w).unwrap_or(1).max(0) as u64;
        }
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        let sketches: Result<Vec<CountMinSketch>, &'static str> = src_keys
            .iter()
            .map(|k| match d.get_fresh(k).map(|e| &e.value) {
                Some(RedisValue::Cms(c)) => Ok(c.clone()),
                Some(_) => Err(WRONGTYPE),
                None => Err("ERR not found"),
            })
            .collect();
        let sketches = sketches?;
        let refs: Vec<&CountMinSketch> = sketches.iter().collect();
        if let Some(e) = d.get_fresh_mut(dest) {
            match &mut e.value {
                RedisValue::Cms(c) => c.merge(&refs, &weights)?,
                _ => return Err(WRONGTYPE),
            }
        } else if let Some(first) = sketches.first() {
            let mut base = first.clone();
            let rest: Vec<&CountMinSketch> = sketches[1..].iter().collect();
            let w_rest = &weights[1..];
            base.merge(&rest, w_rest)?;
            if weights[0] != 1 {
                for c in &mut base.counters {
                    *c = c.saturating_mul(weights[0]);
                }
            }
            d.map.insert(dest.clone(), Entry::new(RedisValue::Cms(base)));
        }
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, dest, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_cms_info(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("cms.info");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Err("ERR not found"),
            Some(RedisValue::Cms(c)) => Ok(c.info_pairs()),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(pairs) => info_array(&pairs),
    }
}

// ─── TopK ────────────────────────────────────────────────────────────

pub(crate) fn cmd_topk_reserve(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 || args.len() > 6 {
        return wrong_arity("topk.reserve");
    }
    let key = &args[1];
    let k = match parse_i64(&args[2]) {
        Some(v) if v > 0 => v as usize,
        _ => return err(ERR_INT),
    };
    let width = args
        .get(3)
        .and_then(|b| parse_i64(b))
        .unwrap_or(7)
        .max(1) as usize;
    let depth = args
        .get(4)
        .and_then(|b| parse_i64(b))
        .unwrap_or(8)
        .max(1) as usize;
    let decay = args.get(5).and_then(|b| parse_f64(b)).unwrap_or(0.9);
    let r = ctx.db.with_db(state.db_index, |d| {
        if d.get_fresh(key).is_some() {
            return Err("ERR item exists");
        }
        d.map.insert(
            key.clone(),
            Entry::new(RedisValue::TopK(TopK::reserve(k, width, depth, decay))),
        );
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_topk_add(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("topk.add");
    }
    let key = &args[1];
    let r = ctx.db.with_db(state.db_index, |d| {
        let tk = topk_mut(d, key)?;
        let mut out = Vec::new();
        for item in &args[2..] {
            match tk.add(item.clone()) {
                Some(evicted) => out.push(RespValue::bulk(evicted)),
                None => out.push(RespValue::Null),
            }
        }
        Ok(out)
    });
    match r {
        Err(e) => err(e),
        Ok(out) => {
            after_write(ctx, state, key, args);
            RespValue::array(out)
        }
    }
}

pub(crate) fn cmd_topk_incrby(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 || args.len() % 2 != 0 {
        return wrong_arity("topk.incrby");
    }
    let key = &args[1];
    let r = ctx.db.with_db(state.db_index, |d| {
        let tk = topk_mut(d, key)?;
        let mut out = Vec::new();
        let mut i = 2;
        while i + 1 < args.len() {
            let delta = match parse_i64(&args[i + 1]) {
                Some(v) if v > 0 => v as u64,
                _ => return Err(ERR_INT),
            };
            match tk.incrby(args[i].clone(), delta) {
                Some(evicted) => out.push(RespValue::bulk(evicted)),
                None => out.push(RespValue::Null),
            }
            i += 2;
        }
        Ok(out)
    });
    match r {
        Err(e) => err(e),
        Ok(out) => {
            after_write(ctx, state, key, args);
            RespValue::array(out)
        }
    }
}

pub(crate) fn cmd_topk_query(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("topk.query");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Err("ERR TopK: key does not exist"),
            Some(RedisValue::TopK(t)) => Ok(args[2..]
                .iter()
                .map(|it| t.query(it) as i64)
                .collect::<Vec<_>>()),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(vals) => RespValue::array(vals.into_iter().map(RespValue::Integer).collect()),
    }
}

pub(crate) fn cmd_topk_count(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("topk.count");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Err("ERR TopK: key does not exist"),
            Some(RedisValue::TopK(t)) => Ok(args[2..]
                .iter()
                .map(|it| t.count(it) as i64)
                .collect::<Vec<_>>()),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(vals) => RespValue::array(vals.into_iter().map(RespValue::Integer).collect()),
    }
}

pub(crate) fn cmd_topk_list(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 || args.len() > 3 {
        return wrong_arity("topk.list");
    }
    let withcount = args.len() == 3 && upper(&args[2]) == "WITHCOUNT";
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Err("ERR TopK: key does not exist"),
            Some(RedisValue::TopK(t)) => Ok((t.list(), withcount)),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok((list, withcount)) => {
            if withcount {
                let mut out = Vec::with_capacity(list.len() * 2);
                for (k, c) in list {
                    out.push(RespValue::bulk(k));
                    out.push(RespValue::Integer(c as i64));
                }
                RespValue::array(out)
            } else {
                RespValue::array(list.into_iter().map(|(k, _)| RespValue::bulk(k)).collect())
            }
        }
    }
}

pub(crate) fn cmd_topk_info(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("topk.info");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Err("ERR TopK: key does not exist"),
            Some(RedisValue::TopK(t)) => Ok(t.info_pairs()),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(pairs) => info_array(&pairs),
    }
}

// ─── t-digest ────────────────────────────────────────────────────────

pub(crate) fn cmd_tdigest_create(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 || args.len() > 4 {
        return wrong_arity("tdigest.create");
    }
    let key = &args[1];
    let mut compression = 100.0;
    if args.len() >= 4 && upper(&args[2]) == "COMPRESSION" {
        compression = parse_f64(&args[3]).unwrap_or(100.0);
    } else if args.len() == 3 {
        return err(ERR_SYNTAX);
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        if d.get_fresh(key).is_some() {
            return Err("ERR T-Digest: key already exists");
        }
        d.map.insert(
            key.clone(),
            Entry::new(RedisValue::TDigest(TDigest::create(compression))),
        );
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_tdigest_add(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("tdigest.add");
    }
    let key = &args[1];
    let mut vals = Vec::new();
    for a in &args[2..] {
        match parse_f64(a) {
            Some(v) => vals.push(v),
            None => return err("ERR T-Digest: parser error"),
        }
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        let td = tdigest_mut(d, key)?;
        td.add(&vals);
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_tdigest_merge(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("tdigest.merge");
    }
    let dest = &args[1];
    let n = match parse_i64(&args[2]) {
        Some(v) if v > 0 => v as usize,
        _ => return err(ERR_INT),
    };
    if args.len() < 3 + n {
        return err(ERR_SYNTAX);
    }
    let srcs: Vec<Bytes> = args[3..3 + n].to_vec();
    let r = ctx.db.with_db(state.db_index, |d| {
        let digests: Result<Vec<TDigest>, &'static str> = srcs
            .iter()
            .map(|k| match d.get_fresh(k).map(|e| &e.value) {
                Some(RedisValue::TDigest(t)) => Ok(t.clone()),
                Some(_) => Err(WRONGTYPE),
                None => Err("ERR T-Digest: key does not exist"),
            })
            .collect();
        let digests = digests?;
        let refs: Vec<&TDigest> = digests.iter().collect();
        if let Some(e) = d.get_fresh_mut(dest) {
            match &mut e.value {
                RedisValue::TDigest(t) => t.merge(&refs),
                _ => return Err(WRONGTYPE),
            }
        } else {
            let mut base = TDigest::create(100.0);
            base.merge(&refs);
            d.map.insert(dest.clone(), Entry::new(RedisValue::TDigest(base)));
        }
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, dest, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_tdigest_reset(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("tdigest.reset");
    }
    let key = &args[1];
    let r = ctx.db.with_db(state.db_index, |d| {
        let td = tdigest_mut(d, key)?;
        td.reset();
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_tdigest_min(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("tdigest.min");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        let td = tdigest_mut(d, &args[1])?;
        Ok(tdigest_min_max(td, true))
    });
    match r {
        Err(e) => err(e),
        Ok(v) => RespValue::bulk(Bytes::from(fmt_f64(v))),
    }
}

pub(crate) fn cmd_tdigest_max(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("tdigest.max");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        let td = tdigest_mut(d, &args[1])?;
        Ok(tdigest_min_max(td, false))
    });
    match r {
        Err(e) => err(e),
        Ok(v) => RespValue::bulk(Bytes::from(fmt_f64(v))),
    }
}

pub(crate) fn cmd_tdigest_quantile(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("tdigest.quantile");
    }
    let mut qs = Vec::new();
    for a in &args[2..] {
        match parse_f64(a) {
            Some(v) => qs.push(v),
            None => return err("ERR T-Digest: parser error"),
        }
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        let td = tdigest_mut(d, &args[1])?;
        Ok(qs
            .into_iter()
            .map(|q| fmt_f64(td.quantile(q)))
            .collect::<Vec<_>>())
    });
    match r {
        Err(e) => err(e),
        Ok(vals) => RespValue::array(vals.into_iter().map(|s| RespValue::bulk(Bytes::from(s))).collect()),
    }
}

pub(crate) fn cmd_tdigest_cdf(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("tdigest.cdf");
    }
    let mut xs = Vec::new();
    for a in &args[2..] {
        match parse_f64(a) {
            Some(v) => xs.push(v),
            None => return err("ERR T-Digest: parser error"),
        }
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        let td = tdigest_mut(d, &args[1])?;
        Ok(xs
            .into_iter()
            .map(|x| fmt_f64(td.cdf(x)))
            .collect::<Vec<_>>())
    });
    match r {
        Err(e) => err(e),
        Ok(vals) => RespValue::array(vals.into_iter().map(|s| RespValue::bulk(Bytes::from(s))).collect()),
    }
}

pub(crate) fn cmd_tdigest_trimmed_mean(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("tdigest.trimmed_mean");
    }
    let lo = match parse_f64(&args[2]) {
        Some(v) => v,
        None => return err("ERR T-Digest: parser error"),
    };
    let hi = match parse_f64(&args[3]) {
        Some(v) => v,
        None => return err("ERR T-Digest: parser error"),
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        let td = tdigest_mut(d, &args[1])?;
        Ok(fmt_f64(td.trimmed_mean(lo, hi)))
    });
    match r {
        Err(e) => err(e),
        Ok(s) => RespValue::bulk(Bytes::from(s)),
    }
}

fn tdigest_rank_cmd(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    name: &str,
    rev: bool,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arity(name);
    }
    let mut xs = Vec::new();
    for a in &args[2..] {
        match parse_f64(a) {
            Some(v) => xs.push(v),
            None => return err("ERR T-Digest: parser error"),
        }
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        let td = tdigest_mut(d, &args[1])?;
        Ok(xs
            .into_iter()
            .map(|x| {
                let v = if rev { td.revrank(x) } else { td.rank(x) };
                fmt_f64(v)
            })
            .collect::<Vec<_>>())
    });
    match r {
        Err(e) => err(e),
        Ok(vals) => RespValue::array(vals.into_iter().map(|s| RespValue::bulk(Bytes::from(s))).collect()),
    }
}

pub(crate) fn cmd_tdigest_rank(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    tdigest_rank_cmd(ctx, state, args, "tdigest.rank", false)
}

pub(crate) fn cmd_tdigest_revrank(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    tdigest_rank_cmd(ctx, state, args, "tdigest.revrank", true)
}

fn tdigest_byrank_cmd(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    name: &str,
    rev: bool,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arity(name);
    }
    let mut rs = Vec::new();
    for a in &args[2..] {
        match parse_f64(a) {
            Some(v) => rs.push(v),
            None => return err("ERR T-Digest: parser error"),
        }
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        let td = tdigest_mut(d, &args[1])?;
        Ok(rs
            .into_iter()
            .map(|rank| {
                let v = if rev { td.byrevrank(rank) } else { td.byrank(rank) };
                fmt_f64(v)
            })
            .collect::<Vec<_>>())
    });
    match r {
        Err(e) => err(e),
        Ok(vals) => RespValue::array(vals.into_iter().map(|s| RespValue::bulk(Bytes::from(s))).collect()),
    }
}

pub(crate) fn cmd_tdigest_byrank(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    tdigest_byrank_cmd(ctx, state, args, "tdigest.byrank", false)
}

pub(crate) fn cmd_tdigest_byrevrank(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
) -> RespValue {
    tdigest_byrank_cmd(ctx, state, args, "tdigest.byrevrank", true)
}

pub(crate) fn cmd_tdigest_info(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("tdigest.info");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => Err("ERR T-Digest: key does not exist"),
            Some(RedisValue::TDigest(t)) => Ok(t.info_pairs()),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(pairs) => info_array(&pairs),
    }
}
