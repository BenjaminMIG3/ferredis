//! Commandes Redis Vector set (`VADD`, `VSIM`, …).

use crate::command::{wrong_arity, ClientState, CommandContext};
use crate::resp::RespValue;
use crate::store::{DbInner, WRONGTYPE};
use crate::util::*;
use crate::value::{Entry, RedisValue};
use crate::vector_set::{Quantization, VectorSet};
use bytes::Bytes;

fn after_write(ctx: &CommandContext, state: &ClientState, key: &Bytes, args: &[Bytes]) {
    ctx.db.touch();
    ctx.notify_key(state.db_index, key, "set");
    ctx.propagate(args);
}

fn ensure_vs<'a>(d: &'a mut DbInner, key: &Bytes, create: bool) -> Result<&'a mut VectorSet, &'static str> {
    let needs_create = match d.get_fresh(key).map(|e| &e.value) {
        Some(RedisValue::VectorSet(_)) => false,
        Some(_) => return Err(WRONGTYPE),
        None if create => true,
        None => return Err("ERR key does not exist"),
    };
    if needs_create {
        d.map
            .insert(key.clone(), Entry::new(RedisValue::VectorSet(VectorSet::new(0))));
    }
    match d.map.get_mut(key).map(|e| &mut e.value) {
        Some(RedisValue::VectorSet(v)) => Ok(v),
        _ => unreachable!(),
    }
}

fn parse_fp32_values(args: &[Bytes], start: usize, dim: usize) -> Result<(Vec<f32>, usize), RespValue> {
    let mut i = start;
    if i < args.len() && upper(&args[i]) == "FP32" {
        i += 1;
        let raw = args.get(i).ok_or_else(|| err(ERR_SYNTAX))?;
        i += 1;
        if raw.len() != dim * 4 && dim > 0 {
            return Err(err("ERR invalid vector dimension"));
        }
        let n = if dim == 0 { raw.len() / 4 } else { dim };
        if raw.len() < n * 4 {
            return Err(err("ERR bad FP32 blob"));
        }
        let mut vals = Vec::with_capacity(n);
        for k in 0..n {
            let o = k * 4;
            vals.push(f32::from_le_bytes(raw[o..o + 4].try_into().unwrap()));
        }
        return Ok((vals, i));
    }
    if i < args.len() && upper(&args[i]) == "VALUES" {
        i += 1;
        let n = parse_i64(args.get(i).ok_or_else(|| err(ERR_SYNTAX))?)
            .ok_or_else(|| err(ERR_INT))? as usize;
        i += 1;
        if args.len() < i + n {
            return Err(err(ERR_SYNTAX));
        }
        let mut vals = Vec::with_capacity(n);
        for j in 0..n {
            vals.push(parse_f64(&args[i + j]).ok_or_else(|| err("ERR invalid vector element"))? as f32);
        }
        return Ok((vals, i + n));
    }
    Err(err(ERR_SYNTAX))
}

/// VADD key [REDUCE dim] (FP32 blob | VALUES n …) ele [CAS] [NOQUANT|Q8|BIN] [EF n] [M n] [SETATTR json]
pub(crate) fn cmd_vadd(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("vadd");
    }
    let key = &args[1];
    let mut i = 2;
    let mut reduce_dim = None;
    if i < args.len() && upper(&args[i]) == "REDUCE" {
        i += 1;
        reduce_dim = Some(
            parse_i64(args.get(i).ok_or(()).ok().map(|b| b.as_ref()).unwrap_or(b"0"))
                .unwrap_or(0)
                .max(1) as usize,
        );
        i += 1;
    }
    let (mut values, next) = match parse_fp32_values(args, i, reduce_dim.unwrap_or(0)) {
        Ok(v) => v,
        Err(e) => return e,
    };
    i = next;
    if reduce_dim.is_some() && reduce_dim.unwrap() < values.len() {
        values.truncate(reduce_dim.unwrap());
    }
    let ele = match args.get(i) {
        Some(e) => e.clone(),
        None => return err(ERR_SYNTAX),
    };
    i += 1;
    let mut cas = false;
    let mut quant = Quantization::F32;
    let mut ef = None;
    let mut m = None;
    let mut setattr = None;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "CAS" => cas = true,
            "NOQUANT" => quant = Quantization::F32,
            "Q8" => quant = Quantization::Q8,
            "BIN" => quant = Quantization::Bin,
            "EF" => {
                i += 1;
                ef = Some(parse_i64(args.get(i).map(|b| b.as_ref()).unwrap_or(b"200")).unwrap_or(200) as usize);
            }
            "M" => {
                i += 1;
                m = Some(parse_i64(args.get(i).map(|b| b.as_ref()).unwrap_or(b"16")).unwrap_or(16) as usize);
            }
            "SETATTR" => {
                i += 1;
                setattr = Some(args.get(i).cloned().unwrap_or_default());
            }
            _ => return err(ERR_SYNTAX),
        }
        i += 1;
    }
    let _ = cas; // accepted for wire parity
    let r = ctx.db.with_db(state.db_index, |d| -> Result<i64, RespValue> {
        let vs = ensure_vs(d, key, true).map_err(err)?;
        if let Some(e) = ef {
            vs.ef = e;
        }
        if let Some(mm) = m {
            vs.m = mm;
        }
        vs.quantization = quant;
        let added = vs.add(ele.clone(), values).map_err(err)?;
        if let Some(attr) = setattr {
            vs.set_attr(ele, attr);
        }
        Ok(added as i64)
    });
    match r {
        Err(e) => e,
        Ok(added) => {
            after_write(ctx, state, key, args);
            RespValue::Integer(added)
        }
    }
}

pub(crate) fn cmd_vcard(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("vcard");
    }
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => RespValue::Integer(0),
            Some(RedisValue::VectorSet(v)) => RespValue::Integer(v.card() as i64),
            Some(_) => err(WRONGTYPE),
        }
    })
}

pub(crate) fn cmd_vdim(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("vdim");
    }
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => err("ERR key does not exist"),
            Some(RedisValue::VectorSet(v)) => RespValue::Integer(v.dim as i64),
            Some(_) => err(WRONGTYPE),
        }
    })
}

pub(crate) fn cmd_vemb(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("vemb");
    }
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => RespValue::Null,
            Some(RedisValue::VectorSet(v)) => match v.emb(&args[2]) {
                Some(vals) => RespValue::array(
                    vals.iter()
                        .map(|f| RespValue::bulk(Bytes::from(fmt_f64(f64::from(*f)))))
                        .collect(),
                ),
                None => RespValue::Null,
            },
            Some(_) => err(WRONGTYPE),
        }
    })
}

pub(crate) fn cmd_vgetattr(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("vgetattr");
    }
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => RespValue::Null,
            Some(RedisValue::VectorSet(v)) => match v.get_attr(&args[2]) {
                Some(a) => RespValue::bulk(a.clone()),
                None => RespValue::Null,
            },
            Some(_) => err(WRONGTYPE),
        }
    })
}

pub(crate) fn cmd_vsetattr(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("vsetattr");
    }
    let key = &args[1];
    let r = ctx.db.with_db(state.db_index, |d| -> Result<i64, &'static str> {
        let vs = ensure_vs(d, key, false)?;
        Ok(vs.set_attr(args[2].clone(), args[3].clone()) as i64)
    });
    match r {
        Err(e) => err(e),
        Ok(ok) => {
            after_write(ctx, state, key, args);
            RespValue::Integer(ok)
        }
    }
}

pub(crate) fn cmd_vinfo(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("vinfo");
    }
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => err("ERR key does not exist"),
            Some(RedisValue::VectorSet(v)) => RespValue::array(vec![
                RespValue::bulk(Bytes::from_static(b"quant-type")),
                RespValue::bulk(Bytes::from_static(match v.quantization {
                    Quantization::F32 => b"f32",
                    Quantization::Q8 => b"q8",
                    Quantization::Bin => b"bin",
                })),
                RespValue::bulk(Bytes::from_static(b"vector-dim")),
                RespValue::Integer(v.dim as i64),
                RespValue::bulk(Bytes::from_static(b"size")),
                RespValue::Integer(v.card() as i64),
                RespValue::bulk(Bytes::from_static(b"max-level")),
                RespValue::Integer(0),
                RespValue::bulk(Bytes::from_static(b"vset-uid")),
                RespValue::Integer(0),
                RespValue::bulk(Bytes::from_static(b"hnsw-m")),
                RespValue::Integer(v.m as i64),
                RespValue::bulk(Bytes::from_static(b"hnsw-ef-construction")),
                RespValue::Integer(v.ef as i64),
            ]),
            Some(_) => err(WRONGTYPE),
        }
    })
}

pub(crate) fn cmd_vismember(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("vismember");
    }
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => RespValue::Integer(0),
            Some(RedisValue::VectorSet(v)) => RespValue::Integer(v.is_member(&args[2]) as i64),
            Some(_) => err(WRONGTYPE),
        }
    })
}

pub(crate) fn cmd_vlinks(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 || args.len() > 4 {
        return wrong_arity("vlinks");
    }
    let withscores = args.len() == 4 && upper(&args[3]) == "WITHSCORES";
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => RespValue::Null,
            Some(RedisValue::VectorSet(v)) => match v.links_of(&args[2]) {
                None => RespValue::Null,
                Some(links) => {
                    if withscores {
                        let emb = v.emb(&args[2]);
                        let mut out = Vec::new();
                        for l in links {
                            out.push(RespValue::bulk(l.clone()));
                            let score = match (emb, v.emb(l)) {
                                (Some(a), Some(b)) => {
                                    let mut dot = 0.0f64;
                                    let mut na = 0.0f64;
                                    let mut nb = 0.0f64;
                                    for (&x, &y) in a.iter().zip(b.iter()) {
                                        let xf = f64::from(x);
                                        let yf = f64::from(y);
                                        dot += xf * yf;
                                        na += xf * xf;
                                        nb += yf * yf;
                                    }
                                    if na == 0.0 || nb == 0.0 {
                                        0.0
                                    } else {
                                        dot / (na.sqrt() * nb.sqrt())
                                    }
                                }
                                _ => 0.0,
                            };
                            out.push(RespValue::bulk(Bytes::from(fmt_f64(score))));
                        }
                        RespValue::array(out)
                    } else {
                        RespValue::array(links.iter().map(|l| RespValue::bulk(l.clone())).collect())
                    }
                }
            },
            Some(_) => err(WRONGTYPE),
        }
    })
}

pub(crate) fn cmd_vrandmember(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 || args.len() > 3 {
        return wrong_arity("vrandmember");
    }
    let count = if args.len() == 3 {
        parse_i64(&args[2]).unwrap_or(1)
    } else {
        1
    };
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(&args[1]).map(|e| &e.value) {
            None => {
                if args.len() == 3 {
                    RespValue::array(vec![])
                } else {
                    RespValue::Null
                }
            }
            Some(RedisValue::VectorSet(v)) => {
                let members = v.rand_member(count as isize);
                if args.len() == 2 {
                    members
                        .into_iter()
                        .next()
                        .map(RespValue::bulk)
                        .unwrap_or(RespValue::Null)
                } else {
                    RespValue::array(members.into_iter().map(RespValue::bulk).collect())
                }
            }
            Some(_) => err(WRONGTYPE),
        }
    })
}

pub(crate) fn cmd_vrem(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("vrem");
    }
    let key = &args[1];
    let r = ctx.db.with_db(state.db_index, |d| -> Result<i64, &'static str> {
        let vs = ensure_vs(d, key, false)?;
        Ok(vs.rem(&args[2]) as i64)
    });
    match r {
        Err(e) => err(e),
        Ok(ok) => {
            after_write(ctx, state, key, args);
            RespValue::Integer(ok)
        }
    }
}

/// VSIM key (ELE ele | FP32 blob | VALUES n …) [WITHSCORES] [COUNT n] [EF n] [FILTER expr] [TRUTH]
pub(crate) fn cmd_vsim(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("vsim");
    }
    let key = &args[1];
    ctx.db.with_db(state.db_index, |d| {
        let vs = match d.get_fresh(key).map(|e| &e.value) {
            None => return RespValue::array(vec![]),
            Some(RedisValue::VectorSet(v)) => v,
            Some(_) => return err(WRONGTYPE),
        };
        let mut i = 2;
        let query = if upper(&args[i]) == "ELE" {
            i += 1;
            let ele = args.get(i);
            i += 1;
            match ele.and_then(|e| vs.emb(e).map(|v| v.to_vec())) {
                Some(v) => v,
                None => return RespValue::array(vec![]),
            }
        } else {
            match parse_fp32_values(args, i, vs.dim) {
                Ok((vals, next)) => {
                    i = next;
                    vals
                }
                Err(e) => return e,
            }
        };
        let mut withscores = false;
        let mut count = 10usize;
        while i < args.len() {
            match upper(&args[i]).as_str() {
                "WITHSCORES" => withscores = true,
                "COUNT" => {
                    i += 1;
                    count = parse_i64(args.get(i).map(|b| b.as_ref()).unwrap_or(b"10"))
                        .unwrap_or(10)
                        .max(1) as usize;
                }
                "EF" | "FILTER" | "TRUTH" => {
                    if upper(&args[i]) == "EF" || upper(&args[i]) == "FILTER" {
                        i += 1;
                    }
                }
                _ => return err(ERR_SYNTAX),
            }
            i += 1;
        }
        let hits = vs.sim(&query, count);
        if withscores {
            let mut out = Vec::with_capacity(hits.len() * 2);
            for (k, s) in hits {
                out.push(RespValue::bulk(k));
                out.push(RespValue::bulk(Bytes::from(fmt_f64(s))));
            }
            RespValue::array(out)
        } else {
            RespValue::array(hits.into_iter().map(|(k, _)| RespValue::bulk(k)).collect())
        }
    })
}
