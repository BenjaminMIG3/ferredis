//! Commandes HyperLogLog : PFADD, PFCOUNT, PFMERGE, PFDEBUG, PFSELFTEST (valeurs `TYPE string`).

use crate::command::{ClientState, CommandContext};
use crate::hll::{self, Corrupt};
use crate::resp::RespValue;
use crate::store::{DbInner, WRONGTYPE};
use crate::util::*;
use crate::value::{Entry, RedisValue};
use bytes::Bytes;

const ERR_NOT_HLL: &str = "WRONGTYPE Key is not a valid HyperLogLog string value.";
const ERR_CORRUPT: &str = "INVALIDOBJ Corrupted HLL object detected";

fn corrupt(_: Corrupt) -> &'static str {
    ERR_CORRUPT
}

/// Valeur HLL existante en lecture (`None` si la clé est absente).
fn read_hll<'a>(d: &'a mut DbInner, key: &Bytes) -> Result<Option<&'a Bytes>, &'static str> {
    match d.get_fresh(key).map(|e| &e.value) {
        None => Ok(None),
        Some(RedisValue::String(s)) if hll::is_valid(s) => Ok(Some(s)),
        Some(RedisValue::String(_)) => Err(ERR_NOT_HLL),
        Some(_) => Err(WRONGTYPE),
    }
}

/// HLL existante (ou créée vide si `create`) éditée en place. `created` indique une création.
fn edit_hll<R>(
    d: &mut DbInner,
    key: &Bytes,
    create: bool,
    f: impl FnOnce(&mut Vec<u8>) -> Result<R, &'static str>,
) -> Result<Option<(R, bool)>, &'static str> {
    let created = read_hll(d, key)?.is_none();
    if created && !create {
        return Ok(None);
    }
    if created {
        d.map.insert(key.clone(), Entry::new(RedisValue::String(Bytes::from(hll::new_sparse()))));
    }
    let Some(Entry { value: RedisValue::String(s), .. }) = d.map.get_mut(key) else {
        return Err(WRONGTYPE);
    };
    let mut v: Vec<u8> = std::mem::take(s).into();
    let r = f(&mut v);
    *s = Bytes::from(v);
    Ok(Some((r?, created)))
}

fn after_write(ctx: &CommandContext, state: &ClientState, key: &Bytes, args: &[Bytes], event: Option<&str>) {
    ctx.db.touch();
    if let Some(ev) = event {
        ctx.notify_key(state.db_index, key, ev);
    }
    ctx.propagate(args);
}

/// PFADD key [element …].
pub(crate) fn cmd_pfadd(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let r = ctx.db.with_db(state.db_index, |d| {
        edit_hll(d, &args[1], true, |v| {
            let mut updated = false;
            for ele in &args[2..] {
                updated |= hll::add(v, ele).map_err(corrupt)?;
            }
            if updated {
                hll::invalidate_cache(v);
            }
            Ok(updated)
        })
    });
    match r {
        Err(e) => err(e),
        Ok(Some((updated, created))) if updated || created => {
            after_write(ctx, state, &args[1], args, Some("pfadd"));
            RespValue::Integer(1)
        }
        Ok(_) => RespValue::Integer(0),
    }
}

/// PFCOUNT key [key …] : cache de cardinalité mis à jour pour une clé unique.
pub(crate) fn cmd_pfcount(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() > 2 {
        let r = ctx.db.with_db(state.db_index, |d| {
            let mut max = hll::new_raw();
            for key in &args[1..] {
                if let Some(s) = read_hll(d, key)? {
                    hll::merge_into(&mut max, s).map_err(corrupt)?;
                }
            }
            Ok::<_, &'static str>(hll::count_raw(&max))
        });
        return reply(r.map(|n| RespValue::Integer(n as i64)));
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        if let Some(card) = read_hll(d, &args[1])?.and_then(|s| hll::cached_card(s)) {
            return Ok((card, false));
        }
        let out = edit_hll(d, &args[1], false, |v| {
            let card = hll::count(v).map_err(corrupt)?;
            hll::set_cached_card(v, card);
            Ok(card)
        })?;
        Ok::<_, &'static str>(out.map_or((0, false), |(card, _)| (card, true)))
    });
    match r {
        Err(e) => err(e),
        Ok((card, refreshed)) => {
            if refreshed {
                after_write(ctx, state, &args[1], args, None);
            }
            RespValue::Integer(card as i64)
        }
    }
}

/// PFMERGE destkey [sourcekey …] (la destination fait partie des sources).
pub(crate) fn cmd_pfmerge(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let r = ctx.db.with_db(state.db_index, |d| {
        let mut max = hll::new_raw();
        let mut use_dense = false;
        for key in &args[1..] {
            if let Some(s) = read_hll(d, key)? {
                use_dense |= hll::encoding(s) == hll::DENSE;
                hll::merge_into(&mut max, s).map_err(corrupt)?;
            }
        }
        edit_hll(d, &args[1], true, |v| {
            if use_dense {
                hll::sparse_to_dense(v).map_err(corrupt)?;
            }
            for (i, &m) in max.iter().enumerate() {
                if m != 0 {
                    hll::set_register(v, i, m).map_err(corrupt)?;
                }
            }
            hll::invalidate_cache(v);
            Ok(())
        })
    });
    match r {
        Err(e) => err(e),
        Ok(_) => {
            after_write(ctx, state, &args[1], args, Some("pfadd"));
            RespValue::ok()
        }
    }
}

/// PFDEBUG GETREG|DECODE|ENCODING|TODENSE key.
pub(crate) fn cmd_pfdebug(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let sub = upper(&args[1]);
    let key = &args[2];
    let r: Result<(RespValue, bool), &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(s) = read_hll(d, key)? else {
            return Err("ERR The specified key does not exist");
        };
        let sparse = hll::encoding(s) == hll::SPARSE;
        match sub.as_str() {
            "ENCODING" => Ok((
                RespValue::simple(Bytes::from_static(if sparse { b"sparse" } else { b"dense" })),
                false,
            )),
            "DECODE" if !sparse => Err("ERR HLL encoding is not sparse"),
            "DECODE" => Ok((RespValue::bulk(Bytes::from(hll::decode_sparse(s))), false)),
            "GETREG" | "TODENSE" => {
                let converted = edit_hll(d, key, false, |v| hll::sparse_to_dense(v).map_err(corrupt))?
                    .is_some_and(|(c, _)| c);
                if sub == "TODENSE" {
                    return Ok((RespValue::Integer(converted as i64), converted));
                }
                let regs = read_hll(d, key)?.map(|s| hll::dense_registers(s)).unwrap_or_default();
                Ok((
                    RespValue::array(regs.into_iter().map(|r| RespValue::Integer(r as i64)).collect()),
                    converted,
                ))
            }
            _ => Err(""),
        }
    });
    match r {
        Err("") => RespValue::err(format!(
            "ERR Unknown PFDEBUG subcommand '{}'",
            String::from_utf8_lossy(&args[1])
        )),
        Err(e) => err(e),
        Ok((reply, converted)) => {
            if converted {
                after_write(ctx, state, key, args, None);
            }
            reply
        }
    }
}

/// PFSELFTEST : volume Redis (`1000` cycles registres + card jusqu'à `10_000_000`).
pub(crate) fn cmd_pfselftest(_: &CommandContext, _: &mut ClientState, _: &[Bytes]) -> RespValue {
    // CPU long : ne pas geler le runtime Tokio (équivalent spawn_blocking).
    match tokio::task::block_in_place(|| hll::selftest(1_000, 10_000_000)) {
        Ok(()) => RespValue::ok(),
        Err(msg) => RespValue::err(msg),
    }
}
