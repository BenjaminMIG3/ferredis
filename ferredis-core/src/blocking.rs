//! Commandes bloquantes : BLPOP…BZMPOP, XREAD/XREADGROUP BLOCK.
//!
//! Modèle unique : enregistrement sur les clés **avant** la tentative, puis
//! `Notify` jusqu'à écriture ou timeout. Hors TCP (MULTI/Lua) ? une seule tentative.

use crate::cmd_stream::{self, XReadGroupSpec, XReadSpec};
use crate::command::{wrong_arity, ClientState, CommandContext};
use crate::command_registry;
use crate::opts::{ranged_long, ERR_COUNT_POSITIVE, ERR_NUMKEYS_POSITIVE};
use crate::resp::RespValue;
use crate::util::*;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{timeout_at, Instant as TokioInstant};

#[derive(Clone)]
enum Kind {
    ListPop { left: bool },
    ZPop { max: bool },
    ListMove { src_left: bool, dst_left: bool },
    ListMPop { left: bool, count: usize },
    ZMPop { max: bool, count: usize },
    XRead(XReadSpec),
    XReadGroup(XReadGroupSpec),
}

struct Spec {
    kind: Kind,
    keys: Vec<Bytes>,
    /// `None` = bloque indéfiniment ; ignoré si `do_block` est faux.
    timeout: Option<Duration>,
    on_timeout: RespValue,
    /// Faux pour XREAD/XREADGROUP sans option BLOCK (une seule tentative).
    do_block: bool,
}

const BLOCKING: [&str; 10] = [
    "BLPOP", "BRPOP", "BZPOPMIN", "BZPOPMAX", "BRPOPLPUSH", "BLMOVE", "BLMPOP", "BZMPOP", "XREAD",
    "XREADGROUP",
];

pub(crate) fn is_blocking(cmd: &str) -> bool {
    BLOCKING.contains(&cmd)
}

pub(crate) fn is_blocking_name(raw: &[u8]) -> bool {
    BLOCKING.iter().any(|n| raw.eq_ignore_ascii_case(n.as_bytes()))
}

fn parse_timeout(raw: &[u8]) -> Result<Option<Duration>, RespValue> {
    let t = std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|t| t.is_finite())
        .ok_or_else(|| err("ERR timeout is not a float or out of range"))?;
    if t < 0.0 {
        return Err(err("ERR timeout is negative"));
    }
    if t == 0.0 {
        return Ok(None);
    }
    Ok(Some(Duration::from_secs_f64(t.min(315_360_000.0))))
}

fn parse_block_ms(ms: u64) -> Option<Duration> {
    if ms == 0 {
        None
    } else {
        Some(Duration::from_millis(ms.min(315_360_000_000)))
    }
}

fn parse_mpop(args: &[Bytes], numkeys_idx: usize, sides: (&str, &str)) -> Result<(Vec<Bytes>, bool, usize), RespValue> {
    let numkeys = ranged_long(&args[numkeys_idx], 1, i64::MAX, ERR_NUMKEYS_POSITIVE)? as usize;
    let where_idx = numkeys_idx.saturating_add(numkeys).saturating_add(1);
    if where_idx >= args.len() {
        return Err(err(ERR_SYNTAX));
    }
    let first = match upper(&args[where_idx]) {
        s if s == sides.0 => true,
        s if s == sides.1 => false,
        _ => return Err(err(ERR_SYNTAX)),
    };
    let mut count = None;
    let mut j = where_idx + 1;
    while j < args.len() {
        if count.is_none() && upper(&args[j]) == "COUNT" && j + 1 < args.len() {
            count = Some(ranged_long(&args[j + 1], 1, i64::MAX, ERR_COUNT_POSITIVE)? as usize);
            j += 2;
        } else {
            return Err(err(ERR_SYNTAX));
        }
    }
    Ok((args[numkeys_idx + 1..where_idx].to_vec(), first, count.unwrap_or(1)))
}

fn parse(ctx: &CommandContext, state: &ClientState, cmd: &str, args: &[Bytes]) -> Result<Spec, RespValue> {
    if !command_registry::arity_ok(cmd, args.len()) {
        return Err(wrong_arity(&cmd.to_ascii_lowercase()));
    }
    match cmd {
        "BLPOP" | "BRPOP" | "BZPOPMIN" | "BZPOPMAX" => {
            let kind = match cmd {
                "BLPOP" => Kind::ListPop { left: true },
                "BRPOP" => Kind::ListPop { left: false },
                "BZPOPMIN" => Kind::ZPop { max: false },
                _ => Kind::ZPop { max: true },
            };
            let timeout = parse_timeout(&args[args.len() - 1])?;
            Ok(Spec {
                kind,
                keys: args[1..args.len() - 1].to_vec(),
                timeout,
                on_timeout: RespValue::null_array(),
                do_block: true,
            })
        }
        "BRPOPLPUSH" => Ok(Spec {
            kind: Kind::ListMove {
                src_left: false,
                dst_left: true,
            },
            keys: vec![args[1].clone()],
            timeout: parse_timeout(&args[3])?,
            on_timeout: RespValue::null_bulk(),
            do_block: true,
        }),
        "BLMOVE" => {
            let side = |b: &[u8]| match upper(b).as_str() {
                "LEFT" => Ok(true),
                "RIGHT" => Ok(false),
                _ => Err(err(ERR_SYNTAX)),
            };
            Ok(Spec {
                kind: Kind::ListMove {
                    src_left: side(&args[3])?,
                    dst_left: side(&args[4])?,
                },
                keys: vec![args[1].clone()],
                timeout: parse_timeout(&args[5])?,
                on_timeout: RespValue::null_bulk(),
                do_block: true,
            })
        }
        "LMPOP" | "BLMPOP" | "ZMPOP" | "BZMPOP" => {
            let block = cmd.starts_with('B');
            let list = cmd.ends_with("LMPOP");
            let sides = if list { ("LEFT", "RIGHT") } else { ("MIN", "MAX") };
            let (keys, first, count) = parse_mpop(args, block as usize + 1, sides)?;
            let kind = if list {
                Kind::ListMPop { left: first, count }
            } else {
                Kind::ZMPop { max: !first, count }
            };
            Ok(Spec {
                kind,
                keys,
                timeout: if block { parse_timeout(&args[1])? } else { None },
                on_timeout: RespValue::null_array(),
                do_block: block,
            })
        }
        "XREAD" => {
            let (mut spec, block_ms) = cmd_stream::parse_xread(args)?;
            if block_ms.is_some() {
                cmd_stream::resolve_xread_ids(ctx, state.db_index, &mut spec);
            }
            let keys: Vec<_> = spec.streams.iter().map(|(k, _)| k.clone()).collect();
            Ok(Spec {
                kind: Kind::XRead(spec),
                keys,
                timeout: block_ms.map_or(Some(Duration::ZERO), parse_block_ms),
                on_timeout: RespValue::null_array(),
                do_block: block_ms.is_some(),
            })
        }
        "XREADGROUP" => {
            let (spec, block_ms) = cmd_stream::parse_xreadgroup(args)?;
            let keys: Vec<_> = spec.streams.iter().map(|(k, _)| k.clone()).collect();
            Ok(Spec {
                kind: Kind::XReadGroup(spec),
                keys,
                timeout: block_ms.map_or(Some(Duration::ZERO), parse_block_ms),
                on_timeout: RespValue::null_array(),
                do_block: block_ms.is_some(),
            })
        }
        _ => Err(err(ERR_SYNTAX)),
    }
}

fn list_pop_notify(ctx: &CommandContext, db: usize, key: &Bytes, left: bool, count: usize) -> Result<Vec<Bytes>, RespValue> {
    let (out, emptied) = ctx.db.with_db(db, |d| {
        let Some(l) = d.list_mut(key).map_err(err)? else {
            return Ok((Vec::new(), false));
        };
        let n = count.min(l.len());
        let out: Vec<Bytes> = if left {
            l.drain(..n).collect()
        } else {
            (0..n).filter_map(|_| l.pop_back()).collect()
        };
        let emptied = l.is_empty();
        d.remove_if_empty(key);
        Ok::<_, RespValue>((out, emptied))
    })?;
    if !out.is_empty() {
        ctx.db.touch();
        ctx.notify_key(db, key, if left { "lpop" } else { "rpop" });
        if emptied {
            ctx.notify_key(db, key, "del");
        }
        let verb = if left { &b"LPOP"[..] } else { b"RPOP" };
        ctx.propagate(&[Bytes::from_static(verb), key.clone(), Bytes::from(out.len().to_string())]);
    }
    Ok(out)
}

fn attempt(ctx: &CommandContext, db: usize, spec: &Spec, args: &[Bytes]) -> Result<Option<RespValue>, RespValue> {
    match &spec.kind {
        Kind::ListPop { left } => {
            for key in &spec.keys {
                if let Some(v) = list_pop_notify(ctx, db, key, *left, 1)?.pop() {
                    return Ok(Some(RespValue::array(vec![RespValue::bulk(key.clone()), RespValue::bulk(v)])));
                }
            }
            Ok(None)
        }
        Kind::ListMPop { left, count } => {
            for key in &spec.keys {
                let vals = list_pop_notify(ctx, db, key, *left, *count)?;
                if !vals.is_empty() {
                    return Ok(Some(RespValue::array(vec![RespValue::bulk(key.clone()), bulk_array(vals)])));
                }
            }
            Ok(None)
        }
        Kind::ZPop { max } => {
            for key in &spec.keys {
                let mut popped = crate::cmd_zset::zpop_impl(ctx, db, key, *max, 1).map_err(err)?;
                if let Some((member, score)) = popped.pop() {
                    return Ok(Some(RespValue::array(vec![
                        RespValue::bulk(key.clone()),
                        RespValue::bulk(member),
                        bulk_f64(score),
                    ])));
                }
            }
            Ok(None)
        }
        Kind::ZMPop { max, count } => {
            for key in &spec.keys {
                let popped = crate::cmd_zset::zpop_impl(ctx, db, key, *max, *count).map_err(err)?;
                if !popped.is_empty() {
                    return Ok(Some(crate::cmd_zset::mpop_reply(key, popped)));
                }
            }
            Ok(None)
        }
        Kind::ListMove { src_left, dst_left } => {
            let moved = crate::cmd_list::lmove_impl(ctx, db, &args[1], &args[2], *src_left, *dst_left)
                .map_err(err)?;
            if moved.is_some() {
                ctx.propagate(args);
            }
            Ok(moved.map(RespValue::bulk))
        }
        Kind::XRead(s) => cmd_stream::xread_attempt(ctx, db, s),
        Kind::XReadGroup(s) => cmd_stream::xreadgroup_attempt(ctx, db, s),
    }
}

pub(crate) fn run_once(ctx: &CommandContext, state: &ClientState, cmd: &str, args: &[Bytes]) -> RespValue {
    let spec = match parse(ctx, state, cmd, args) {
        Ok(s) => s,
        Err(e) => return e,
    };
    match attempt(ctx, state.db_index, &spec, args) {
        Ok(Some(r)) => r,
        Ok(None) => spec.on_timeout,
        Err(e) => e,
    }
}

struct WaitGuard<'a> {
    ctx: &'a CommandContext,
    db: usize,
    keys: &'a [Bytes],
    notify: Arc<Notify>,
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        self.ctx.waiters.unregister(self.db, self.keys, &self.notify);
    }
}

pub(crate) async fn run(
    ctx: &CommandContext,
    state: &ClientState,
    cmd: &str,
    args: &[Bytes],
) -> RespValue {
    let spec = match parse(ctx, state, cmd, args) {
        Ok(s) => s,
        Err(e) => return e,
    };
    if !spec.do_block {
        return match attempt(ctx, state.db_index, &spec, args) {
            Ok(Some(r)) => r,
            Ok(None) => spec.on_timeout,
            Err(e) => e,
        };
    }
    let db = state.db_index;
    let notify = Arc::new(Notify::new());
    ctx.waiters.register(db, &spec.keys, &notify);
    let _guard = WaitGuard {
        ctx,
        db,
        keys: &spec.keys,
        notify: Arc::clone(&notify),
    };
    let deadline = spec.timeout.map(|d| TokioInstant::now() + d);

    loop {
        let tried = {
            let _shared = ctx.exec_lock.read();
            attempt(ctx, db, &spec, args)
        };
        match tried {
            Ok(Some(reply)) => return reply,
            Err(e) => return e,
            Ok(None) => {}
        }
        if let Some(info) = &state.info {
            if let mode @ 1..=2 = info.take_unblock() {
                return if mode == 2 {
                    RespValue::err("UNBLOCKED client unblocked via CLIENT UNBLOCK")
                } else {
                    spec.on_timeout
                };
            }
        }
        let wait = async {
            tokio::select! {
                _ = notify.notified() => {}
                _ = async {
                    if let Some(info) = &state.info {
                        info.unblocked().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {}
            }
        };
        match deadline {
            None => wait.await,
            Some(dl) => {
                if timeout_at(dl, wait).await.is_err() {
                    return spec.on_timeout;
                }
            }
        }
        if let Some(info) = &state.info {
            if let mode @ 1..=2 = info.take_unblock() {
                return if mode == 2 {
                    RespValue::err("UNBLOCKED client unblocked via CLIENT UNBLOCK")
                } else {
                    spec.on_timeout
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(v: &[&str]) -> Vec<Bytes> {
        v.iter().map(|s| Bytes::from(s.to_string())).collect()
    }

    #[test]
    fn mpop_parser_errors() {
        let e = |a: &[&str]| match parse_mpop(&b(a), 1, ("LEFT", "RIGHT")) {
            Err(RespValue::Error(m)) => String::from_utf8_lossy(&m).into_owned(),
            _ => "ok".into(),
        };
        assert_eq!(e(&["LMPOP", "0", "k", "LEFT"]), ERR_NUMKEYS_POSITIVE);
        assert_eq!(e(&["LMPOP", "2", "k", "LEFT"]), ERR_SYNTAX);
        assert_eq!(e(&["LMPOP", "1", "k", "UP"]), ERR_SYNTAX);
        assert_eq!(e(&["LMPOP", "1", "k", "LEFT", "COUNT", "0"]), ERR_COUNT_POSITIVE);
        assert_eq!(e(&["LMPOP", "1", "k", "LEFT", "COUNT", "1", "COUNT", "2"]), ERR_SYNTAX);
        assert_eq!(e(&["LMPOP", "1", "k", "right", "count", "3"]), "ok");
    }
}
