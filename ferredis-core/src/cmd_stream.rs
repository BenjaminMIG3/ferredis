//! Streams Redis 7.2 : XADD/XREAD/XRANGE/… + consumer groups (XGROUP, XREADGROUP, XACK, …).

use crate::command::{arg_str, wrong_arity, ClientState, CommandContext};
use crate::resp::RespValue;
use crate::util::*;
use crate::value::{
    parse_stream_id, parse_stream_range_id, Stream, StreamConsumer, StreamGroup, StreamId,
    StreamNack, STREAM_ENTRIES_READ_INVALID,
};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn entry_resp(id: StreamId, fields: &[(Bytes, Bytes)]) -> RespValue {
    let mut flat = Vec::with_capacity(fields.len() * 2);
    for (k, v) in fields {
        flat.push(RespValue::bulk(k.clone()));
        flat.push(RespValue::bulk(v.clone()));
    }
    RespValue::array(vec![
        RespValue::bulk(Bytes::from(id.to_string_id())),
        RespValue::array(flat),
    ])
}

fn entries_resp(entries: impl IntoIterator<Item = (StreamId, Vec<(Bytes, Bytes)>)>) -> RespValue {
    RespValue::array(entries.into_iter().map(|(id, f)| entry_resp(id, &f)).collect())
}

fn xread_outer(rows: Vec<(Bytes, Vec<(StreamId, Vec<(Bytes, Bytes)>)>)>) -> RespValue {
    if rows.is_empty() {
        return RespValue::null_array();
    }
    RespValue::array(
        rows.into_iter()
            .map(|(key, ents)| {
                RespValue::array(vec![
                    RespValue::bulk(key),
                    RespValue::array(ents.into_iter().map(|(id, f)| entry_resp(id, &f)).collect()),
                ])
            })
            .collect(),
    )
}

fn after_write(ctx: &CommandContext, db: usize, key: &Bytes, args: &[Bytes], event: &str) {
    ctx.db.touch();
    ctx.waiters.notify(db, key);
    ctx.notify_key(db, key, event);
    ctx.propagate(args);
}

fn no_group_fmt(key: &Bytes, group: &Bytes) -> RespValue {
    err(&format!(
        "NOGROUP No such key '{}' or consumer group '{}'",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group)
    ))
}

// -- trim ----------------------------------------------------------------------

#[derive(Clone, Copy)]
enum TrimStrategy {
    MaxLen { threshold: u64, approx: bool, limit: Option<u64> },
    MinId { threshold: StreamId, approx: bool, limit: Option<u64> },
}

fn drop_entry(stream: &mut Stream, id: StreamId) {
    stream.mark_deleted(id);
    for g in stream.groups.values_mut() {
        if let Some(nack) = g.pel.remove(&id) {
            if let Some(c) = g.consumers.get_mut(&nack.consumer) {
                c.pending.remove(&id);
            }
        }
    }
}

fn apply_trim(stream: &mut Stream, strat: TrimStrategy) -> u64 {
    let limit = match strat {
        TrimStrategy::MaxLen { limit, approx, .. } | TrimStrategy::MinId { limit, approx, .. } => {
            limit.unwrap_or(if approx { 100 } else { u64::MAX })
        }
    };
    let mut deleted = 0u64;
    match strat {
        TrimStrategy::MaxLen { threshold, .. } => {
            while stream.len() as u64 > threshold && deleted < limit {
                let Some((id, _)) = stream.entries.pop_first() else {
                    break;
                };
                drop_entry(stream, id);
                deleted += 1;
            }
        }
        TrimStrategy::MinId { threshold, .. } => {
            while deleted < limit {
                let Some((&first, _)) = stream.entries.first_key_value() else {
                    break;
                };
                if first >= threshold {
                    break;
                }
                let Some((id, _)) = stream.entries.pop_first() else {
                    break;
                };
                drop_entry(stream, id);
                deleted += 1;
            }
        }
    }
    stream.refresh_first_id();
    deleted
}

fn parse_trim_opts(args: &[Bytes], mut i: usize) -> Result<(Option<TrimStrategy>, usize), RespValue> {
    let mut strat = None;
    while i < args.len() {
        let opt = upper(&args[i]);
        match opt.as_str() {
            "MAXLEN" | "MINID" => {
                i += 1;
                let mut approx = false;
                if i < args.len() {
                    let t = std::str::from_utf8(&args[i]).unwrap_or("");
                    if t == "~" {
                        approx = true;
                        i += 1;
                    } else if t == "=" {
                        i += 1;
                    }
                }
                if i >= args.len() {
                    return Err(err(ERR_SYNTAX));
                }
                if opt == "MAXLEN" {
                    let threshold = parse_i64(&args[i]).ok_or_else(|| err(ERR_INT))?;
                    if threshold < 0 {
                        return Err(err("ERR The MAXLEN argument must be >= 0."));
                    }
                    i += 1;
                    let mut lim = None;
                    if i + 1 < args.len() && upper(&args[i]) == "LIMIT" {
                        let n = parse_i64(&args[i + 1]).ok_or_else(|| err(ERR_INT))?;
                        if n < 0 {
                            return Err(err(ERR_INT));
                        }
                        lim = Some(n as u64);
                        i += 2;
                    }
                    strat = Some(TrimStrategy::MaxLen {
                        threshold: threshold as u64,
                        approx,
                        limit: lim,
                    });
                } else {
                    let threshold = parse_stream_id(std::str::from_utf8(&args[i]).unwrap_or(""))
                        .map_err(err)?;
                    i += 1;
                    let mut lim = None;
                    if i + 1 < args.len() && upper(&args[i]) == "LIMIT" {
                        let n = parse_i64(&args[i + 1]).ok_or_else(|| err(ERR_INT))?;
                        if n < 0 {
                            return Err(err(ERR_INT));
                        }
                        lim = Some(n as u64);
                        i += 2;
                    }
                    strat = Some(TrimStrategy::MinId {
                        threshold,
                        approx,
                        limit: lim,
                    });
                }
            }
            "LIMIT" => return Err(err(ERR_SYNTAX)),
            _ => break,
        }
    }
    Ok((strat, i))
}

// -- XADD ----------------------------------------------------------------------

pub(crate) fn cmd_xadd(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 5 {
        return wrong_arity("xadd");
    }
    let key = &args[1];
    let mut i = 2;
    let mut nomkstream = false;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "NOMKSTREAM" => {
                nomkstream = true;
                i += 1;
            }
            _ => break,
        }
    }
    let (trim, mut i) = match parse_trim_opts(args, i) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if i >= args.len() {
        return wrong_arity("xadd");
    }
    let id_spec = String::from_utf8_lossy(&args[i]).into_owned();
    i += 1;
    if i >= args.len() || (args.len() - i) % 2 != 0 {
        return wrong_arity("xadd");
    }
    let mut fields = Vec::with_capacity((args.len() - i) / 2);
    while i + 1 < args.len() {
        fields.push((args[i].clone(), args[i + 1].clone()));
        i += 2;
    }
    if fields.is_empty() {
        return wrong_arity("xadd");
    }

    let r: Result<StreamId, RespValue> = ctx.db.with_db(state.db_index, |d| {
        d.purge_expired(key);
        if !d.map.contains_key(key) {
            if nomkstream {
                return Err(RespValue::null_bulk());
            }
            let mut stream = Stream::default();
            let id = stream.alloc_id(&id_spec).map_err(err)?;
            stream.insert_entry(id, fields);
            if let Some(t) = trim {
                apply_trim(&mut stream, t);
            }
            d.map.insert(
                key.clone(),
                crate::value::Entry::new(crate::value::RedisValue::Stream(stream)),
            );
            return Ok(id);
        }
        let stream = d.stream_mut(key).map_err(err)?.ok_or_else(|| err(ERR_NO_SUCH_KEY))?;
        let id = stream.alloc_id(&id_spec).map_err(err)?;
        stream.insert_entry(id, fields);
        if let Some(t) = trim {
            apply_trim(stream, t);
        }
        Ok(id)
    });

    match r {
        Err(e) => e,
        Ok(id) => {
            after_write(ctx, state.db_index, key, args, "xadd");
            RespValue::bulk(Bytes::from(id.to_string_id()))
        }
    }
}

// -- range helpers -------------------------------------------------------------

fn collect_range(
    stream: &Stream,
    start: StreamId,
    start_excl: bool,
    end: StreamId,
    end_excl: bool,
    count: Option<usize>,
    rev: bool,
) -> Vec<(StreamId, Vec<(Bytes, Bytes)>)> {
    let lb = if start_excl {
        Bound::Excluded(start)
    } else {
        Bound::Included(start)
    };
    let ub = if end_excl {
        Bound::Excluded(end)
    } else {
        Bound::Included(end)
    };
    let take = count.unwrap_or(usize::MAX);
    if rev {
        stream
            .entries
            .range((lb, ub))
            .rev()
            .take(take)
            .map(|(id, f)| (*id, f.clone()))
            .collect()
    } else {
        stream
            .entries
            .range((lb, ub))
            .take(take)
            .map(|(id, f)| (*id, f.clone()))
            .collect()
    }
}

fn parse_count_tail(args: &[Bytes], from: usize) -> Result<Option<usize>, RespValue> {
    if from >= args.len() {
        return Ok(None);
    }
    if upper(&args[from]) != "COUNT" || from + 1 >= args.len() {
        return Err(err(ERR_SYNTAX));
    }
    let n = parse_i64(&args[from + 1]).ok_or_else(|| err(ERR_INT))?;
    if n < 0 {
        return Err(err(ERR_INT));
    }
    if from + 2 != args.len() {
        return Err(err(ERR_SYNTAX));
    }
    Ok(Some(n as usize))
}

pub(crate) fn cmd_xrange(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    xrange_generic(ctx, state, args, false)
}

pub(crate) fn cmd_xrevrange(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    xrange_generic(ctx, state, args, true)
}

fn xrange_generic(
    ctx: &CommandContext,
    state: &ClientState,
    args: &[Bytes],
    rev: bool,
) -> RespValue {
    let name = if rev { "xrevrange" } else { "xrange" };
    if args.len() < 4 {
        return wrong_arity(name);
    }
    // XRANGE key start end ; XREVRANGE key end start (same ID bounds, reversed order).
    let (start_raw, end_raw) = if rev {
        (arg_str(args, 3).unwrap_or("-"), arg_str(args, 2).unwrap_or("+"))
    } else {
        (arg_str(args, 2).unwrap_or("-"), arg_str(args, 3).unwrap_or("+"))
    };
    let (start, start_excl) = match parse_stream_range_id(start_raw, false) {
        Ok(v) => v,
        Err(e) => return err(e),
    };
    let (end, end_excl) = match parse_stream_range_id(end_raw, true) {
        Ok(v) => v,
        Err(e) => return err(e),
    };
    let count = match parse_count_tail(args, 4) {
        Ok(c) => c,
        Err(e) => return e,
    };
    let r: Result<_, &'static str> = ctx.db.with_db(state.db_index, |d| {
        match d.stream_mut(&args[1])? {
            None => Ok(Vec::new()),
            Some(s) => Ok(collect_range(s, start, start_excl, end, end_excl, count, rev)),
        }
    });
    match r {
        Ok(ents) => entries_resp(ents),
        Err(e) => err(e),
    }
}

pub(crate) fn cmd_xlen(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("xlen");
    }
    match ctx.db.with_db(state.db_index, |d| d.stream_mut(&args[1]).map(|s| s.map(|s| s.len() as i64).unwrap_or(0))) {
        Ok(n) => RespValue::Integer(n),
        Err(e) => err(e),
    }
}

pub(crate) fn cmd_xdel(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("xdel");
    }
    let key = &args[1];
    let mut ids = Vec::new();
    for a in &args[2..] {
        match parse_stream_id(std::str::from_utf8(a).unwrap_or("")) {
            Ok(id) => ids.push(id),
            Err(e) => return err(e),
        }
    }
    let r: Result<i64, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(stream) = d.stream_mut(key)? else {
            return Ok(0);
        };
        let mut deleted = 0i64;
        for id in ids {
            if stream.entries.remove(&id).is_some() {
                stream.mark_deleted(id);
                for g in stream.groups.values_mut() {
                    if let Some(nack) = g.pel.remove(&id) {
                        if let Some(c) = g.consumers.get_mut(&nack.consumer) {
                            c.pending.remove(&id);
                        }
                    }
                }
                deleted += 1;
            }
        }
        stream.refresh_first_id();
        Ok(deleted)
    });
    match r {
        Ok(n) => {
            if n > 0 {
                after_write(ctx, state.db_index, key, args, "xdel");
            }
            RespValue::Integer(n)
        }
        Err(e) => err(e),
    }
}

pub(crate) fn cmd_xtrim(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("xtrim");
    }
    let (strat, i) = match parse_trim_opts(args, 2) {
        Ok((Some(s), i)) => (s, i),
        Ok((None, _)) => return err(ERR_SYNTAX),
        Err(e) => return e,
    };
    if i != args.len() {
        return err(ERR_SYNTAX);
    }
    let key = &args[1];
    let r: Result<i64, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(stream) = d.stream_mut(key)? else {
            return Ok(0);
        };
        Ok(apply_trim(stream, strat) as i64)
    });
    match r {
        Ok(n) => {
            if n > 0 {
                after_write(ctx, state.db_index, key, args, "xtrim");
            }
            RespValue::Integer(n)
        }
        Err(e) => err(e),
    }
}

pub(crate) fn cmd_xsetid(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("xsetid");
    }
    let key = &args[1];
    let last_id = match parse_stream_id(arg_str(args, 2).unwrap_or("")) {
        Ok(id) => id,
        Err(e) => return err(e),
    };
    let mut entries_added = None;
    let mut max_deleted = None;
    let mut i = 3;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "ENTRIESADDED" if i + 1 < args.len() => {
                let n = match parse_i64(&args[i + 1]) {
                    Some(n) if n >= 0 => n as u64,
                    _ => return err(ERR_INT),
                };
                entries_added = Some(n);
                i += 2;
            }
            "MAXDELETEDID" if i + 1 < args.len() => {
                match parse_stream_id(arg_str(args, i + 1).unwrap_or("")) {
                    Ok(id) => max_deleted = Some(id),
                    Err(e) => return err(e),
                }
                i += 2;
            }
            _ => return err(ERR_SYNTAX),
        }
    }
    let r: Result<(), RespValue> = ctx.db.with_db(state.db_index, |d| {
        let stream = d.stream_mut(key).map_err(err)?.ok_or_else(|| err(ERR_NO_SUCH_KEY))?;
        if last_id < stream.last_id {
            // Redis autorise XSETID à baisser last_id pour la réplication
        }
        if let Some(top) = stream.entries.keys().next_back().copied() {
            if last_id < top {
                return Err(err(
                    "ERR The ID specified in XSETID is smaller than the current top entry ID",
                ));
            }
        }
        stream.last_id = last_id;
        if let Some(n) = entries_added {
            if n < stream.len() as u64 {
                return Err(err(
                    "ERR The entries_added specified in XSETID is smaller than stream length",
                ));
            }
            stream.entries_added = n;
            for g in stream.groups.values_mut() {
                if g.entries_read != STREAM_ENTRIES_READ_INVALID && g.entries_read > n as i64 {
                    g.entries_read = n as i64;
                }
            }
        }
        if let Some(id) = max_deleted {
            if id > stream.max_deleted_id {
                stream.max_deleted_id = id;
            }
        }
        Ok(())
    });
    match r {
        Ok(()) => {
            after_write(ctx, state.db_index, key, args, "xsetid");
            RespValue::ok()
        }
        Err(e) => e,
    }
}

// -- XREAD (non-blocking attempt) ----------------------------------------------

#[derive(Clone)]
pub(crate) struct XReadSpec {
    pub streams: Vec<(Bytes, String)>,
    pub count: Option<usize>,
}

pub(crate) fn parse_xread(args: &[Bytes]) -> Result<(XReadSpec, Option<u64>), RespValue> {
    // returns (spec, block_ms) ; block_ms None = non-blocking, Some(0)=forever
    let mut i = 1;
    let mut count = None;
    let mut block = None;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "COUNT" if i + 1 < args.len() => {
                let n = parse_i64(&args[i + 1]).ok_or_else(|| err(ERR_INT))?;
                if n < 0 {
                    return Err(err(ERR_INT));
                }
                count = Some(n as usize);
                i += 2;
            }
            "BLOCK" if i + 1 < args.len() => {
                let n = parse_i64(&args[i + 1]).ok_or_else(|| err("ERR timeout is not a integer or out of range"))?;
                if n < 0 {
                    return Err(err("ERR timeout is negative"));
                }
                block = Some(n as u64);
                i += 2;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => return Err(err(ERR_SYNTAX)),
        }
    }
    let rest = &args[i..];
    if rest.len() < 2 || rest.len() % 2 != 0 {
        return Err(wrong_arity("xread"));
    }
    let half = rest.len() / 2;
    let mut streams = Vec::with_capacity(half);
    for j in 0..half {
        streams.push((
            rest[j].clone(),
            String::from_utf8_lossy(&rest[j + half]).into_owned(),
        ));
    }
    Ok((XReadSpec { streams, count }, block))
}

/// Résout `$` vers le last_id courant (sémantique XREAD BLOCK).
pub(crate) fn resolve_xread_ids(ctx: &CommandContext, db: usize, spec: &mut XReadSpec) {
    for (key, id) in &mut spec.streams {
        if id == "$" {
            let last = ctx.db.with_db(db, |d| {
                d.stream_mut(key)
                    .ok()
                    .flatten()
                    .map(|s| s.last_id.to_string_id())
                    .unwrap_or_else(|| "0-0".into())
            });
            *id = last;
        }
    }
}

pub(crate) fn xread_attempt(
    ctx: &CommandContext,
    db: usize,
    spec: &XReadSpec,
) -> Result<Option<RespValue>, RespValue> {
    let mut rows = Vec::new();
    for (key, id) in &spec.streams {
        let after = if id == "$" {
            // déjà résolu normalement ; `$` non résolu = rien
            continue;
        } else {
            parse_stream_id(id).map_err(err)?
        };
        let ents = ctx.db.with_db(db, |d| -> Result<_, RespValue> {
            match d.stream_mut(key).map_err(err)? {
                None => Ok(Vec::new()),
                Some(s) => Ok(collect_range(
                    s,
                    after,
                    true,
                    StreamId::new(u64::MAX, u64::MAX),
                    false,
                    spec.count,
                    false,
                )),
            }
        })?;
        if !ents.is_empty() {
            rows.push((key.clone(), ents));
        }
    }
    if rows.is_empty() {
        Ok(None)
    } else {
        Ok(Some(xread_outer(rows)))
    }
}


// -- XGROUP --------------------------------------------------------------------

pub(crate) fn cmd_xgroup(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("xgroup");
    }
    match upper(&args[1]).as_str() {
        "CREATE" => xgroup_create(ctx, state, args),
        "CREATECONSUMER" => xgroup_createconsumer(ctx, state, args),
        "DELCONSUMER" => xgroup_delconsumer(ctx, state, args),
        "DESTROY" => xgroup_destroy(ctx, state, args),
        "SETID" => xgroup_setid(ctx, state, args),
        "HELP" => xgroup_help(),
        other => err(&format!(
            "ERR unknown subcommand '{other}'. Try XGROUP HELP."
        )),
    }
}

fn xgroup_help() -> RespValue {
    RespValue::array(
        [
            "XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "CREATE <key> <groupname> <id|$> [MKSTREAM] [ENTRIESREAD entries_read]",
            "    Create a new consumer group.",
            "CREATECONSUMER <key> <groupname> <consumer>",
            "    Create a new consumer in the group.",
            "DELCONSUMER <key> <groupname> <consumer>",
            "    Remove a consumer from the group.",
            "DESTROY <key> <groupname>",
            "    Remove a consumer group.",
            "SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
            "    Set the last delivered ID for a consumer group.",
            "HELP",
            "    Print this help.",
        ]
        .into_iter()
        .map(|s| RespValue::bulk(Bytes::from_static(s.as_bytes())))
        .collect(),
    )
}

fn xgroup_create(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    // XGROUP CREATE key group id [MKSTREAM] [ENTRIESREAD n]
    if args.len() < 5 {
        return wrong_arity("xgroup|create");
    }
    let key = &args[2];
    let group = args[3].clone();
    let id_raw = arg_str(args, 4).unwrap_or("$");
    let mut mkstream = false;
    let mut entries_read_opt = None;
    let mut i = 5;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "MKSTREAM" => {
                mkstream = true;
                i += 1;
            }
            "ENTRIESREAD" if i + 1 < args.len() => {
                let n = match parse_i64(&args[i + 1]) {
                    Some(n) => n,
                    None => return err(ERR_INT),
                };
                entries_read_opt = Some(n);
                i += 2;
            }
            _ => return err(ERR_SYNTAX),
        }
    }

    let r: Result<(), RespValue> = ctx.db.with_db(state.db_index, |d| {
        d.purge_expired(key);
        if !d.map.contains_key(key) {
            if mkstream {
                d.map.insert(
                    key.clone(),
                    crate::value::Entry::new(crate::value::RedisValue::Stream(Stream::default())),
                );
            } else {
                return Err(err(ERR_NO_SUCH_KEY));
            }
        }
        let stream = d.stream_mut(key).map_err(err)?.ok_or_else(|| err(ERR_NO_SUCH_KEY))?;
        if stream.groups.contains_key(&group) {
            return Err(err("BUSYGROUP Consumer Group name already exists"));
        }
        let last_id = if id_raw == "$" {
            stream.last_id
        } else {
            parse_stream_id(id_raw).map_err(err)?
        };
        let er = match entries_read_opt {
            Some(n) => {
                if n < 0 {
                    STREAM_ENTRIES_READ_INVALID
                } else {
                    n.min(stream.entries_added as i64)
                }
            }
            None => stream.entries_read_for_id(last_id),
        };
        stream
            .groups
            .insert(group.clone(), StreamGroup::new(group.clone(), last_id, er));
        Ok(())
    });
    match r {
        Ok(()) => {
            after_write(ctx, state.db_index, key, args, "xgroup-create");
            RespValue::ok()
        }
        Err(e) => e,
    }
}

fn xgroup_setid(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 5 {
        return wrong_arity("xgroup|setid");
    }
    let key = &args[2];
    let group = &args[3];
    let id_raw = arg_str(args, 4).unwrap_or("0");
    let mut entries_read_opt = None;
    let mut i = 5;
    while i < args.len() {
        if upper(&args[i]) == "ENTRIESREAD" && i + 1 < args.len() {
            entries_read_opt = Some(match parse_i64(&args[i + 1]) {
                Some(n) => n,
                None => return err(ERR_INT),
            });
            i += 2;
        } else {
            return err(ERR_SYNTAX);
        }
    }
    let r: Result<(), RespValue> = ctx.db.with_db(state.db_index, |d| {
        let stream = d.stream_mut(key).map_err(err)?.ok_or_else(|| no_group_fmt(key, group))?;
        if !stream.groups.contains_key(group) {
            return Err(no_group_fmt(key, group));
        }
        let last_id = if id_raw == "$" {
            stream.last_id
        } else {
            parse_stream_id(id_raw).map_err(err)?
        };
        let er = match entries_read_opt {
            Some(n) if n < 0 => STREAM_ENTRIES_READ_INVALID,
            Some(n) => n.min(stream.entries_added as i64),
            None => stream.entries_read_for_id(last_id),
        };
        let g = stream.groups.get_mut(group).unwrap();
        g.last_id = last_id;
        g.entries_read = er;
        Ok(())
    });
    match r {
        Ok(()) => {
            after_write(ctx, state.db_index, key, args, "xgroup-setid");
            RespValue::ok()
        }
        Err(e) => e,
    }
}

fn xgroup_destroy(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("xgroup|destroy");
    }
    let key = &args[2];
    let group = &args[3];
    let r: Result<i64, RespValue> = ctx.db.with_db(state.db_index, |d| {
        let stream = match d.stream_mut(key).map_err(err)? {
            Some(s) => s,
            None => return Ok(0),
        };
        Ok(if stream.groups.remove(group).is_some() {
            1
        } else {
            0
        })
    });
    match r {
        Ok(n) => {
            if n > 0 {
                after_write(ctx, state.db_index, key, args, "xgroup-destroy");
            }
            RespValue::Integer(n)
        }
        Err(e) => e,
    }
}

fn xgroup_createconsumer(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 5 {
        return wrong_arity("xgroup|createconsumer");
    }
    let key = &args[2];
    let group = &args[3];
    let consumer = args[4].clone();
    let r: Result<i64, RespValue> = ctx.db.with_db(state.db_index, |d| {
        let stream = d.stream_mut(key).map_err(err)?.ok_or_else(|| no_group_fmt(key, group))?;
        let g = stream
            .groups
            .get_mut(group)
            .ok_or_else(|| no_group_fmt(key, group))?;
        if g.consumers.contains_key(&consumer) {
            return Ok(0);
        }
        let t = now_ms();
        g.consumers.insert(
            consumer.clone(),
            StreamConsumer {
                name: consumer,
                seen_time_ms: t,
                active_time_ms: t as i64,
                pending: Default::default(),
            },
        );
        Ok(1)
    });
    match r {
        Ok(n) => {
            if n > 0 {
                after_write(ctx, state.db_index, key, args, "xgroup-createconsumer");
            }
            RespValue::Integer(n)
        }
        Err(e) => e,
    }
}

fn xgroup_delconsumer(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 5 {
        return wrong_arity("xgroup|delconsumer");
    }
    let key = &args[2];
    let group = &args[3];
    let consumer = &args[4];
    let r: Result<i64, RespValue> = ctx.db.with_db(state.db_index, |d| {
        let stream = d.stream_mut(key).map_err(err)?.ok_or_else(|| no_group_fmt(key, group))?;
        let g = stream
            .groups
            .get_mut(group)
            .ok_or_else(|| no_group_fmt(key, group))?;
        let Some(c) = g.consumers.remove(consumer) else {
            return Ok(0);
        };
        let pending = c.pending.len() as i64;
        for id in &c.pending {
            g.pel.remove(id);
        }
        Ok(pending)
    });
    match r {
        Ok(n) => {
            after_write(ctx, state.db_index, key, args, "xgroup-delconsumer");
            RespValue::Integer(n)
        }
        Err(e) => e,
    }
}

// -- XREADGROUP ----------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct XReadGroupSpec {
    pub group: Bytes,
    pub consumer: Bytes,
    pub streams: Vec<(Bytes, String)>,
    pub count: Option<usize>,
    pub noack: bool,
}

pub(crate) fn parse_xreadgroup(args: &[Bytes]) -> Result<(XReadGroupSpec, Option<u64>), RespValue> {
    // XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] [NOACK] STREAMS key id...
    if args.len() < 6 {
        return Err(wrong_arity("xreadgroup"));
    }
    if upper(&args[1]) != "GROUP" {
        return Err(err(ERR_SYNTAX));
    }
    let group = args[2].clone();
    let consumer = args[3].clone();
    let mut i = 4;
    let mut count = None;
    let mut block = None;
    let mut noack = false;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "COUNT" if i + 1 < args.len() => {
                let n = parse_i64(&args[i + 1]).ok_or_else(|| err(ERR_INT))?;
                if n < 0 {
                    return Err(err(ERR_INT));
                }
                count = Some(n as usize);
                i += 2;
            }
            "BLOCK" if i + 1 < args.len() => {
                let n = parse_i64(&args[i + 1])
                    .ok_or_else(|| err("ERR timeout is not a integer or out of range"))?;
                if n < 0 {
                    return Err(err("ERR timeout is negative"));
                }
                block = Some(n as u64);
                i += 2;
            }
            "NOACK" => {
                noack = true;
                i += 1;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => return Err(err(ERR_SYNTAX)),
        }
    }
    let rest = &args[i..];
    if rest.len() < 2 || rest.len() % 2 != 0 {
        return Err(wrong_arity("xreadgroup"));
    }
    let half = rest.len() / 2;
    let mut streams = Vec::with_capacity(half);
    for j in 0..half {
        streams.push((
            rest[j].clone(),
            String::from_utf8_lossy(&rest[j + half]).into_owned(),
        ));
    }
    Ok((
        XReadGroupSpec {
            group,
            consumer,
            streams,
            count,
            noack,
        },
        block,
    ))
}

fn ensure_consumer<'a>(g: &'a mut StreamGroup, name: &Bytes) -> &'a mut StreamConsumer {
    let t = now_ms();
    g.consumers
        .entry(name.clone())
        .or_insert_with(|| StreamConsumer {
            name: name.clone(),
            seen_time_ms: t,
            active_time_ms: t as i64,
            pending: Default::default(),
        })
}

pub(crate) fn xreadgroup_attempt(
    ctx: &CommandContext,
    db: usize,
    spec: &XReadGroupSpec,
) -> Result<Option<RespValue>, RespValue> {
    let mut rows = Vec::new();
    for (key, id_raw) in &spec.streams {
        let delivered = ctx.db.with_db(db, |d| -> Result<_, RespValue> {
            let stream = d
                .stream_mut(key)
                .map_err(err)?
                .ok_or_else(|| no_group_fmt(key, &spec.group))?;
            if !stream.groups.contains_key(&spec.group) {
                return Err(no_group_fmt(key, &spec.group));
            }
            let stream_last = stream.last_id;
            let entries_added = stream.entries_added;
            let t = now_ms();

            if id_raw == ">" {
                let after = stream.groups.get(&spec.group).unwrap().last_id;
                let take = spec.count.unwrap_or(usize::MAX);
                let batch: Vec<(StreamId, Vec<(Bytes, Bytes)>)> = stream
                    .entries
                    .range((Bound::Excluded(after), Bound::Unbounded))
                    .take(take)
                    .map(|(id, f)| (*id, f.clone()))
                    .collect();
                if batch.is_empty() {
                    // Touche le consumer même sans message (vu Redis)
                    let g = stream.groups.get_mut(&spec.group).unwrap();
                    let c = ensure_consumer(g, &spec.consumer);
                    c.seen_time_ms = t;
                    return Ok(Vec::new());
                }
                let g = stream.groups.get_mut(&spec.group).unwrap();
                {
                    let c = ensure_consumer(g, &spec.consumer);
                    c.seen_time_ms = t;
                    c.active_time_ms = t as i64;
                }
                let mut out = Vec::with_capacity(batch.len());
                for (id, fields) in batch {
                    if !spec.noack {
                        g.pel.insert(
                            id,
                            StreamNack {
                                consumer: spec.consumer.clone(),
                                delivery_time_ms: t,
                                delivery_count: 1,
                            },
                        );
                        if let Some(c) = g.consumers.get_mut(&spec.consumer) {
                            c.pending.insert(id);
                        }
                    }
                    out.push((id, fields));
                }
                if let Some((id, _)) = out.last() {
                    g.last_id = *id;
                    if *id == stream_last {
                        g.entries_read = entries_added as i64;
                    } else if g.entries_read != STREAM_ENTRIES_READ_INVALID {
                        g.entries_read = g.entries_read.saturating_add(out.len() as i64);
                    }
                }
                Ok(out)
            } else {
                let after = parse_stream_id(id_raw).map_err(err)?;
                let take = spec.count.unwrap_or(usize::MAX);
                let pending_ids: Vec<StreamId> = {
                    let g = stream.groups.get_mut(&spec.group).unwrap();
                    let c = ensure_consumer(g, &spec.consumer);
                    c.seen_time_ms = t;
                    g.pel
                        .range((Bound::Excluded(after), Bound::Unbounded))
                        .filter(|(_, n)| n.consumer == spec.consumer)
                        .take(take)
                        .map(|(id, _)| *id)
                        .collect()
                };
                let mut out = Vec::with_capacity(pending_ids.len());
                for id in pending_ids {
                    if let Some(fields) = stream.entries.get(&id) {
                        out.push((id, fields.clone()));
                    }
                }
                Ok(out)
            }
        })?;
        if !delivered.is_empty() {
            rows.push((key.clone(), delivered));
        }
    }
    if rows.is_empty() {
        Ok(None)
    } else {
        Ok(Some(xread_outer(rows)))
    }
}


// -- XACK / XCLAIM / XAUTOCLAIM / XPENDING -------------------------------------

pub(crate) fn cmd_xack(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("xack");
    }
    let key = &args[1];
    let group = &args[2];
    let mut ids = Vec::new();
    for a in &args[3..] {
        match parse_stream_id(std::str::from_utf8(a).unwrap_or("")) {
            Ok(id) => ids.push(id),
            Err(e) => return err(e),
        }
    }
    let r: Result<i64, RespValue> = ctx.db.with_db(state.db_index, |d| {
        let stream = d.stream_mut(key).map_err(err)?.ok_or_else(|| no_group_fmt(key, group))?;
        let g = stream
            .groups
            .get_mut(group)
            .ok_or_else(|| no_group_fmt(key, group))?;
        let mut acked = 0i64;
        for id in ids {
            if let Some(nack) = g.pel.remove(&id) {
                if let Some(c) = g.consumers.get_mut(&nack.consumer) {
                    c.pending.remove(&id);
                }
                acked += 1;
            }
        }
        Ok(acked)
    });
    match r {
        Ok(n) => {
            if n > 0 {
                after_write(ctx, state.db_index, key, args, "xack");
            }
            RespValue::Integer(n)
        }
        Err(e) => e,
    }
}

struct ClaimOpts {
    min_idle: u64,
    ids: Vec<StreamId>,
    idle: Option<u64>,
    time_ms: Option<u64>,
    retrycount: Option<u64>,
    force: bool,
    justid: bool,
    lastid: Option<StreamId>,
}

fn parse_claim(args: &[Bytes]) -> Result<(Bytes, Bytes, Bytes, ClaimOpts), RespValue> {
    // XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id]
    if args.len() < 6 {
        return Err(wrong_arity("xclaim"));
    }
    let key = args[1].clone();
    let group = args[2].clone();
    let consumer = args[3].clone();
    let min_idle = parse_i64(&args[4]).ok_or_else(|| err(ERR_INT))?;
    if min_idle < 0 {
        return Err(err(ERR_INT));
    }
    let mut i = 5;
    let mut ids = Vec::new();
    while i < args.len() {
        let u = upper(&args[i]);
        if matches!(
            u.as_str(),
            "IDLE" | "TIME" | "RETRYCOUNT" | "FORCE" | "JUSTID" | "LASTID"
        ) {
            break;
        }
        ids.push(parse_stream_id(std::str::from_utf8(&args[i]).unwrap_or("")).map_err(err)?);
        i += 1;
    }
    if ids.is_empty() {
        return Err(wrong_arity("xclaim"));
    }
    let mut idle = None;
    let mut time_ms = None;
    let mut retrycount = None;
    let mut force = false;
    let mut justid = false;
    let mut lastid = None;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "IDLE" if i + 1 < args.len() => {
                let n = parse_i64(&args[i + 1]).ok_or_else(|| err(ERR_INT))?;
                if n < 0 {
                    return Err(err(ERR_INT));
                }
                idle = Some(n as u64);
                i += 2;
            }
            "TIME" if i + 1 < args.len() => {
                let n = parse_i64(&args[i + 1]).ok_or_else(|| err(ERR_INT))?;
                if n < 0 {
                    return Err(err(ERR_INT));
                }
                time_ms = Some(n as u64);
                i += 2;
            }
            "RETRYCOUNT" if i + 1 < args.len() => {
                let n = parse_i64(&args[i + 1]).ok_or_else(|| err(ERR_INT))?;
                if n < 0 {
                    return Err(err(ERR_INT));
                }
                retrycount = Some(n as u64);
                i += 2;
            }
            "FORCE" => {
                force = true;
                i += 1;
            }
            "JUSTID" => {
                justid = true;
                i += 1;
            }
            "LASTID" if i + 1 < args.len() => {
                lastid = Some(
                    parse_stream_id(std::str::from_utf8(&args[i + 1]).unwrap_or("")).map_err(err)?,
                );
                i += 2;
            }
            _ => return Err(err(ERR_SYNTAX)),
        }
    }
    Ok((
        key,
        group,
        consumer,
        ClaimOpts {
            min_idle: min_idle as u64,
            ids,
            idle,
            time_ms,
            retrycount,
            force,
            justid,
            lastid,
        },
    ))
}

pub(crate) fn cmd_xclaim(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let (key, group, consumer, opts) = match parse_claim(args) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let r: Result<Vec<(StreamId, Option<Vec<(Bytes, Bytes)>>)>, RespValue> =
        ctx.db.with_db(state.db_index, |d| {
            let stream = d.stream_mut(&key).map_err(err)?.ok_or_else(|| no_group_fmt(&key, &group))?;
            let g = stream
                .groups
                .get_mut(&group)
                .ok_or_else(|| no_group_fmt(&key, &group))?;
            if let Some(lid) = opts.lastid {
                if lid > g.last_id {
                    g.last_id = lid;
                }
            }
            let t = now_ms();
            let delivery_time = opts.time_ms.unwrap_or_else(|| {
                opts.idle.map(|idle| t.saturating_sub(idle)).unwrap_or(t)
            });
            ensure_consumer(g, &consumer);
            let mut claimed = Vec::new();
            for id in &opts.ids {
                let exists_in_pel = g.pel.contains_key(id);
                if !exists_in_pel && !opts.force {
                    continue;
                }
                if !exists_in_pel && opts.force && !stream.entries.contains_key(id) {
                    continue;
                }
                if let Some(nack) = g.pel.get(id) {
                    let idle_for = t.saturating_sub(nack.delivery_time_ms);
                    if idle_for < opts.min_idle {
                        continue;
                    }
                    let old = nack.consumer.clone();
                    if let Some(c) = g.consumers.get_mut(&old) {
                        c.pending.remove(id);
                    }
                }
                let prev_count = g.pel.get(id).map(|n| n.delivery_count).unwrap_or(0);
                let delivery_count = opts.retrycount.unwrap_or(prev_count.saturating_add(1));
                g.pel.insert(
                    *id,
                    StreamNack {
                        consumer: consumer.clone(),
                        delivery_time_ms: delivery_time,
                        delivery_count,
                    },
                );
                if let Some(c) = g.consumers.get_mut(&consumer) {
                    c.pending.insert(*id);
                    c.seen_time_ms = t;
                    c.active_time_ms = t as i64;
                }
                let fields = stream.entries.get(id).cloned();
                claimed.push((*id, fields));
            }
            Ok(claimed)
        });
    match r {
        Ok(claimed) => {
            if !claimed.is_empty() {
                after_write(ctx, state.db_index, &key, args, "xclaim");
            }
            if opts.justid {
                RespValue::array(
                    claimed
                        .into_iter()
                        .map(|(id, _)| RespValue::bulk(Bytes::from(id.to_string_id())))
                        .collect(),
                )
            } else {
                RespValue::array(
                    claimed
                        .into_iter()
                        .filter_map(|(id, f)| f.map(|fields| entry_resp(id, &fields)))
                        .collect(),
                )
            }
        }
        Err(e) => e,
    }
}

pub(crate) fn cmd_xautoclaim(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
) -> RespValue {
    // XAUTOCLAIM key group consumer min-idle start [COUNT count] [JUSTID]
    if args.len() < 6 {
        return wrong_arity("xautoclaim");
    }
    let key = &args[1];
    let group = &args[2];
    let consumer = args[3].clone();
    let min_idle = match parse_i64(&args[4]) {
        Some(n) if n >= 0 => n as u64,
        _ => return err(ERR_INT),
    };
    let start = match parse_stream_id(arg_str(args, 5).unwrap_or("0-0")) {
        Ok(id) => id,
        Err(e) => return err(e),
    };
    let mut count = 100usize;
    let mut justid = false;
    let mut i = 6;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "COUNT" if i + 1 < args.len() => {
                let n = match parse_i64(&args[i + 1]) {
                    Some(n) if n > 0 => n as usize,
                    _ => return err(ERR_INT),
                };
                count = n;
                i += 2;
            }
            "JUSTID" => {
                justid = true;
                i += 1;
            }
            _ => return err(ERR_SYNTAX),
        }
    }

    let r: Result<(StreamId, Vec<(StreamId, Option<Vec<(Bytes, Bytes)>>)>, Vec<StreamId>), RespValue> =
        ctx.db.with_db(state.db_index, |d| {
            let stream = d.stream_mut(key).map_err(err)?.ok_or_else(|| no_group_fmt(key, group))?;
            let g = stream
                .groups
                .get_mut(group)
                .ok_or_else(|| no_group_fmt(key, group))?;
            ensure_consumer(g, &consumer);
            let t = now_ms();
            let mut claimed = Vec::new();
            let mut deleted_ids = Vec::new();
            let mut cursor = StreamId::ZERO;
            let candidates: Vec<StreamId> = g
                .pel
                .range(start..)
                .map(|(id, _)| *id)
                .collect();
            for id in candidates {
                if claimed.len() >= count {
                    cursor = id;
                    break;
                }
                let Some(nack) = g.pel.get(&id).cloned() else {
                    continue;
                };
                if t.saturating_sub(nack.delivery_time_ms) < min_idle {
                    continue;
                }
                if !stream.entries.contains_key(&id) {
                    // entrée supprimée : retirer du PEL, signaler
                    if let Some(n) = g.pel.remove(&id) {
                        if let Some(c) = g.consumers.get_mut(&n.consumer) {
                            c.pending.remove(&id);
                        }
                    }
                    deleted_ids.push(id);
                    continue;
                }
                if let Some(c) = g.consumers.get_mut(&nack.consumer) {
                    c.pending.remove(&id);
                }
                g.pel.insert(
                    id,
                    StreamNack {
                        consumer: consumer.clone(),
                        delivery_time_ms: t,
                        delivery_count: nack.delivery_count.saturating_add(1),
                    },
                );
                if let Some(c) = g.consumers.get_mut(&consumer) {
                    c.pending.insert(id);
                    c.seen_time_ms = t;
                    c.active_time_ms = t as i64;
                }
                let fields = stream.entries.get(&id).cloned();
                claimed.push((id, fields));
                cursor = StreamId::new(id.ms, id.seq.saturating_add(1));
            }
            if claimed.len() < count {
                // fin de PEL
                cursor = StreamId::ZERO;
            }
            Ok((cursor, claimed, deleted_ids))
        });

    match r {
        Ok((cursor, claimed, deleted)) => {
            if !claimed.is_empty() || !deleted.is_empty() {
                after_write(ctx, state.db_index, key, args, "xautoclaim");
            }
            let entries = if justid {
                RespValue::array(
                    claimed
                        .into_iter()
                        .map(|(id, _)| RespValue::bulk(Bytes::from(id.to_string_id())))
                        .collect(),
                )
            } else {
                RespValue::array(
                    claimed
                        .into_iter()
                        .filter_map(|(id, f)| f.map(|fields| entry_resp(id, &fields)))
                        .collect(),
                )
            };
            let deleted_resp = RespValue::array(
                deleted
                    .into_iter()
                    .map(|id| RespValue::bulk(Bytes::from(id.to_string_id())))
                    .collect(),
            );
            RespValue::array(vec![
                RespValue::bulk(Bytes::from(cursor.to_string_id())),
                entries,
                deleted_resp,
            ])
        }
        Err(e) => e,
    }
}

pub(crate) fn cmd_xpending(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("xpending");
    }
    let key = &args[1];
    let group = &args[2];
    if args.len() == 3 {
        // summary
        let r: Result<RespValue, RespValue> = ctx.db.with_db(state.db_index, |d| {
            let stream = d.stream_mut(key).map_err(err)?.ok_or_else(|| no_group_fmt(key, group))?;
            let g = stream
                .groups
                .get(group)
                .ok_or_else(|| no_group_fmt(key, group))?;
            if g.pel.is_empty() {
                return Ok(RespValue::array(vec![
                    RespValue::Integer(0),
                    RespValue::null_bulk(),
                    RespValue::null_bulk(),
                    RespValue::null_array(),
                ]));
            }
            let min = g.pel.keys().next().unwrap().to_string_id();
            let max = g.pel.keys().next_back().unwrap().to_string_id();
            let mut by_consumer: BTreeMap<Bytes, i64> = BTreeMap::new();
            for nack in g.pel.values() {
                *by_consumer.entry(nack.consumer.clone()).or_default() += 1;
            }
            let consumers = RespValue::array(
                by_consumer
                    .into_iter()
                    .map(|(name, n)| {
                        RespValue::array(vec![
                            RespValue::bulk(name),
                            RespValue::bulk(Bytes::from(n.to_string())),
                        ])
                    })
                    .collect(),
            );
            Ok(RespValue::array(vec![
                RespValue::Integer(g.pel.len() as i64),
                RespValue::bulk(Bytes::from(min)),
                RespValue::bulk(Bytes::from(max)),
                consumers,
            ]))
        });
        return match r {
            Ok(v) => v,
            Err(e) => e,
        };
    }

    // extended: [IDLE min] start end count [consumer]
    let mut i = 3;
    let mut min_idle = 0u64;
    if upper(&args[i]) == "IDLE" {
        if i + 1 >= args.len() {
            return err(ERR_SYNTAX);
        }
        min_idle = match parse_i64(&args[i + 1]) {
            Some(n) if n >= 0 => n as u64,
            _ => return err(ERR_INT),
        };
        i += 2;
    }
    if args.len() - i < 3 {
        return wrong_arity("xpending");
    }
    let start = match parse_stream_id(arg_str(args, i).unwrap_or("-")) {
        Ok(id) => id,
        Err(e) => return err(e),
    };
    let end = match parse_stream_id(arg_str(args, i + 1).unwrap_or("+")) {
        Ok(id) => id,
        Err(e) => return err(e),
    };
    let count = match parse_i64(&args[i + 2]) {
        Some(n) if n >= 0 => n as usize,
        _ => return err(ERR_INT),
    };
    let consumer_filter = args.get(i + 3).cloned();
    if args.len() > i + 4 {
        return err(ERR_SYNTAX);
    }
    let t = now_ms();
    let r: Result<RespValue, RespValue> = ctx.db.with_db(state.db_index, |d| {
        let stream = d.stream_mut(key).map_err(err)?.ok_or_else(|| no_group_fmt(key, group))?;
        let g = stream
            .groups
            .get(group)
            .ok_or_else(|| no_group_fmt(key, group))?;
        let mut out = Vec::new();
        for (id, nack) in g.pel.range(start..=end) {
            if let Some(ref c) = consumer_filter {
                if &nack.consumer != c {
                    continue;
                }
            }
            let idle = t.saturating_sub(nack.delivery_time_ms);
            if idle < min_idle {
                continue;
            }
            out.push(RespValue::array(vec![
                RespValue::bulk(Bytes::from(id.to_string_id())),
                RespValue::bulk(nack.consumer.clone()),
                RespValue::Integer(idle as i64),
                RespValue::Integer(nack.delivery_count as i64),
            ]));
            if out.len() >= count {
                break;
            }
        }
        Ok(RespValue::array(out))
    });
    match r {
        Ok(v) => v,
        Err(e) => e,
    }
}

// -- XINFO ---------------------------------------------------------------------

pub(crate) fn cmd_xinfo(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("xinfo");
    }
    match upper(&args[1]).as_str() {
        "STREAM" => xinfo_stream(ctx, state, args),
        "GROUPS" => xinfo_groups(ctx, state, args),
        "CONSUMERS" => xinfo_consumers(ctx, state, args),
        "HELP" => xinfo_help(),
        other => err(&format!(
            "ERR unknown subcommand '{other}'. Try XINFO HELP."
        )),
    }
}

fn xinfo_help() -> RespValue {
    RespValue::array(
        [
            "XINFO <subcommand> ... Available subcommands:",
            "CONSUMERS <key> <groupname>",
            "GROUPS <key>",
            "STREAM <key> [FULL [<COUNT> count]]",
            "HELP",
        ]
        .into_iter()
        .map(|s| RespValue::bulk(Bytes::from_static(s.as_bytes())))
        .collect(),
    )
}

fn kv(k: &str, v: RespValue) -> [RespValue; 2] {
    [RespValue::bulk(Bytes::from(k.to_string())), v]
}

fn flatten_pairs(pairs: Vec<[RespValue; 2]>) -> RespValue {
    RespValue::array(pairs.into_iter().flatten().collect())
}

fn xinfo_stream(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("xinfo|stream");
    }
    let key = &args[2];
    let mut full = false;
    let mut count = 10usize;
    let mut i = 3;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "FULL" => {
                full = true;
                i += 1;
            }
            "COUNT" if i + 1 < args.len() => {
                count = match parse_i64(&args[i + 1]) {
                    Some(n) if n >= 0 => n as usize,
                    _ => return err(ERR_INT),
                };
                i += 2;
            }
            _ => return err(ERR_SYNTAX),
        }
    }
    let r: Result<RespValue, RespValue> = ctx.db.with_db(state.db_index, |d| {
        let stream = d.stream_mut(key).map_err(err)?.ok_or_else(|| err(ERR_NO_SUCH_KEY))?;
        let first = stream
            .entries
            .first_key_value()
            .map(|(id, f)| entry_resp(*id, f));
        let last = stream
            .entries
            .last_key_value()
            .map(|(id, f)| entry_resp(*id, f));
        if !full {
            let pairs = vec![
                kv("length", RespValue::Integer(stream.len() as i64)),
                kv("radix-tree-keys", RespValue::Integer(stream.len() as i64)),
                kv("radix-tree-nodes", RespValue::Integer(stream.len().saturating_mul(2) as i64)),
                kv("last-generated-id", RespValue::bulk(Bytes::from(stream.last_id.to_string_id()))),
                kv("max-deleted-entry-id", RespValue::bulk(Bytes::from(stream.max_deleted_id.to_string_id()))),
                kv("entries-added", RespValue::Integer(stream.entries_added as i64)),
                kv("recorded-first-entry-id", RespValue::bulk(Bytes::from(stream.recorded_first_id.to_string_id()))),
                kv("groups", RespValue::Integer(stream.groups.len() as i64)),
                kv("first-entry", first.unwrap_or_else(RespValue::null_bulk)),
                kv("last-entry", last.unwrap_or_else(RespValue::null_bulk)),
            ];
            return Ok(flatten_pairs(pairs));
        }
        let entries: Vec<_> = stream
            .entries
            .iter()
            .take(count)
            .map(|(id, f)| entry_resp(*id, f))
            .collect();
        let groups: Vec<_> = stream
            .groups
            .values()
            .map(|g| {
                let consumers: Vec<_> = g
                    .consumers
                    .values()
                    .map(|c| {
                        flatten_pairs(vec![
                            kv("name", RespValue::bulk(c.name.clone())),
                            kv("seen-time", RespValue::Integer(c.seen_time_ms as i64)),
                            kv("active-time", RespValue::Integer(c.active_time_ms)),
                            kv("pel-count", RespValue::Integer(c.pending.len() as i64)),
                            kv(
                                "pending",
                                RespValue::array(
                                    c.pending
                                        .iter()
                                        .take(count)
                                        .filter_map(|id| {
                                            g.pel.get(id).map(|n| {
                                                RespValue::array(vec![
                                                    RespValue::bulk(Bytes::from(id.to_string_id())),
                                                    RespValue::bulk(n.consumer.clone()),
                                                    RespValue::Integer(n.delivery_time_ms as i64),
                                                    RespValue::Integer(n.delivery_count as i64),
                                                ])
                                            })
                                        })
                                        .collect(),
                                ),
                            ),
                        ])
                    })
                    .collect();
                let pending: Vec<_> = g
                    .pel
                    .iter()
                    .take(count)
                    .map(|(id, n)| {
                        RespValue::array(vec![
                            RespValue::bulk(Bytes::from(id.to_string_id())),
                            RespValue::bulk(n.consumer.clone()),
                            RespValue::Integer(n.delivery_time_ms as i64),
                            RespValue::Integer(n.delivery_count as i64),
                        ])
                    })
                    .collect();
                flatten_pairs(vec![
                    kv("name", RespValue::bulk(g.name.clone())),
                    kv("last-delivered-id", RespValue::bulk(Bytes::from(g.last_id.to_string_id()))),
                    kv(
                        "entries-read",
                        if g.entries_read == STREAM_ENTRIES_READ_INVALID {
                            RespValue::null_bulk()
                        } else {
                            RespValue::Integer(g.entries_read)
                        },
                    ),
                    kv(
                        "lag",
                        match g.lag(stream.entries_added) {
                            Some(l) => RespValue::Integer(l),
                            None => RespValue::null_bulk(),
                        },
                    ),
                    kv("pel-count", RespValue::Integer(g.pel.len() as i64)),
                    kv("pending", RespValue::array(pending)),
                    kv("consumers", RespValue::array(consumers)),
                ])
            })
            .collect();
        Ok(flatten_pairs(vec![
            kv("length", RespValue::Integer(stream.len() as i64)),
            kv("radix-tree-keys", RespValue::Integer(stream.len() as i64)),
            kv("radix-tree-nodes", RespValue::Integer(stream.len().saturating_mul(2) as i64)),
            kv("last-generated-id", RespValue::bulk(Bytes::from(stream.last_id.to_string_id()))),
            kv("max-deleted-entry-id", RespValue::bulk(Bytes::from(stream.max_deleted_id.to_string_id()))),
            kv("entries-added", RespValue::Integer(stream.entries_added as i64)),
            kv("recorded-first-entry-id", RespValue::bulk(Bytes::from(stream.recorded_first_id.to_string_id()))),
            kv("entries", RespValue::array(entries)),
            kv("groups", RespValue::array(groups)),
        ]))
    });
    match r {
        Ok(v) => v,
        Err(e) => e,
    }
}

fn xinfo_groups(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("xinfo|groups");
    }
    let key = &args[2];
    let r: Result<RespValue, RespValue> = ctx.db.with_db(state.db_index, |d| {
        let stream = d.stream_mut(key).map_err(err)?.ok_or_else(|| err(ERR_NO_SUCH_KEY))?;
        let groups: Vec<_> = stream
            .groups
            .values()
            .map(|g| {
                flatten_pairs(vec![
                    kv("name", RespValue::bulk(g.name.clone())),
                    kv("consumers", RespValue::Integer(g.consumers.len() as i64)),
                    kv("pending", RespValue::Integer(g.pel.len() as i64)),
                    kv(
                        "last-delivered-id",
                        RespValue::bulk(Bytes::from(g.last_id.to_string_id())),
                    ),
                    kv(
                        "entries-read",
                        if g.entries_read == STREAM_ENTRIES_READ_INVALID {
                            RespValue::null_bulk()
                        } else {
                            RespValue::Integer(g.entries_read)
                        },
                    ),
                    kv(
                        "lag",
                        match g.lag(stream.entries_added) {
                            Some(l) => RespValue::Integer(l),
                            None => RespValue::null_bulk(),
                        },
                    ),
                ])
            })
            .collect();
        Ok(RespValue::array(groups))
    });
    match r {
        Ok(v) => v,
        Err(e) => e,
    }
}

fn xinfo_consumers(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("xinfo|consumers");
    }
    let key = &args[2];
    let group = &args[3];
    let t = now_ms();
    let r: Result<RespValue, RespValue> = ctx.db.with_db(state.db_index, |d| {
        let stream = d.stream_mut(key).map_err(err)?.ok_or_else(|| no_group_fmt(key, group))?;
        let g = stream
            .groups
            .get(group)
            .ok_or_else(|| no_group_fmt(key, group))?;
        let consumers: Vec<_> = g
            .consumers
            .values()
            .map(|c| {
                flatten_pairs(vec![
                    kv("name", RespValue::bulk(c.name.clone())),
                    kv("pending", RespValue::Integer(c.pending.len() as i64)),
                    kv(
                        "idle",
                        RespValue::Integer(t.saturating_sub(c.seen_time_ms) as i64),
                    ),
                    kv("inactive", RespValue::Integer(t as i64 - c.active_time_ms)),
                ])
            })
            .collect();
        Ok(RespValue::array(consumers))
    });
    match r {
        Ok(v) => v,
        Err(e) => e,
    }
}
