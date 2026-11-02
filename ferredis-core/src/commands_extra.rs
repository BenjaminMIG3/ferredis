//! Additional Redis commands: hash, set, zset, scripts, persistence, tx helpers.

use crate::command::{arg_str, wrong_arity, ClientState, CommandContext, WatchedKey};
use crate::persist::load_rdb;
use crate::resp::RespValue;
use crate::script::run_script;
use bytes::Bytes;
use std::sync::Arc;

pub fn cmd_hset(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 || args.len() % 2 != 0 {
        return wrong_arity("hset");
    }
    let mut fields = Vec::new();
    let mut i = 2;
    while i + 1 < args.len() {
        fields.push((args[i].clone(), args[i + 1].clone()));
        i += 2;
    }
    match ctx.db.hset(state.db_index, &args[1], &fields) {
        Ok(n) => {
            for (f, v) in &fields {
                crate::cmd_query::notify_hash_field(ctx, &args[1], f, v);
            }
            ctx.propagate(args);
            ctx.apply_durability(state);
            ctx.notify_key(state.db_index, &args[1], "hset");
            RespValue::Integer(n)
        }
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_hget(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("hget");
    }
    match ctx.db.hget(state.db_index, &args[1], &args[2]) {
        Ok(Some(v)) => RespValue::bulk(v),
        Ok(None) => RespValue::null_bulk(),
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_hdel(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("hdel");
    }
    match ctx.db.hdel(state.db_index, &args[1], &args[2..]) {
        Ok(n) => {
            ctx.propagate(args);
            if n > 0 {
                ctx.notify_key(state.db_index, &args[1], "hdel");
            }
            RespValue::Integer(n)
        }
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_hgetall(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("hgetall");
    }
    match ctx.db.hgetall(state.db_index, &args[1]) {
        Ok(pairs) => {
            let mut items = Vec::new();
            for (k, v) in pairs {
                items.push(RespValue::bulk(k));
                items.push(RespValue::bulk(v));
            }
            RespValue::array(items)
        }
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_hexists(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("hexists");
    }
    match ctx.db.hexists(state.db_index, &args[1], &args[2]) {
        Ok(n) => RespValue::Integer(n),
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_hlen(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("hlen");
    }
    match ctx.db.hlen(state.db_index, &args[1]) {
        Ok(n) => RespValue::Integer(n),
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_hincrby(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("hincrby");
    }
    let delta: i64 = match arg_str(args, 3).and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    match ctx.db.hincrby(state.db_index, &args[1], &args[2], delta) {
        Ok(n) => {
            ctx.propagate(args);
            RespValue::Integer(n)
        }
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_hmget(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("hmget");
    }
    let mut items = Vec::new();
    for f in &args[2..] {
        match ctx.db.hget(state.db_index, &args[1], f) {
            Ok(Some(v)) => items.push(RespValue::bulk(v)),
            Ok(None) => items.push(RespValue::null_bulk()),
            Err(e) => return RespValue::err(e),
        }
    }
    RespValue::array(items)
}

pub fn cmd_hmset(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 || args.len() % 2 != 0 {
        return wrong_arity("hmset");
    }
    match cmd_hset(ctx, state, args) {
        RespValue::Integer(_) => RespValue::ok(),
        other => other,
    }
}

pub fn cmd_hkeys(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("hkeys");
    }
    match ctx.db.hgetall(state.db_index, &args[1]) {
        Ok(pairs) => RespValue::array(pairs.into_iter().map(|(k, _)| RespValue::bulk(k)).collect()),
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_hvals(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("hvals");
    }
    match ctx.db.hgetall(state.db_index, &args[1]) {
        Ok(pairs) => RespValue::array(pairs.into_iter().map(|(_, v)| RespValue::bulk(v)).collect()),
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_sadd(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("sadd");
    }
    match ctx.db.sadd(state.db_index, &args[1], &args[2..]) {
        Ok(n) => {
            ctx.propagate(args);
            if n > 0 {
                ctx.notify_key(state.db_index, &args[1], "sadd");
            }
            RespValue::Integer(n)
        }
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_srem(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("srem");
    }
    match ctx.db.srem(state.db_index, &args[1], &args[2..]) {
        Ok(n) => {
            ctx.propagate(args);
            RespValue::Integer(n)
        }
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_smembers(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("smembers");
    }
    match ctx.db.smembers(state.db_index, &args[1]) {
        Ok(m) => RespValue::array(m.into_iter().map(RespValue::bulk).collect()),
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_sismember(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("sismember");
    }
    match ctx.db.sismember(state.db_index, &args[1], &args[2]) {
        Ok(n) => RespValue::Integer(n),
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_scard(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("scard");
    }
    match ctx.db.scard(state.db_index, &args[1]) {
        Ok(n) => RespValue::Integer(n),
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_sinter(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("sinter");
    }
    match ctx.db.sinter(state.db_index, &args[1..]) {
        Ok(m) => RespValue::array(m.into_iter().map(RespValue::bulk).collect()),
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_sunion(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("sunion");
    }
    match ctx.db.sunion(state.db_index, &args[1..]) {
        Ok(m) => RespValue::array(m.into_iter().map(RespValue::bulk).collect()),
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_sdiff(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("sdiff");
    }
    match ctx.db.sdiff(state.db_index, &args[1..]) {
        Ok(m) => RespValue::array(m.into_iter().map(RespValue::bulk).collect()),
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_zrem(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("zrem");
    }
    match ctx.db.zrem(state.db_index, &args[1], &args[2..]) {
        Ok(n) => {
            ctx.propagate(args);
            RespValue::Integer(n)
        }
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_zscore(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("zscore");
    }
    match ctx.db.zscore(state.db_index, &args[1], &args[2]) {
        Ok(Some(s)) => RespValue::bulk(Bytes::from(format_score(s))),
        Ok(None) => RespValue::null_bulk(),
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_zcard(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("zcard");
    }
    match ctx.db.zcard(state.db_index, &args[1]) {
        Ok(n) => RespValue::Integer(n),
        Err(e) => RespValue::err(e),
    }
}

pub fn cmd_script(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("script");
    }
    let sub = arg_str(args, 1).unwrap_or("").to_ascii_uppercase();
    match sub.as_str() {
        "LOAD" => {
            if args.len() != 3 {
                return wrong_arity("script");
            }
            let src = String::from_utf8_lossy(&args[2]);
            RespValue::bulk(Bytes::from(ctx.scripts.load(&src)))
        }
        "EXISTS" => {
            let shas: Vec<String> = args[2..]
                .iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect();
            RespValue::array(
                ctx.scripts
                    .exists(&shas)
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect(),
            )
        }
        "FLUSH" => {
            let _ = crate::opts::trailing_sync_mode(args, 2);
            ctx.scripts.flush();
            RespValue::ok()
        }
        "KILL" => match crate::script::request_script_kill() {
            Ok(()) => RespValue::ok(),
            Err(e) => RespValue::err(e),
        },
        "DEBUG" => {
            if args.len() != 3 {
                return wrong_arity("script|debug");
            }
            match crate::script::set_script_debug(arg_str(args, 2).unwrap_or("")) {
                Ok(()) => RespValue::ok(),
                Err(e) => RespValue::err(e),
            }
        }
        "HELP" => RespValue::array(
            ["LOAD", "EXISTS", "FLUSH", "KILL", "DEBUG", "HELP"]
                .iter()
                .map(|s| RespValue::bulk(Bytes::from_static(s.as_bytes())))
                .collect(),
        ),
        _ => RespValue::err("ERR unknown SCRIPT subcommand"),
    }
}

pub fn cmd_save(ctx: &CommandContext) -> RespValue {
    let path = {
        let cfg = ctx.config.read();
        cfg.persist.dir.join(&cfg.persist.dbfilename)
    };
    let db = Arc::clone(&ctx.db);
    let fns = Arc::clone(&ctx.functions);
    let search = Arc::clone(&ctx.search);
    let cfg = Arc::clone(&ctx.config);
    let run = move || {
        crate::persist::save_rdb_with_stack(&path, &db, Some(&fns), Some(&search)).map(|_| {
            crate::cmd_server::mark_last_save(&cfg.read());
        })
    };
    let result = if let Some(pool) = crate::io_threads::global() {
        pool.run_blocking(run)
    } else {
        run()
    };
    match result {
        Ok(()) => RespValue::ok(),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

pub fn cmd_bgsave(ctx: &CommandContext) -> RespValue {
    let path = {
        let cfg = ctx.config.read();
        cfg.persist.dir.join(&cfg.persist.dbfilename)
    };
    let db = ArcCloneDb(ctx.db.clone());
    let cfg = Arc::clone(&ctx.config);
    let fns = Arc::clone(&ctx.functions);
    let search = Arc::clone(&ctx.search);
    std::thread::spawn(move || {
        if crate::persist::save_rdb_with_stack(&path, &db.0, Some(&fns), Some(&search)).is_ok() {
            crate::cmd_server::mark_last_save(&cfg.read());
        }
    });
    RespValue::simple(Bytes::from_static(b"Background saving started"))
}

struct ArcCloneDb(crate::store::SharedDb);

pub fn cmd_lastsave(ctx: &CommandContext) -> RespValue {
    use std::sync::atomic::Ordering;
    RespValue::Integer(ctx.config.read().last_save.load(Ordering::Relaxed))
}

pub fn cmd_bgrewriteaof(ctx: &CommandContext) -> RespValue {
    match ctx.aof.rewrite_with_functions(&ctx.db, Some(&ctx.functions)) {
        Ok(()) => RespValue::ok(),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

#[allow(dead_code)]
pub fn load_rdb_on_boot(ctx: &CommandContext) {
    let path = {
        let cfg = ctx.config.read();
        cfg.persist.dir.join(&cfg.persist.dbfilename)
    };
    let _ = load_rdb(&path, &ctx.db);
}

fn format_score(s: f64) -> String {
    crate::util::fmt_f64(s)
}

/// EVAL / EVALSHA (+ variantes `_RO`). L'appelant (`dispatch`) tient le verrou d'écriture global.
pub fn cmd_eval(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    by_sha: bool,
    read_only: bool,
) -> RespValue {
    let name = if by_sha {
        if read_only {
            "evalsha_ro"
        } else {
            "evalsha"
        }
    } else if read_only {
        "eval_ro"
    } else {
        "eval"
    };
    if args.len() < 3 {
        return wrong_arity(name);
    }
    let numkeys = match arg_str(args, 2).and_then(|s| s.parse::<i64>().ok()) {
        Some(n) if n < 0 => return RespValue::err("ERR Number of keys can't be negative"),
        Some(n) => n as usize,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    if args.len() < 3 + numkeys {
        return RespValue::err("ERR Number of keys can't be greater than number of args");
    }
    let source = if by_sha {
        match ctx.scripts.get(arg_str(args, 1).unwrap_or("")) {
            Some(s) => s,
            None => return RespValue::err("NOSCRIPT No matching script. Please use EVAL."),
        }
    } else {
        let src = String::from_utf8_lossy(&args[1]).into_owned();
        ctx.scripts.load(&src);
        src
    };
    run_script(
        ctx,
        state,
        &source,
        &args[3..3 + numkeys],
        &args[3 + numkeys..],
        read_only,
    )
}

/// WATCH key [key …] : mémorise l'état courant des clés pour détecter toute modification
/// (y compris par un autre client) au moment d'EXEC.
pub fn cmd_watch(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("watch");
    }
    if state.in_multi {
        return RespValue::err("ERR WATCH inside MULTI is not allowed");
    }
    for key in &args[1..] {
        let db = state.db_index;
        if state.watched.iter().any(|w| w.db == db && &w.key == key) {
            continue;
        }
        // notouch : un WATCH ne doit pas lui-même « salir » la clé via LRU.
        let version = ctx
            .db
            .with_db(db, |d| d.get_fresh_notouch(key).map(|e| e.version));
        state.watched.push(WatchedKey {
            db,
            key: key.clone(),
            version,
        });
    }
    RespValue::ok()
}

pub fn cmd_unwatch(state: &mut ClientState) -> RespValue {
    state.watched.clear();
    RespValue::ok()
}

/// `true` si une clé surveillée a été **écrite** depuis le WATCH (sémantique Redis).
pub fn watched_keys_changed(ctx: &CommandContext, watched: &[WatchedKey]) -> bool {
    watched.iter().any(|w| {
        let current = ctx
            .db
            .with_db(w.db, |d| d.get_fresh_notouch(&w.key).map(|e| e.version));
        current != w.version
    })
}