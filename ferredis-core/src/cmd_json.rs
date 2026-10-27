//! Commandes RedisJSON embarquées (`JSON.*`).

use crate::command::{arg_str, wrong_arity, ClientState, CommandContext};
use crate::json_path::{self, Path};
use crate::resp::RespValue;
use crate::store::WRONGTYPE;
use crate::util::*;
use crate::value::{Entry, RedisValue};
use bytes::Bytes;
use serde_json::Value;

fn after_write(ctx: &CommandContext, state: &ClientState, key: &Bytes, args: &[Bytes]) {
    ctx.db.touch();
    ctx.notify_key(state.db_index, key, "json.set");
    ctx.propagate(args);
}

fn get_json<'a>(
    d: &'a mut crate::store::DbInner,
    key: &Bytes,
) -> Result<Option<&'a mut Value>, &'static str> {
    match d.get_fresh_mut(key).map(|e| &mut e.value) {
        None => Ok(None),
        Some(RedisValue::Json(v)) => Ok(Some(v)),
        Some(_) => Err(WRONGTYPE),
    }
}

fn parse_json_arg(b: &[u8]) -> Result<Value, &'static str> {
    serde_json::from_slice(b).map_err(|_| "ERR failed to parse JSON")
}

fn path_or_root(args: &[Bytes], i: usize) -> Result<Path, &'static str> {
    match args.get(i) {
        None => Ok(Path {
            recursive: false,
            segs: Vec::new(),
        }),
        Some(p) => Path::parse(std::str::from_utf8(p).unwrap_or("")),
    }
}

fn bulk_json(v: &Value) -> RespValue {
    RespValue::bulk(Bytes::from(v.to_string()))
}

/// JSON.SET key path value [NX|XX]
pub(crate) fn cmd_json_set(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("json.set");
    }
    let key = &args[1];
    let path = match Path::parse(arg_str(args, 2).unwrap_or("")) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    let val = match parse_json_arg(&args[3]) {
        Ok(v) => v,
        Err(e) => return err(e),
    };
    let mut nx = false;
    let mut xx = false;
    for a in args.iter().skip(4) {
        match upper(a).as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            _ => return err(ERR_SYNTAX),
        }
    }
    if nx && xx {
        return err(ERR_SYNTAX);
    }

    let r = ctx.db.with_db(state.db_index, |d| -> Result<Option<()>, &'static str> {
        let exists = d.get_fresh(key).is_some();
        if nx && exists && path.is_root() {
            return Ok(None);
        }
        if xx && !exists {
            return Ok(None);
        }
        match d.get_fresh_mut(key) {
            None => {
                let mut root = Value::Null;
                json_path::set_path(&mut root, &path, val, false, false)?;
                d.map.insert(key.clone(), Entry::new(RedisValue::Json(root)));
                Ok(Some(()))
            }
            Some(Entry {
                value: RedisValue::Json(doc),
                ..
            }) => {
                if !json_path::set_path(doc, &path, val, nx, xx)? {
                    return Ok(None);
                }
                Ok(Some(()))
            }
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some(())) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

/// JSON.GET key [path…]
pub(crate) fn cmd_json_get(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("json.get");
    }
    let key = &args[1];
    let paths: Vec<Path> = if args.len() == 2 {
        vec![Path {
            recursive: false,
            segs: Vec::new(),
        }]
    } else {
        let mut ps = Vec::new();
        for p in &args[2..] {
            match Path::parse(std::str::from_utf8(p).unwrap_or("")) {
                Ok(path) => ps.push(path),
                Err(e) => return err(e),
            }
        }
        ps
    };

    let r = ctx.db.with_db(state.db_index, |d| -> Result<Option<Value>, &'static str> {
        let Some(RedisValue::Json(doc)) = d.get_fresh(key).map(|e| &e.value) else {
            return match d.get_fresh(key) {
                None => Ok(None),
                Some(_) => Err(WRONGTYPE),
            };
        };
        if paths.len() == 1 {
            let hits = json_path::get_all(doc, &paths[0]);
            if hits.is_empty() {
                return Ok(Some(Value::Null));
            }
            if hits.len() == 1 {
                return Ok(Some(hits[0].clone()));
            }
            return Ok(Some(Value::Array(hits.iter().map(|v| (*v).clone()).collect())));
        }
        let mut obj = serde_json::Map::new();
        for (i, p) in paths.iter().enumerate() {
            let label = args.get(i + 2).map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default();
            let hits = json_path::get_all(doc, p);
            let v = if hits.is_empty() {
                Value::Null
            } else if hits.len() == 1 {
                hits[0].clone()
            } else {
                Value::Array(hits.iter().map(|x| (*x).clone()).collect())
            };
            obj.insert(label, v);
        }
        Ok(Some(Value::Object(obj)))
    });
    match r {
        Err(e) => err(e),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some(Value::Null)) => RespValue::null_bulk(),
        Ok(Some(v)) => bulk_json(&v),
    }
}

/// JSON.MGET key [key…] path
pub(crate) fn cmd_json_mget(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("json.mget");
    }
    let path = match Path::parse(arg_str(args, args.len() - 1).unwrap_or("$")) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    let keys = &args[1..args.len() - 1];
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        let item = ctx.db.with_db(state.db_index, |d| {
            match d.get_fresh(key).map(|e| &e.value) {
                None => RespValue::null_bulk(),
                Some(RedisValue::Json(doc)) => {
                    let hits = json_path::get_all(doc, &path);
                    if hits.is_empty() {
                        RespValue::null_bulk()
                    } else if hits.len() == 1 {
                        bulk_json(hits[0])
                    } else {
                        bulk_json(&Value::Array(hits.iter().map(|v| (*v).clone()).collect()))
                    }
                }
                Some(_) => err(WRONGTYPE),
            }
        });
        if matches!(&item, RespValue::Error(_)) {
            return item;
        }
        out.push(item);
    }
    RespValue::array(out)
}

/// JSON.MSET key path value [key path value…]
pub(crate) fn cmd_json_mset(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 || (args.len() - 1) % 3 != 0 {
        return wrong_arity("json.mset");
    }
    let triples: Vec<_> = args[1..].chunks(3).collect();
    for chunk in &triples {
        let path = match Path::parse(std::str::from_utf8(&chunk[1]).unwrap_or("")) {
            Ok(p) => p,
            Err(e) => return err(e),
        };
        let val = match parse_json_arg(&chunk[2]) {
            Ok(v) => v,
            Err(e) => return err(e),
        };
        let key = &chunk[0];
        let r = ctx.db.with_db(state.db_index, |d| -> Result<(), &'static str> {
            match d.get_fresh_mut(key) {
                None => {
                    let mut root = Value::Null;
                    json_path::set_path(&mut root, &path, val, false, false)?;
                    d.map.insert(key.clone(), Entry::new(RedisValue::Json(root)));
                    Ok(())
                }
                Some(Entry {
                    value: RedisValue::Json(doc),
                    ..
                }) => {
                    json_path::set_path(doc, &path, val, false, false)?;
                    Ok(())
                }
                Some(_) => Err(WRONGTYPE),
            }
        });
        if let Err(e) = r {
            return err(e);
        }
        ctx.notify_key(state.db_index, key, "json.set");
    }
    ctx.db.touch();
    ctx.propagate(args);
    RespValue::ok()
}

fn cmd_del_like(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes], name: &str) -> RespValue {
    if args.len() < 2 {
        return wrong_arity(name);
    }
    let key = &args[1];
    let path = match path_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    let r = ctx.db.with_db(state.db_index, |d| -> Result<i64, &'static str> {
        match d.get_fresh_mut(key) {
            None => Ok(0),
            Some(Entry {
                value: RedisValue::Json(doc),
                ..
            }) => {
                if path.is_root() {
                    d.map.remove(key);
                    return Ok(1);
                }
                Ok(json_path::delete_path(doc, &path) as i64)
            }
            Some(_) => Err(WRONGTYPE),
        }
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

pub(crate) fn cmd_json_del(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    cmd_del_like(ctx, state, args, "json.del")
}

pub(crate) fn cmd_json_forget(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    cmd_del_like(ctx, state, args, "json.forget")
}

pub(crate) fn cmd_json_type(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("json.type");
    }
    let key = &args[1];
    let path = match path_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(key).map(|e| &e.value) {
            None => RespValue::null_bulk(),
            Some(RedisValue::Json(doc)) => {
                let hits = json_path::get_all(doc, &path);
                if hits.is_empty() {
                    RespValue::null_bulk()
                } else if hits.len() == 1 {
                    RespValue::bulk(Bytes::from_static(json_path::json_type_name(hits[0]).as_bytes()))
                } else {
                    RespValue::array(
                        hits.iter()
                            .map(|v| {
                                RespValue::bulk(Bytes::from_static(json_path::json_type_name(v).as_bytes()))
                            })
                            .collect(),
                    )
                }
            }
            Some(_) => err(WRONGTYPE),
        }
    })
}

pub(crate) fn cmd_json_strlen(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    json_scalar_len(ctx, state, args, "json.strlen", |v| {
        v.as_str().map(|s| s.len() as i64)
    })
}

pub(crate) fn cmd_json_objlen(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    json_scalar_len(ctx, state, args, "json.objlen", |v| {
        v.as_object().map(|o| o.len() as i64)
    })
}

pub(crate) fn cmd_json_arrlen(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    json_scalar_len(ctx, state, args, "json.arrlen", |v| {
        v.as_array().map(|a| a.len() as i64)
    })
}

fn json_scalar_len(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    name: &str,
    f: impl Fn(&Value) -> Option<i64>,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arity(name);
    }
    let key = &args[1];
    let path = match path_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(key).map(|e| &e.value) {
            None => RespValue::null_bulk(),
            Some(RedisValue::Json(doc)) => {
                let hits = json_path::get_all(doc, &path);
                if hits.is_empty() {
                    RespValue::null_bulk()
                } else if hits.len() == 1 {
                    match f(hits[0]) {
                        Some(n) => RespValue::Integer(n),
                        None => RespValue::null_bulk(),
                    }
                } else {
                    RespValue::array(
                        hits.iter()
                            .map(|v| match f(v) {
                                Some(n) => RespValue::Integer(n),
                                None => RespValue::null_bulk(),
                            })
                            .collect(),
                    )
                }
            }
            Some(_) => err(WRONGTYPE),
        }
    })
}

pub(crate) fn cmd_json_objkeys(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("json.objkeys");
    }
    let key = &args[1];
    let path = match path_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(key).map(|e| &e.value) {
            None => RespValue::null_bulk(),
            Some(RedisValue::Json(doc)) => {
                let hits = json_path::get_all(doc, &path);
                if hits.is_empty() {
                    return RespValue::null_bulk();
                }
                let one = |v: &Value| -> RespValue {
                    match v.as_object() {
                        Some(o) => RespValue::array(
                            o.keys()
                                .map(|k| RespValue::bulk(Bytes::from(k.clone())))
                                .collect(),
                        ),
                        None => RespValue::null_bulk(),
                    }
                };
                if hits.len() == 1 {
                    one(hits[0])
                } else {
                    RespValue::array(hits.iter().map(|v| one(v)).collect())
                }
            }
            Some(_) => err(WRONGTYPE),
        }
    })
}

pub(crate) fn cmd_json_numincrby(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    json_num_op(ctx, state, args, "json.numincrby", |a, b| a + b)
}

pub(crate) fn cmd_json_nummultby(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    json_num_op(ctx, state, args, "json.nummultby", |a, b| a * b)
}

fn json_num_op(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    name: &str,
    op: impl Fn(f64, f64) -> f64,
) -> RespValue {
    if args.len() != 4 {
        return wrong_arity(name);
    }
    let key = &args[1];
    let path = match Path::parse(arg_str(args, 2).unwrap_or("$")) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    let by = match parse_f64(&args[3]) {
        Some(v) => v,
        None => return err(ERR_FLOAT),
    };
    let r = ctx.db.with_db(state.db_index, |d| -> Result<Option<Value>, &'static str> {
        let Some(doc) = get_json(d, key)? else {
            return Ok(None);
        };
        let mut out = None;
        json_path::with_mut(doc, &path, |node| {
            let n = node.as_f64().ok_or("ERR wrong type of path value - expected a number")?;
            let next = op(n, by);
            *node = serde_json::Number::from_f64(next)
                .map(Value::Number)
                .unwrap_or(Value::Null);
            out = Some(node.clone());
            Ok::<(), &'static str>(())
        })??;
        Ok(out)
    });
    match r {
        Err(e) => err(e),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some(v)) => {
            after_write(ctx, state, key, args);
            bulk_json(&v)
        }
    }
}

pub(crate) fn cmd_json_strappend(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("json.strappend");
    }
    let key = &args[1];
    let (path, json_arg) = if args.len() == 3 {
        (
            Path {
                recursive: false,
                segs: Vec::new(),
            },
            &args[2],
        )
    } else {
        (
            match Path::parse(arg_str(args, 2).unwrap_or("$")) {
                Ok(p) => p,
                Err(e) => return err(e),
            },
            &args[3],
        )
    };
    let append = match parse_json_arg(json_arg) {
        Ok(Value::String(s)) => s,
        Ok(_) => return err("ERR wrong type of path value - expected a string"),
        Err(e) => return err(e),
    };
    let r = ctx.db.with_db(state.db_index, |d| -> Result<Option<i64>, &'static str> {
        let Some(doc) = get_json(d, key)? else {
            return Ok(None);
        };
        let mut len = None;
        json_path::with_mut(doc, &path, |node| {
            let s = node
                .as_str()
                .ok_or("ERR wrong type of path value - expected a string")?;
            let next = format!("{s}{append}");
            let n = next.len() as i64;
            *node = Value::String(next);
            len = Some(n);
            Ok::<(), &'static str>(())
        })??;
        Ok(len)
    });
    match r {
        Err(e) => err(e),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some(n)) => {
            after_write(ctx, state, key, args);
            RespValue::Integer(n)
        }
    }
}

pub(crate) fn cmd_json_arrappend(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("json.arrappend");
    }
    let key = &args[1];
    let path = match Path::parse(arg_str(args, 2).unwrap_or("$")) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    let mut values = Vec::new();
    for a in &args[3..] {
        match parse_json_arg(a) {
            Ok(v) => values.push(v),
            Err(e) => return err(e),
        }
    }
    let r = ctx.db.with_db(state.db_index, |d| -> Result<Option<i64>, &'static str> {
        let Some(doc) = get_json(d, key)? else {
            return Ok(None);
        };
        let mut len = None;
        json_path::with_mut(doc, &path, |node| {
            let arr = node
                .as_array_mut()
                .ok_or("ERR wrong type of path value - expected an array")?;
            arr.extend(values);
            len = Some(arr.len() as i64);
            Ok::<(), &'static str>(())
        })??;
        Ok(len)
    });
    match r {
        Err(e) => err(e),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some(n)) => {
            after_write(ctx, state, key, args);
            RespValue::Integer(n)
        }
    }
}

pub(crate) fn cmd_json_arrinsert(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 5 {
        return wrong_arity("json.arrinsert");
    }
    let key = &args[1];
    let path = match Path::parse(arg_str(args, 2).unwrap_or("$")) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    let index = match parse_i64(&args[3]) {
        Some(i) => i,
        None => return err(ERR_INT),
    };
    let mut values = Vec::new();
    for a in &args[4..] {
        match parse_json_arg(a) {
            Ok(v) => values.push(v),
            Err(e) => return err(e),
        }
    }
    let r = ctx.db.with_db(state.db_index, |d| -> Result<Option<i64>, &'static str> {
        let Some(doc) = get_json(d, key)? else {
            return Ok(None);
        };
        let mut len = None;
        json_path::with_mut(doc, &path, |node| {
            let arr = node
                .as_array_mut()
                .ok_or("ERR wrong type of path value - expected an array")?;
            let mut idx = if index < 0 {
                (arr.len() as i64 + index).max(0) as usize
            } else {
                index as usize
            };
            if idx > arr.len() {
                idx = arr.len();
            }
            for (i, v) in values.into_iter().enumerate() {
                arr.insert(idx + i, v);
            }
            len = Some(arr.len() as i64);
            Ok::<(), &'static str>(())
        })??;
        Ok(len)
    });
    match r {
        Err(e) => err(e),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some(n)) => {
            after_write(ctx, state, key, args);
            RespValue::Integer(n)
        }
    }
}

pub(crate) fn cmd_json_arrpop(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("json.arrpop");
    }
    let key = &args[1];
    let path = match path_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    let index = args.get(3).and_then(|b| parse_i64(b)).unwrap_or(-1);
    let r = ctx.db.with_db(state.db_index, |d| -> Result<Option<Value>, &'static str> {
        let Some(doc) = get_json(d, key)? else {
            return Ok(None);
        };
        let mut popped = None;
        json_path::with_mut(doc, &path, |node| {
            let arr = node
                .as_array_mut()
                .ok_or("ERR wrong type of path value - expected an array")?;
            if arr.is_empty() {
                return Ok(());
            }
            let idx = if index < 0 {
                (arr.len() as i64 + index).max(0) as usize
            } else {
                (index as usize).min(arr.len() - 1)
            };
            popped = Some(arr.remove(idx));
            Ok::<(), &'static str>(())
        })??;
        Ok(popped)
    });
    match r {
        Err(e) => err(e),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some(v)) => {
            after_write(ctx, state, key, args);
            bulk_json(&v)
        }
    }
}

pub(crate) fn cmd_json_arrtrim(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 5 {
        return wrong_arity("json.arrtrim");
    }
    let key = &args[1];
    let path = match Path::parse(arg_str(args, 2).unwrap_or("$")) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    let start = match parse_i64(&args[3]) {
        Some(i) => i,
        None => return err(ERR_INT),
    };
    let stop = match parse_i64(&args[4]) {
        Some(i) => i,
        None => return err(ERR_INT),
    };
    let r = ctx.db.with_db(state.db_index, |d| -> Result<Option<i64>, &'static str> {
        let Some(doc) = get_json(d, key)? else {
            return Ok(None);
        };
        let mut len = None;
        json_path::with_mut(doc, &path, |node| {
            let arr = node
                .as_array_mut()
                .ok_or("ERR wrong type of path value - expected an array")?;
            let n = arr.len() as i64;
            let mut s = if start < 0 { n + start } else { start };
            let mut e = if stop < 0 { n + stop } else { stop };
            if s < 0 {
                s = 0;
            }
            if e < 0 {
                e = 0;
            }
            if s as usize >= arr.len() || s > e {
                arr.clear();
            } else {
                let e = (e as usize).min(arr.len() - 1);
                let s = s as usize;
                *arr = arr[s..=e].to_vec();
            }
            len = Some(arr.len() as i64);
            Ok::<(), &'static str>(())
        })??;
        Ok(len)
    });
    match r {
        Err(e) => err(e),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some(n)) => {
            after_write(ctx, state, key, args);
            RespValue::Integer(n)
        }
    }
}

pub(crate) fn cmd_json_arrindex(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("json.arrindex");
    }
    let key = &args[1];
    let path = match Path::parse(arg_str(args, 2).unwrap_or("$")) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    let needle = match parse_json_arg(&args[3]) {
        Ok(v) => v,
        Err(e) => return err(e),
    };
    let start = args.get(4).and_then(|b| parse_i64(b)).unwrap_or(0);
    let end = args.get(5).and_then(|b| parse_i64(b)).unwrap_or(-1);
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(key).map(|e| &e.value) {
            None => RespValue::null_bulk(),
            Some(RedisValue::Json(doc)) => {
                let hits = json_path::get_all(doc, &path);
                if hits.is_empty() {
                    return RespValue::null_bulk();
                }
                let find = |v: &Value| -> RespValue {
                    let Some(arr) = v.as_array() else {
                        return RespValue::null_bulk();
                    };
                    let n = arr.len() as i64;
                    let mut s = if start < 0 { n + start } else { start };
                    let mut e = if end < 0 { n + end } else { end };
                    if s < 0 {
                        s = 0;
                    }
                    if e < 0 {
                        e = -1;
                    }
                    if s > e || s as usize >= arr.len() {
                        return RespValue::Integer(-1);
                    }
                    let e = (e as usize).min(arr.len() - 1);
                    for i in s as usize..=e {
                        if arr[i] == needle {
                            return RespValue::Integer(i as i64);
                        }
                    }
                    RespValue::Integer(-1)
                };
                if hits.len() == 1 {
                    find(hits[0])
                } else {
                    RespValue::array(hits.iter().map(|v| find(v)).collect())
                }
            }
            Some(_) => err(WRONGTYPE),
        }
    })
}

pub(crate) fn cmd_json_clear(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("json.clear");
    }
    let key = &args[1];
    let path = match path_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    let r = ctx.db.with_db(state.db_index, |d| -> Result<i64, &'static str> {
        let Some(doc) = get_json(d, key)? else {
            return Ok(0);
        };
        let mut n = 0i64;
        let hits_count = json_path::get_all(doc, &path).len();
        if hits_count == 0 {
            return Ok(0);
        }
        // clear via mutation on path
        let _ = json_path::with_mut(doc, &path, |node| {
            match node {
                Value::Array(a) => {
                    if !a.is_empty() {
                        a.clear();
                        n = 1;
                    }
                }
                Value::Object(o) => {
                    if !o.is_empty() {
                        o.clear();
                        n = 1;
                    }
                }
                Value::Number(_) => {
                    *node = Value::Number(0.into());
                    n = 1;
                }
                Value::String(s) => {
                    if !s.is_empty() {
                        *s = String::new();
                        n = 1;
                    }
                }
                Value::Bool(_) => {
                    *node = Value::Bool(false);
                    n = 1;
                }
                Value::Null => {}
            }
        });
        Ok(n)
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

pub(crate) fn cmd_json_toggle(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("json.toggle");
    }
    let key = &args[1];
    let path = match path_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    let r = ctx.db.with_db(state.db_index, |d| -> Result<Option<i64>, &'static str> {
        let Some(doc) = get_json(d, key)? else {
            return Ok(None);
        };
        let mut out = None;
        json_path::with_mut(doc, &path, |node| {
            let b = node
                .as_bool()
                .ok_or("ERR wrong type of path value - expected a boolean")?;
            *node = Value::Bool(!b);
            out = Some(if !b { 1 } else { 0 });
            Ok::<(), &'static str>(())
        })??;
        Ok(out)
    });
    match r {
        Err(e) => err(e),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some(n)) => {
            after_write(ctx, state, key, args);
            RespValue::Integer(n)
        }
    }
}

pub(crate) fn cmd_json_merge(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("json.merge");
    }
    let key = &args[1];
    let path = match Path::parse(arg_str(args, 2).unwrap_or("$")) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    let patch = match parse_json_arg(&args[3]) {
        Ok(v) => v,
        Err(e) => return err(e),
    };
    let r = ctx.db.with_db(state.db_index, |d| -> Result<(), &'static str> {
        match d.get_fresh_mut(key) {
            None => {
                let mut root = Value::Null;
                json_path::set_path(&mut root, &path, patch, false, false)?;
                d.map.insert(key.clone(), Entry::new(RedisValue::Json(root)));
                Ok(())
            }
            Some(Entry {
                value: RedisValue::Json(doc),
                ..
            }) => {
                json_path::with_mut(doc, &path, |node| {
                    merge_values(node, patch);
                })?;
                Ok(())
            }
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

fn merge_values(dst: &mut Value, src: Value) {
    match (dst, src) {
        (Value::Object(d), Value::Object(s)) => {
            for (k, v) in s {
                match d.get_mut(&k) {
                    Some(existing) => merge_values(existing, v),
                    None => {
                        d.insert(k, v);
                    }
                }
            }
        }
        (d, s) => *d = s,
    }
}

pub(crate) fn cmd_json_resp(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("json.resp");
    }
    let key = &args[1];
    let path = match path_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(key).map(|e| &e.value) {
            None => RespValue::null_bulk(),
            Some(RedisValue::Json(doc)) => {
                let hits = json_path::get_all(doc, &path);
                if hits.is_empty() {
                    RespValue::null_bulk()
                } else if hits.len() == 1 {
                    json_path::to_resp_json(hits[0])
                } else {
                    RespValue::array(hits.iter().map(|v| json_path::to_resp_json(v)).collect())
                }
            }
            Some(_) => err(WRONGTYPE),
        }
    })
}

pub(crate) fn cmd_json_debug(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("json.debug");
    }
    match upper(&args[1]).as_str() {
        "HELP" => RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(b"JSON.DEBUG HELP")),
            RespValue::bulk(Bytes::from_static(b"JSON.DEBUG MEMORY <key> [path]")),
        ]),
        "MEMORY" => {
            if args.len() < 3 {
                return wrong_arity("json.debug");
            }
            let key = &args[2];
            let path = match path_or_root(args, 3) {
                Ok(p) => p,
                Err(e) => return err(e),
            };
            ctx.db.with_db(state.db_index, |d| {
                match d.get_fresh(key).map(|e| &e.value) {
                    None => RespValue::null_bulk(),
                    Some(RedisValue::Json(doc)) => {
                        let hits = json_path::get_all(doc, &path);
                        if hits.is_empty() {
                            RespValue::null_bulk()
                        } else {
                            let n: usize = hits.iter().map(|v| v.to_string().len()).sum();
                            RespValue::Integer(n as i64)
                        }
                    }
                    Some(_) => err(WRONGTYPE),
                }
            })
        }
        _ => err("ERR unknown subcommand for JSON.DEBUG"),
    }
}
