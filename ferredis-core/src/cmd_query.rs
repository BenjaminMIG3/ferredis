//! Commandes FQ.* et INDEX.* (phase F5).

use crate::command::{wrong_arity, ClientState, CommandContext};
use crate::query::parse_select;
use crate::resp::RespValue;
use crate::util::{err, upper};
use bytes::Bytes;

fn text(s: impl Into<String>) -> RespValue {
    RespValue::bulk(Bytes::from(s.into()))
}

pub fn cmd_fq(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return wrong_arity("fq");
    }
    let sub = upper(&args[0]);
    match sub.as_str() {
        "FQ.SELECT" | "SELECT" => fq_select(ctx, args),
        "FQ.INDEX" | "INDEX" => fq_index(ctx, args),
        "FQ.DROPINDEX" | "DROPINDEX" => {
            if args.len() != 2 {
                return wrong_arity("fq.dropindex");
            }
            let name = String::from_utf8_lossy(&args[1]);
            let ok = ctx.query.lock().drop_index(&name);
            RespValue::Integer(ok as i64)
        }
        "FQ.LIST" | "LIST" => {
            let names = ctx.query.lock().list();
            RespValue::array(names.into_iter().map(text).collect())
        }
        _ => err("ERR unknown FQ subcommand"),
    }
}

fn fq_index(ctx: &CommandContext, args: &[Bytes]) -> RespValue {
    // FQ.INDEX CREATE name PREFIX prefix FIELD field
    // ou INDEX CREATE ...
    let a = if upper(&args[0]).starts_with("FQ.") {
        &args[1..]
    } else {
        &args[1..]
    };
    if a.len() < 6 || upper(&a[0]) != "CREATE" {
        return err("ERR usage: FQ.INDEX CREATE name PREFIX prefix FIELD field");
    }
    let name = String::from_utf8_lossy(&a[1]).into_owned();
    if upper(&a[2]) != "PREFIX" || upper(&a[4]) != "FIELD" {
        return err("ERR syntax: PREFIX <p> FIELD <f>");
    }
    let prefix = String::from_utf8_lossy(&a[3]).into_owned();
    let field = String::from_utf8_lossy(&a[5]).into_owned();
    match ctx.query.lock().create_index(name, prefix, field) {
        Ok(()) => RespValue::ok(),
        Err(e) => err(e),
    }
}

fn fq_select(ctx: &CommandContext, args: &[Bytes]) -> RespValue {
    let q = match parse_select(args) {
        Ok(q) => q,
        Err(e) => return err(e),
    };
    let keys = match ctx.query.lock().lookup(&q.index, &q.value) {
        Some(k) => k,
        None => return err("ERR no such index"),
    };
    let limited: Vec<_> = keys.into_iter().take(q.limit).map(RespValue::bulk).collect();
    RespValue::array(limited)
}

/// INDEX CREATE / DROP / LIST — alias Redis-friendly.
pub fn cmd_index(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("index");
    }
    match upper(&args[1]).as_str() {
        "CREATE" => {
            // INDEX CREATE name PREFIX p FIELD f
            if args.len() < 7 {
                return err("ERR usage: INDEX CREATE name PREFIX prefix FIELD field");
            }
            let mut fake = vec![Bytes::from_static(b"FQ.INDEX")];
            fake.extend(args[1..].iter().cloned());
            fq_index(ctx, &fake)
        }
        "DROP" | "DROPINDEX" => {
            if args.len() != 3 {
                return wrong_arity("index|drop");
            }
            let name = String::from_utf8_lossy(&args[2]);
            RespValue::Integer(ctx.query.lock().drop_index(&name) as i64)
        }
        "LIST" => {
            let names = ctx.query.lock().list();
            RespValue::array(names.into_iter().map(text).collect())
        }
        _ => err("ERR unknown INDEX subcommand"),
    }
}

/// Hook post-HSET pour maintenir les index secondaires.
pub fn notify_hash_field(ctx: &CommandContext, key: &Bytes, field: &Bytes, value: &Bytes) {
    ctx.query.lock().on_hash_set(key, field, value);
}
