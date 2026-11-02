//! Connexion / serveur / ACL / CLIENT / HELLO — sémantique Redis OSS.

use super::harness::BurnCtx;
use ferredis_core::resp::RespValue;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "server_conn";
    let mut c = ctx.connect().await;

    ctx.expect_eq(
        S,
        "ping",
        &mut c,
        &["PING"],
        ferredis_core::resp::RespValue::pong(),
    )
    .await;
    ctx.expect_bulk(S, "ping_msg", &mut c, &["PING", "hello"], "hello")
        .await;
    ctx.expect_bulk(S, "echo", &mut c, &["ECHO", "world"], "world")
        .await;

    ctx.expect_pred(S, "hello2", &mut c, &["HELLO", "2"], |v| match v {
        RespValue::Array(Some(_)) | RespValue::Map(_) => Ok(()),
        _ => Err("HELLO 2 → map/array".into()),
    })
    .await;
    ctx.expect_pred(S, "hello3", &mut c, &["HELLO", "3"], |v| match v {
        RespValue::Array(Some(_)) | RespValue::Map(_) => Ok(()),
        _ => Err("HELLO 3 → map/array RESP3".into()),
    })
    .await;
    // Revenir en RESP2 pour le reste du burn
    let _ = ctx.cmd(&mut c, &["HELLO", "2"]).await;

    ctx.expect_pred(S, "time", &mut c, &["TIME"], |v| match v {
        RespValue::Array(Some(items)) if items.len() == 2 => Ok(()),
        _ => Err("TIME → [sec, us]".into()),
    })
    .await;

    ctx.expect_pred(S, "info_server", &mut c, &["INFO", "server"], |v| {
        let text = match v {
            RespValue::BulkString(Some(b))
            | RespValue::SimpleString(b)
            | RespValue::Verbatim { data: b, .. } => String::from_utf8_lossy(b).into_owned(),
            _ => return Err("INFO → bulk".into()),
        };
        if text.contains("redis_version:") {
            Ok(())
        } else {
            Err("INFO server contient redis_version:".into())
        }
    })
    .await;

    ctx.expect_pred(
        S,
        "command_count",
        &mut c,
        &["COMMAND", "COUNT"],
        |v| match v {
            RespValue::Integer(n) if *n > 100 => Ok(()),
            _ => Err("COMMAND COUNT Redis 8 >> 100".into()),
        },
    )
    .await;

    ctx.expect_ok(S, "client_setname", &mut c, &["CLIENT", "SETNAME", "burn"])
        .await;
    ctx.expect_bulk(S, "client_getname", &mut c, &["CLIENT", "GETNAME"], "burn")
        .await;
    ctx.expect_pred(S, "client_id", &mut c, &["CLIENT", "ID"], |v| match v {
        RespValue::Integer(n) if *n >= 0 => Ok(()),
        _ => Err("CLIENT ID ≥ 0".into()),
    })
    .await;
    ctx.expect_pred(S, "client_list", &mut c, &["CLIENT", "LIST"], |v| {
        let text = match v {
            RespValue::BulkString(Some(b)) | RespValue::Verbatim { data: b, .. } => {
                String::from_utf8_lossy(b).into_owned()
            }
            _ => return Err("CLIENT LIST → bulk".into()),
        };
        if text.contains("name=burn") {
            Ok(())
        } else {
            Err(format!("CLIENT LIST contient name=burn, got {text}"))
        }
    })
    .await;

    ctx.expect_pred(S, "acl_whoami", &mut c, &["ACL", "WHOAMI"], |v| match v {
        RespValue::BulkString(Some(_)) | RespValue::SimpleString(_) => Ok(()),
        _ => Err("ACL WHOAMI → username".into()),
    })
    .await;
    ctx.expect_pred(S, "acl_list", &mut c, &["ACL", "LIST"], |v| match v {
        RespValue::Array(Some(_)) => Ok(()),
        _ => Err("ACL LIST → array".into()),
    })
    .await;

    ctx.expect_ok(
        S,
        "config_set_slowlog",
        &mut c,
        &["CONFIG", "SET", "slowlog-log-slower-than", "0"],
    )
    .await;
    ctx.expect_pred(
        S,
        "config_get",
        &mut c,
        &["CONFIG", "GET", "slowlog-log-slower-than"],
        |v| match v {
            RespValue::Array(Some(items)) if items.len() == 2 => Ok(()),
            _ => Err("CONFIG GET → [k,v]".into()),
        },
    )
    .await;

    ctx.expect_pred(
        S,
        "slowlog_get",
        &mut c,
        &["SLOWLOG", "GET", "1"],
        |v| match v {
            RespValue::Array(Some(_)) => Ok(()),
            _ => Err("SLOWLOG GET → array".into()),
        },
    )
    .await;
    ctx.expect_pred(S, "slowlog_len", &mut c, &["SLOWLOG", "LEN"], |v| match v {
        RespValue::Integer(_) => Ok(()),
        _ => Err("SLOWLOG LEN → int".into()),
    })
    .await;

    ctx.expect_pred(
        S,
        "memory_stats",
        &mut c,
        &["MEMORY", "STATS"],
        |v| match v {
            RespValue::Array(Some(_)) | RespValue::Map(_) => Ok(()),
            _ => Err("MEMORY STATS → array/map".into()),
        },
    )
    .await;

    ctx.expect_pred(
        S,
        "latency_latest",
        &mut c,
        &["LATENCY", "LATEST"],
        |v| match v {
            RespValue::Array(Some(_)) => Ok(()),
            _ => Err("LATENCY LATEST → array".into()),
        },
    )
    .await;

    // RESET (Redis 6.2+) → simple string +RESET (pas +OK)
    ctx.expect_eq(
        S,
        "reset",
        &mut c,
        &["RESET"],
        ferredis_core::resp::RespValue::SimpleString(bytes::Bytes::from_static(b"RESET")),
    )
    .await;
}
