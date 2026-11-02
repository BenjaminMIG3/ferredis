//! Clés génériques, TTL, SCAN, COPY, SORT — sémantique Redis OSS.

use super::harness::{arr, bulk, BurnCtx};
use ferredis_core::resp::RespValue;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "keys_expire";
    let mut c = ctx.connect().await;

    let k = ctx.unique("k");
    ctx.expect_ok(S, "set", &mut c, &["SET", &k, "v"]).await;
    ctx.expect_int(S, "exists", &mut c, &["EXISTS", &k], 1).await;
    // Redis TYPE → simple string (+string), pas bulk.
    ctx.expect_eq(
        S,
        "type",
        &mut c,
        &["TYPE", &k],
        ferredis_core::resp::RespValue::SimpleString(bytes::Bytes::from_static(b"string")),
    )
    .await;
    ctx.expect_int(S, "expire", &mut c, &["EXPIRE", &k, "30"], 1).await;
    ctx.expect_pred(S, "ttl", &mut c, &["TTL", &k], |v| match v {
        RespValue::Integer(n) if *n > 0 && *n <= 30 => Ok(()),
        _ => Err("TTL (0,30]".into()),
    })
    .await;
    ctx.expect_pred(S, "expiretime", &mut c, &["EXPIRETIME", &k], |v| match v {
        RespValue::Integer(n) if *n > 0 => Ok(()),
        _ => Err("EXPIRETIME unix > 0".into()),
    })
    .await;
    ctx.expect_int(S, "persist", &mut c, &["PERSIST", &k], 1).await;
    ctx.expect_int(S, "ttl_persist", &mut c, &["TTL", &k], -1).await;

    ctx.expect_int(S, "pexpire", &mut c, &["PEXPIRE", &k, "2000"], 1)
        .await;
    ctx.expect_pred(S, "pttl", &mut c, &["PTTL", &k], |v| match v {
        RespValue::Integer(n) if *n > 0 && *n <= 2000 => Ok(()),
        _ => Err("PTTL (0,2000]".into()),
    })
    .await;

    let k2 = ctx.unique("k2");
    ctx.expect_ok(S, "set2", &mut c, &["SET", &k2, "v2"]).await;
    ctx.expect_ok(S, "rename", &mut c, &["RENAME", &k2, &format!("{k2}:r")])
        .await;
    let renamed = format!("{k2}:r");
    ctx.expect_int(S, "renamenx_fail", &mut c, &["RENAMENX", &renamed, &renamed], 0)
        .await;

    let src = ctx.unique("copy_src");
    let dst = ctx.unique("copy_dst");
    ctx.expect_ok(S, "copy_prep", &mut c, &["SET", &src, "data"]).await;
    ctx.expect_int(S, "copy", &mut c, &["COPY", &src, &dst], 1).await;
    ctx.expect_bulk(S, "copy_get", &mut c, &["GET", &dst], "data").await;
    ctx.expect_int(S, "copy_nx_fail", &mut c, &["COPY", &src, &dst], 0)
        .await;

    ctx.expect_int(S, "touch", &mut c, &["TOUCH", &src, &dst], 2).await;
    ctx.expect_int(S, "unlink", &mut c, &["UNLINK", &src], 1).await;
    ctx.expect_int(S, "del", &mut c, &["DEL", &dst], 1).await;

    // RANDOMKEY / DBSIZE / SCAN
    ctx.expect_ok(S, "scan_prep", &mut c, &["SET", &ctx.unique("scan"), "1"])
        .await;
    ctx.expect_pred(S, "dbsize", &mut c, &["DBSIZE"], |v| match v {
        RespValue::Integer(n) if *n >= 1 => Ok(()),
        _ => Err("DBSIZE ≥ 1".into()),
    })
    .await;
    ctx.expect_pred(S, "randomkey", &mut c, &["RANDOMKEY"], |v| match v {
        RespValue::BulkString(Some(_)) | RespValue::BulkString(None) => Ok(()),
        _ => Err("RANDOMKEY bulk|null".into()),
    })
    .await;
    ctx.expect_pred(S, "scan", &mut c, &["SCAN", "0", "COUNT", "10"], |v| {
        match v {
            RespValue::Array(Some(items)) if items.len() == 2 => Ok(()),
            _ => Err("SCAN → [cursor, keys]".into()),
        }
    })
    .await;

    // OBJECT / DUMP / RESTORE
    let d = ctx.unique("dump");
    ctx.expect_ok(S, "dump_prep", &mut c, &["SET", &d, "payload"]).await;
    let dump = ctx.cmd(&mut c, &["DUMP", &d]).await;
    match dump {
        Ok(RespValue::BulkString(Some(bytes))) => {
            let restored = ctx.unique("restored");
            let t0 = std::time::Instant::now();
            // RESTORE key ttl serialized-value
            match ctx
                .cmd_raw(
                    &mut c,
                    &[
                        b"RESTORE",
                        restored.as_bytes(),
                        b"0",
                        &bytes,
                    ],
                )
                .await
            {
                Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => {
                    ctx.check(S, "restore", t0.elapsed(), true, "");
                    ctx.expect_bulk(S, "restore_get", &mut c, &["GET", &restored], "payload")
                        .await;
                }
                Ok(other) => ctx.check(
                    S,
                    "restore",
                    t0.elapsed(),
                    false,
                    format!("RESTORE Redis → OK, got {other:?}"),
                ),
                Err(e) => ctx.err_case(S, "restore", t0.elapsed(), e),
            }
        }
        Ok(other) => {
            ctx.check(
                S,
                "dump",
                std::time::Duration::ZERO,
                false,
                format!("DUMP Redis → bulk payload, got {other:?}"),
            );
        }
        Err(e) => ctx.err_case(S, "dump", std::time::Duration::ZERO, e),
    }

    // SORT
    let ls = ctx.unique("sort");
    ctx.expect_int(S, "sort_prep", &mut c, &["RPUSH", &ls, "3", "1", "2"], 3)
        .await;
    ctx.expect_eq(
        S,
        "sort",
        &mut c,
        &["SORT", &ls],
        arr(vec![bulk("1"), bulk("2"), bulk("3")]),
    )
    .await;

    // Keys pattern
    ctx.expect_pred(S, "keys", &mut c, &["KEYS", "*"], |v| match v {
        RespValue::Array(Some(_)) => Ok(()),
        _ => Err("KEYS → array".into()),
    })
    .await;
}
