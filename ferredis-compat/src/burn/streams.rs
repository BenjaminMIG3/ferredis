//! Streams + consumer groups — sémantique Redis OSS.

use super::harness::{arr, bulk, BurnCtx};
use ferredis_core::resp::RespValue;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "streams";
    let mut c = ctx.connect().await;
    let s = ctx.unique("stream");

    ctx.expect_bulk(S, "xadd", &mut c, &["XADD", &s, "1-0", "a", "1"], "1-0")
        .await;
    ctx.expect_bulk(S, "xadd2", &mut c, &["XADD", &s, "2-0", "a", "2", "b", "3"], "2-0")
        .await;
    ctx.expect_int(S, "xlen", &mut c, &["XLEN", &s], 2).await;

    ctx.expect_eq(
        S,
        "xrange",
        &mut c,
        &["XRANGE", &s, "-", "+"],
        arr(vec![
            arr(vec![
                bulk("1-0"),
                arr(vec![bulk("a"), bulk("1")]),
            ]),
            arr(vec![
                bulk("2-0"),
                arr(vec![bulk("a"), bulk("2"), bulk("b"), bulk("3")]),
            ]),
        ]),
    )
    .await;
    ctx.expect_eq(
        S,
        "xrevrange_count",
        &mut c,
        &["XREVRANGE", &s, "+", "-", "COUNT", "1"],
        arr(vec![arr(vec![
            bulk("2-0"),
            arr(vec![bulk("a"), bulk("2"), bulk("b"), bulk("3")]),
        ])]),
    )
    .await;

    ctx.expect_pred(
        S,
        "xread",
        &mut c,
        &["XREAD", "COUNT", "1", "STREAMS", &s, "0-0"],
        |v| match v {
            RespValue::Array(Some(items)) if !items.is_empty() => Ok(()),
            _ => Err("XREAD → [[stream, entries]]".into()),
        },
    )
    .await;

    ctx.expect_ok(
        S,
        "xgroup_create",
        &mut c,
        &["XGROUP", "CREATE", &s, "g", "0", "MKSTREAM"],
    )
    .await;
    // MKSTREAM on existing is ok; CREATE without MKSTREAM on existing stream with same group → BUSYGROUP
    ctx.expect_error_contains(
        S,
        "xgroup_busy",
        &mut c,
        &["XGROUP", "CREATE", &s, "g", "0"],
        "BUSYGROUP",
    )
    .await;

    ctx.expect_pred(
        S,
        "xreadgroup",
        &mut c,
        &["XREADGROUP", "GROUP", "g", "c1", "COUNT", "1", "STREAMS", &s, ">"],
        |v| match v {
            RespValue::Array(Some(_)) | RespValue::Array(None) => Ok(()),
            _ => Err("XREADGROUP → array|null".into()),
        },
    )
    .await;

    // Après lecture, pending / ack
    ctx.expect_pred(S, "xpending", &mut c, &["XPENDING", &s, "g"], |v| match v {
        RespValue::Array(Some(items)) if !items.is_empty() => Ok(()),
        _ => Err("XPENDING summary array".into()),
    })
    .await;

    // Récupérer un ID pending pour XACK
    let pending = ctx.cmd(&mut c, &["XPENDING", &s, "g", "-", "+", "1"]).await;
    if let Ok(RespValue::Array(Some(entries))) = &pending {
        if let Some(RespValue::Array(Some(row))) = entries.first() {
            if let Some(RespValue::BulkString(Some(id))) = row.first() {
                let id = String::from_utf8_lossy(id).into_owned();
                ctx.expect_int(S, "xack", &mut c, &["XACK", &s, "g", &id], 1)
                    .await;
            }
        }
    }

    ctx.expect_int(S, "xdel", &mut c, &["XDEL", &s, "1-0"], 1).await;
    ctx.expect_int(S, "xtrim", &mut c, &["XTRIM", &s, "MAXLEN", "=", "0"], 1)
        .await;

    ctx.expect_pred(S, "xinfo_stream", &mut c, &["XINFO", "STREAM", &s], |v| {
        match v {
            RespValue::Array(Some(_)) => Ok(()),
            _ => Err("XINFO STREAM → array".into()),
        }
    })
    .await;
    ctx.expect_pred(S, "xinfo_groups", &mut c, &["XINFO", "GROUPS", &s], |v| {
        match v {
            RespValue::Array(Some(_)) => Ok(()),
            _ => Err("XINFO GROUPS → array".into()),
        }
    })
    .await;

    // XREAD BLOCK timeout
    ctx.expect_null_array(
        S,
        "xread_block_timeout",
        &mut c,
        &[
            "XREAD",
            "BLOCK",
            "100",
            "STREAMS",
            &ctx.unique("nostream"),
            "$",
        ],
    )
    .await;
}
