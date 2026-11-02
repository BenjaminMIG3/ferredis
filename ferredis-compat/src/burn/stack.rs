//! Redis Stack intégré (JSON / TimeSeries / Search / Bloom / Vector) — sémantique officielle.

use super::harness::{arr, bulk, BurnCtx};
use ferredis_core::resp::RespValue;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "stack";
    let mut c = ctx.connect().await;

    // JSON
    let jk = ctx.unique("json");
    ctx.expect_ok(
        S,
        "json_set",
        &mut c,
        &["JSON.SET", &jk, "$", r#"{"a":1,"b":[1,2,3],"c":{"d":true}}"#],
    )
    .await;
    ctx.expect_pred(S, "json_get", &mut c, &["JSON.GET", &jk, "$.a"], |v| match v {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(b);
            if s.contains('1') {
                Ok(())
            } else {
                Err(format!("JSON.GET $.a contient 1, got {s}"))
            }
        }
        _ => Err("JSON.GET → bulk".into()),
    })
    .await;
    ctx.expect_eq(
        S,
        "json_type",
        &mut c,
        &["JSON.TYPE", &jk, "$.b"],
        arr(vec![bulk("array")]),
    )
    .await;
    ctx.expect_eq(
        S,
        "json_arrlen",
        &mut c,
        &["JSON.ARRLEN", &jk, "$.b"],
        arr(vec![ferredis_core::resp::RespValue::Integer(3)]),
    )
    .await;
    ctx.expect_pred(
        S,
        "json_numincrby",
        &mut c,
        &["JSON.NUMINCRBY", &jk, "$.a", "2"],
        |v| match v {
            RespValue::BulkString(Some(b)) => {
                let s = String::from_utf8_lossy(b);
                if s.contains('3') {
                    Ok(())
                } else {
                    Err(format!("NUMINCRBY → 3, got {s}"))
                }
            }
            RespValue::Array(Some(items)) => {
                if items.iter().any(|i| matches!(i, RespValue::Integer(3))
                    || matches!(i, RespValue::BulkString(Some(b)) if String::from_utf8_lossy(b).contains('3')))
                {
                    Ok(())
                } else {
                    Err(format!("NUMINCRBY array with 3, got {items:?}"))
                }
            }
            other => Err(format!("JSON.NUMINCRBY unexpected {other:?}")),
        },
    )
    .await;

    // TimeSeries
    let ts = ctx.unique("ts");
    ctx.expect_ok(S, "ts_create", &mut c, &["TS.CREATE", &ts, "RETENTION", "0"])
        .await;
    ctx.expect_int(S, "ts_add", &mut c, &["TS.ADD", &ts, "1000", "1.5"], 1000)
        .await;
    ctx.expect_int(S, "ts_add2", &mut c, &["TS.ADD", &ts, "2000", "2.5"], 2000)
        .await;
    ctx.expect_pred(S, "ts_get", &mut c, &["TS.GET", &ts], |v| match v {
        RespValue::Array(Some(items)) if items.len() == 2 => Ok(()),
        _ => Err("TS.GET → [ts, value]".into()),
    })
    .await;
    ctx.expect_pred(
        S,
        "ts_range",
        &mut c,
        &["TS.RANGE", &ts, "0", "3000"],
        |v| match v {
            RespValue::Array(Some(items)) if items.len() == 2 => Ok(()),
            _ => Err("TS.RANGE → 2 samples".into()),
        },
    )
    .await;

    // Search
    let idx = ctx.unique("idx");
    let doc = ctx.unique("doc");
    ctx.expect_ok(
        S,
        "ft_create",
        &mut c,
        &[
            "FT.CREATE",
            &idx,
            "ON",
            "HASH",
            "PREFIX",
            "1",
            &format!("{doc}:"),
            "SCHEMA",
            "name",
            "TEXT",
            "age",
            "NUMERIC",
        ],
    )
    .await;
    let d1 = format!("{doc}:1");
    ctx.expect_int(
        S,
        "ft_doc",
        &mut c,
        &["HSET", &d1, "name", "alice", "age", "30"],
        2,
    )
    .await;
    ctx.expect_pred(
        S,
        "ft_search",
        &mut c,
        &["FT.SEARCH", &idx, "@name:alice"],
        |v| match v {
            RespValue::Array(Some(items)) if !items.is_empty() => Ok(()),
            _ => Err("FT.SEARCH → [count, …]".into()),
        },
    )
    .await;

    // Bloom
    let bf = ctx.unique("bf");
    ctx.expect_ok(S, "bf_reserve", &mut c, &["BF.RESERVE", &bf, "0.01", "100"])
        .await;
    ctx.expect_int(S, "bf_add", &mut c, &["BF.ADD", &bf, "item1"], 1)
        .await;
    ctx.expect_int(S, "bf_exists", &mut c, &["BF.EXISTS", &bf, "item1"], 1)
        .await;
    ctx.expect_int(S, "bf_missing", &mut c, &["BF.EXISTS", &bf, "nope"], 0)
        .await;

    // Cuckoo
    let cf = ctx.unique("cf");
    ctx.expect_ok(S, "cf_reserve", &mut c, &["CF.RESERVE", &cf, "100"])
        .await;
    ctx.expect_int(S, "cf_add", &mut c, &["CF.ADD", &cf, "x"], 1).await;
    ctx.expect_int(S, "cf_exists", &mut c, &["CF.EXISTS", &cf, "x"], 1)
        .await;

    // CMS / TOPK / TDIGEST
    let cms = ctx.unique("cms");
    ctx.expect_ok(S, "cms_init", &mut c, &["CMS.INITBYDIM", &cms, "2000", "5"])
        .await;
    ctx.expect_pred(
        S,
        "cms_incr",
        &mut c,
        &["CMS.INCRBY", &cms, "foo", "10"],
        |v| match v {
            RespValue::Array(Some(_)) | RespValue::Integer(_) => Ok(()),
            _ => Err("CMS.INCRBY → array|int".into()),
        },
    )
    .await;

    let topk = ctx.unique("topk");
    ctx.expect_ok(S, "topk_reserve", &mut c, &["TOPK.RESERVE", &topk, "3"])
        .await;
    ctx.expect_pred(
        S,
        "topk_add",
        &mut c,
        &["TOPK.ADD", &topk, "a", "b", "c"],
        |v| match v {
            RespValue::Array(Some(items)) if items.len() == 3 => Ok(()),
            _ => Err("TOPK.ADD → 3 slots".into()),
        },
    )
    .await;

    let td = ctx.unique("td");
    ctx.expect_ok(S, "tdigest_create", &mut c, &["TDIGEST.CREATE", &td])
        .await;
    ctx.expect_ok(S, "tdigest_add", &mut c, &["TDIGEST.ADD", &td, "1", "2", "3"])
        .await;
    ctx.expect_pred(
        S,
        "tdigest_quantile",
        &mut c,
        &["TDIGEST.QUANTILE", &td, "0.5"],
        |v| match v {
            RespValue::Array(Some(_)) | RespValue::BulkString(Some(_)) | RespValue::Double(_) => {
                Ok(())
            }
            _ => Err("TDIGEST.QUANTILE → value".into()),
        },
    )
    .await;

    // Vector set
    let vk = ctx.unique("vec");
    ctx.expect_pred(
        S,
        "vadd",
        &mut c,
        &["VADD", &vk, "VALUES", "2", "1", "0", "a"],
        |v| match v {
            RespValue::Integer(1) | RespValue::SimpleString(_) | RespValue::BulkString(Some(_)) => {
                Ok(())
            }
            other => Err(format!("VADD success, got {other:?}")),
        },
    )
    .await;
    ctx.expect_int(S, "vcard", &mut c, &["VCARD", &vk], 1).await;

    // MODULE LIST
    ctx.expect_pred(S, "module_list", &mut c, &["MODULE", "LIST"], |v| match v {
        RespValue::Array(Some(_)) => Ok(()),
        _ => Err("MODULE LIST → array".into()),
    })
    .await;
}
