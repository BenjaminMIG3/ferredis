//! Hashes + hash field TTL (Redis 7.4 / 8.0) — sémantique redis.io.

use super::harness::{arr, bulk, int, BurnCtx};
use ferredis_core::resp::RespValue;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "hashes";
    let mut c = ctx.connect().await;
    let h = ctx.unique("hash");

    ctx.expect_int(S, "hset", &mut c, &["HSET", &h, "a", "1", "b", "2"], 2)
        .await;
    ctx.expect_bulk(S, "hget", &mut c, &["HGET", &h, "a"], "1").await;
    ctx.expect_null_bulk(S, "hget_miss", &mut c, &["HGET", &h, "z"]).await;
    ctx.expect_int(S, "hexists", &mut c, &["HEXISTS", &h, "a"], 1).await;
    ctx.expect_int(S, "hlen", &mut c, &["HLEN", &h], 2).await;
    ctx.expect_int(S, "hsetnx_fail", &mut c, &["HSETNX", &h, "a", "9"], 0)
        .await;
    ctx.expect_int(S, "hsetnx_ok", &mut c, &["HSETNX", &h, "c", "3"], 1)
        .await;
    ctx.expect_int(S, "hincrby", &mut c, &["HINCRBY", &h, "a", "4"], 5)
        .await;
    ctx.expect_bulk(
        S,
        "hincrbyfloat",
        &mut c,
        &["HINCRBYFLOAT", &h, "a", "0.5"],
        "5.5",
    )
    .await;
    ctx.expect_int(S, "hstrlen", &mut c, &["HSTRLEN", &h, "b"], 1).await;

    ctx.expect_eq(
        S,
        "hmget",
        &mut c,
        &["HMGET", &h, "a", "missing", "b"],
        arr(vec![bulk("5.5"), RespValue::null_bulk(), bulk("2")]),
    )
    .await;

    ctx.expect_ok(S, "hmset", &mut c, &["HMSET", &h, "d", "4", "e", "5"])
        .await;
    ctx.expect_int(S, "hlen_after_hmset", &mut c, &["HLEN", &h], 5).await;

    ctx.expect_pred(S, "hkeys", &mut c, &["HKEYS", &h], |v| match v {
        RespValue::Array(Some(items)) if items.len() == 5 => Ok(()),
        _ => Err("HKEYS doit retourner 5 champs".into()),
    })
    .await;
    ctx.expect_pred(S, "hvals", &mut c, &["HVALS", &h], |v| match v {
        RespValue::Array(Some(items)) if items.len() == 5 => Ok(()),
        _ => Err("HVALS doit retourner 5 valeurs".into()),
    })
    .await;
    ctx.expect_pred(S, "hgetall", &mut c, &["HGETALL", &h], |v| match v {
        RespValue::Array(Some(items)) if items.len() == 10 => Ok(()),
        _ => Err("HGETALL flat array len=2*fields".into()),
    })
    .await;

    ctx.expect_pred(S, "hrandfield", &mut c, &["HRANDFIELD", &h], |v| match v {
        RespValue::BulkString(Some(_)) => Ok(()),
        _ => Err("HRANDFIELD sans count → bulk".into()),
    })
    .await;
    ctx.expect_pred(
        S,
        "hrandfield_count",
        &mut c,
        &["HRANDFIELD", &h, "2"],
        |v| match v {
            RespValue::Array(Some(items)) if items.len() == 2 => Ok(()),
            _ => Err("HRANDFIELD count → 2 éléments".into()),
        },
    )
    .await;

    ctx.expect_int(S, "hdel", &mut c, &["HDEL", &h, "e", "nope"], 1).await;

    // HSCAN
    ctx.expect_pred(S, "hscan", &mut c, &["HSCAN", &h, "0"], |v| match v {
        RespValue::Array(Some(items)) if items.len() == 2 => Ok(()),
        _ => Err("HSCAN → [cursor, fields]".into()),
    })
    .await;

    // Hash field TTL (Redis 7.4+)
    let hf = ctx.unique("hfttl");
    ctx.expect_int(S, "hset_ttl", &mut c, &["HSET", &hf, "f", "v"], 1)
        .await;
    ctx.expect_eq(
        S,
        "hexpire",
        &mut c,
        &["HEXPIRE", &hf, "60", "FIELDS", "1", "f"],
        arr(vec![int(1)]),
    )
    .await;
    // Redis : TTL entier ≤60 juste après HEXPIRE 60 (peut tick à 59).
    ctx.expect_pred(S, "httl_bound", &mut c, &["HTTL", &hf, "FIELDS", "1", "f"], |v| {
        match v {
            RespValue::Array(Some(items)) => match items.first() {
                Some(RespValue::Integer(n)) if *n > 0 && *n <= 60 => Ok(()),
                other => Err(format!("HTTL field attendu (0,60], got {other:?}")),
            },
            _ => Err("HTTL → array".into()),
        }
    })
    .await;

    ctx.expect_eq(
        S,
        "hpersist",
        &mut c,
        &["HPERSIST", &hf, "FIELDS", "1", "f"],
        arr(vec![int(1)]),
    )
    .await;

    // HGETEX / HSETEX / HGETDEL (Redis 8)
    let h8 = ctx.unique("h8");
    ctx.expect_eq(
        S,
        "hsetex",
        &mut c,
        &["HSETEX", &h8, "FIELDS", "1", "x", "1"],
        int(1),
    )
    .await;
    ctx.expect_eq(
        S,
        "hgetex",
        &mut c,
        &["HGETEX", &h8, "EX", "20", "FIELDS", "1", "x"],
        arr(vec![bulk("1")]),
    )
    .await;
    ctx.expect_eq(
        S,
        "hgetdel",
        &mut c,
        &["HGETDEL", &h8, "FIELDS", "1", "x"],
        arr(vec![bulk("1")]),
    )
    .await;
    ctx.expect_int(S, "hgetdel_gone", &mut c, &["HLEN", &h8], 0).await;
}
