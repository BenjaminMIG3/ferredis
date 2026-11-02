//! Strings — attentes issues des exemples redis.io / sémantique Redis OSS.

use super::harness::{arr, bulk, BurnCtx};
use ferredis_core::resp::RespValue;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "strings";
    let mut c = ctx.connect().await;
    let k = ctx.unique("str");

    ctx.expect_ok(S, "set", &mut c, &["SET", &k, "hello"]).await;
    ctx.expect_bulk(S, "get", &mut c, &["GET", &k], "hello").await;
    ctx.expect_int(S, "strlen", &mut c, &["STRLEN", &k], 5).await;
    ctx.expect_int(S, "append", &mut c, &["APPEND", &k, " world"], 11)
        .await;
    ctx.expect_bulk(S, "get_after_append", &mut c, &["GET", &k], "hello world")
        .await;
    ctx.expect_bulk(S, "getrange", &mut c, &["GETRANGE", &k, "0", "4"], "hello")
        .await;
    ctx.expect_bulk(S, "substr", &mut c, &["SUBSTR", &k, "6", "-1"], "world")
        .await;

    // SET options Redis
    let k2 = ctx.unique("str");
    ctx.expect_ok(S, "set_nx", &mut c, &["SET", &k2, "a", "NX"]).await;
    ctx.expect_null_bulk(S, "set_nx_fail", &mut c, &["SET", &k2, "b", "NX"])
        .await;
    ctx.expect_ok(S, "set_xx", &mut c, &["SET", &k2, "b", "XX"]).await;
    ctx.expect_bulk(S, "getset", &mut c, &["GETSET", &k2, "c"], "b").await;

    let k3 = ctx.unique("str");
    ctx.expect_ok(S, "set_base", &mut c, &["SET", &k3, "Hello World"]).await;
    ctx.expect_int(S, "setrange_len", &mut c, &["SETRANGE", &k3, "6", "Redis"], 11)
        .await;
    ctx.expect_bulk(S, "setrange_val", &mut c, &["GET", &k3], "Hello Redis")
        .await;

    // Compteurs
    let n = ctx.unique("num");
    ctx.expect_int(S, "incr", &mut c, &["INCR", &n], 1).await;
    ctx.expect_int(S, "incrby", &mut c, &["INCRBY", &n, "4"], 5).await;
    ctx.expect_int(S, "decr", &mut c, &["DECR", &n], 4).await;
    ctx.expect_int(S, "decrby", &mut c, &["DECRBY", &n, "2"], 2).await;
    ctx.expect_bulk(S, "incrbyfloat", &mut c, &["INCRBYFLOAT", &n, "0.5"], "2.5")
        .await;

    // MSET / MGET / MSETNX
    let a = ctx.unique("a");
    let b = ctx.unique("b");
    ctx.expect_ok(S, "mset", &mut c, &["MSET", &a, "1", &b, "2"]).await;
    ctx.expect_eq(
        S,
        "mget",
        &mut c,
        &["MGET", &a, &b, "nope"],
        arr(vec![bulk("1"), bulk("2"), RespValue::null_bulk()]),
    )
    .await;
    ctx.expect_int(S, "msetnx_fail", &mut c, &["MSETNX", &a, "x", &b, "y"], 0)
        .await;
    let c1 = ctx.unique("c1");
    let c2 = ctx.unique("c2");
    ctx.expect_int(S, "msetnx_ok", &mut c, &["MSETNX", &c1, "x", &c2, "y"], 1)
        .await;

    // TTL string helpers
    let t = ctx.unique("ttl");
    ctx.expect_ok(S, "setex", &mut c, &["SETEX", &t, "10", "v"]).await;
    ctx.expect_pred(S, "ttl_setex", &mut c, &["TTL", &t], |v| match v {
        RespValue::Integer(n) if *n > 0 && *n <= 10 => Ok(()),
        _ => Err("TTL après SETEX doit être dans (0,10]".into()),
    })
    .await;
    let t2 = ctx.unique("pttl");
    ctx.expect_ok(S, "psetex", &mut c, &["PSETEX", &t2, "5000", "v"])
        .await;
    ctx.expect_pred(S, "pttl_psetex", &mut c, &["PTTL", &t2], |v| match v {
        RespValue::Integer(n) if *n > 0 && *n <= 5000 => Ok(()),
        _ => Err("PTTL après PSETEX doit être dans (0,5000]".into()),
    })
    .await;

    let g = ctx.unique("getex");
    ctx.expect_ok(S, "set_getex", &mut c, &["SET", &g, "z"]).await;
    ctx.expect_bulk(S, "getex", &mut c, &["GETEX", &g, "EX", "30"], "z")
        .await;
    ctx.expect_pred(S, "ttl_getex", &mut c, &["TTL", &g], |v| match v {
        RespValue::Integer(n) if *n > 0 => Ok(()),
        _ => Err("GETEX EX doit poser un TTL".into()),
    })
    .await;
    ctx.expect_bulk(S, "getdel", &mut c, &["GETDEL", &g], "z").await;
    ctx.expect_null_bulk(S, "getdel_gone", &mut c, &["GET", &g]).await;

    ctx.expect_int(S, "setnx_ok", &mut c, &["SETNX", &ctx.unique("nx"), "1"], 1)
        .await;

    // LCS (Redis 7+)
    let l1 = ctx.unique("lcs1");
    let l2 = ctx.unique("lcs2");
    ctx.expect_ok(S, "lcs_prep1", &mut c, &["SET", &l1, "ohmytext"]).await;
    ctx.expect_ok(S, "lcs_prep2", &mut c, &["SET", &l2, "mynewtext"]).await;
    ctx.expect_bulk(S, "lcs", &mut c, &["LCS", &l1, &l2], "mytext").await;
    ctx.expect_int(S, "lcs_len", &mut c, &["LCS", &l1, &l2, "LEN"], 6)
        .await;

    // Type error Redis
    let list = ctx.unique("list");
    ctx.expect_int(S, "prep_wrongtype", &mut c, &["LPUSH", &list, "x"], 1)
        .await;
    ctx.expect_error_contains(S, "get_wrongtype", &mut c, &["GET", &list], "WRONGTYPE")
        .await;
}
