//! Sorted sets — sémantique Redis OSS (scores, lex, aggregations).

use super::harness::{arr, bulk, BurnCtx};
use ferredis_core::resp::RespValue;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "zsets";
    let mut c = ctx.connect().await;
    let z = ctx.unique("z");

    ctx.expect_int(
        S,
        "zadd",
        &mut c,
        &["ZADD", &z, "1", "one", "2", "two", "3", "three"],
        3,
    )
    .await;
    ctx.expect_int(S, "zcard", &mut c, &["ZCARD", &z], 3).await;
    ctx.expect_bulk(S, "zscore", &mut c, &["ZSCORE", &z, "two"], "2").await;
    ctx.expect_int(S, "zrank", &mut c, &["ZRANK", &z, "two"], 1).await;
    ctx.expect_int(S, "zrevrank", &mut c, &["ZREVRANK", &z, "two"], 1)
        .await;
    ctx.expect_int(S, "zcount", &mut c, &["ZCOUNT", &z, "1", "2"], 2)
        .await;
    // Avant incrément : ordre lexique des scores 1,2,3
    ctx.expect_eq(
        S,
        "zrange",
        &mut c,
        &["ZRANGE", &z, "0", "-1"],
        arr(vec![bulk("one"), bulk("two"), bulk("three")]),
    )
    .await;
    ctx.expect_bulk(S, "zincrby", &mut c, &["ZINCRBY", &z, "2", "two"], "4")
        .await;
    // Après ZINCRBY two=4 : one(1), three(3), two(4)
    ctx.expect_eq(
        S,
        "zrange_after_incr",
        &mut c,
        &["ZRANGE", &z, "0", "-1"],
        arr(vec![bulk("one"), bulk("three"), bulk("two")]),
    )
    .await;
    ctx.expect_eq(
        S,
        "zrange_withscores",
        &mut c,
        &["ZRANGE", &z, "0", "-1", "WITHSCORES"],
        arr(vec![
            bulk("one"),
            bulk("1"),
            bulk("three"),
            bulk("3"),
            bulk("two"),
            bulk("4"),
        ]),
    )
    .await;

    ctx.expect_eq(
        S,
        "zrevrange",
        &mut c,
        &["ZREVRANGE", &z, "0", "1"],
        arr(vec![bulk("two"), bulk("three")]),
    )
    .await;
    ctx.expect_eq(
        S,
        "zrangebyscore",
        &mut c,
        &["ZRANGEBYSCORE", &z, "1", "3"],
        arr(vec![bulk("one"), bulk("three")]),
    )
    .await;

    ctx.expect_eq(
        S,
        "zmscore",
        &mut c,
        &["ZMSCORE", &z, "one", "missing", "two"],
        arr(vec![bulk("1"), RespValue::null_bulk(), bulk("4")]),
    )
    .await;

    // Lex
    let zl = ctx.unique("zl");
    ctx.expect_int(
        S,
        "zadd_lex",
        &mut c,
        &["ZADD", &zl, "0", "a", "0", "b", "0", "c", "0", "d", "0", "e"],
        5,
    )
    .await;
    ctx.expect_int(S, "zlexcount", &mut c, &["ZLEXCOUNT", &zl, "[b", "[d"], 3)
        .await;
    ctx.expect_eq(
        S,
        "zrangebylex",
        &mut c,
        &["ZRANGEBYLEX", &zl, "[b", "[d"],
        arr(vec![bulk("b"), bulk("c"), bulk("d")]),
    )
    .await;

    // Set ops
    let z1 = ctx.unique("z1");
    let z2 = ctx.unique("z2");
    ctx.expect_int(S, "prep_z1", &mut c, &["ZADD", &z1, "1", "a", "2", "b"], 2)
        .await;
    ctx.expect_int(S, "prep_z2", &mut c, &["ZADD", &z2, "2", "b", "3", "c"], 2)
        .await;
    ctx.expect_pred(S, "zinter", &mut c, &["ZINTER", "2", &z1, &z2], |v| match v {
        RespValue::Array(Some(items)) if items.len() == 1 => Ok(()),
        _ => Err("ZINTER → {b}".into()),
    })
    .await;
    ctx.expect_int(S, "zintercard", &mut c, &["ZINTERCARD", "2", &z1, &z2], 1)
        .await;
    ctx.expect_pred(S, "zunion", &mut c, &["ZUNION", "2", &z1, &z2], |v| match v {
        RespValue::Array(Some(items)) if items.len() == 3 => Ok(()),
        _ => Err("ZUNION card=3".into()),
    })
    .await;
    ctx.expect_pred(S, "zdiff", &mut c, &["ZDIFF", "2", &z1, &z2], |v| match v {
        RespValue::Array(Some(items)) if items.len() == 1 => Ok(()),
        _ => Err("ZDIFF → {a}".into()),
    })
    .await;

    let dest = ctx.unique("zd");
    ctx.expect_int(
        S,
        "zinterstore",
        &mut c,
        &["ZINTERSTORE", &dest, "2", &z1, &z2],
        1,
    )
    .await;
    ctx.expect_int(
        S,
        "zunionstore",
        &mut c,
        &["ZUNIONSTORE", &dest, "2", &z1, &z2],
        3,
    )
    .await;
    ctx.expect_int(S, "zdiffstore", &mut c, &["ZDIFFSTORE", &dest, "2", &z1, &z2], 1)
        .await;

    ctx.expect_eq(
        S,
        "zpopmin",
        &mut c,
        &["ZPOPMIN", &z],
        arr(vec![bulk("one"), bulk("1")]),
    )
    .await;
    ctx.expect_eq(
        S,
        "zpopmax",
        &mut c,
        &["ZPOPMAX", &z],
        arr(vec![bulk("two"), bulk("4")]),
    )
    .await;

    ctx.expect_int(S, "zrem", &mut c, &["ZREM", &zl, "e"], 1).await;
    ctx.expect_int(
        S,
        "zremrangebyrank",
        &mut c,
        &["ZREMRANGEBYRANK", &zl, "0", "0"],
        1,
    )
    .await;
    ctx.expect_int(
        S,
        "zremrangebylex",
        &mut c,
        &["ZREMRANGEBYLEX", &zl, "[c", "[c"],
        1,
    )
    .await;

    ctx.expect_null_array(
        S,
        "bzpopmin_timeout",
        &mut c,
        &["BZPOPMIN", &ctx.unique("emptyz"), "0.1"],
    )
    .await;

    ctx.expect_pred(S, "zscan", &mut c, &["ZSCAN", &z1, "0"], |v| match v {
        RespValue::Array(Some(items)) if items.len() == 2 => Ok(()),
        _ => Err("ZSCAN → [cursor, pairs]".into()),
    })
    .await;

    // ZADD options NX/XX/GT/LT/CH/INCR
    let zo = ctx.unique("zo");
    ctx.expect_int(S, "zadd_nx", &mut c, &["ZADD", &zo, "NX", "1", "m"], 1)
        .await;
    ctx.expect_int(S, "zadd_nx_fail", &mut c, &["ZADD", &zo, "NX", "5", "m"], 0)
        .await;
    ctx.expect_int(S, "zadd_xx_ch", &mut c, &["ZADD", &zo, "XX", "CH", "2", "m"], 1)
        .await;
    ctx.expect_bulk(S, "zadd_incr", &mut c, &["ZADD", &zo, "INCR", "1", "m"], "3")
        .await;
}
