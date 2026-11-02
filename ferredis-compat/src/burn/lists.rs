//! Lists + bloquants — sémantique Redis OSS.

use super::harness::{arr, bulk, BurnCtx};
use ferredis_core::resp::RespValue;
use ferredis_core::server::read_resp;
use std::time::Duration;
use tokio::time::timeout;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "lists";
    let mut c = ctx.connect().await;
    let l = ctx.unique("list");

    ctx.expect_int(S, "rpush", &mut c, &["RPUSH", &l, "a", "b", "c"], 3)
        .await;
    ctx.expect_int(S, "lpush", &mut c, &["LPUSH", &l, "z"], 4).await;
    ctx.expect_int(S, "llen", &mut c, &["LLEN", &l], 4).await;
    ctx.expect_eq(
        S,
        "lrange",
        &mut c,
        &["LRANGE", &l, "0", "-1"],
        arr(vec![bulk("z"), bulk("a"), bulk("b"), bulk("c")]),
    )
    .await;
    ctx.expect_bulk(S, "lindex", &mut c, &["LINDEX", &l, "1"], "a").await;
    ctx.expect_ok(S, "lset", &mut c, &["LSET", &l, "1", "A"]).await;
    ctx.expect_int(S, "linsert", &mut c, &["LINSERT", &l, "BEFORE", "b", "ab"], 5)
        .await;
    ctx.expect_pred(S, "lpos", &mut c, &["LPOS", &l, "b"], |v| match v {
        RespValue::Integer(n) if *n >= 0 => Ok(()),
        _ => Err("LPOS → index entier".into()),
    })
    .await;

    ctx.expect_bulk(S, "lpop", &mut c, &["LPOP", &l], "z").await;
    ctx.expect_bulk(S, "rpop", &mut c, &["RPOP", &l], "c").await;
    ctx.expect_eq(
        S,
        "lpop_count",
        &mut c,
        &["LPOP", &l, "2"],
        arr(vec![bulk("A"), bulk("ab")]),
    )
    .await;

    let src = ctx.unique("src");
    let dst = ctx.unique("dst");
    ctx.expect_int(S, "prep_move", &mut c, &["RPUSH", &src, "1", "2"], 2)
        .await;
    ctx.expect_bulk(
        S,
        "lmove",
        &mut c,
        &["LMOVE", &src, &dst, "RIGHT", "LEFT"],
        "2",
    )
    .await;
    ctx.expect_bulk(
        S,
        "rpoplpush",
        &mut c,
        &["RPOPLPUSH", &src, &dst],
        "1",
    )
    .await;
    ctx.expect_eq(
        S,
        "dst_order",
        &mut c,
        &["LRANGE", &dst, "0", "-1"],
        arr(vec![bulk("1"), bulk("2")]),
    )
    .await;

    ctx.expect_int(S, "lpushx_miss", &mut c, &["LPUSHX", &ctx.unique("nox"), "a"], 0)
        .await;
    ctx.expect_int(S, "rpushx_hit", &mut c, &["RPUSHX", &dst, "3"], 3)
        .await;

    ctx.expect_ok(S, "ltrim", &mut c, &["LTRIM", &dst, "0", "1"]).await;
    ctx.expect_int(S, "lrem", &mut c, &["LREM", &dst, "1", "1"], 1).await;

    // LMPOP
    let m1 = ctx.unique("m1");
    let m2 = ctx.unique("m2");
    ctx.expect_int(S, "prep_lmpop", &mut c, &["RPUSH", &m1, "x"], 1).await;
    ctx.expect_eq(
        S,
        "lmpop",
        &mut c,
        &["LMPOP", "2", &m1, &m2, "LEFT"],
        arr(vec![bulk(&m1), arr(vec![bulk("x")])]),
    )
    .await;

    // BLPOP timeout null
    ctx.expect_null_array(
        S,
        "blpop_timeout",
        &mut c,
        &["BLPOP", &ctx.unique("empty"), "0.1"],
    )
    .await;

    // BLPOP wakeup
    let blk = ctx.unique("blk");
    let addr = ctx.addr;
    let blk_wait = blk.clone();
    let waiter = tokio::spawn(async move {
        let mut w = tokio::net::TcpStream::connect(addr).await.unwrap();
        ferredis_core::server::write_command(&mut w, &[b"BRPOP", blk_wait.as_bytes(), b"2"])
            .await
            .unwrap();
        timeout(Duration::from_secs(3), read_resp(&mut w))
            .await
            .expect("timeout")
            .expect("resp")
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    ctx.expect_int(S, "wake_rpush", &mut c, &["RPUSH", &blk, "msg"], 1)
        .await;
    let got = waiter.await.unwrap();
    let t0 = std::time::Instant::now();
    let ok = matches!(
        &got,
        RespValue::Array(Some(items))
            if items.len() == 2 && items[1] == bulk("msg")
    );
    ctx.check(
        S,
        "brpop_wakeup",
        t0.elapsed(),
        ok,
        format!("BRPOP wakeup Redis = [key, msg], got {got:?}"),
    );
}
