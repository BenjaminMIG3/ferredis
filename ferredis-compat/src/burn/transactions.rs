//! Transactions MULTI/EXEC/DISCARD/WATCH — sémantique Redis OSS.

use super::harness::{arr, BurnCtx};
use ferredis_core::resp::RespValue;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "transactions";
    let mut a = ctx.connect().await;
    let mut b = ctx.connect().await;

    let k = ctx.unique("tx");
    ctx.expect_ok(S, "multi", &mut a, &["MULTI"]).await;
    // Redis : commandes en MULTI → +QUEUED
    ctx.expect_pred(S, "queued_set", &mut a, &["SET", &k, "1"], |v| match v {
        RespValue::SimpleString(s) if s.as_ref() == b"QUEUED" => Ok(()),
        _ => Err("MULTI → QUEUED".into()),
    })
    .await;
    ctx.expect_pred(S, "queued_incr", &mut a, &["INCR", &k], |v| match v {
        RespValue::SimpleString(s) if s.as_ref() == b"QUEUED" => Ok(()),
        _ => Err("MULTI → QUEUED".into()),
    })
    .await;
    ctx.expect_eq(
        S,
        "exec",
        &mut a,
        &["EXEC"],
        arr(vec![
            ferredis_core::resp::RespValue::ok(),
            ferredis_core::resp::RespValue::Integer(2),
        ]),
    )
    .await;
    ctx.expect_bulk(S, "exec_result", &mut a, &["GET", &k], "2").await;

    // DISCARD
    ctx.expect_ok(S, "multi2", &mut a, &["MULTI"]).await;
    let _ = ctx.cmd(&mut a, &["SET", &k, "discard-me"]).await;
    ctx.expect_ok(S, "discard", &mut a, &["DISCARD"]).await;
    ctx.expect_bulk(S, "discard_keeps", &mut a, &["GET", &k], "2").await;

    // WATCH conflict → EXEC null
    let w = ctx.unique("watch");
    ctx.expect_ok(S, "watch_prep", &mut a, &["SET", &w, "1"]).await;
    ctx.expect_ok(S, "watch", &mut a, &["WATCH", &w]).await;
    ctx.expect_ok(S, "other_write", &mut b, &["SET", &w, "2"]).await;
    ctx.expect_ok(S, "multi_watch", &mut a, &["MULTI"]).await;
    let _ = ctx.cmd(&mut a, &["SET", &w, "3"]).await;
    ctx.expect_null_array(S, "exec_aborted", &mut a, &["EXEC"]).await;
    ctx.expect_bulk(S, "watch_value", &mut a, &["GET", &w], "2").await;

    // WATCH clean
    ctx.expect_ok(S, "watch2", &mut a, &["WATCH", &w]).await;
    ctx.expect_ok(S, "multi_clean", &mut a, &["MULTI"]).await;
    let _ = ctx.cmd(&mut a, &["INCR", &w]).await;
    ctx.expect_eq(
        S,
        "exec_clean",
        &mut a,
        &["EXEC"],
        arr(vec![ferredis_core::resp::RespValue::Integer(3)]),
    )
    .await;

    ctx.expect_ok(S, "unwatch", &mut a, &["UNWATCH"]).await;

    // EXEC sans MULTI
    ctx.expect_error_contains(S, "exec_without_multi", &mut a, &["EXEC"], "ERR")
        .await;
}
