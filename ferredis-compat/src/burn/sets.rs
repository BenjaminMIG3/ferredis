//! Sets — sémantique Redis OSS.

use super::harness::{arr, BurnCtx};
use ferredis_core::resp::RespValue;
use std::collections::HashSet;

fn set_of(v: &RespValue) -> Option<HashSet<String>> {
    match v {
        RespValue::Array(Some(items)) => {
            let mut s = HashSet::new();
            for i in items {
                match i {
                    RespValue::BulkString(Some(b)) => {
                        s.insert(String::from_utf8_lossy(b).into_owned());
                    }
                    _ => return None,
                }
            }
            Some(s)
        }
        _ => None,
    }
}

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "sets";
    let mut c = ctx.connect().await;
    let a = ctx.unique("sa");
    let b = ctx.unique("sb");

    ctx.expect_int(S, "sadd_a", &mut c, &["SADD", &a, "1", "2", "3"], 3)
        .await;
    ctx.expect_int(S, "sadd_dup", &mut c, &["SADD", &a, "2"], 0).await;
    ctx.expect_int(S, "scard", &mut c, &["SCARD", &a], 3).await;
    ctx.expect_int(S, "sismember", &mut c, &["SISMEMBER", &a, "2"], 1)
        .await;
    ctx.expect_eq(
        S,
        "smismember",
        &mut c,
        &["SMISMEMBER", &a, "1", "9", "3"],
        arr(vec![
            ferredis_core::resp::RespValue::Integer(1),
            ferredis_core::resp::RespValue::Integer(0),
            ferredis_core::resp::RespValue::Integer(1),
        ]),
    )
    .await;

    ctx.expect_int(S, "sadd_b", &mut c, &["SADD", &b, "2", "3", "4"], 3)
        .await;

    ctx.expect_pred(S, "sinter", &mut c, &["SINTER", &a, &b], |v| {
        let s = set_of(v).ok_or("SINTER array")?;
        if s == HashSet::from(["2".into(), "3".into()]) {
            Ok(())
        } else {
            Err(format!("SINTER Redis={{2,3}}, got {s:?}"))
        }
    })
    .await;
    ctx.expect_int(S, "sintercard", &mut c, &["SINTERCARD", "2", &a, &b], 2)
        .await;
    ctx.expect_pred(S, "sunion", &mut c, &["SUNION", &a, &b], |v| {
        let s = set_of(v).ok_or("SUNION array")?;
        if s.len() == 4 {
            Ok(())
        } else {
            Err(format!("SUNION card=4, got {s:?}"))
        }
    })
    .await;
    ctx.expect_pred(S, "sdiff", &mut c, &["SDIFF", &a, &b], |v| {
        let s = set_of(v).ok_or("SDIFF array")?;
        if s == HashSet::from(["1".into()]) {
            Ok(())
        } else {
            Err(format!("SDIFF Redis={{1}}, got {s:?}"))
        }
    })
    .await;

    let store = ctx.unique("store");
    ctx.expect_int(
        S,
        "sinterstore",
        &mut c,
        &["SINTERSTORE", &store, &a, &b],
        2,
    )
    .await;
    ctx.expect_int(S, "sunionstore", &mut c, &["SUNIONSTORE", &store, &a, &b], 4)
        .await;
    ctx.expect_int(S, "sdiffstore", &mut c, &["SDIFFSTORE", &store, &a, &b], 1)
        .await;

    ctx.expect_int(S, "smove", &mut c, &["SMOVE", &a, &b, "1"], 1).await;
    ctx.expect_int(S, "srem", &mut c, &["SREM", &b, "4"], 1).await;

    ctx.expect_pred(S, "spop", &mut c, &["SPOP", &b], |v| match v {
        RespValue::BulkString(Some(_)) => Ok(()),
        _ => Err("SPOP → bulk member".into()),
    })
    .await;
    ctx.expect_pred(S, "srandmember", &mut c, &["SRANDMEMBER", &a], |v| match v {
        RespValue::BulkString(Some(_)) | RespValue::BulkString(None) => Ok(()),
        _ => Err("SRANDMEMBER → bulk|null".into()),
    })
    .await;

    ctx.expect_pred(S, "sscan", &mut c, &["SSCAN", &a, "0"], |v| match v {
        RespValue::Array(Some(items)) if items.len() == 2 => Ok(()),
        _ => Err("SSCAN → [cursor, members]".into()),
    })
    .await;
}
