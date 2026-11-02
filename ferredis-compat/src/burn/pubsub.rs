//! Pub/Sub + sharded pubsub — sémantique Redis OSS.

use super::harness::{bulk, BurnCtx};
use ferredis_core::resp::RespValue;
use ferredis_core::server::read_resp;
use std::time::Duration;
use tokio::time::timeout;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "pubsub";
    let mut sub = ctx.connect().await;
    let mut pubc = ctx.connect().await;
    let ch = ctx.unique("ch");

    ctx.expect_pred(S, "subscribe", &mut sub, &["SUBSCRIBE", &ch], |v| match v {
        RespValue::Array(Some(items))
            if items.len() == 3
                && matches!(&items[0], RespValue::BulkString(Some(b)) if b.as_ref() == b"subscribe") =>
        {
            Ok(())
        }
        _ => Err("SUBSCRIBE → [subscribe, channel, count]".into()),
    })
    .await;

    ctx.expect_int(S, "publish", &mut pubc, &["PUBLISH", &ch, "hello"], 1)
        .await;

    let t0 = std::time::Instant::now();
    match timeout(Duration::from_secs(2), read_resp(&mut sub)).await {
        Ok(Ok(RespValue::Array(Some(items))))
            if items.len() == 3
                && items[0] == bulk("message")
                && items[2] == bulk("hello") =>
        {
            ctx.check(S, "message_delivery", t0.elapsed(), true, "");
        }
        Ok(Ok(other)) => ctx.check(
            S,
            "message_delivery",
            t0.elapsed(),
            false,
            format!("message Redis = [message, ch, hello], got {other:?}"),
        ),
        Ok(Err(e)) => ctx.err_case(S, "message_delivery", t0.elapsed(), e.to_string()),
        Err(_) => ctx.check(
            S,
            "message_delivery",
            t0.elapsed(),
            false,
            "timeout attente message PUBLISH",
        ),
    }

    // PSUBSCRIBE
    let mut psub = ctx.connect().await;
    let pat = format!("{}:*", ctx.unique("pat"));
    ctx.expect_pred(
        S,
        "psubscribe",
        &mut psub,
        &["PSUBSCRIBE", &pat],
        |v| match v {
            RespValue::Array(Some(items)) if items.len() == 3 => Ok(()),
            _ => Err("PSUBSCRIBE confirm array".into()),
        },
    )
    .await;
    let base = pat.trim_end_matches(":*");
    let pch = format!("{base}:x");
    ctx.expect_int(S, "publish_pattern", &mut pubc, &["PUBLISH", &pch, "p"], 1)
        .await;
    let t0 = std::time::Instant::now();
    match timeout(Duration::from_secs(2), read_resp(&mut psub)).await {
        Ok(Ok(RespValue::Array(Some(items))))
            if items.len() == 4
                && matches!(&items[0], RespValue::BulkString(Some(b)) if b.as_ref() == b"pmessage") =>
        {
            ctx.check(S, "pmessage_delivery", t0.elapsed(), true, "");
        }
        Ok(Ok(other)) => ctx.check(
            S,
            "pmessage_delivery",
            t0.elapsed(),
            false,
            format!("pmessage Redis 4-éléments, got {other:?}"),
        ),
        Ok(Err(e)) => ctx.err_case(S, "pmessage_delivery", t0.elapsed(), e.to_string()),
        Err(_) => ctx.check(
            S,
            "pmessage_delivery",
            t0.elapsed(),
            false,
            "timeout pmessage",
        ),
    }

    // PUBSUB CHANNELS / NUMSUB
    ctx.expect_pred(
        S,
        "pubsub_channels",
        &mut pubc,
        &["PUBSUB", "CHANNELS"],
        |v| match v {
            RespValue::Array(Some(_)) => Ok(()),
            _ => Err("PUBSUB CHANNELS → array".into()),
        },
    )
    .await;
    ctx.expect_pred(
        S,
        "pubsub_numsub",
        &mut pubc,
        &["PUBSUB", "NUMSUB", &ch],
        |v| match v {
            RespValue::Array(Some(items)) if items.len() == 2 => Ok(()),
            _ => Err("PUBSUB NUMSUB → [ch, count]".into()),
        },
    )
    .await;

    // Sharded pubsub (Redis 7+)
    let mut ssub = ctx.connect().await;
    let sch = ctx.unique("sch");
    ctx.expect_pred(
        S,
        "ssubscribe",
        &mut ssub,
        &["SSUBSCRIBE", &sch],
        |v| match v {
            RespValue::Array(Some(items)) if items.len() == 3 => Ok(()),
            RespValue::Error(e) => Err(format!(
                "SSUBSCRIBE Redis 7+ requis: {}",
                String::from_utf8_lossy(e)
            )),
            other => Err(format!("SSUBSCRIBE confirm, got {other:?}")),
        },
    )
    .await;
    ctx.expect_int(S, "spublish", &mut pubc, &["SPUBLISH", &sch, "shard"], 1)
        .await;
}
