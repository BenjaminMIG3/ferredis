//! Tests TCP Streams Redis 7.2 (groups, BLOCK, claim, pending, info, trim).

use crate::tests::{cmd, connect, dbg};
use bytes::Bytes;
use ferredis_core::resp::RespValue;
use ferredis_core::server::{read_resp, spawn_test_server, write_command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

fn b(s: &str) -> RespValue {
    RespValue::bulk(Bytes::from(s.to_string()))
}

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

fn arr(items: Vec<RespValue>) -> RespValue {
    RespValue::array(items)
}

fn error(v: &RespValue) -> String {
    match v {
        RespValue::Error(e) => String::from_utf8_lossy(e).into_owned(),
        other => panic!("erreur attendue, reçu {other:?}"),
    }
}

async fn client() -> TcpStream {
    connect(spawn_test_server().await).await
}

fn entry(id: &str, fields: &[(&str, &str)]) -> RespValue {
    let mut flat = Vec::new();
    for (k, v) in fields {
        flat.push(b(k));
        flat.push(b(v));
    }
    arr(vec![b(id), arr(flat)])
}

#[tokio::test]
async fn xadd_xrange_xlen_xtrim_xdel() {
    let mut c = client().await;
    assert_eq!(
        cmd(&mut c, &["XADD", "s", "1-0", "a", "1"]).await,
        b("1-0")
    );
    assert_eq!(
        cmd(&mut c, &["XADD", "s", "1-1", "a", "2"]).await,
        b("1-1")
    );
    assert_eq!(cmd(&mut c, &["XLEN", "s"]).await, int(2));
    assert_eq!(
        cmd(&mut c, &["XRANGE", "s", "-", "+"]).await,
        arr(vec![entry("1-0", &[("a", "1")]), entry("1-1", &[("a", "2")])])
    );
    assert_eq!(
        cmd(&mut c, &["XREVRANGE", "s", "+", "-", "COUNT", "1"]).await,
        arr(vec![entry("1-1", &[("a", "2")])])
    );
    assert_eq!(cmd(&mut c, &["XTRIM", "s", "MAXLEN", "=", "1"]).await, int(1));
    assert_eq!(cmd(&mut c, &["XLEN", "s"]).await, int(1));
    assert_eq!(cmd(&mut c, &["XDEL", "s", "1-1"]).await, int(1));
    assert_eq!(cmd(&mut c, &["XLEN", "s"]).await, int(0));
    let e0 = error(&cmd(&mut c, &["XADD", "fresh", "0-0", "a", "x"]).await);
    assert!(e0.contains("greater than 0-0"), "{e0}");
    assert_eq!(
        cmd(&mut c, &["XADD", "nope", "NOMKSTREAM", "*", "f", "v"]).await,
        RespValue::null_bulk()
    );
}

#[tokio::test]
async fn xread_block_woken_by_xadd() {
    let addr = spawn_test_server().await;
    let mut reader = connect(addr).await;
    let mut writer = connect(addr).await;
    cmd(&mut writer, &["XADD", "blk", "1-0", "f", "v0"]).await;

    let blocked = tokio::spawn(async move {
        write_command(
            &mut reader,
            &[
                b"XREAD",
                b"BLOCK",
                b"5000",
                b"STREAMS",
                b"blk",
                b"$",
            ],
        )
        .await
        .unwrap();
        read_resp(&mut reader).await.unwrap()
    });

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(matches!(
        cmd(&mut writer, &["XADD", "blk", "*", "f", "v1"]).await,
        RespValue::BulkString(Some(_))
    ));
    let reply = timeout(Duration::from_secs(3), blocked)
        .await
        .expect("timeout XREAD BLOCK")
        .unwrap();
    let s = dbg(&reply);
    assert!(s.contains("v1"), "{s}");
}

#[tokio::test]
async fn consumer_groups_ack_pending_claim_autoclaim_info() {
    let mut c = client().await;
    cmd(&mut c, &["XADD", "mystream", "1-0", "n", "1"]).await;
    cmd(&mut c, &["XADD", "mystream", "1-1", "n", "2"]).await;
    cmd(&mut c, &["XADD", "mystream", "1-2", "n", "3"]).await;
    assert_eq!(
        cmd(&mut c, &["XGROUP", "CREATE", "mystream", "g1", "0"]).await,
        RespValue::ok()
    );
    assert!(error(&cmd(&mut c, &["XGROUP", "CREATE", "mystream", "g1", "0"]).await).contains("BUSYGROUP"));

    let read = cmd(
        &mut c,
        &["XREADGROUP", "GROUP", "g1", "c1", "COUNT", "2", "STREAMS", "mystream", ">"],
    )
    .await;
    let rs = dbg(&read);
    assert!(rs.contains("1-0") && rs.contains("1-1"), "{rs}");

    let pending = cmd(&mut c, &["XPENDING", "mystream", "g1"]).await;
    assert_eq!(
        match &pending {
            RespValue::Array(Some(v)) => &v[0],
            _ => panic!("{pending:?}"),
        },
        &int(2)
    );

    let ext = cmd(
        &mut c,
        &["XPENDING", "mystream", "g1", "-", "+", "10"],
    )
    .await;
    assert!(dbg(&ext).contains("c1"), "{ext:?}");

    assert_eq!(cmd(&mut c, &["XACK", "mystream", "g1", "1-0"]).await, int(1));

    // Claim idle (min-idle 0) vers c2
    let claimed = cmd(
        &mut c,
        &["XCLAIM", "mystream", "g1", "c2", "0", "1-1"],
    )
    .await;
    assert!(dbg(&claimed).contains("1-1"), "{claimed:?}");

    let auto = cmd(
        &mut c,
        &[
            "XAUTOCLAIM",
            "mystream",
            "g1",
            "c1",
            "0",
            "0-0",
            "COUNT",
            "10",
        ],
    )
    .await;
    assert!(matches!(auto, RespValue::Array(Some(ref v)) if v.len() == 3), "{auto:?}");

    let info = cmd(&mut c, &["XINFO", "STREAM", "mystream"]).await;
    let is = dbg(&info);
    assert!(is.contains("entries-added") && is.contains("last-generated-id"), "{is}");

    let groups = cmd(&mut c, &["XINFO", "GROUPS", "mystream"]).await;
    assert!(dbg(&groups).contains("g1"), "{groups:?}");

    let consumers = cmd(&mut c, &["XINFO", "CONSUMERS", "mystream", "g1"]).await;
    assert!(dbg(&consumers).contains("c1") || dbg(&consumers).contains("c2"), "{consumers:?}");

    assert_eq!(
        cmd(&mut c, &["XSETID", "mystream", "1-2", "ENTRIESADDED", "3"]).await,
        RespValue::ok()
    );
}

#[tokio::test]
async fn xreadgroup_block_woken_by_xadd() {
    let addr = spawn_test_server().await;
    let mut setup = connect(addr).await;
    cmd(&mut setup, &["XADD", "sg", "1-0", "f", "0"]).await;
    cmd(&mut setup, &["XGROUP", "CREATE", "sg", "g", "$"]).await;

    let mut reader = connect(addr).await;
    let mut writer = connect(addr).await;

    let blocked = tokio::spawn(async move {
        write_command(
            &mut reader,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"g",
                b"alice",
                b"BLOCK",
                b"5000",
                b"STREAMS",
                b"sg",
                b">",
            ],
        )
        .await
        .unwrap();
        read_resp(&mut reader).await.unwrap()
    });

    tokio::time::sleep(Duration::from_millis(80)).await;
    cmd(&mut writer, &["XADD", "sg", "*", "f", "new"]).await;
    let reply = timeout(Duration::from_secs(3), blocked)
        .await
        .expect("timeout XREADGROUP BLOCK")
        .unwrap();
    assert!(dbg(&reply).contains("new"), "{reply:?}");
}

#[tokio::test]
async fn stream_commands_listed_not_deferred() {
    let mut c = client().await;
    for name in [
        "XGROUP", "XREADGROUP", "XACK", "XCLAIM", "XAUTOCLAIM", "XPENDING", "XINFO", "XDEL", "XTRIM",
        "XSETID", "XREVRANGE",
    ] {
        let info = dbg(&cmd(&mut c, &["COMMAND", "INFO", name]).await);
        assert!(info.contains(&name.to_ascii_lowercase()), "{name}: {info}");
    }
}
