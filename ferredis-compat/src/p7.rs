//! Tests P7 — Sentinel (MONITOR, GET-MASTER-ADDR-BY-NAME, FAILOVER).

use bytes::Bytes;
use ferredis_core::command::ServerConfig;
use ferredis_core::resp::RespValue;
use ferredis_core::server::{read_resp, spawn_test_server, spawn_test_server_with, write_command};
use std::time::Duration;
use tokio::net::TcpStream;

async fn cmd(sock: &mut TcpStream, args: &[&str]) -> RespValue {
    let raw: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
    write_command(sock, &raw).await.unwrap();
    read_resp(sock).await.unwrap()
}

fn dbg(v: &RespValue) -> String {
    format!("{v:?}")
}

async fn spawn_sentinel() -> std::net::SocketAddr {
    let mut cfg = ServerConfig::default();
    cfg.sentinel_mode = true;
    cfg.port = 0;
    spawn_test_server_with(cfg).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sentinel_monitor_and_get_addr() {
    let master = spawn_test_server().await;
    let sent = spawn_sentinel().await;
    let mut c = TcpStream::connect(sent).await.unwrap();

    assert_eq!(
        cmd(
            &mut c,
            &[
                "SENTINEL",
                "MONITOR",
                "mymaster",
                "127.0.0.1",
                &master.port().to_string(),
                "1"
            ]
        )
        .await,
        RespValue::ok()
    );

    match cmd(&mut c, &["SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"]).await {
        RespValue::Array(Some(items)) => {
            assert_eq!(items[0], RespValue::bulk(Bytes::from_static(b"127.0.0.1")));
            assert_eq!(
                items[1],
                RespValue::bulk(Bytes::from(master.port().to_string()))
            );
        }
        other => panic!("{other:?}"),
    }

    let masters = dbg(&cmd(&mut c, &["SENTINEL", "MASTERS"]).await);
    assert!(masters.contains("mymaster"), "{masters}");

    assert_eq!(
        cmd(
            &mut c,
            &[
                "SENTINEL",
                "SET",
                "mymaster",
                "down-after-milliseconds",
                "2000"
            ]
        )
        .await,
        RespValue::ok()
    );
    assert_eq!(cmd(&mut c, &["SENTINEL", "CKQUORUM", "mymaster"]).await, RespValue::ok());
    assert!(matches!(
        cmd(&mut c, &["SENTINEL", "MYID"]).await,
        RespValue::BulkString(Some(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sentinel_failover_promotes_replica() {
    let master = spawn_test_server().await;
    let replica = spawn_test_server().await;

    let mut r = TcpStream::connect(replica).await.unwrap();
    assert_eq!(
        cmd(
            &mut r,
            &["REPLICAOF", "127.0.0.1", &master.port().to_string()]
        )
        .await,
        RespValue::ok()
    );
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut m = TcpStream::connect(master).await.unwrap();
    assert_eq!(cmd(&mut m, &["SET", "k", "v"]).await, RespValue::ok());
    // Laisser la propagation.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let sent = spawn_sentinel().await;
    let mut s = TcpStream::connect(sent).await.unwrap();
    assert_eq!(
        cmd(
            &mut s,
            &[
                "SENTINEL",
                "MONITOR",
                "mymaster",
                "127.0.0.1",
                &master.port().to_string(),
                "1"
            ]
        )
        .await,
        RespValue::ok()
    );
    // Laisser le ping / INFO replication peupler les slaves.
    tokio::time::sleep(Duration::from_millis(1200)).await;

    let fo = cmd(&mut s, &["SENTINEL", "FAILOVER", "mymaster"]).await;
    assert_eq!(fo, RespValue::ok(), "{fo:?}");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut addr_ok = false;
    while tokio::time::Instant::now() < deadline {
        match cmd(&mut s, &["SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"]).await {
            RespValue::Array(Some(items)) => {
                let port = match &items[1] {
                    RespValue::BulkString(Some(b)) => {
                        String::from_utf8_lossy(b).parse::<u16>().unwrap_or(0)
                    }
                    _ => 0,
                };
                if port == replica.port() {
                    addr_ok = true;
                    break;
                }
            }
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(addr_ok, "après FAILOVER, master doit être le réplica");

    match cmd(&mut r, &["ROLE"]).await {
        RespValue::Array(Some(items)) => {
            assert_eq!(items[0], RespValue::bulk(Bytes::from_static(b"master")));
        }
        other => panic!("ROLE after failover: {other:?}"),
    }
}

#[tokio::test]
async fn sentinel_mode_rejects_set() {
    let sent = spawn_sentinel().await;
    let mut c = TcpStream::connect(sent).await.unwrap();
    let r = dbg(&cmd(&mut c, &["SET", "a", "1"]).await);
    assert!(r.contains("sentinel") || r.contains("ERR"), "{r}");
}
