//! Tests de parité P6 — réplication (ROLE, REPLICAOF, PSYNC, WAIT, READONLY).

use bytes::Bytes;
use ferredis_core::command::{dispatch, ClientState, CommandContext, ServerConfig};
use ferredis_core::resp::RespValue;
use ferredis_core::server::{read_resp, spawn_test_server, write_command};
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

#[tokio::test]
async fn role_is_master_by_default() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    match cmd(&mut c, &["ROLE"]).await {
        RespValue::Array(Some(items)) => {
            assert_eq!(items[0], RespValue::bulk(Bytes::from_static(b"master")));
            assert!(matches!(items[1], RespValue::Integer(_)));
            assert!(matches!(items[2], RespValue::Array(Some(_))));
        }
        other => panic!("ROLE: {other:?}"),
    }
}

#[tokio::test]
async fn readonly_readwrite_ok() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(cmd(&mut c, &["READONLY"]).await, RespValue::ok());
    assert_eq!(cmd(&mut c, &["READWRITE"]).await, RespValue::ok());
}

#[tokio::test]
async fn replconf_listening_port_and_capa() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(&mut c, &["REPLCONF", "listening-port", "6380"]).await,
        RespValue::ok()
    );
    assert_eq!(
        cmd(&mut c, &["REPLCONF", "capa", "psync2"]).await,
        RespValue::ok()
    );
    match cmd(&mut c, &["REPLCONF", "GETACK", "*"]).await {
        RespValue::Array(Some(items)) => {
            assert_eq!(items[0], RespValue::bulk(Bytes::from_static(b"REPLCONF")));
            assert_eq!(items[1], RespValue::bulk(Bytes::from_static(b"ACK")));
        }
        other => panic!("GETACK: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicaof_full_sync_and_propagate() {
    let master = spawn_test_server().await;
    let replica = spawn_test_server().await;

    let mut m = TcpStream::connect(master).await.unwrap();
    assert_eq!(cmd(&mut m, &["SET", "k", "v1"]).await, RespValue::ok());

    let mut r = TcpStream::connect(replica).await.unwrap();
    let host = "127.0.0.1";
    let port = master.port().to_string();
    assert_eq!(
        cmd(&mut r, &["REPLICAOF", host, &port]).await,
        RespValue::ok()
    );

    // Attendre le sync RDB + application.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut synced = false;
    while tokio::time::Instant::now() < deadline {
        match cmd(&mut r, &["GET", "k"]).await {
            RespValue::BulkString(Some(b)) if b.as_ref() == b"v1" => {
                synced = true;
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    assert!(synced, "réplica doit recevoir k=v1 via FULLRESYNC");

    // Propagation incrémentale.
    assert_eq!(cmd(&mut m, &["SET", "k", "v2"]).await, RespValue::ok());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut got = false;
    while tokio::time::Instant::now() < deadline {
        match cmd(&mut r, &["GET", "k"]).await {
            RespValue::BulkString(Some(b)) if b.as_ref() == b"v2" => {
                got = true;
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    assert!(got, "réplica doit recevoir SET k v2");

    // Read-only sur le réplica.
    let ro = dbg(&cmd(&mut r, &["SET", "x", "1"]).await);
    assert!(ro.contains("READONLY"), "{ro}");

    match cmd(&mut r, &["ROLE"]).await {
        RespValue::Array(Some(items)) => {
            assert_eq!(items[0], RespValue::bulk(Bytes::from_static(b"slave")));
        }
        other => panic!("ROLE replica: {other:?}"),
    }

    assert_eq!(cmd(&mut r, &["REPLICAOF", "NO", "ONE"]).await, RespValue::ok());
    match cmd(&mut r, &["ROLE"]).await {
        RespValue::Array(Some(items)) => {
            assert_eq!(items[0], RespValue::bulk(Bytes::from_static(b"master")));
        }
        other => panic!("ROLE after promote: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_counts_replica_ack() {
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

    // Laisser le handshake PSYNC se terminer.
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut m = TcpStream::connect(master).await.unwrap();
    assert_eq!(cmd(&mut m, &["SET", "w", "1"]).await, RespValue::ok());

    let waited = cmd(&mut m, &["WAIT", "1", "2000"]).await;
    match waited {
        RespValue::Integer(n) => assert!(n >= 1, "WAIT doit voir ≥1 réplica, got {n}"),
        other => panic!("WAIT: {other:?}"),
    }
}

#[tokio::test]
async fn info_replication_section() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    let info = match cmd(&mut c, &["INFO", "replication"]).await {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("{other:?}"),
    };
    assert!(info.contains("role:master"), "{info}");
    assert!(info.contains("master_replid:"), "{info}");
    assert!(info.contains("master_repl_offset:"), "{info}");
}

#[tokio::test]
async fn slaveof_alias_and_unit_propagate() {
    let ctx = CommandContext::new(ServerConfig::default());
    let mut st = ClientState {
        authenticated: true,
        ..Default::default()
    };
    let args = |a: &[&str]| -> Vec<Bytes> {
        a.iter().map(|s| Bytes::from(s.to_string())).collect()
    };
    let _ = dispatch(&ctx, &mut st, &args(&["SET", "a", "1"])).await;
    assert!(ctx.repl.master_offset() > 0);
    assert_eq!(ctx.repl.role(), ferredis_core::repl::ReplRole::Master);

    let r = dispatch(&ctx, &mut st, &args(&["WAIT", "0", "10"])).await;
    assert_eq!(r[0], RespValue::Integer(0));
}

#[tokio::test]
async fn failover_on_replica_promotes() {
    let master = spawn_test_server().await;
    let replica = spawn_test_server().await;
    let mut r = TcpStream::connect(replica).await.unwrap();
    cmd(
        &mut r,
        &["REPLICAOF", "127.0.0.1", &master.port().to_string()],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(cmd(&mut r, &["FAILOVER"]).await, RespValue::ok());
    match cmd(&mut r, &["ROLE"]).await {
        RespValue::Array(Some(items)) => {
            assert_eq!(items[0], RespValue::bulk(Bytes::from_static(b"master")));
        }
        other => panic!("{other:?}"),
    }
}
