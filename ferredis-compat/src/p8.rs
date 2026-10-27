//! Tests P8 — Cluster (KEYSLOT, ADDSLOTS, MOVED, ASKING).

use bytes::Bytes;
use ferredis_core::cluster::key_slot;
use ferredis_core::command::ServerConfig;
use ferredis_core::resp::RespValue;
use ferredis_core::server::{read_resp, spawn_test_server_with, write_command};
use tokio::net::TcpStream;

async fn cmd(sock: &mut TcpStream, args: &[&str]) -> RespValue {
    let raw: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
    write_command(sock, &raw).await.unwrap();
    read_resp(sock).await.unwrap()
}

fn dbg(v: &RespValue) -> String {
    format!("{v:?}")
}

async fn spawn_cluster() -> std::net::SocketAddr {
    let mut cfg = ServerConfig::default();
    cfg.cluster_enabled = true;
    cfg.port = 0;
    spawn_test_server_with(cfg).await
}

#[tokio::test]
async fn cluster_keyslot_matches_crc16() {
    let addr = spawn_cluster().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    for key in ["foo", "bar", "{user100}.profile", "x"] {
        let slot = key_slot(key.as_bytes());
        assert_eq!(
            cmd(&mut c, &["CLUSTER", "KEYSLOT", key]).await,
            RespValue::Integer(i64::from(slot))
        );
    }
}

#[tokio::test]
async fn cluster_addslots_and_set_get() {
    let addr = spawn_cluster().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    // Assigner tous les slots à ce nœud.
    assert_eq!(
        cmd(&mut c, &["CLUSTER", "ADDSLOTSRANGE", "0", "16383"]).await,
        RespValue::ok()
    );
    assert_eq!(cmd(&mut c, &["SET", "k", "v"]).await, RespValue::ok());
    assert_eq!(
        cmd(&mut c, &["GET", "k"]).await,
        RespValue::bulk(Bytes::from_static(b"v"))
    );
    let nodes = dbg(&cmd(&mut c, &["CLUSTER", "NODES"]).await);
    assert!(nodes.contains("myself"), "{nodes}");
    let info = dbg(&cmd(&mut c, &["CLUSTER", "INFO"]).await);
    assert!(info.contains("cluster_state:ok"), "{info}");
}

#[tokio::test]
async fn cluster_moved_when_slot_unassigned() {
    let addr = spawn_cluster().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    // Aucun slot → CLUSTERDOWN
    let r = dbg(&cmd(&mut c, &["SET", "a", "1"]).await);
    assert!(r.contains("CLUSTERDOWN") || r.contains("MOVED"), "{r}");
}

#[tokio::test]
async fn cluster_myid_and_slots() {
    let addr = spawn_cluster().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert!(matches!(
        cmd(&mut c, &["CLUSTER", "MYID"]).await,
        RespValue::BulkString(Some(_))
    ));
    assert_eq!(
        cmd(&mut c, &["CLUSTER", "ADDSLOTS", "1", "2", "3"]).await,
        RespValue::ok()
    );
    let slots = dbg(&cmd(&mut c, &["CLUSTER", "SLOTS"]).await);
    assert!(slots.contains("Integer(1)"), "{slots}");
}
