//! Tests phases F1–F6 — encodings, mémoire, query, raft config.

use ferredis_core::resp::RespValue;
use ferredis_core::server::{read_resp, spawn_test_server, write_command};
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
async fn f1_intset_encoding_and_memory_purge() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(cmd(&mut c, &["SADD", "is", "1", "2", "3"]).await, RespValue::Integer(3));
    let enc = dbg(&cmd(&mut c, &["OBJECT", "ENCODING", "is"]).await);
    assert!(enc.contains("intset"), "{enc}");
    assert_eq!(cmd(&mut c, &["MEMORY", "PURGE"]).await, RespValue::ok());
    let info = dbg(&cmd(&mut c, &["INFO", "memory"]).await);
    assert!(info.contains("mem_fragmentation_ratio:"), "{info}");
    assert!(info.contains("storage_mode:"), "{info}");
}

#[tokio::test]
async fn f3_client_durability_and_group_commit_config() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(&mut c, &["CLIENT", "DURABILITY", "always"]).await,
        RespValue::ok()
    );
    let d = dbg(&cmd(&mut c, &["CLIENT", "DURABILITY"]).await);
    assert!(d.contains("always"), "{d}");
    assert_eq!(
        cmd(&mut c, &["CONFIG", "SET", "aof-group-commit-ms", "5"]).await,
        RespValue::ok()
    );
}

#[tokio::test]
async fn f4_hybrid_storage_mode() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(&mut c, &["CONFIG", "SET", "storage-mode", "hybrid"]).await,
        RespValue::ok()
    );
    let info = dbg(&cmd(&mut c, &["INFO", "memory"]).await);
    assert!(info.contains("storage_mode:hybrid"), "{info}");
}

#[tokio::test]
async fn f5_secondary_index_and_fq_select() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(
            &mut c,
            &["FQ.INDEX", "CREATE", "by_city", "PREFIX", "user:", "FIELD", "city"]
        )
        .await,
        RespValue::ok()
    );
    assert_eq!(
        cmd(&mut c, &["HSET", "user:1", "city", "Paris"]).await,
        RespValue::Integer(1)
    );
    assert_eq!(
        cmd(&mut c, &["HSET", "user:2", "city", "Lyon"]).await,
        RespValue::Integer(1)
    );
    let r = cmd(
        &mut c,
        &["FQ.SELECT", "by_city", "WHERE", "city", "=", "Paris"],
    )
    .await;
    let s = dbg(&r);
    assert!(s.contains("user:1"), "{s}");
}

#[tokio::test]
async fn f6_cluster_mode_cp() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(&mut c, &["CONFIG", "SET", "cluster-mode", "cp"]).await,
        RespValue::ok()
    );
    let info = dbg(&cmd(&mut c, &["INFO", "cluster"]).await);
    assert!(info.contains("cluster_consistency:cp"), "{info}");
    assert!(info.contains("raft_role:leader"), "{info}");
}
