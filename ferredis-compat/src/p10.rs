//! Tests de parité P10 — probabilistes + vector set.

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
async fn module_list_includes_prob_vector() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    let r = dbg(&cmd(&mut c, &["MODULE", "LIST"]).await);
    assert!(r.contains("bf"), "{r}");
    assert!(r.contains("vectorset"), "{r}");
}

#[tokio::test]
async fn bloom_reserve_add_exists() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(&mut c, &["BF.RESERVE", "b", "0.01", "100"]).await,
        RespValue::ok()
    );
    assert_eq!(
        cmd(&mut c, &["BF.ADD", "b", "hello"]).await,
        RespValue::Integer(1)
    );
    assert_eq!(
        cmd(&mut c, &["BF.EXISTS", "b", "hello"]).await,
        RespValue::Integer(1)
    );
    assert_eq!(
        cmd(&mut c, &["BF.EXISTS", "b", "nope"]).await,
        RespValue::Integer(0)
    );
    assert_eq!(
        cmd(&mut c, &["BF.CARD", "b"]).await,
        RespValue::Integer(1)
    );
}

#[tokio::test]
async fn cuckoo_add_del_count() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(&mut c, &["CF.RESERVE", "cf", "100"]).await,
        RespValue::ok()
    );
    assert_eq!(
        cmd(&mut c, &["CF.ADD", "cf", "x"]).await,
        RespValue::Integer(1)
    );
    assert_eq!(
        cmd(&mut c, &["CF.EXISTS", "cf", "x"]).await,
        RespValue::Integer(1)
    );
    let cnt = cmd(&mut c, &["CF.COUNT", "cf", "x"]).await;
    assert!(matches!(cnt, RespValue::Integer(n) if n >= 1), "{cnt:?}");
    assert_eq!(
        cmd(&mut c, &["CF.DEL", "cf", "x"]).await,
        RespValue::Integer(1)
    );
}

#[tokio::test]
async fn cms_topk_tdigest_smoke() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(&mut c, &["CMS.INITBYDIM", "cms", "100", "5"]).await,
        RespValue::ok()
    );
    let incr = dbg(&cmd(&mut c, &["CMS.INCRBY", "cms", "a", "3"]).await);
    assert!(incr.contains('3'), "{incr}");
    assert_eq!(
        cmd(&mut c, &["TOPK.RESERVE", "tk", "3", "10", "3", "0.9"]).await,
        RespValue::ok()
    );
    let _ = cmd(&mut c, &["TOPK.ADD", "tk", "a", "b", "c"]).await;
    let list = dbg(&cmd(&mut c, &["TOPK.LIST", "tk"]).await);
    assert!(list.contains('a') || list.contains('b') || list.contains('c'), "{list}");
    assert_eq!(
        cmd(&mut c, &["TDIGEST.CREATE", "td", "COMPRESSION", "100"]).await,
        RespValue::ok()
    );
    assert_eq!(
        cmd(&mut c, &["TDIGEST.ADD", "td", "1", "2", "3", "4", "5"]).await,
        RespValue::ok()
    );
    let q = dbg(&cmd(&mut c, &["TDIGEST.QUANTILE", "td", "0.5"]).await);
    assert!(!q.contains("Error") && !q.contains("ERR"), "{q}");
}

#[tokio::test]
async fn vector_add_sim_card() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(
            &mut c,
            &["VADD", "vs", "VALUES", "3", "1", "0", "0", "a"]
        )
        .await,
        RespValue::Integer(1)
    );
    assert_eq!(
        cmd(
            &mut c,
            &["VADD", "vs", "VALUES", "3", "0.9", "0.1", "0", "b"]
        )
        .await,
        RespValue::Integer(1)
    );
    assert_eq!(
        cmd(&mut c, &["VCARD", "vs"]).await,
        RespValue::Integer(2)
    );
    assert_eq!(
        cmd(&mut c, &["VDIM", "vs"]).await,
        RespValue::Integer(3)
    );
    let sim = dbg(&cmd(
        &mut c,
        &["VSIM", "vs", "VALUES", "3", "1", "0", "0", "COUNT", "2", "WITHSCORES"],
    )
    .await);
    assert!(sim.contains('a'), "{sim}");
    assert_eq!(
        cmd(&mut c, &["VISMEMBER", "vs", "a"]).await,
        RespValue::Integer(1)
    );
    assert_eq!(
        cmd(&mut c, &["VREM", "vs", "b"]).await,
        RespValue::Integer(1)
    );
}
