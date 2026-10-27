//! Tests de parité P11 — TTL par champ de hash (Redis 7.4 / 8).

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
async fn hexpire_httl_hpersist() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(&mut c, &["HSET", "h", "a", "1", "b", "2"]).await,
        RespValue::Integer(2)
    );
    let exp = cmd(&mut c, &["HEXPIRE", "h", "60", "FIELDS", "2", "a", "missing"]).await;
    assert!(dbg(&exp).contains("Integer(1)") && dbg(&exp).contains("Integer(-2)"), "{exp:?}");
    let ttl = cmd(&mut c, &["HTTL", "h", "FIELDS", "2", "a", "b"]).await;
    let t = dbg(&ttl);
    assert!(t.contains("Integer(-1)"), "b sans expire: {t}");
    // a doit avoir un TTL > 0
    match &ttl {
        RespValue::Array(Some(xs)) => match xs.first() {
            Some(RespValue::Integer(n)) if *n > 0 => {}
            _ => panic!("expected positive TTL: {ttl:?}"),
        },
        _ => panic!("expected array: {ttl:?}"),
    }
    assert_eq!(
        cmd(&mut c, &["HPERSIST", "h", "FIELDS", "1", "a"]).await,
        RespValue::array(vec![RespValue::Integer(1)])
    );
    assert_eq!(
        cmd(&mut c, &["HTTL", "h", "FIELDS", "1", "a"]).await,
        RespValue::array(vec![RespValue::Integer(-1)])
    );
}

#[tokio::test]
async fn hpexpire_lazy_hget_gone() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(cmd(&mut c, &["HSET", "h", "x", "v"]).await, RespValue::Integer(1));
    assert_eq!(
        cmd(&mut c, &["HPEXPIRE", "h", "400", "FIELDS", "1", "x"]).await,
        RespValue::array(vec![RespValue::Integer(1)])
    );
    // encore présent juste après
    assert_eq!(
        cmd(&mut c, &["HGET", "h", "x"]).await,
        RespValue::bulk(bytes::Bytes::from_static(b"v"))
    );
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(cmd(&mut c, &["HGET", "h", "x"]).await, RespValue::null_bulk());
    assert_eq!(cmd(&mut c, &["HLEN", "h"]).await, RespValue::Integer(0));
}

#[tokio::test]
async fn hsetex_hgetex_hgetdel() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(
            &mut c,
            &["HSETEX", "h", "EX", "30", "FIELDS", "1", "f", "val"]
        )
        .await,
        RespValue::Integer(1)
    );
    let got = cmd(&mut c, &["HGETEX", "h", "FIELDS", "1", "f"]).await;
    assert!(dbg(&got).contains("val"), "{got:?}");
    let ttl = cmd(&mut c, &["HPTTL", "h", "FIELDS", "1", "f"]).await;
    match &ttl {
        RespValue::Array(Some(xs)) => match xs.first() {
            Some(RespValue::Integer(n)) if *n > 0 => {}
            _ => panic!("expected positive HPTTL: {ttl:?}"),
        },
        _ => panic!("expected array: {ttl:?}"),
    }
    assert_eq!(
        cmd(&mut c, &["HGETDEL", "h", "FIELDS", "1", "f"]).await,
        RespValue::array(vec![RespValue::bulk(bytes::Bytes::from_static(b"val"))])
    );
    assert_eq!(cmd(&mut c, &["HGET", "h", "f"]).await, RespValue::null_bulk());
}

#[tokio::test]
async fn hexpire_nx_xx() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(cmd(&mut c, &["HSET", "h", "a", "1"]).await, RespValue::Integer(1));
    assert_eq!(
        cmd(&mut c, &["HEXPIRE", "h", "10", "NX", "FIELDS", "1", "a"]).await,
        RespValue::array(vec![RespValue::Integer(1)])
    );
    // déjà un TTL → NX échoue
    assert_eq!(
        cmd(&mut c, &["HEXPIRE", "h", "20", "NX", "FIELDS", "1", "a"]).await,
        RespValue::array(vec![RespValue::Integer(0)])
    );
    assert_eq!(
        cmd(&mut c, &["HEXPIRE", "h", "20", "XX", "FIELDS", "1", "a"]).await,
        RespValue::array(vec![RespValue::Integer(1)])
    );
}
