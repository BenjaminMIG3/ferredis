//! Tests TCP P4 : HELLO 2/3, types RESP3, CLIENT TRACKING + push invalidate.

use crate::tests::{cmd, connect, dbg};
use bytes::{Bytes, BytesMut};
use ferredis_core::resp::{parse, RespValue};
use ferredis_core::server::{read_resp, spawn_test_server, write_command};
use tokio::net::TcpStream;

fn b(s: &str) -> RespValue {
    RespValue::bulk(Bytes::from(s.to_string()))
}

async fn client() -> TcpStream {
    connect(spawn_test_server().await).await
}

fn hello_proto(v: &RespValue) -> i64 {
    match v {
        RespValue::Map(entries) => entries
            .iter()
            .find(|(k, _)| k.as_bytes() == Some(b"proto"))
            .and_then(|(_, v)| match v {
                RespValue::Integer(n) => Some(*n),
                _ => None,
            })
            .expect("proto in map"),
        RespValue::Array(Some(items)) => {
            let mut i = 0;
            while i + 1 < items.len() {
                if items[i].as_bytes() == Some(b"proto") {
                    if let RespValue::Integer(n) = items[i + 1] {
                        return n;
                    }
                }
                i += 2;
            }
            panic!("proto missing in array: {v:?}");
        }
        other => panic!("HELLO reply unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn hello_2_and_3() {
    let mut c = client().await;
    let h2 = cmd(&mut c, &["HELLO", "2"]).await;
    assert_eq!(hello_proto(&h2), 2);
    assert!(matches!(h2, RespValue::Array(Some(_))));

    let h3 = cmd(&mut c, &["HELLO", "3"]).await;
    assert_eq!(hello_proto(&h3), 3);
    assert!(matches!(h3, RespValue::Map(_)), "{h3:?}");

    // Null RESP3 après HELLO 3.
    let miss = cmd(&mut c, &["GET", "no-such-key-p4"]).await;
    assert!(
        matches!(miss, RespValue::Null | RespValue::BulkString(None)),
        "{miss:?}"
    );
}

#[tokio::test]
async fn hello_noproto() {
    let mut c = client().await;
    let e = cmd(&mut c, &["HELLO", "99"]).await;
    match e {
        RespValue::Error(msg) => {
            assert!(
                String::from_utf8_lossy(&msg).contains("NOPROTO"),
                "{msg:?}"
            );
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn resp3_bool_double_via_eval() {
    let mut c = client().await;
    assert!(matches!(cmd(&mut c, &["HELLO", "3"]).await, RespValue::Map(_)));
    let t = cmd(&mut c, &["EVAL", "redis.setresp(3); return true", "0"]).await;
    assert_eq!(t, RespValue::Bool(true), "{t:?}");
    let f = cmd(&mut c, &["EVAL", "redis.setresp(3); return false", "0"]).await;
    assert_eq!(f, RespValue::Bool(false), "{f:?}");
    let d = cmd(&mut c, &["EVAL", "redis.setresp(3); return 1.5", "0"]).await;
    assert!(matches!(d, RespValue::Double(n) if (n - 1.5).abs() < 1e-9), "{d:?}");
}

#[tokio::test]
async fn client_tracking_push_on_mutation() {
    let addr = spawn_test_server().await;
    let mut reader = connect(addr).await;
    let mut writer = connect(addr).await;

    assert!(matches!(
        cmd(&mut reader, &["HELLO", "3"]).await,
        RespValue::Map(_)
    ));
    assert_eq!(
        cmd(&mut reader, &["CLIENT", "TRACKING", "ON"]).await,
        RespValue::ok()
    );
    // Enregistre la clé via lecture.
    let _ = cmd(&mut reader, &["GET", "track-key"]).await;

    // Mutation depuis un autre client.
    assert_eq!(
        cmd(&mut writer, &["SET", "track-key", "v"]).await,
        RespValue::ok()
    );

    // Push invalidate attendu (déjà encodé RESP3 côté serveur).
    let mut buf = BytesMut::new();
    let mut tmp = [0u8; 4096];
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let push = loop {
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for invalidate push");
        }
        match parse(&mut buf) {
            Ok(v) => break v,
            Err(_) => {
                use tokio::io::AsyncReadExt;
                let n = tokio::time::timeout(
                    std::time::Duration::from_millis(200),
                    reader.read(&mut tmp),
                )
                .await
                .expect("read timeout")
                .expect("read");
                if n == 0 {
                    panic!("eof");
                }
                buf.extend_from_slice(&tmp[..n]);
            }
        }
    };

    match push {
        RespValue::Push(items) => {
            assert_eq!(items[0], b("invalidate"));
            match &items[1] {
                RespValue::Array(Some(keys)) => {
                    assert!(keys.iter().any(|k| k.as_bytes() == Some(b"track-key")));
                }
                other => panic!("keys: {other:?}"),
            }
        }
        other => panic!("expected push, got {other:?}"),
    }

    let info = dbg(&cmd(&mut reader, &["CLIENT", "TRACKINGINFO"]).await);
    assert!(info.contains("on"), "{info}");
}

#[tokio::test]
async fn tracking_off_and_getredir() {
    let mut c = client().await;
    assert_eq!(cmd(&mut c, &["CLIENT", "TRACKING", "ON"]).await, RespValue::ok());
    assert_eq!(
        cmd(&mut c, &["CLIENT", "GETREDIR"]).await,
        RespValue::Integer(-1)
    );
    assert_eq!(
        cmd(&mut c, &["CLIENT", "TRACKING", "OFF"]).await,
        RespValue::ok()
    );
}

#[tokio::test]
async fn config_tls_and_io_threads() {
    let mut c = client().await;
    let v = cmd(&mut c, &["CONFIG", "GET", "tls-port"]).await;
    assert!(dbg(&v).contains("tls-port"), "{v:?}");
    assert_eq!(
        cmd(&mut c, &["CONFIG", "SET", "io-threads", "2"]).await,
        RespValue::ok()
    );
    let io = cmd(&mut c, &["CONFIG", "GET", "io-threads"]).await;
    assert!(dbg(&io).contains("2"), "{io:?}");
}

/// HELLO 3 puis lecture brute d'un null `_`.
#[tokio::test]
async fn resp3_null_wire_format() {
    let mut c = client().await;
    write_command(&mut c, &[b"HELLO", b"3"]).await.unwrap();
    let h = read_resp(&mut c).await.unwrap();
    assert!(matches!(h, RespValue::Map(_)));
    write_command(&mut c, &[b"GET", b"missing-wire"]).await.unwrap();
    // GET manquant : handlers renvoient encore BulkString(None) encodé `_` en RESP3.
    let mut buf = BytesMut::new();
    let mut tmp = [0u8; 64];
    use tokio::io::AsyncReadExt;
    loop {
        match parse(&mut buf) {
            Ok(v) => {
                assert!(
                    matches!(v, RespValue::Null | RespValue::BulkString(None)),
                    "{v:?}"
                );
                break;
            }
            Err(_) => {
                let n = c.read(&mut tmp).await.unwrap();
                assert!(n > 0);
                buf.extend_from_slice(&tmp[..n]);
            }
        }
    }
}
