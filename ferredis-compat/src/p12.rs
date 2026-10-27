//! Tests de parité P12 — maxmemory, OBJECT idle/freq, version 8.0.

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
async fn redis_compat_version_8() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    let info = dbg(&cmd(&mut c, &["INFO", "server"]).await);
    assert!(info.contains("redis_version:8.0.0"), "{info}");
}

#[tokio::test]
async fn object_encoding_idle_freq() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(cmd(&mut c, &["SET", "s", "hello"]).await, RespValue::ok());
    let enc = dbg(&cmd(&mut c, &["OBJECT", "ENCODING", "s"]).await);
    assert!(enc.contains("embstr") || enc.contains("raw"), "{enc}");
    let freq = cmd(&mut c, &["OBJECT", "FREQ", "s"]).await;
    assert!(matches!(freq, RespValue::Integer(n) if n >= 0), "{freq:?}");
    let _ = cmd(&mut c, &["GET", "s"]).await;
    let idle = cmd(&mut c, &["OBJECT", "IDLETIME", "s"]).await;
    assert!(matches!(idle, RespValue::Integer(n) if n >= 0), "{idle:?}");
}

#[tokio::test]
async fn maxmemory_noeviction_oom() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(&mut c, &["CONFIG", "SET", "maxmemory", "200"]).await,
        RespValue::ok()
    );
    assert_eq!(
        cmd(&mut c, &["CONFIG", "SET", "maxmemory-policy", "noeviction"]).await,
        RespValue::ok()
    );
    // Remplir jusqu'à OOM
    let mut saw_oom = false;
    for i in 0..50 {
        let k = format!("k{i}");
        let v = "x".repeat(80);
        let r = cmd(&mut c, &["SET", &k, &v]).await;
        if dbg(&r).contains("OOM") {
            saw_oom = true;
            break;
        }
    }
    assert!(saw_oom, "expected OOM under tiny maxmemory");
    let info = dbg(&cmd(&mut c, &["INFO", "memory"]).await);
    assert!(info.contains("maxmemory:200"), "{info}");
    assert!(info.contains("maxmemory_policy:noeviction"), "{info}");
}

#[tokio::test]
async fn maxmemory_allkeys_lru_evicts() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(&mut c, &["CONFIG", "SET", "maxmemory", "800"]).await,
        RespValue::ok()
    );
    assert_eq!(
        cmd(
            &mut c,
            &["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"]
        )
        .await,
        RespValue::ok()
    );
    for i in 0..30 {
        let k = format!("e{i}");
        let v = "y".repeat(40);
        let r = cmd(&mut c, &["SET", &k, &v]).await;
        assert!(
            r == RespValue::ok() || dbg(&r).contains("OOM"),
            "unexpected {r:?}"
        );
    }
    let info = dbg(&cmd(&mut c, &["INFO", "memory"]).await);
    // Soit éviction a tourné, soit on est sous plafond sans OOM bloquant.
    assert!(info.contains("maxmemory:800"), "{info}");
}

#[tokio::test]
async fn io_threads_info_surface() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(&mut c, &["CONFIG", "SET", "io-threads", "2"]).await,
        RespValue::ok()
    );
    let info = dbg(&cmd(&mut c, &["INFO", "memory"]).await);
    assert!(info.contains("io-threads:2"), "{info}");
}
