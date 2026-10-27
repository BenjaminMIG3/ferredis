//! Tests TCP P3 : Lua RO, FUNCTION/FCALL, ACL, CLIENT, MEMORY.

use crate::tests::{cmd, connect, dbg};
use bytes::Bytes;
use ferredis_core::resp::RespValue;
use ferredis_core::server::spawn_test_server;
use tokio::net::TcpStream;

fn b(s: &str) -> RespValue {
    RespValue::bulk(Bytes::from(s.to_string()))
}

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
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

#[tokio::test]
async fn eval_ro_rejects_writes() {
    let mut c = client().await;
    let e = error(
        &cmd(
            &mut c,
            &["EVAL_RO", "return redis.call('SET', KEYS[1], 'x')", "1", "k"],
        )
        .await,
    );
    assert!(
        e.contains("Write commands") || e.contains("read-only") || e.contains("ReadOnly"),
        "{e}"
    );
    assert_eq!(
        cmd(&mut c, &["EVAL_RO", "return redis.call('GET', KEYS[1])", "1", "k"]).await,
        RespValue::null_bulk()
    );
    assert_eq!(
        cmd(&mut c, &["EVAL", "return bit.band(6,3)", "0"]).await,
        int(2)
    );
    let enc = cmd(&mut c, &["EVAL", "return cjson.encode({ok=true})", "0"]).await;
    assert!(matches!(enc, RespValue::BulkString(Some(_))), "{enc:?}");
}

#[tokio::test]
async fn function_load_and_fcall() {
    let mut c = client().await;
    let code = "#!lua name=mylib\nredis.register_function('echo', function(keys, args) return args[1] end)";
    assert_eq!(
        cmd(&mut c, &["FUNCTION", "LOAD", code]).await,
        b("mylib")
    );
    assert_eq!(
        cmd(&mut c, &["FCALL", "echo", "0", "hello"]).await,
        b("hello")
    );
    let list = dbg(&cmd(&mut c, &["FUNCTION", "LIST"]).await);
    assert!(list.contains("mylib"), "{list}");
    assert_eq!(cmd(&mut c, &["FUNCTION", "DELETE", "mylib"]).await, RespValue::ok());
    let e = error(&cmd(&mut c, &["FCALL", "echo", "0", "x"]).await);
    assert!(e.contains("Function not found") || e.contains("not found"), "{e}");
}

#[tokio::test]
async fn acl_denies_command() {
    let mut c = client().await;
    assert_eq!(
        cmd(
            &mut c,
            &[
                "ACL",
                "SETUSER",
                "alice",
                "on",
                ">secret",
                "~*",
                "&*",
                "+@read",
                "-@write",
            ]
        )
        .await,
        RespValue::ok()
    );
    assert_eq!(
        cmd(&mut c, &["AUTH", "alice", "secret"]).await,
        RespValue::ok()
    );
    assert_eq!(cmd(&mut c, &["GET", "k"]).await, RespValue::null_bulk());
    let e = error(&cmd(&mut c, &["SET", "k", "v"]).await);
    assert!(e.contains("NOPERM"), "{e}");
    assert_eq!(cmd(&mut c, &["ACL", "WHOAMI"]).await, b("alice"));
}

#[tokio::test]
async fn client_list_and_kill() {
    let addr = spawn_test_server().await;
    let mut victim = connect(addr).await;
    let id = match cmd(&mut victim, &["CLIENT", "ID"]).await {
        RespValue::Integer(n) => n.to_string(),
        other => panic!("CLIENT ID: {other:?}"),
    };
    let mut admin = connect(addr).await;
    let list = dbg(&cmd(&mut admin, &["CLIENT", "LIST"]).await);
    assert!(list.contains("id="), "{list}");
    assert_eq!(
        cmd(&mut admin, &["CLIENT", "KILL", "ID", &id]).await,
        int(1)
    );
}

#[tokio::test]
async fn memory_usage_and_stats() {
    let mut c = client().await;
    assert_eq!(cmd(&mut c, &["SET", "m", "hello"]).await, RespValue::ok());
    let usage = cmd(&mut c, &["MEMORY", "USAGE", "m"]).await;
    assert!(matches!(usage, RespValue::Integer(n) if n > 0), "{usage:?}");
    let stats = dbg(&cmd(&mut c, &["MEMORY", "STATS"]).await);
    assert!(stats.contains("total.allocated") || stats.contains("dataset.bytes"), "{stats}");
    assert!(matches!(
        cmd(&mut c, &["MEMORY", "DOCTOR"]).await,
        RespValue::BulkString(Some(_))
    ));
    assert_eq!(cmd(&mut c, &["MEMORY", "PURGE"]).await, RespValue::ok());
}
