//! Tests de parité P5 — RDB natif, DUMP/RESTORE/MIGRATE, WAITAOF.

use bytes::Bytes;
use ferredis_core::command::{dispatch, ClientState, CommandContext, ServerConfig};
use ferredis_core::persist::{load_rdb_with_functions, save_rdb_with_functions};
use ferredis_core::rdb::{create_dump_payload, decode_rdb, encode_rdb_bytes, verify_and_decode_dump};
use ferredis_core::resp::RespValue;
use ferredis_core::server::{read_resp, spawn_test_server, write_command};
use ferredis_core::value::{Entry, HashObject, RedisValue, Stream, StreamId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use tokio::net::TcpStream;

async fn cmd(sock: &mut TcpStream, args: &[&str]) -> RespValue {
    let raw: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
    write_command(sock, &raw).await.unwrap();
    read_resp(sock).await.unwrap()
}

async fn cmd_raw(sock: &mut TcpStream, args: &[&[u8]]) -> RespValue {
    write_command(sock, args).await.unwrap();
    read_resp(sock).await.unwrap()
}

fn dbg(v: &RespValue) -> String {
    format!("{v:?}")
}

#[tokio::test]
async fn dump_restore_roundtrip_tcp() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(cmd(&mut c, &["SET", "k", "hello"]).await, RespValue::ok());
    let dump = match cmd(&mut c, &["DUMP", "k"]).await {
        RespValue::BulkString(Some(b)) => b,
        other => panic!("DUMP: {other:?}"),
    };
    assert_eq!(cmd(&mut c, &["DEL", "k"]).await, RespValue::Integer(1));
    assert_eq!(
        cmd_raw(
            &mut c,
            &[b"RESTORE", b"k2", b"0", dump.as_ref()]
        )
        .await,
        RespValue::ok()
    );
    assert_eq!(
        cmd(&mut c, &["GET", "k2"]).await,
        RespValue::bulk(Bytes::from_static(b"hello"))
    );
}

#[tokio::test]
async fn dump_restore_replace_and_busykey() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    cmd(&mut c, &["SET", "a", "1"]).await;
    cmd(&mut c, &["SET", "b", "2"]).await;
    let dump = match cmd(&mut c, &["DUMP", "a"]).await {
        RespValue::BulkString(Some(b)) => b,
        other => panic!("{other:?}"),
    };
    let busy = dbg(
        &cmd_raw(&mut c, &[b"RESTORE", b"b", b"0", dump.as_ref()]).await,
    );
    assert!(busy.contains("BUSYKEY"), "{busy}");
    assert_eq!(
        cmd_raw(
            &mut c,
            &[b"RESTORE", b"b", b"0", dump.as_ref(), b"REPLACE"]
        )
        .await,
        RespValue::ok()
    );
    assert_eq!(
        cmd(&mut c, &["GET", "b"]).await,
        RespValue::bulk(Bytes::from_static(b"1"))
    );
}

#[tokio::test]
async fn dump_missing_is_null() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(cmd(&mut c, &["DUMP", "nope"]).await, RespValue::null_bulk());
}

#[tokio::test]
async fn waitaof_without_aof_errors_when_numlocal() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    let r = dbg(&cmd(&mut c, &["WAITAOF", "1", "0", "100"]).await);
    assert!(r.contains("appendonly"), "{r}");
    assert_eq!(
        cmd(&mut c, &["WAITAOF", "0", "0", "100"]).await,
        RespValue::array(vec![RespValue::Integer(0), RespValue::Integer(0)])
    );
}

#[tokio::test]
async fn rdb_save_load_roundtrip_unit() {
    let dir = std::env::temp_dir().join(format!("ferredis-p5-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("dump.rdb");

    let ctx = CommandContext::new(ServerConfig::default());
    let mut st = ClientState {
        authenticated: true,
        ..Default::default()
    };
    let set = |args: &[&str]| {
        let args: Vec<Bytes> = args.iter().map(|a| Bytes::from(a.to_string())).collect();
        // sync dispatch via block_on not needed — use execute path through futures
        args
    };
    let args = set(&["SET", "s", "v"]);
    let _ = dispatch(&ctx, &mut st, &args).await;
    let args = set(&["HSET", "h", "f", "1"]);
    let _ = dispatch(&ctx, &mut st, &args).await;
    let args = set(&["SADD", "set", "m"]);
    let _ = dispatch(&ctx, &mut st, &args).await;
    let args = set(&["ZADD", "z", "1.5", "zm"]);
    let _ = dispatch(&ctx, &mut st, &args).await;
    let args = set(&["RPUSH", "l", "a", "b"]);
    let _ = dispatch(&ctx, &mut st, &args).await;
    let args = set(&["XADD", "stream", "1-0", "f", "v"]);
    let _ = dispatch(&ctx, &mut st, &args).await;

    save_rdb_with_functions(&path, &ctx.db, Some(&ctx.functions)).unwrap();

    let ctx2 = CommandContext::new(ServerConfig::default());
    load_rdb_with_functions(&path, &ctx2.db, Some(&ctx2.functions)).unwrap();
    let mut st2 = ClientState {
        authenticated: true,
        ..Default::default()
    };
    assert_eq!(
        dispatch(&ctx2, &mut st2, &set(&["GET", "s"])).await[0],
        RespValue::bulk(Bytes::from_static(b"v"))
    );
    assert_eq!(
        dispatch(&ctx2, &mut st2, &set(&["HGET", "h", "f"])).await[0],
        RespValue::bulk(Bytes::from_static(b"1"))
    );
    assert_eq!(
        dispatch(&ctx2, &mut st2, &set(&["SCARD", "set"])).await[0],
        RespValue::Integer(1)
    );
    assert_eq!(
        dispatch(&ctx2, &mut st2, &set(&["ZSCORE", "z", "zm"])).await[0],
        RespValue::bulk(Bytes::from_static(b"1.5"))
    );
    assert_eq!(
        dispatch(&ctx2, &mut st2, &set(&["LLEN", "l"])).await[0],
        RespValue::Integer(2)
    );
    assert_eq!(
        dispatch(&ctx2, &mut st2, &set(&["XLEN", "stream"])).await[0],
        RespValue::Integer(1)
    );

    let _ = std::fs::remove_dir_all(&dir);
    let _ = PathBuf::from(".");
}

#[test]
fn dump_payload_types_unit() {
    let mut hmap = HashMap::new();
    hmap.insert(Bytes::from_static(b"a"), Bytes::from_static(b"b"));
    let h = HashObject::from_map(hmap);
    let mut stream = Stream::default();
    let id = StreamId::new(10, 1);
    stream.entries.insert(
        id,
        vec![(Bytes::from_static(b"x"), Bytes::from_static(b"y"))],
    );
    stream.last_id = id;
    stream.first_id = id;
    stream.recorded_first_id = id;
    stream.entries_added = 1;

    for v in [
        RedisValue::String(Bytes::from_static(b"s")),
        RedisValue::List(VecDeque::from([Bytes::from_static(b"1")])),
        RedisValue::Set(HashSet::from([Bytes::from_static(b"m")]).into()),
        RedisValue::Hash(h),
        RedisValue::Stream(stream),
    ] {
        let p = create_dump_payload(&v);
        assert_eq!(verify_and_decode_dump(&p).unwrap(), v);
    }
}

#[test]
fn rdb_encode_decode_empty_and_keyed() {
    let dump = vec![(
        0usize,
        vec![(
            Bytes::from_static(b"k"),
            Entry::new(RedisValue::String(Bytes::from_static(b"v"))),
        )],
    )];
    let bytes = encode_rdb_bytes(&dump, &[]).unwrap();
    assert!(bytes.starts_with(b"REDIS0011"));
    let snap = decode_rdb(&bytes).unwrap();
    assert_eq!(snap.dbs[0].1.len(), 1);
}

#[tokio::test]
async fn migrate_nokey() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    let r = cmd(&mut c, &["MIGRATE", "127.0.0.1", "1", "missing", "0", "100"]).await;
    assert_eq!(r, RespValue::simple(Bytes::from_static(b"NOKEY")));
}

#[tokio::test]
async fn commands_listed() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    for name in ["DUMP", "RESTORE", "MIGRATE", "WAITAOF", "RESTORE-ASKING"] {
        let info = dbg(&cmd(&mut c, &["COMMAND", "INFO", name]).await);
        assert!(info.contains(&name.to_ascii_lowercase()), "{name}: {info}");
    }
}
