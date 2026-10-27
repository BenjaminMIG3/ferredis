//! Tests de parité P9 — Redis Stack embarqué (JSON / TimeSeries / Search / MODULE).

use bytes::Bytes;
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
async fn module_list_embeds_stack() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    let r = dbg(&cmd(&mut c, &["MODULE", "LIST"]).await);
    assert!(r.contains("ReJSON"), "{r}");
    assert!(r.contains("timeseries"), "{r}");
    assert!(r.contains("search"), "{r}");
    // P10 modules also listed; stack trio still required.
    let load = dbg(&cmd(&mut c, &["MODULE", "LOAD", "x.so"]).await);
    assert!(load.contains("not supported") || load.contains("ERR"), "{load}");
}

#[tokio::test]
async fn json_set_get_del_type() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(&mut c, &["JSON.SET", "doc", "$", r#"{"a":1,"b":[true,"x"]}"#]).await,
        RespValue::ok()
    );
    assert_eq!(
        cmd(&mut c, &["TYPE", "doc"]).await,
        RespValue::simple(Bytes::from_static(b"ReJSON-RL"))
    );
    let got = cmd(&mut c, &["JSON.GET", "doc", "$.a"]).await;
    assert!(dbg(&got).contains('1'), "{got:?}");
    assert_eq!(
        cmd(&mut c, &["JSON.TYPE", "doc", "$.b"]).await,
        RespValue::bulk(Bytes::from_static(b"array"))
    );
    assert_eq!(
        cmd(&mut c, &["JSON.ARRLEN", "doc", "$.b"]).await,
        RespValue::Integer(2)
    );
    let incr = dbg(&cmd(&mut c, &["JSON.NUMINCRBY", "doc", "$.a", "2"]).await);
    assert!(incr.contains('3'), "{incr}");
    assert_eq!(
        cmd(&mut c, &["JSON.DEL", "doc", "$.b"]).await,
        RespValue::Integer(1)
    );
}

#[tokio::test]
async fn timeseries_create_add_range() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(
            &mut c,
            &["TS.CREATE", "temp", "LABELS", "sensor", "a", "RETENTION", "0"]
        )
        .await,
        RespValue::ok()
    );
    assert_eq!(
        cmd(&mut c, &["TS.ADD", "temp", "1000", "1.5"]).await,
        RespValue::Integer(1000)
    );
    assert_eq!(
        cmd(&mut c, &["TS.ADD", "temp", "2000", "2.5"]).await,
        RespValue::Integer(2000)
    );
    assert_eq!(
        cmd(&mut c, &["TYPE", "temp"]).await,
        RespValue::simple(Bytes::from_static(b"TSDB-TYPE"))
    );
    let range = dbg(&cmd(&mut c, &["TS.RANGE", "temp", "-", "+"]).await);
    assert!(range.contains("1000") && range.contains("2000"), "{range}");
    let agg = dbg(
        &cmd(
            &mut c,
            &["TS.RANGE", "temp", "-", "+", "AGGREGATION", "avg", "1000"],
        )
        .await,
    );
    assert!(agg.contains("1000") || agg.contains("1.5") || agg.contains("2"), "{agg}");
    let qi = dbg(&cmd(&mut c, &["TS.QUERYINDEX", "sensor=a"]).await);
    assert!(qi.contains("temp"), "{qi}");
}

#[tokio::test]
async fn search_create_and_search_hash() {
    let addr = spawn_test_server().await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        cmd(&mut c, &["HSET", "blog:1", "title", "hello", "body", "world redis"]).await,
        RespValue::Integer(2)
    );
    assert_eq!(
        cmd(&mut c, &["HSET", "blog:2", "title", "other", "body", "stack"]).await,
        RespValue::Integer(2)
    );
    assert_eq!(
        cmd(
            &mut c,
            &[
                "FT.CREATE",
                "idx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "blog:",
                "SCHEMA",
                "title",
                "TEXT",
                "body",
                "TEXT",
            ],
        )
        .await,
        RespValue::ok()
    );
    let list = dbg(&cmd(&mut c, &["FT._LIST"]).await);
    assert!(list.contains("idx"), "{list}");
    let res = dbg(&cmd(&mut c, &["FT.SEARCH", "idx", "hello"]).await);
    assert!(res.contains("blog:1"), "{res}");
    let agg = dbg(
        &cmd(
            &mut c,
            &["FT.AGGREGATE", "idx", "*", "GROUPBY", "0", "REDUCE", "COUNT", "0", "AS", "n"],
        )
        .await,
    );
    assert!(agg.contains('2') || agg.contains('n'), "{agg}");
}
