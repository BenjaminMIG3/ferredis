//! Tests TCP de compatibilité Redis 7.2 pour les familles P1 (valeurs issues de la doc Redis).

use crate::tests::{cmd, cmd_raw, connect, dbg};
use bytes::Bytes;
use ferredis_core::resp::RespValue;
use ferredis_core::server::{read_resp, spawn_test_server};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

fn b(s: &str) -> RespValue {
    RespValue::bulk(Bytes::from(s.to_string()))
}

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

fn arr(items: Vec<RespValue>) -> RespValue {
    RespValue::array(items)
}

fn bulks(items: &[&str]) -> RespValue {
    arr(items.iter().map(|s| b(s)).collect())
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

// ── bitmaps ──

#[tokio::test]
async fn bitmaps_bitcount_bitpos_bitop() {
    let mut c = client().await;
    cmd(&mut c, &["SET", "mykey", "foobar"]).await;
    assert_eq!(cmd(&mut c, &["BITCOUNT", "mykey"]).await, int(26));
    assert_eq!(cmd(&mut c, &["BITCOUNT", "mykey", "0", "0"]).await, int(4));
    assert_eq!(cmd(&mut c, &["BITCOUNT", "mykey", "1", "1", "BYTE"]).await, int(6));
    assert_eq!(cmd(&mut c, &["BITCOUNT", "mykey", "5", "30", "BIT"]).await, int(17));
    assert_eq!(error(&cmd(&mut c, &["BITCOUNT", "mykey", "0"]).await), "ERR syntax error");

    cmd_raw(&mut c, &[b"SET", b"p", b"\xff\xf0\x00"]).await;
    assert_eq!(cmd(&mut c, &["BITPOS", "p", "0"]).await, int(12));
    cmd_raw(&mut c, &[b"SET", b"p", b"\x00\xff\xf0"]).await;
    assert_eq!(cmd(&mut c, &["BITPOS", "p", "1", "0"]).await, int(8));
    assert_eq!(cmd(&mut c, &["BITPOS", "p", "1", "2"]).await, int(16));
    assert_eq!(cmd(&mut c, &["BITPOS", "p", "1", "2", "-1", "BYTE"]).await, int(16));
    assert_eq!(cmd(&mut c, &["BITPOS", "p", "1", "7", "15", "BIT"]).await, int(8));
    cmd_raw(&mut c, &[b"SET", b"p", b"\x00\x00\x00"]).await;
    assert_eq!(cmd(&mut c, &["BITPOS", "p", "1"]).await, int(-1));
    assert_eq!(error(&cmd(&mut c, &["BITPOS", "p", "2"]).await), "ERR The bit argument must be 1 or 0.");

    cmd(&mut c, &["SET", "key1", "foobar"]).await;
    cmd(&mut c, &["SET", "key2", "abcdef"]).await;
    assert_eq!(cmd(&mut c, &["BITOP", "AND", "dest", "key1", "key2"]).await, int(6));
    assert_eq!(cmd(&mut c, &["GET", "dest"]).await, b("`bc`ab"));
    assert_eq!(cmd(&mut c, &["BITOP", "NOT", "dest", "key1"]).await, int(6));
    assert_eq!(
        error(&cmd(&mut c, &["BITOP", "NOT", "dest", "key1", "key2"]).await),
        "ERR BITOP NOT must be called with a single source key."
    );
    assert_eq!(cmd(&mut c, &["BITOP", "OR", "dest", "nokey"]).await, int(0));
    assert_eq!(cmd(&mut c, &["EXISTS", "dest"]).await, int(0));
    assert_eq!(cmd(&mut c, &["SETBIT", "bits", "7", "1"]).await, int(0));
    assert_eq!(cmd(&mut c, &["GETBIT", "bits", "7"]).await, int(1));
    assert_eq!(cmd(&mut c, &["GET", "bits"]).await, b("\u{1}"));
}

#[tokio::test]
async fn bitfield_overflow_and_ro() {
    let mut c = client().await;
    assert_eq!(cmd(&mut c, &["BITFIELD", "mykey", "INCRBY", "i5", "100", "1", "GET", "u4", "0"]).await, arr(vec![int(1), int(0)]));
    let sat = ["BITFIELD", "bf", "INCRBY", "u2", "100", "1", "OVERFLOW", "SAT", "INCRBY", "u2", "102", "1"];
    let mut got = Vec::new();
    for _ in 0..4 {
        got.push(cmd(&mut c, &sat).await);
    }
    let pair = |a, b| arr(vec![int(a), int(b)]);
    assert_eq!(got, vec![pair(1, 1), pair(2, 2), pair(3, 3), pair(0, 3)]);
    let fail = cmd(&mut c, &["BITFIELD", "bf", "OVERFLOW", "FAIL", "INCRBY", "u2", "102", "1"]).await;
    assert_eq!(fail, arr(vec![RespValue::null_bulk()]));
    assert_eq!(cmd(&mut c, &["BITFIELD", "bf", "SET", "i8", "#1", "-1"]).await, arr(vec![int(0)]));
    assert_eq!(cmd(&mut c, &["BITFIELD_RO", "bf", "GET", "i8", "#1"]).await, arr(vec![int(-1)]));
    assert_eq!(
        error(&cmd(&mut c, &["BITFIELD_RO", "bf", "SET", "u8", "0", "1"]).await),
        "ERR BITFIELD_RO only supports the GET subcommand"
    );
    assert!(error(&cmd(&mut c, &["BITFIELD", "bf", "GET", "u64", "0"]).await).starts_with("ERR Invalid bitfield type"));
}

// ── listes / ensembles / hashes ──

#[tokio::test]
async fn lmpop_and_blmpop() {
    let addr = spawn_test_server().await;
    let mut c = connect(addr).await;
    cmd(&mut c, &["RPUSH", "l", "a", "b", "c", "d"]).await;
    let r = cmd(&mut c, &["LMPOP", "2", "nokey", "l", "LEFT", "COUNT", "2"]).await;
    assert_eq!(r, arr(vec![b("l"), bulks(&["a", "b"])]));
    assert_eq!(cmd(&mut c, &["LMPOP", "1", "l", "RIGHT"]).await, arr(vec![b("l"), bulks(&["d"])]));
    assert_eq!(cmd(&mut c, &["LMPOP", "1", "nokey", "LEFT"]).await, RespValue::Array(None));
    assert_eq!(error(&cmd(&mut c, &["LMPOP", "0", "l", "LEFT"]).await), "ERR numkeys should be greater than 0");
    assert_eq!(error(&cmd(&mut c, &["LMPOP", "1", "l", "UP"]).await), "ERR syntax error");
    assert_eq!(error(&cmd(&mut c, &["LMPOP", "1", "l", "LEFT", "COUNT", "0"]).await), "ERR count should be greater than 0");
    assert!(error(&cmd(&mut c, &["LMPOP", "1", "l"]).await).contains("wrong number of arguments for 'lmpop'"));

    let start = Instant::now();
    assert_eq!(cmd(&mut c, &["BLMPOP", "0.1", "1", "empty", "LEFT"]).await, RespValue::Array(None));
    assert!(start.elapsed() >= Duration::from_millis(80));

    let waiter = tokio::spawn(async move {
        let mut w = connect(addr).await;
        cmd(&mut w, &["BLMPOP", "5", "2", "q1", "q2", "RIGHT", "COUNT", "5"]).await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    cmd(&mut c, &["RPUSH", "q2", "x", "y"]).await;
    let got = tokio::time::timeout(Duration::from_secs(2), waiter).await.expect("BLMPOP doit se réveiller").unwrap();
    assert_eq!(got, arr(vec![b("q2"), bulks(&["y", "x"])]));
    assert_eq!(cmd(&mut c, &["EXISTS", "q2"]).await, int(0));
}

#[tokio::test]
async fn sintercard_and_hrandfield() {
    let mut c = client().await;
    cmd(&mut c, &["SADD", "s1", "a", "b", "c", "d"]).await;
    cmd(&mut c, &["SADD", "s2", "c", "d", "e"]).await;
    assert_eq!(cmd(&mut c, &["SINTERCARD", "2", "s1", "s2"]).await, int(2));
    assert_eq!(cmd(&mut c, &["SINTERCARD", "2", "s1", "s2", "LIMIT", "1"]).await, int(1));
    assert_eq!(cmd(&mut c, &["SINTERCARD", "2", "s1", "nokey"]).await, int(0));
    assert_eq!(error(&cmd(&mut c, &["SINTERCARD", "0", "s1"]).await), "ERR numkeys should be greater than 0");
    assert_eq!(error(&cmd(&mut c, &["SINTERCARD", "3", "s1", "s2"]).await), "ERR Number of keys can't be greater than number of args");
    assert_eq!(error(&cmd(&mut c, &["SINTERCARD", "2", "s1", "s2", "LIMIT", "-1"]).await), "ERR LIMIT can't be negative");

    cmd(&mut c, &["HSET", "h", "f1", "v1", "f2", "v2", "f3", "v3"]).await;
    assert!(matches!(cmd(&mut c, &["HRANDFIELD", "h"]).await, RespValue::BulkString(Some(_))));
    match cmd(&mut c, &["HRANDFIELD", "h", "5", "WITHVALUES"]).await {
        RespValue::Array(Some(v)) => assert_eq!(v.len(), 6),
        other => panic!("{other:?}"),
    }
    match cmd(&mut c, &["HRANDFIELD", "h", "-7"]).await {
        RespValue::Array(Some(v)) => assert_eq!(v.len(), 7),
        other => panic!("{other:?}"),
    }
    assert_eq!(cmd(&mut c, &["HRANDFIELD", "nokey"]).await, RespValue::null_bulk());
    assert_eq!(cmd(&mut c, &["HRANDFIELD", "nokey", "3"]).await, arr(vec![]));
    match cmd(&mut c, &["SRANDMEMBER", "s1", "-6"]).await {
        RespValue::Array(Some(v)) => assert_eq!(v.len(), 6),
        other => panic!("{other:?}"),
    }
}

// ── sorted sets ──

#[tokio::test]
async fn zset_lex_family() {
    let mut c = client().await;
    cmd(&mut c, &["ZADD", "z", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e", "0", "f", "0", "g"]).await;
    assert_eq!(cmd(&mut c, &["ZRANGEBYLEX", "z", "-", "[c"]).await, bulks(&["a", "b", "c"]));
    assert_eq!(cmd(&mut c, &["ZRANGEBYLEX", "z", "-", "(c"]).await, bulks(&["a", "b"]));
    assert_eq!(cmd(&mut c, &["ZRANGEBYLEX", "z", "[aaa", "(g"]).await, bulks(&["b", "c", "d", "e", "f"]));
    assert_eq!(cmd(&mut c, &["ZREVRANGEBYLEX", "z", "[c", "-", "LIMIT", "1", "2"]).await, bulks(&["b", "a"]));
    assert_eq!(cmd(&mut c, &["ZRANGE", "z", "(g", "[e", "BYLEX", "REV"]).await, bulks(&["f", "e"]));
    assert_eq!(cmd(&mut c, &["ZLEXCOUNT", "z", "-", "+"]).await, int(7));
    assert_eq!(cmd(&mut c, &["ZLEXCOUNT", "z", "[b", "[f"]).await, int(5));
    assert_eq!(error(&cmd(&mut c, &["ZRANGEBYLEX", "z", "a", "+"]).await), "ERR min or max not valid string range item");
    assert_eq!(cmd(&mut c, &["ZREMRANGEBYLEX", "z", "[a", "[c"]).await, int(3));
    assert_eq!(cmd(&mut c, &["ZRANGE", "z", "0", "-1"]).await, bulks(&["d", "e", "f", "g"]));
    assert_eq!(
        error(&cmd(&mut c, &["ZRANGE", "z", "-", "+", "BYLEX", "WITHSCORES"]).await),
        "ERR syntax error, WITHSCORES not supported in combination with BYLEX"
    );
    assert_eq!(cmd(&mut c, &["ZRANGESTORE", "dst", "z", "[e", "+", "BYLEX"]).await, int(3));
    assert_eq!(cmd(&mut c, &["ZRANGE", "dst", "0", "-1"]).await, bulks(&["e", "f", "g"]));
}

#[tokio::test]
async fn zset_setops_rand_and_mpop() {
    let addr = spawn_test_server().await;
    let mut c = connect(addr).await;
    cmd(&mut c, &["ZADD", "zset1", "1", "one", "2", "two"]).await;
    cmd(&mut c, &["ZADD", "zset2", "1", "one", "2", "two", "3", "three"]).await;
    assert_eq!(cmd(&mut c, &["ZINTER", "2", "zset1", "zset2", "WITHSCORES"]).await, bulks(&["one", "2", "two", "4"]));
    assert_eq!(cmd(&mut c, &["ZUNION", "2", "zset1", "zset2"]).await, bulks(&["one", "three", "two"]));
    assert_eq!(
        cmd(&mut c, &["ZUNION", "2", "zset1", "zset2", "WEIGHTS", "2", "1", "AGGREGATE", "MAX", "WITHSCORES"]).await,
        bulks(&["one", "2", "three", "3", "two", "4"])
    );
    assert_eq!(cmd(&mut c, &["ZDIFF", "2", "zset2", "zset1", "WITHSCORES"]).await, bulks(&["three", "3"]));
    assert_eq!(cmd(&mut c, &["ZDIFFSTORE", "out", "2", "zset2", "zset1"]).await, int(1));
    assert_eq!(cmd(&mut c, &["ZINTERCARD", "2", "zset1", "zset2"]).await, int(2));
    assert_eq!(cmd(&mut c, &["ZINTERCARD", "2", "zset1", "zset2", "LIMIT", "1"]).await, int(1));
    assert_eq!(cmd(&mut c, &["ZINTERSTORE", "out", "2", "zset1", "zset2", "WEIGHTS", "2", "3"]).await, int(2));
    assert_eq!(cmd(&mut c, &["ZRANGE", "out", "0", "-1", "WITHSCORES"]).await, bulks(&["one", "5", "two", "10"]));
    assert_eq!(error(&cmd(&mut c, &["ZINTER", "0", "zset1"]).await), "ERR at least 1 input key is needed for 'zinter' command");
    assert_eq!(error(&cmd(&mut c, &["ZUNION", "2", "zset1", "zset2", "WEIGHTS", "1"]).await), "ERR syntax error");

    match cmd(&mut c, &["ZRANDMEMBER", "zset2", "-5", "WITHSCORES"]).await {
        RespValue::Array(Some(v)) => assert_eq!(v.len(), 10),
        other => panic!("{other:?}"),
    }
    match cmd(&mut c, &["ZRANDMEMBER", "zset2", "10"]).await {
        RespValue::Array(Some(v)) => assert_eq!(v.len(), 3),
        other => panic!("{other:?}"),
    }
    assert_eq!(cmd(&mut c, &["ZRANDMEMBER", "nokey"]).await, RespValue::null_bulk());

    let pair = |m: &str, s: &str| bulks(&[m, s]);
    assert_eq!(
        cmd(&mut c, &["ZMPOP", "2", "nokey", "zset2", "MIN", "COUNT", "2"]).await,
        arr(vec![b("zset2"), arr(vec![pair("one", "1"), pair("two", "2")])])
    );
    assert_eq!(cmd(&mut c, &["ZMPOP", "1", "zset2", "MAX"]).await, arr(vec![b("zset2"), arr(vec![pair("three", "3")])]));
    assert_eq!(cmd(&mut c, &["ZMPOP", "1", "zset2", "MAX"]).await, RespValue::Array(None));

    let waiter = tokio::spawn(async move {
        let mut w = connect(addr).await;
        cmd(&mut w, &["BZMPOP", "5", "1", "bz", "MAX"]).await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    cmd(&mut c, &["ZADD", "bz", "1", "lo", "9", "hi"]).await;
    let got = tokio::time::timeout(Duration::from_secs(2), waiter).await.expect("BZMPOP doit se réveiller").unwrap();
    assert_eq!(got, arr(vec![b("bz"), arr(vec![pair("hi", "9")])]));
}

// ── HyperLogLog ──

#[tokio::test]
async fn hyperloglog_real_encoding() {
    let mut c = client().await;
    assert_eq!(cmd(&mut c, &["PFADD", "hll", "a", "b", "c", "d", "e", "f", "g"]).await, int(1));
    assert_eq!(cmd(&mut c, &["PFADD", "hll", "a", "b"]).await, int(0));
    assert_eq!(cmd(&mut c, &["PFCOUNT", "hll"]).await, int(7));
    assert_eq!(cmd(&mut c, &["PFADD", "empty"]).await, int(1));
    assert_eq!(cmd(&mut c, &["PFCOUNT", "empty"]).await, int(0));
    match cmd(&mut c, &["GET", "hll"]).await {
        RespValue::BulkString(Some(v)) => assert!(v.starts_with(b"HYLL")),
        other => panic!("{other:?}"),
    }
    cmd(&mut c, &["PFADD", "hll2", "f", "g", "h", "i"]).await;
    assert_eq!(cmd(&mut c, &["PFCOUNT", "hll", "hll2"]).await, int(9));
    assert_eq!(cmd(&mut c, &["PFMERGE", "merged", "hll", "hll2"]).await, RespValue::ok());
    assert_eq!(cmd(&mut c, &["PFCOUNT", "merged"]).await, int(9));
    assert_eq!(cmd(&mut c, &["PFDEBUG", "ENCODING", "merged"]).await, RespValue::simple("sparse"));
    assert_eq!(cmd(&mut c, &["PFDEBUG", "TODENSE", "merged"]).await, int(1));
    assert_eq!(cmd(&mut c, &["PFDEBUG", "ENCODING", "merged"]).await, RespValue::simple("dense"));
    assert_eq!(cmd(&mut c, &["PFCOUNT", "merged"]).await, int(9));
    match cmd(&mut c, &["PFDEBUG", "GETREG", "merged"]).await {
        RespValue::Array(Some(v)) => assert_eq!(v.len(), 16384),
        other => panic!("{other:?}"),
    }
    cmd(&mut c, &["SET", "str", "notanhll"]).await;
    assert_eq!(error(&cmd(&mut c, &["PFADD", "str", "x"]).await), "WRONGTYPE Key is not a valid HyperLogLog string value.");
    assert_eq!(error(&cmd(&mut c, &["PFCOUNT", "str"]).await), "WRONGTYPE Key is not a valid HyperLogLog string value.");

    let many: Vec<String> = (0..20_000).map(|i| format!("elem:{i}")).collect();
    let mut args = vec!["PFADD", "big"];
    args.extend(many.iter().map(String::as_str));
    cmd(&mut c, &args).await;
    let n = match cmd(&mut c, &["PFCOUNT", "big"]).await {
        RespValue::Integer(n) => n,
        other => panic!("{other:?}"),
    };
    assert!((n - 20_000).abs() < 20_000 * 3 / 100, "estimation hors tolérance : {n}");
}

// ── géo ──

#[tokio::test]
async fn geo_family_matches_redis_docs() {
    let mut c = client().await;
    assert_eq!(cmd(&mut c, &["GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"]).await, int(2));
    assert_eq!(cmd(&mut c, &["GEODIST", "Sicily", "Palermo", "Catania"]).await, b("166274.1516"));
    assert_eq!(cmd(&mut c, &["GEODIST", "Sicily", "Palermo", "Catania", "km"]).await, b("166.2742"));
    assert_eq!(cmd(&mut c, &["GEODIST", "Sicily", "Palermo", "Catania", "mi"]).await, b("103.3182"));
    assert_eq!(cmd(&mut c, &["GEODIST", "Sicily", "Foo", "Bar"]).await, RespValue::null_bulk());
    assert_eq!(cmd(&mut c, &["GEOHASH", "Sicily", "Palermo", "Catania", "nope"]).await, arr(vec![b("sqc8b49rny0"), b("sqdtr74hyu0"), RespValue::null_bulk()]));
    assert_eq!(
        cmd(&mut c, &["GEOPOS", "Sicily", "Palermo", "nope"]).await,
        arr(vec![bulks(&["13.36138933897018433", "38.11555639549629859"]), RespValue::Array(None)])
    );
    assert_eq!(
        cmd(&mut c, &["GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHDIST", "ASC"]).await,
        arr(vec![bulks(&["Catania", "56.4413"]), bulks(&["Palermo", "190.4424"])])
    );
    assert_eq!(cmd(&mut c, &["GEORADIUS", "Sicily", "15", "37", "100", "km"]).await, bulks(&["Catania"]));
    assert_eq!(
        cmd(&mut c, &["GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHCOORD", "DESC", "COUNT", "1"]).await,
        arr(vec![arr(vec![b("Palermo"), bulks(&["13.36138933897018433", "38.11555639549629859"])])])
    );
    cmd(&mut c, &["GEOADD", "Sicily", "12.758489", "38.788135", "edge1", "17.241510", "38.788135", "edge2"]).await;
    assert_eq!(cmd(&mut c, &["GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC"]).await, bulks(&["Catania", "Palermo"]));
    assert_eq!(
        cmd(&mut c, &["GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "ASC", "WITHDIST"]).await,
        arr(vec![
            bulks(&["Catania", "56.4413"]),
            bulks(&["Palermo", "190.4424"]),
            bulks(&["edge2", "279.7403"]),
            bulks(&["edge1", "279.7405"]),
        ])
    );
    assert_eq!(cmd(&mut c, &["GEOSEARCH", "Sicily", "FROMMEMBER", "Palermo", "BYRADIUS", "1", "km", "WITHHASH"]).await, arr(vec![arr(vec![b("Palermo"), int(3479099956230698)])]));
    assert_eq!(cmd(&mut c, &["GEOSEARCHSTORE", "dst", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "ASC", "COUNT", "3", "STOREDIST"]).await, int(3));
    assert_eq!(
        cmd(&mut c, &["ZRANGE", "dst", "0", "-1", "WITHSCORES"]).await,
        bulks(&["Catania", "56.4412578701582", "Palermo", "190.44242984775784", "edge2", "279.7403417843143"])
    );
    assert_eq!(cmd(&mut c, &["GEORADIUS", "Sicily", "15", "37", "200", "km", "STORE", "st"]).await, int(2));
    assert_eq!(cmd(&mut c, &["ZSCORE", "st", "Palermo"]).await, b("3479099956230698"));

    cmd(&mut c, &["GEOADD", "Sicily", "13.583333", "37.316667", "Agrigento"]).await;
    assert_eq!(cmd(&mut c, &["GEORADIUSBYMEMBER", "Sicily", "Agrigento", "100", "km", "ASC"]).await, bulks(&["Agrigento", "Palermo"]));
    assert_eq!(cmd(&mut c, &["GEORADIUSBYMEMBER_RO", "nokey", "x", "100", "km"]).await, arr(vec![]));

    assert_eq!(error(&cmd(&mut c, &["GEOADD", "g", "200", "100", "m"]).await), "ERR invalid longitude,latitude pair 200.000000,100.000000");
    assert_eq!(error(&cmd(&mut c, &["GEOADD", "g", "NX", "XX", "1", "1", "m"]).await), "ERR syntax error");
    assert_eq!(error(&cmd(&mut c, &["GEODIST", "Sicily", "Palermo", "Catania", "yd"]).await), "ERR unsupported unit provided. please use M, KM, FT, MI");
    assert_eq!(
        error(&cmd(&mut c, &["GEOSEARCH", "Sicily", "BYRADIUS", "10", "km", "ASC", "WITHDIST"]).await),
        "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH"
    );
    assert!(error(&cmd(&mut c, &["GEOSEARCH", "Sicily", "BYRADIUS", "10", "km"]).await).contains("wrong number of arguments for 'geosearch'"));
    assert_eq!(
        error(&cmd(&mut c, &["GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "10", "km", "ANY"]).await),
        "ERR the ANY argument requires COUNT argument"
    );
    assert_eq!(error(&cmd(&mut c, &["GEORADIUS_RO", "Sicily", "15", "37", "200", "km", "STORE", "x"]).await), "ERR syntax error");
    assert_eq!(error(&cmd(&mut c, &["GEORADIUSBYMEMBER", "Sicily", "nope", "1", "km"]).await), "ERR could not decode requested zset member");
}

// ── SORT / LCS / SWAPDB ──

#[tokio::test]
async fn sort_lcs_swapdb() {
    let mut c = client().await;
    cmd(&mut c, &["RPUSH", "s", "3", "1", "2", "10"]).await;
    assert_eq!(cmd(&mut c, &["SORT", "s"]).await, bulks(&["1", "2", "3", "10"]));
    assert_eq!(cmd(&mut c, &["SORT", "s", "ALPHA"]).await, bulks(&["1", "10", "2", "3"]));
    assert_eq!(cmd(&mut c, &["SORT", "s", "DESC", "LIMIT", "0", "2"]).await, bulks(&["10", "3"]));
    for (k, w) in [("1", "40"), ("2", "30"), ("3", "20"), ("10", "10")] {
        cmd(&mut c, &["SET", &format!("w_{k}"), w]).await;
        cmd(&mut c, &["HSET", &format!("o_{k}"), "name", &format!("n{k}")]).await;
    }
    assert_eq!(cmd(&mut c, &["SORT", "s", "BY", "w_*"]).await, bulks(&["10", "3", "2", "1"]));
    assert_eq!(cmd(&mut c, &["SORT", "s", "BY", "nosort", "GET", "#", "GET", "o_*->name"]).await, bulks(&["3", "n3", "1", "n1", "2", "n2", "10", "n10"]));
    assert_eq!(cmd(&mut c, &["SORT", "s", "LIMIT", "1", "2", "GET", "missing_*"]).await, arr(vec![RespValue::null_bulk(), RespValue::null_bulk()]));
    assert_eq!(cmd(&mut c, &["SORT", "s", "STORE", "sorted"]).await, int(4));
    assert_eq!(cmd(&mut c, &["LRANGE", "sorted", "0", "-1"]).await, bulks(&["1", "2", "3", "10"]));
    assert_eq!(cmd(&mut c, &["SORT_RO", "s", "DESC"]).await, bulks(&["10", "3", "2", "1"]));
    assert_eq!(error(&cmd(&mut c, &["SORT_RO", "s", "STORE", "x"]).await), "ERR syntax error");
    cmd(&mut c, &["RPUSH", "bad", "a", "1"]).await;
    assert_eq!(error(&cmd(&mut c, &["SORT", "bad"]).await), "ERR One or more scores can't be converted into double");
    cmd(&mut c, &["SADD", "set", "b", "a", "c"]).await;
    assert_eq!(cmd(&mut c, &["SORT", "set", "ALPHA"]).await, bulks(&["a", "b", "c"]));
    cmd(&mut c, &["ZADD", "zs", "3", "x", "1", "y", "2", "z"]).await;
    assert_eq!(cmd(&mut c, &["SORT", "zs", "BY", "nosort", "DESC"]).await, bulks(&["x", "z", "y"]));
    assert_eq!(cmd(&mut c, &["SORT", "nokey", "STORE", "sorted"]).await, int(0));
    assert_eq!(cmd(&mut c, &["EXISTS", "sorted"]).await, int(0));

    cmd(&mut c, &["SET", "key1", "ohmytext"]).await;
    cmd(&mut c, &["SET", "key2", "mynewtext"]).await;
    assert_eq!(cmd(&mut c, &["LCS", "key1", "key2"]).await, b("mytext"));
    assert_eq!(cmd(&mut c, &["LCS", "key1", "key2", "LEN"]).await, int(6));
    let m = |a: i64, b: i64, c: i64, d: i64| arr(vec![arr(vec![int(a), int(b)]), arr(vec![int(c), int(d)])]);
    assert_eq!(
        cmd(&mut c, &["LCS", "key1", "key2", "IDX"]).await,
        arr(vec![b("matches"), arr(vec![m(4, 7, 5, 8), m(2, 3, 0, 1)]), b("len"), int(6)])
    );
    assert_eq!(
        cmd(&mut c, &["LCS", "key1", "key2", "IDX", "MINMATCHLEN", "4", "WITHMATCHLEN"]).await,
        arr(vec![b("matches"), arr(vec![arr(vec![arr(vec![int(4), int(7)]), arr(vec![int(5), int(8)]), int(4)])]), b("len"), int(6)])
    );
    assert_eq!(error(&cmd(&mut c, &["LCS", "key1", "key2", "LEN", "IDX"]).await), "ERR If you want both the length and indexes, please just use IDX.");
    assert_eq!(error(&cmd(&mut c, &["LCS", "key1", "s"]).await), "ERR The specified keys must contain string values");

    cmd(&mut c, &["SET", "swapme", "db0"]).await;
    assert_eq!(cmd(&mut c, &["SWAPDB", "0", "1"]).await, RespValue::ok());
    assert_eq!(cmd(&mut c, &["GET", "swapme"]).await, RespValue::null_bulk());
    cmd(&mut c, &["SELECT", "1"]).await;
    assert_eq!(cmd(&mut c, &["GET", "swapme"]).await, b("db0"));
    assert_eq!(error(&cmd(&mut c, &["SWAPDB", "x", "1"]).await), "ERR invalid first DB index");
    assert_eq!(error(&cmd(&mut c, &["SWAPDB", "0", "y"]).await), "ERR invalid second DB index");
    assert_eq!(error(&cmd(&mut c, &["SWAPDB", "0", "999"]).await), "ERR DB index is out of range");
}

#[tokio::test]
async fn swapdb_wakes_blocked_client() {
    let addr = spawn_test_server().await;
    let mut c = connect(addr).await;
    cmd(&mut c, &["SELECT", "1"]).await;
    cmd(&mut c, &["RPUSH", "jobs", "j1"]).await;
    let waiter = tokio::spawn(async move {
        let mut w = connect(addr).await;
        cmd(&mut w, &["BLPOP", "jobs", "5"]).await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(cmd(&mut c, &["SWAPDB", "0", "1"]).await, RespValue::ok());
    let got = tokio::time::timeout(Duration::from_secs(2), waiter).await.expect("SWAPDB doit réveiller BLPOP").unwrap();
    assert_eq!(got, bulks(&["jobs", "j1"]));
}

// ── Pub/Sub shardé ──

#[tokio::test]
async fn sharded_pubsub() {
    let addr = spawn_test_server().await;
    let mut sub = connect(addr).await;
    assert_eq!(cmd(&mut sub, &["SSUBSCRIBE", "orders"]).await, arr(vec![b("ssubscribe"), b("orders"), int(1)]));
    assert!(error(&cmd(&mut sub, &["GET", "x"]).await).contains("only (P|S)SUBSCRIBE"));

    let mut p = connect(addr).await;
    assert_eq!(cmd(&mut p, &["PUBLISH", "orders", "classic"]).await, int(0));
    assert_eq!(cmd(&mut p, &["SPUBLISH", "orders", "o1"]).await, int(1));
    assert_eq!(read_resp(&mut sub).await.unwrap(), bulks(&["smessage", "orders", "o1"]));
    assert_eq!(cmd(&mut p, &["PUBSUB", "SHARDCHANNELS"]).await, bulks(&["orders"]));
    assert_eq!(cmd(&mut p, &["PUBSUB", "SHARDCHANNELS", "ord*"]).await, bulks(&["orders"]));
    assert_eq!(cmd(&mut p, &["PUBSUB", "SHARDNUMSUB", "orders", "none"]).await, arr(vec![b("orders"), int(1), b("none"), int(0)]));
    assert_eq!(cmd(&mut p, &["PUBSUB", "CHANNELS"]).await, arr(vec![]));
    assert_eq!(error(&cmd(&mut p, &["PUBSUB", "NOPE"]).await), "ERR unknown subcommand 'NOPE'. Try PUBSUB HELP.");

    assert_eq!(cmd(&mut sub, &["SUNSUBSCRIBE"]).await, arr(vec![b("sunsubscribe"), b("orders"), int(0)]));
    assert_eq!(cmd(&mut sub, &["PING"]).await, RespValue::pong());
    assert_eq!(cmd(&mut p, &["SPUBLISH", "orders", "o2"]).await, int(0));
}

// ── transactions ──

#[tokio::test]
async fn transaction_edge_cases() {
    let mut c = client().await;
    assert_eq!(error(&cmd(&mut c, &["EXEC"]).await), "ERR EXEC without MULTI");
    assert_eq!(error(&cmd(&mut c, &["DISCARD"]).await), "ERR DISCARD without MULTI");

    cmd(&mut c, &["MULTI"]).await;
    assert_eq!(error(&cmd(&mut c, &["MULTI"]).await), "ERR MULTI calls can not be nested");
    assert_eq!(cmd(&mut c, &["SET", "a", "1"]).await, RespValue::simple("QUEUED"));
    assert_eq!(error(&cmd(&mut c, &["NOSUCHCMD", "x", "y"]).await), "ERR unknown command 'NOSUCHCMD', with args beginning with: 'x' 'y' ");
    assert_eq!(error(&cmd(&mut c, &["EXEC"]).await), "EXECABORT Transaction discarded because of previous errors.");
    assert_eq!(cmd(&mut c, &["EXISTS", "a"]).await, int(0));

    cmd(&mut c, &["MULTI"]).await;
    assert_eq!(error(&cmd(&mut c, &["GET"]).await), "ERR wrong number of arguments for 'get' command");
    assert_eq!(error(&cmd(&mut c, &["EXEC"]).await), "EXECABORT Transaction discarded because of previous errors.");

    cmd(&mut c, &["MULTI"]).await;
    cmd(&mut c, &["SET", "a", "1"]).await;
    cmd(&mut c, &["LPUSH", "a", "x"]).await;
    cmd(&mut c, &["INCR", "a"]).await;
    match cmd(&mut c, &["EXEC"]).await {
        RespValue::Array(Some(v)) => {
            assert_eq!(v[0], RespValue::ok());
            assert!(error(&v[1]).starts_with("WRONGTYPE"));
            assert_eq!(v[2], int(2));
        }
        other => panic!("{other:?}"),
    }

    cmd(&mut c, &["MULTI"]).await;
    assert_eq!(error(&cmd(&mut c, &["WATCH", "a"]).await), "ERR WATCH inside MULTI is not allowed");
    cmd(&mut c, &["SET", "b", "1"]).await;
    assert_eq!(cmd(&mut c, &["DISCARD"]).await, RespValue::ok());
    assert_eq!(cmd(&mut c, &["EXISTS", "b"]).await, int(0));

    let start = Instant::now();
    cmd(&mut c, &["MULTI"]).await;
    cmd(&mut c, &["BLPOP", "nothing", "0"]).await;
    cmd(&mut c, &["BLMPOP", "0", "1", "nothing", "LEFT"]).await;
    cmd(&mut c, &["SSUBSCRIBE", "ch"]).await;
    let r = cmd(&mut c, &["EXEC"]).await;
    assert!(start.elapsed() < Duration::from_secs(1), "les commandes bloquantes ne doivent pas attendre dans EXEC");
    match r {
        RespValue::Array(Some(v)) => {
            assert_eq!(v[0], RespValue::Array(None));
            assert_eq!(v[1], RespValue::Array(None));
            assert_eq!(error(&v[2]), "ERR SSUBSCRIBE isn't allowed for a DENY BLOCKING client");
        }
        other => panic!("{other:?}"),
    }

    cmd(&mut c, &["SET", "w", "1"]).await;
    cmd(&mut c, &["WATCH", "w"]).await;
    cmd(&mut c, &["MULTI"]).await;
    cmd(&mut c, &["NOPE"]).await;
    assert!(error(&cmd(&mut c, &["EXEC"]).await).starts_with("EXECABORT"));
    cmd(&mut c, &["SET", "w", "2"]).await;
    cmd(&mut c, &["MULTI"]).await;
    cmd(&mut c, &["GET", "w"]).await;
    assert_eq!(cmd(&mut c, &["EXEC"]).await, bulks(&["2"]), "EXECABORT doit lever les WATCH");
}

#[tokio::test]
async fn watch_sees_swapdb_and_expire() {
    let addr = spawn_test_server().await;
    let mut a = connect(addr).await;
    let mut other = connect(addr).await;
    cmd(&mut a, &["SET", "k", "v"]).await;
    cmd(&mut a, &["WATCH", "k"]).await;
    cmd(&mut other, &["SWAPDB", "0", "3"]).await;
    cmd(&mut a, &["MULTI"]).await;
    cmd(&mut a, &["PING"]).await;
    assert_eq!(cmd(&mut a, &["EXEC"]).await, RespValue::Array(None));
}

// ── LATENCY ──

#[tokio::test]
async fn latency_graph_and_histogram() {
    let mut c = client().await;
    assert_eq!(cmd(&mut c, &["CONFIG", "SET", "latency-monitor-threshold", "5"]).await, RespValue::ok());
    let busy = "local t = redis.call('TIME'); local s = t[1] * 1000000 + t[2]; \
                repeat local n = redis.call('TIME') until (n[1] * 1000000 + n[2]) - s > 20000; return 1";
    assert_eq!(cmd(&mut c, &["EVAL", busy, "0"]).await, int(1));
    cmd(&mut c, &["SET", "a", "1"]).await;
    cmd(&mut c, &["SET", "a", "2"]).await;

    let latest = dbg(&cmd(&mut c, &["LATENCY", "LATEST"]).await);
    assert!(latest.contains("\"command\""), "{latest}");
    match cmd(&mut c, &["LATENCY", "GRAPH", "command"]).await {
        RespValue::BulkString(Some(g)) => {
            let g = String::from_utf8_lossy(&g);
            let mut lines = g.lines();
            assert!(lines.next().unwrap().starts_with("command - high "), "{g}");
            assert_eq!(lines.next().unwrap(), "-".repeat(80));
            assert!(g.contains('#') || g.contains('_'), "{g}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(error(&cmd(&mut c, &["LATENCY", "GRAPH", "nope"]).await), "ERR No samples available for event 'nope'");
    match cmd(&mut c, &["LATENCY", "HISTORY", "command"]).await {
        RespValue::Array(Some(v)) => assert!(!v.is_empty()),
        other => panic!("{other:?}"),
    }

    match cmd(&mut c, &["LATENCY", "HISTOGRAM", "set", "SET", "nosuch"]).await {
        RespValue::Array(Some(v)) => {
            assert_eq!(v.len(), 2);
            assert_eq!(v[0], b("set"));
            match &v[1] {
                RespValue::Array(Some(m)) => {
                    assert_eq!(m[0], b("calls"));
                    assert_eq!(m[1], int(2));
                    assert_eq!(m[2], b("histogram_usec"));
                    let RespValue::Array(Some(h)) = &m[3] else { panic!("{m:?}") };
                    assert!(!h.is_empty() && h.len() % 2 == 0);
                    assert_eq!(h[h.len() - 1], int(2), "le dernier palier cumule tous les appels");
                }
                other => panic!("{other:?}"),
            }
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(cmd(&mut c, &["LATENCY", "RESET", "nope"]).await, int(0));
    assert_eq!(cmd(&mut c, &["LATENCY", "RESET"]).await, int(1));
    assert_eq!(cmd(&mut c, &["LATENCY", "LATEST"]).await, arr(vec![]));
    assert_eq!(error(&cmd(&mut c, &["LATENCY", "BOGUS"]).await), "ERR unknown subcommand 'BOGUS'. Try LATENCY HELP.");
    assert_eq!(error(&cmd(&mut c, &["LATENCY", "GRAPH"]).await), "ERR wrong number of arguments for 'latency|graph' command");
}

#[tokio::test]
async fn new_commands_are_listed_and_not_deferred() {
    let mut c = client().await;
    for name in ["BITFIELD", "LMPOP", "ZRANGESTORE", "PFMERGE", "GEOSEARCH", "SORT_RO", "LCS", "SWAPDB", "SSUBSCRIBE"] {
        let info = dbg(&cmd(&mut c, &["COMMAND", "INFO", name]).await);
        assert!(info.contains(&name.to_ascii_lowercase()), "{name}: {info}");
    }
    let r = dbg(&cmd(&mut c, &["DUMP", "missing"]).await);
    assert!(r.contains("BulkString(None)") || r.contains("null"), "{r}");
}
