//! Bitmaps, HyperLogLog, Geo — valeurs d'exemples redis.io.

use super::harness::{arr, BurnCtx};
use ferredis_core::resp::RespValue;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "bitmaps_geo_hll";
    let mut c = ctx.connect().await;

    // Bitmaps — exemples redis.io BITCOUNT
    let bk = ctx.unique("bit");
    ctx.expect_ok(S, "bit_set", &mut c, &["SET", &bk, "foobar"]).await;
    ctx.expect_int(S, "bitcount", &mut c, &["BITCOUNT", &bk], 26).await;
    ctx.expect_int(S, "bitcount_byte", &mut c, &["BITCOUNT", &bk, "0", "0"], 4)
        .await;
    ctx.expect_int(S, "setbit", &mut c, &["SETBIT", &bk, "7", "1"], 0)
        .await;
    ctx.expect_int(S, "getbit", &mut c, &["GETBIT", &bk, "7"], 1).await;

    let k1 = ctx.unique("b1");
    let k2 = ctx.unique("b2");
    let dest = ctx.unique("bdest");
    ctx.expect_ok(S, "bitop_prep1", &mut c, &["SET", &k1, "foobar"]).await;
    ctx.expect_ok(S, "bitop_prep2", &mut c, &["SET", &k2, "abcdef"]).await;
    ctx.expect_int(S, "bitop_and", &mut c, &["BITOP", "AND", &dest, &k1, &k2], 6)
        .await;
    ctx.expect_bulk(S, "bitop_and_val", &mut c, &["GET", &dest], "`bc`ab")
        .await;

    // BITFIELD
    let bf = ctx.unique("bf");
    ctx.expect_eq(
        S,
        "bitfield",
        &mut c,
        &["BITFIELD", &bf, "INCRBY", "i5", "100", "1", "GET", "u4", "0"],
        arr(vec![
            ferredis_core::resp::RespValue::Integer(1),
            ferredis_core::resp::RespValue::Integer(0),
        ]),
    )
    .await;

    // HyperLogLog
    let h = ctx.unique("hll");
    ctx.expect_int(S, "pfadd", &mut c, &["PFADD", &h, "a", "b", "c"], 1)
        .await;
    ctx.expect_int(S, "pfcount", &mut c, &["PFCOUNT", &h], 3).await;
    let h2 = ctx.unique("hll2");
    ctx.expect_int(S, "pfadd2", &mut c, &["PFADD", &h2, "c", "d"], 1)
        .await;
    let hm = ctx.unique("hllm");
    ctx.expect_ok(S, "pfmerge", &mut c, &["PFMERGE", &hm, &h, &h2])
        .await;
    ctx.expect_pred(S, "pfcount_merged", &mut c, &["PFCOUNT", &hm], |v| match v {
        RespValue::Integer(n) if *n >= 3 && *n <= 5 => Ok(()),
        _ => Err("PFCOUNT merge ≈ 4".into()),
    })
    .await;

    // Geo — exemples redis.io
    let g = ctx.unique("geo");
    ctx.expect_int(
        S,
        "geoadd",
        &mut c,
        &[
            "GEOADD", &g, "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ],
        2,
    )
    .await;
    ctx.expect_pred(
        S,
        "geodist",
        &mut c,
        &["GEODIST", &g, "Palermo", "Catania", "km"],
        |v| match v {
            RespValue::BulkString(Some(b)) => {
                let d: f64 = String::from_utf8_lossy(b).parse().map_err(|e| format!("{e}"))?;
                // Redis doc ≈ 166.2742 km
                if (160.0..175.0).contains(&d) {
                    Ok(())
                } else {
                    Err(format!("GEODIST Palermo-Catania ≈166km, got {d}"))
                }
            }
            _ => Err("GEODIST → bulk float".into()),
        },
    )
    .await;
    ctx.expect_pred(S, "geopos", &mut c, &["GEOPOS", &g, "Palermo"], |v| match v {
        RespValue::Array(Some(items)) if items.len() == 1 => Ok(()),
        _ => Err("GEOPOS → [[lon,lat]]".into()),
    })
    .await;
    ctx.expect_pred(
        S,
        "geohash",
        &mut c,
        &["GEOHASH", &g, "Palermo"],
        |v| match v {
            RespValue::Array(Some(items)) if items.len() == 1 => Ok(()),
            _ => Err("GEOHASH → [hash]".into()),
        },
    )
    .await;
    ctx.expect_pred(
        S,
        "geosearch",
        &mut c,
        &[
            "GEOSEARCH", &g, "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km",
        ],
        |v| match v {
            RespValue::Array(Some(items)) if items.len() >= 1 => Ok(()),
            _ => Err("GEOSEARCH → members".into()),
        },
    )
    .await;
}
