//! Multi-database Redis : SELECT / MOVE / SWAPDB / FLUSHDB / isolation.

use super::harness::BurnCtx;
use ferredis_core::resp::RespValue;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "multidb";
    let mut c = ctx.connect().await;

    // Isolation SELECT
    let k = ctx.unique("mdb");
    ctx.expect_ok(S, "select0", &mut c, &["SELECT", "0"]).await;
    ctx.expect_ok(S, "set_db0", &mut c, &["SET", &k, "db0"]).await;
    ctx.expect_ok(S, "select1", &mut c, &["SELECT", "1"]).await;
    ctx.expect_null_bulk(S, "get_isolated", &mut c, &["GET", &k]).await;
    ctx.expect_ok(S, "set_db1", &mut c, &["SET", &k, "db1"]).await;
    ctx.expect_bulk(S, "get_db1", &mut c, &["GET", &k], "db1").await;
    ctx.expect_ok(S, "select0_back", &mut c, &["SELECT", "0"]).await;
    ctx.expect_bulk(S, "get_db0", &mut c, &["GET", &k], "db0").await;

    // MOVE
    let m = ctx.unique("move");
    ctx.expect_ok(S, "move_prep", &mut c, &["SET", &m, "x"]).await;
    ctx.expect_int(S, "move", &mut c, &["MOVE", &m, "2"], 1).await;
    ctx.expect_null_bulk(S, "move_gone_src", &mut c, &["GET", &m]).await;
    ctx.expect_ok(S, "select2", &mut c, &["SELECT", "2"]).await;
    ctx.expect_bulk(S, "move_in_dst", &mut c, &["GET", &m], "x").await;
    // Redis : MOVE vers la DB courante → erreur (pas 0).
    ctx.expect_error_contains(
        S,
        "move_same_db",
        &mut c,
        &["MOVE", &m, "2"],
        "same",
    )
    .await;

    // FLUSHDB only current
    ctx.expect_ok(S, "flushdb", &mut c, &["FLUSHDB"]).await;
    ctx.expect_int(S, "dbsize_after_flushdb", &mut c, &["DBSIZE"], 0)
        .await;
    ctx.expect_ok(S, "select0_check", &mut c, &["SELECT", "0"]).await;
    ctx.expect_bulk(S, "db0_survives_flushdb1", &mut c, &["GET", &k], "db0")
        .await;

    // SWAPDB
    let s0 = ctx.unique("swap0");
    let s1 = ctx.unique("swap1");
    ctx.expect_ok(S, "swap_prep0", &mut c, &["SET", &s0, "A"]).await;
    ctx.expect_ok(S, "select_for_swap1", &mut c, &["SELECT", "1"]).await;
    // nettoyer db1 puis poser s1
    let _ = ctx.cmd(&mut c, &["FLUSHDB"]).await;
    ctx.expect_ok(S, "swap_prep1", &mut c, &["SET", &s1, "B"]).await;
    ctx.expect_ok(S, "select0_swap", &mut c, &["SELECT", "0"]).await;
    ctx.expect_ok(S, "swapdb", &mut c, &["SWAPDB", "0", "1"]).await;
    // Après SWAPDB 0↔1 : clé s1 (posée en db1) doit être visible en db0
    ctx.expect_bulk(S, "swap_see_b", &mut c, &["GET", &s1], "B").await;
    ctx.expect_null_bulk(S, "swap_a_gone", &mut c, &["GET", &s0]).await;
    ctx.expect_ok(S, "select1_swap", &mut c, &["SELECT", "1"]).await;
    ctx.expect_bulk(S, "swap_see_a", &mut c, &["GET", &s0], "A").await;

    // SELECT invalide (Redis default databases=16 → 0..15)
    ctx.expect_ok(S, "select0_reset", &mut c, &["SELECT", "0"]).await;
    ctx.expect_error_contains(S, "select_oob", &mut c, &["SELECT", "999"], "out of range")
        .await;

    // Deux clients, DBs indépendantes
    let mut a = ctx.connect().await;
    let mut b = ctx.connect().await;
    let shared = ctx.unique("shared");
    ctx.expect_ok(S, "cli_a_sel1", &mut a, &["SELECT", "3"]).await;
    ctx.expect_ok(S, "cli_b_sel4", &mut b, &["SELECT", "4"]).await;
    ctx.expect_ok(S, "cli_a_set", &mut a, &["SET", &shared, "from-a"])
        .await;
    ctx.expect_null_bulk(S, "cli_b_isolated", &mut b, &["GET", &shared])
        .await;
    ctx.expect_ok(S, "cli_b_set", &mut b, &["SET", &shared, "from-b"])
        .await;
    ctx.expect_bulk(S, "cli_a_own", &mut a, &["GET", &shared], "from-a")
        .await;
    ctx.expect_bulk(S, "cli_b_own", &mut b, &["GET", &shared], "from-b")
        .await;

    // FLUSHALL
    ctx.expect_ok(S, "flushall", &mut c, &["FLUSHALL"]).await;
    ctx.expect_ok(S, "sel0_after_flushall", &mut c, &["SELECT", "0"])
        .await;
    ctx.expect_int(S, "dbsize_flushall", &mut c, &["DBSIZE"], 0).await;
    ctx.expect_ok(S, "sel3_after_flushall", &mut c, &["SELECT", "3"])
        .await;
    ctx.expect_int(S, "db3_empty", &mut c, &["DBSIZE"], 0).await;

    // INFO keyspace après écriture
    ctx.expect_ok(S, "sel0_info", &mut c, &["SELECT", "0"]).await;
    ctx.expect_ok(S, "info_prep", &mut c, &["SET", &ctx.unique("ik"), "1"])
        .await;
    ctx.expect_pred(S, "info_keyspace", &mut c, &["INFO", "keyspace"], |v| {
        let text = match v {
            RespValue::BulkString(Some(b)) | RespValue::SimpleString(b) | RespValue::Verbatim { data: b, .. } => {
                String::from_utf8_lossy(b).into_owned()
            }
            _ => return Err("INFO keyspace → bulk/verbatim".into()),
        };
        if text.contains("db0:") {
            Ok(())
        } else {
            Err(format!("INFO keyspace Redis contient db0:, got: {text}"))
        }
    })
    .await;
}
