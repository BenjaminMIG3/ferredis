//! Burn test Redis Open Source 8.0 — source de vérité = comportement Redis officiel.
//!
//! **Règle :** les attentes ne sont jamais adaptées aux particularités Ferredis.
//! Tout écart (commande absente, mauvaise valeur, erreur non conforme) est un gap.
//!
//! Lancer (ignoré par défaut pour ne pas casser la CI tant que des gaps existent) :
//! ```text
//! cargo test -p ferredis-compat burn::burn_redis_official -- --ignored --nocapture
//! ```
//!
//! - `FERREDIS_BURN_REPORT=1` : imprime le rapport même si 0 gap
//! - `FERREDIS_BURN_ALLOW_GAPS=1` : affiche le rapport sans faire échouer le test

mod bitmaps_geo_hll;
mod harness;
mod hashes;
mod inventory;
mod keys_expire;
mod lists;
mod load;
mod multidb;
mod presence;
mod pubsub;
mod scripting;
mod server_conn;
mod sets;
mod stack;
mod streams;
mod strings;
mod transactions;
mod zsets;

use harness::BurnCtx;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "burn Redis 8 officiel — lancer avec --ignored --nocapture"]
async fn burn_redis_official() {
    let run = async {
        let mut ctx = BurnCtx::new().await;

        // Ordre : inventaire → familles de données → multi-DB → scripts → stack → charge.
        presence::run(&mut ctx).await;
        strings::run(&mut ctx).await;
        hashes::run(&mut ctx).await;
        lists::run(&mut ctx).await;
        sets::run(&mut ctx).await;
        zsets::run(&mut ctx).await;
        streams::run(&mut ctx).await;
        bitmaps_geo_hll::run(&mut ctx).await;
        keys_expire::run(&mut ctx).await;
        multidb::run(&mut ctx).await;
        transactions::run(&mut ctx).await;
        pubsub::run(&mut ctx).await;
        scripting::run(&mut ctx).await;
        server_conn::run(&mut ctx).await;
        stack::run(&mut ctx).await;
        load::run(&mut ctx).await;

        ctx
    };

    let ctx = tokio::time::timeout(std::time::Duration::from_secs(180), run)
        .await
        .expect("burn Redis-officiel : timeout global 180s (blocage probable type MONITOR/SYNC/SUBSCRIBE)");

    let summary = ctx.report.summary();
    let fail = ctx.report.fail_count();
    let pass = ctx.report.pass_count();

    eprintln!("{summary}");
    if std::env::var_os("FERREDIS_BURN_REPORT").is_some() {
        println!("{summary}");
    }

    if fail == 0 {
        return;
    }
    if std::env::var_os("FERREDIS_BURN_ALLOW_GAPS").is_some() {
        eprintln!("FERREDIS_BURN_ALLOW_GAPS=1 : {fail} gap(s) toléré(s) ({pass} ok).");
        return;
    }
    panic!(
        "Burn Redis-officiel : {fail} gap(s) / {pass} ok — voir rapport stderr. Les attentes suivent Redis 8, pas Ferredis."
    );
}
