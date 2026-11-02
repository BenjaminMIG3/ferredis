//! Présence des commandes Redis 8 via `COMMAND INFO` (sonde sûre, pas de mode MONITOR/SYNC).

use super::harness::BurnCtx;
use super::inventory::REDIS_8_COMMANDS;
use ferredis_core::resp::RespValue;
use std::time::Instant;

/// Redis officiel : `COMMAND INFO <name>`
/// - commande connue → tableau à 1 élément (détails)
/// - inconnue → tableau à 1 élément null (`*-1` dans l'entrée)
pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "presence";
    let mut sock = ctx.connect().await;

    for &cmd in REDIS_8_COMMANDS {
        let t0 = Instant::now();
        match ctx.cmd(&mut sock, &["COMMAND", "INFO", cmd]).await {
            Ok(RespValue::Array(Some(items))) => {
                let known = match items.first() {
                    Some(RespValue::Array(Some(_))) => true,
                    Some(RespValue::Array(None)) | Some(RespValue::Null) | None => false,
                    Some(RespValue::BulkString(None)) => false,
                    // Certains serveurs renvoient directement le descripteur
                    Some(_) => true,
                };
                ctx.check(
                    S,
                    format!("command_exists:{cmd}"),
                    t0.elapsed(),
                    known,
                    format!(
                        "Redis 8 expose `{cmd}` ; COMMAND INFO ne renvoie pas de descripteur → gap"
                    ),
                );
            }
            Ok(other) => {
                // Fallback : si COMMAND INFO non supporté, sonde d'arité sur connexion jetable
                // (évite MONITOR/SYNC/SUBSCRIBE qui capturent la session).
                let dangerous = matches!(
                    cmd,
                    "MONITOR"
                        | "SYNC"
                        | "PSYNC"
                        | "SUBSCRIBE"
                        | "PSUBSCRIBE"
                        | "SSUBSCRIBE"
                        | "DEBUG"
                );
                if dangerous {
                    ctx.check(
                        S,
                        format!("command_exists:{cmd}"),
                        t0.elapsed(),
                        false,
                        format!(
                            "COMMAND INFO indisponible ({other:?}) et `{cmd}` non sondable sans capturer la session"
                        ),
                    );
                    continue;
                }
                let mut probe = ctx.connect().await;
                match ctx.cmd(&mut probe, &[cmd]).await {
                    Ok(resp) => {
                        let unknown = matches!(&resp, RespValue::Error(e) if String::from_utf8_lossy(e).to_ascii_lowercase().contains("unknown command"));
                        ctx.check(
                            S,
                            format!("command_exists:{cmd}"),
                            t0.elapsed(),
                            !unknown,
                            format!("Redis 8 expose `{cmd}` ; réponse sonde: {resp:?}"),
                        );
                    }
                    Err(e) => ctx.err_case(S, format!("command_exists:{cmd}"), t0.elapsed(), e),
                }
            }
            Err(e) => ctx.err_case(S, format!("command_exists:{cmd}"), t0.elapsed(), e),
        }
    }

    ctx.expect_pred(
        S,
        "command_list_includes_get",
        &mut sock,
        &["COMMAND", "LIST"],
        |v| match v {
            RespValue::Array(Some(items)) => {
                let names: Vec<String> = items
                    .iter()
                    .filter_map(|i| match i {
                        RespValue::BulkString(Some(b)) => {
                            Some(String::from_utf8_lossy(b).to_ascii_lowercase())
                        }
                        RespValue::SimpleString(b) => {
                            Some(String::from_utf8_lossy(b).to_ascii_lowercase())
                        }
                        _ => None,
                    })
                    .collect();
                if names.iter().any(|n| n == "get") && names.iter().any(|n| n == "set") {
                    Ok(())
                } else {
                    Err("COMMAND LIST Redis doit inclure get et set".into())
                }
            }
            _ => Err("COMMAND LIST doit retourner un tableau".into()),
        },
    )
    .await;
}
