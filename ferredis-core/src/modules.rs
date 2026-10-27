//! Modules Stack embarqués + commande `MODULE`.

use crate::command::{arg_str, wrong_arity, ClientState, CommandContext};
use crate::resp::RespValue;
use crate::util::*;
use bytes::Bytes;

/// Modules internes exposés par `MODULE LIST` (parité Redis 8 Stack embarqué).
pub const EMBEDDED_MODULES: &[(&str, &str)] = &[
    ("ReJSON", "2.8.0"),
    ("timeseries", "1.12.0"),
    ("search", "2.10.0"),
    ("bf", "2.8.0"),
    ("cf", "2.8.0"),
    ("cms", "2.8.0"),
    ("topk", "2.8.0"),
    ("tdigest", "2.8.0"),
    ("vectorset", "1.0.0"),
];

/// MODULE LIST | LOAD | LOADEX | UNLOAD | HELP
pub(crate) fn cmd_module(
    _ctx: &CommandContext,
    _state: &mut ClientState,
    args: &[Bytes],
) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("module");
    }
    match upper(&args[1]).as_str() {
        "HELP" => RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(
                b"MODULE <subcommand> [<arg> ...]. Subcommands are:",
            )),
            RespValue::bulk(Bytes::from_static(
                b"LIST -- lists embedded Stack modules (ReJSON, timeseries, search, bf, ...)",
            )),
            RespValue::bulk(Bytes::from_static(
                b"LOAD path [args] -- external .so/.dll loading is not supported yet",
            )),
            RespValue::bulk(Bytes::from_static(
                b"LOADEX path [CONFIG name value ...] [ARGS ...] -- not supported yet",
            )),
            RespValue::bulk(Bytes::from_static(
                b"UNLOAD name -- unloading embedded modules is not supported",
            )),
            RespValue::bulk(Bytes::from_static(b"HELP")),
        ]),
        "LIST" => {
            let items: Vec<RespValue> = EMBEDDED_MODULES
                .iter()
                .map(|(name, ver)| {
                    RespValue::array(vec![
                        RespValue::bulk(Bytes::from_static(b"name")),
                        RespValue::bulk(Bytes::from(*name)),
                        RespValue::bulk(Bytes::from_static(b"ver")),
                        RespValue::Integer(parse_module_ver(ver)),
                    ])
                })
                .collect();
            RespValue::array(items)
        }
        "LOAD" | "LOADEX" => err(
            "ERR Ferredis embeds Redis Stack modules; loading external module libraries is not supported",
        ),
        "UNLOAD" => {
            let name = arg_str(args, 2).unwrap_or("");
            if EMBEDDED_MODULES
                .iter()
                .any(|(n, _)| n.eq_ignore_ascii_case(name))
            {
                err("ERR cannot unload embedded Ferredis Stack modules")
            } else {
                err("ERR cannot unload module that is not loaded")
            }
        }
        _ => err("ERR unknown subcommand for MODULE"),
    }
}

fn parse_module_ver(v: &str) -> i64 {
    // Redis MODULE LIST ver = major*10000 + minor*100 + patch
    let mut parts = v.split('.');
    let major: i64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor: i64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let patch: i64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    major * 10_000 + minor * 100 + patch
}
