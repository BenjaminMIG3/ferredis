//! Pub/Sub : PUBLISH / SPUBLISH, (P|S)SUBSCRIBE, (P|S)UNSUBSCRIBE et PUBSUB.

use crate::command::{wrong_arity, ClientState, CommandContext};
use crate::pubsub::{PubSubEvent, SubKind};
use crate::resp::RespValue;
use crate::util::{bulk_array, err, upper};
use bytes::Bytes;

fn verb(v: &'static str) -> RespValue {
    RespValue::bulk(Bytes::from_static(v.as_bytes()))
}

pub(crate) fn cmd_publish(ctx: &CommandContext, _: &mut ClientState, args: &[Bytes]) -> RespValue {
    RespValue::Integer(ctx.pubsub.publish(&args[1], args[2].clone()))
}

pub(crate) fn cmd_spublish(ctx: &CommandContext, _: &mut ClientState, args: &[Bytes]) -> RespValue {
    RespValue::Integer(ctx.pubsub.spublish(&args[1], args[2].clone()))
}

/// SUBSCRIBE / PSUBSCRIBE / SSUBSCRIBE : une confirmation par nom.
pub(crate) fn subscribe(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    kind: SubKind,
) -> Vec<RespValue> {
    if kind == SubKind::Shard && state.deny_blocking {
        return vec![err("ERR SSUBSCRIBE isn't allowed for a DENY BLOCKING client")];
    }
    let id = *state.pubsub_id.get_or_insert_with(|| ctx.pubsub.register().0);
    args[1..]
        .iter()
        .map(|name| {
            let count = ctx.pubsub.subscribe(kind, id, name.clone());
            RespValue::array(vec![
                verb(kind.subscribe_verb()),
                RespValue::bulk(name.clone()),
                RespValue::Integer(count),
            ])
        })
        .collect()
}

/// UNSUBSCRIBE / PUNSUBSCRIBE / SUNSUBSCRIBE (sans argument : tous les abonnements du type).
pub(crate) fn unsubscribe(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    kind: SubKind,
) -> Vec<RespValue> {
    let results = match state.pubsub_id {
        None => Vec::new(),
        Some(id) if args.len() <= 1 => ctx.pubsub.unsubscribe(kind, id, None),
        Some(id) => args[1..]
            .iter()
            .flat_map(|n| ctx.pubsub.unsubscribe(kind, id, Some(n.clone())))
            .collect(),
    };
    if results.is_empty() {
        return vec![RespValue::array(vec![
            verb(kind.unsubscribe_verb()),
            RespValue::null_bulk(),
            RespValue::Integer(0),
        ])];
    }
    results
        .into_iter()
        .map(|(name, count)| {
            RespValue::array(vec![
                verb(kind.unsubscribe_verb()),
                RespValue::bulk(name),
                RespValue::Integer(count),
            ])
        })
        .collect()
}

fn numsub(ctx: &CommandContext, args: &[Bytes], kind: SubKind) -> RespValue {
    let mut out = Vec::with_capacity((args.len() - 2) * 2);
    for ch in &args[2..] {
        out.push(RespValue::bulk(ch.clone()));
        out.push(RespValue::Integer(ctx.pubsub.numsub(kind, ch) as i64));
    }
    RespValue::array(out)
}

/// PUBSUB CHANNELS|NUMSUB|NUMPAT|SHARDCHANNELS|SHARDNUMSUB|HELP.
pub(crate) fn cmd_pubsub(ctx: &CommandContext, _: &mut ClientState, args: &[Bytes]) -> RespValue {
    let sub = upper(&args[1]);
    let pattern = || args.get(2).map(|b| b.as_ref());
    match (sub.as_str(), args.len()) {
        ("CHANNELS", 2 | 3) => bulk_array(ctx.pubsub.active(SubKind::Channel, pattern())),
        ("SHARDCHANNELS", 2 | 3) => bulk_array(ctx.pubsub.active(SubKind::Shard, pattern())),
        ("NUMSUB", _) => numsub(ctx, args, SubKind::Channel),
        ("SHARDNUMSUB", _) => numsub(ctx, args, SubKind::Shard),
        ("NUMPAT", 2) => RespValue::Integer(ctx.pubsub.count(SubKind::Pattern) as i64),
        ("HELP", 2) => bulk_array(
            [
                "PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "CHANNELS [<pattern>]",
                "NUMPAT",
                "NUMSUB [<channel> ...]",
                "SHARDCHANNELS [<pattern>]",
                "SHARDNUMSUB [<shardchannel> ...]",
                "HELP",
            ]
            .into_iter()
            .map(|s| Bytes::from_static(s.as_bytes())),
        ),
        ("CHANNELS" | "SHARDCHANNELS" | "NUMPAT" | "HELP", _) => {
            wrong_arity(&format!("pubsub|{}", sub.to_ascii_lowercase()))
        }
        _ => RespValue::err(format!(
            "ERR unknown subcommand '{}'. Try PUBSUB HELP.",
            String::from_utf8_lossy(&args[1])
        )),
    }
}

/// Message poussé vers un abonné (RESP2 array / RESP3 push).
pub fn format_pubsub_event(ev: &PubSubEvent) -> RespValue {
    format_pubsub_event_proto(ev, 2)
}

pub fn format_pubsub_event_proto(ev: &PubSubEvent, proto: u8) -> RespValue {
    let items = match ev {
        PubSubEvent::Message { channel, payload } => vec![
            verb("message"),
            RespValue::bulk(channel.clone()),
            RespValue::bulk(payload.clone()),
        ],
        PubSubEvent::SMessage { channel, payload } => vec![
            verb("smessage"),
            RespValue::bulk(channel.clone()),
            RespValue::bulk(payload.clone()),
        ],
        PubSubEvent::PMessage {
            pattern,
            channel,
            payload,
        } => vec![
            verb("pmessage"),
            RespValue::bulk(pattern.clone()),
            RespValue::bulk(channel.clone()),
            RespValue::bulk(payload.clone()),
        ],
    };
    if proto >= 3 {
        RespValue::push(items)
    } else {
        RespValue::array(items)
    }
}
