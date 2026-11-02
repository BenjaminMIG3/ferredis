//! Commandes sur les sorted sets : ZADD complet, intervalles génériques (rang / score / lex),
//! pops, opérations ensemblistes (union / inter / diff / card) et tirages aléatoires.

use crate::command::{ClientState, CommandContext};
use crate::opts::{ranged_long, ERR_LIMIT_NEGATIVE};
use crate::resp::RespValue;
use crate::store::{paginate, DbInner, WRONGTYPE};
use crate::util::*;
use crate::value::{Entry, LexBound, RedisValue, Score, ScoreBound, ZRange, ZSet};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};

const ERR_NOT_FLOAT_RANGE: &str = "ERR min or max is not a float";
const ERR_NOT_LEX_RANGE: &str = "ERR min or max not valid string range item";

fn score_bound(arg: &[u8]) -> Result<ScoreBound, RespValue> {
    ScoreBound::parse(arg).ok_or_else(|| err(ERR_NOT_FLOAT_RANGE))
}

fn lex_bound(arg: &[u8]) -> Result<LexBound, RespValue> {
    LexBound::parse(arg).ok_or_else(|| err(ERR_NOT_LEX_RANGE))
}

fn int_arg(arg: &[u8]) -> Result<i64, RespValue> {
    parse_i64(arg).ok_or_else(|| err(ERR_INT))
}

fn notify_added(ctx: &CommandContext, state: &ClientState, key: &Bytes, event: &str) {
    ctx.db.touch();
    ctx.waiters.notify(state.db_index, key);
    ctx.notify_key(state.db_index, key, event);
}

/// Options de ZADD.
#[derive(Default, Clone, Copy)]
pub(crate) struct ZaddOpts {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
    pub ch: bool,
    pub incr: bool,
}

/// Cœur de ZADD / ZINCRBY / GEOADD. Retourne la réponse prête à émettre.
pub(crate) fn zadd_impl(
    ctx: &CommandContext,
    state: &ClientState,
    key: &Bytes,
    opts: ZaddOpts,
    pairs: &[(f64, Bytes)],
) -> RespValue {
    let r: Result<(i64, i64, Option<f64>), &'static str> = ctx.db.with_db(state.db_index, |d| {
        let z = d.zset_or_create(key)?;
        let (mut added, mut changed, mut last) = (0i64, 0i64, None);
        for (score, member) in pairs {
            match z.score(member) {
                None => {
                    if opts.xx {
                        continue;
                    }
                    z.insert(member.clone(), *score);
                    added += 1;
                    changed += 1;
                    last = Some(*score);
                }
                Some(old) => {
                    if opts.nx {
                        continue;
                    }
                    let next = if opts.incr { old + score } else { *score };
                    if next.is_nan() {
                        d.remove_if_empty(key);
                        return Err("ERR resulting score is not a number (NaN)");
                    }
                    if (opts.gt && next <= old) || (opts.lt && next >= old) {
                        continue;
                    }
                    if next != old {
                        z.insert(member.clone(), next);
                        changed += 1;
                    }
                    last = Some(next);
                }
            }
        }
        d.remove_if_empty(key);
        Ok((added, changed, last))
    });
    match r {
        Err(e) => err(e),
        Ok((added, changed, last)) => {
            if changed > 0 {
                notify_added(ctx, state, key, if opts.incr { "zincr" } else { "zadd" });
                ctx.propagate(&aof_zadd(key, pairs, opts));
            }
            if opts.incr {
                last.map_or_else(RespValue::null_bulk, bulk_f64)
            } else {
                RespValue::Integer(if opts.ch { changed } else { added })
            }
        }
    }
}

/// Rejeu AOF : la commande est réécrite avec ses options et les scores normalisés.
fn aof_zadd(key: &Bytes, pairs: &[(f64, Bytes)], opts: ZaddOpts) -> Vec<Bytes> {
    let mut out = vec![Bytes::from_static(b"ZADD")];
    for (on, flag) in [
        (opts.nx, &b"NX"[..]),
        (opts.xx, b"XX"),
        (opts.gt, b"GT"),
        (opts.lt, b"LT"),
        (opts.ch, b"CH"),
        (opts.incr, b"INCR"),
    ] {
        if on {
            out.push(Bytes::from_static(flag));
        }
    }
    out.push(key.clone());
    for (s, m) in pairs {
        out.push(Bytes::from(fmt_f64(*s)));
        out.push(m.clone());
    }
    out
}

/// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member …].
pub(crate) fn cmd_zadd(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let mut opts = ZaddOpts::default();
    let mut i = 2;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "NX" => opts.nx = true,
            "XX" => opts.xx = true,
            "GT" => opts.gt = true,
            "LT" => opts.lt = true,
            "CH" => opts.ch = true,
            "INCR" => opts.incr = true,
            _ => break,
        }
        i += 1;
    }
    if opts.nx && opts.xx {
        return err("ERR XX and NX options at the same time are not compatible");
    }
    if (opts.gt && opts.lt) || (opts.nx && (opts.gt || opts.lt)) {
        return err("ERR GT, LT, and/or NX options at the same time are not compatible");
    }
    let rest = &args[i..];
    if rest.is_empty() || rest.len() % 2 != 0 {
        return err(ERR_SYNTAX);
    }
    if opts.incr && rest.len() != 2 {
        return err("ERR INCR option supports a single increment-element pair");
    }
    let mut pairs = Vec::with_capacity(rest.len() / 2);
    for chunk in rest.chunks(2) {
        let Some(score) = parse_f64(&chunk[0]) else {
            return err(ERR_FLOAT);
        };
        pairs.push((score, chunk[1].clone()));
    }
    zadd_impl(ctx, state, &args[1], opts, &pairs)
}

/// ZINCRBY key increment member.
pub(crate) fn cmd_zincrby(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let Some(delta) = parse_f64(&args[2]) else {
        return err(ERR_FLOAT);
    };
    let opts = ZaddOpts { incr: true, ..Default::default() };
    zadd_impl(ctx, state, &args[1], opts, &[(delta, args[3].clone())])
}

// ----------------------------- intervalles -----------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RangeKind {
    Rank,
    Score,
    Lex,
}

fn parse_range(kind: RangeKind, min: &[u8], max: &[u8]) -> Result<ZRange, RespValue> {
    Ok(match kind {
        RangeKind::Rank => ZRange::Rank(int_arg(min)?, int_arg(max)?),
        RangeKind::Score => ZRange::Score(score_bound(min)?, score_bound(max)?),
        RangeKind::Lex => ZRange::Lex(lex_bound(min)?, lex_bound(max)?),
    })
}

/// Variante de la famille ZRANGE (paramètres figés de `zrangeGenericCommand`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ZRangeCmd {
    Range,
    RangeStore,
    RangeByScore,
    RevRangeByScore,
    RangeByLex,
    RevRangeByLex,
    RevRange,
}

impl ZRangeCmd {
    /// `(type imposé, sens imposé)` — `None` = choisi par les options.
    fn fixed(self) -> (Option<RangeKind>, Option<bool>) {
        match self {
            Self::Range | Self::RangeStore => (None, None),
            Self::RangeByScore => (Some(RangeKind::Score), Some(false)),
            Self::RevRangeByScore => (Some(RangeKind::Score), Some(true)),
            Self::RangeByLex => (Some(RangeKind::Lex), Some(false)),
            Self::RevRangeByLex => (Some(RangeKind::Lex), Some(true)),
            Self::RevRange => (Some(RangeKind::Rank), Some(true)),
        }
    }
}

/// Fenêtre LIMIT : offset négatif ? vide, limite négative ? illimitée.
fn limit_window(offset: i64, limit: i64) -> Option<(usize, Option<usize>)> {
    if offset < 0 {
        return None;
    }
    Some((offset as usize, (limit >= 0).then_some(limit as usize)))
}

/// ZRANGE / ZRANGESTORE / Z[REV]RANGEBY{SCORE,LEX} / ZREVRANGE.
pub(crate) fn cmd_zrange_generic(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    which: ZRangeCmd,
) -> RespValue {
    let store = which == ZRangeCmd::RangeStore;
    let start = if store { 2 } else { 1 };
    let (fixed_kind, fixed_rev) = which.fixed();
    let (mut kind, mut rev) = (fixed_kind, fixed_rev);
    let (mut withscores, mut offset, mut limit) = (false, 0i64, -1i64);
    let mut j = start + 3;
    while j < args.len() {
        let opt = upper(&args[j]);
        let left = args.len() - j - 1;
        match opt.as_str() {
            "WITHSCORES" if !store => withscores = true,
            "LIMIT" if left >= 2 => {
                offset = match int_arg(&args[j + 1]) {
                    Ok(v) => v,
                    Err(e) => return e,
                };
                limit = match int_arg(&args[j + 2]) {
                    Ok(v) => v,
                    Err(e) => return e,
                };
                j += 2;
            }
            "REV" if fixed_rev.is_none() && rev.is_none() => rev = Some(true),
            "BYLEX" if fixed_kind.is_none() && kind.is_none() => kind = Some(RangeKind::Lex),
            "BYSCORE" if fixed_kind.is_none() && kind.is_none() => kind = Some(RangeKind::Score),
            _ => return err(ERR_SYNTAX),
        }
        j += 1;
    }
    let kind = kind.unwrap_or(RangeKind::Rank);
    let rev = rev.unwrap_or(false);
    if limit != -1 && kind == RangeKind::Rank {
        return err("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
    }
    if withscores && kind == RangeKind::Lex {
        return err("ERR syntax error, WITHSCORES not supported in combination with BYLEX");
    }
    let (mut min, mut max) = (&args[start + 1], &args[start + 2]);
    if rev && kind != RangeKind::Rank {
        std::mem::swap(&mut min, &mut max);
    }
    let range = match parse_range(kind, min, max) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let window = if kind == RangeKind::Rank { Some((0, None)) } else { limit_window(offset, limit) };
    let src = &args[start];
    let r: Result<Vec<(Bytes, f64)>, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(z) = d.zset_mut(src)? else {
            return Ok(Vec::new());
        };
        Ok(window.map_or_else(Vec::new, |(off, count)| z.range(&range, rev, off, count)))
    });
    let pairs = match r {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    if store {
        return store_pairs(ctx, state, &args[1], pairs, "zrangestore", args);
    }
    pairs_to_resp(pairs, withscores)
}

/// Écrit `pairs` dans `dst` (supprimé si vide) ; retourne la cardinalité.
fn store_pairs(
    ctx: &CommandContext,
    state: &ClientState,
    dst: &Bytes,
    pairs: Vec<(Bytes, f64)>,
    event: &str,
    args: &[Bytes],
) -> RespValue {
    let n = pairs.len() as i64;
    let existed = ctx.db.with_db(state.db_index, |d| {
        let existed = d.map.remove(dst).is_some_and(|e| !e.is_expired());
        if !pairs.is_empty() {
            let mut z = ZSet::default();
            for (m, s) in pairs {
                z.insert(m, s);
            }
            d.map.insert(dst.clone(), Entry::new(RedisValue::ZSet(z)));
        }
        existed
    });
    if n > 0 {
        notify_added(ctx, state, dst, event);
        ctx.propagate(args);
    } else if existed {
        ctx.db.touch();
        ctx.notify_key(state.db_index, dst, "del");
        ctx.propagate(&[Bytes::from_static(b"DEL"), dst.clone()]);
    }
    RespValue::Integer(n)
}

/// ZCOUNT key min max / ZLEXCOUNT key min max.
pub(crate) fn cmd_zcount_generic(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    kind: RangeKind,
) -> RespValue {
    let range = match parse_range(kind, &args[2], &args[3]) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let r: Result<i64, &'static str> = ctx.db.with_db(state.db_index, |d| {
        Ok(d.zset_mut(&args[1])?.map_or(0, |z| z.count_in(&range) as i64))
    });
    reply(r.map(RespValue::Integer))
}

/// ZREMRANGEBYRANK / ZREMRANGEBYSCORE / ZREMRANGEBYLEX key min max.
pub(crate) fn cmd_zremrange(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    kind: RangeKind,
) -> RespValue {
    let range = match parse_range(kind, &args[2], &args[3]) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let key = &args[1];
    let r: Result<(i64, bool), &'static str> = ctx.db.with_db(state.db_index, |d| {
        let n = d.zset_mut(key)?.map_or(0, |z| z.remove_in(&range) as i64);
        d.remove_if_empty(key);
        Ok((n, n > 0 && !d.map.contains_key(key)))
    });
    match r {
        Err(e) => err(e),
        Ok((n, deleted)) => {
            if n > 0 {
                let event = match kind {
                    RangeKind::Rank => "zremrangebyrank",
                    RangeKind::Score => "zremrangebyscore",
                    RangeKind::Lex => "zremrangebylex",
                };
                ctx.db.touch();
                ctx.notify_key(state.db_index, key, event);
                if deleted {
                    ctx.notify_key(state.db_index, key, "del");
                }
                ctx.propagate(args);
            }
            RespValue::Integer(n)
        }
    }
}

// ----------------------------- pops -----------------------------

/// ZPOPMIN / ZPOPMAX key [count].
pub(crate) fn cmd_zpop(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes], max: bool) -> RespValue {
    if args.len() > 3 {
        return err(ERR_SYNTAX);
    }
    let count = match args.get(2) {
        None => 1,
        Some(raw) => match parse_i64(raw) {
            Some(n) if n >= 0 => n as usize,
            _ => return err("ERR value is out of range, must be positive"),
        },
    };
    match zpop_impl(ctx, state.db_index, &args[1], max, count) {
        Err(e) => err(e),
        Ok(popped) => pairs_to_resp(popped, true),
    }
}

/// Retire jusqu'à `count` éléments extrêmes ; supprime la clé si le zset devient vide.
pub(crate) fn zpop_impl(
    ctx: &CommandContext,
    db: usize,
    key: &Bytes,
    max: bool,
    count: usize,
) -> Result<Vec<(Bytes, f64)>, &'static str> {
    let popped = ctx.db.with_db(db, |d| {
        let Some(z) = d.zset_mut(key)? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::with_capacity(count.min(z.len()));
        while out.len() < count {
            match z.pop(max) {
                Some(p) => out.push(p),
                None => break,
            }
        }
        d.remove_if_empty(key);
        Ok(out)
    })?;
    if !popped.is_empty() {
        ctx.db.touch();
        ctx.notify_key(db, key, if max { "zpopmax" } else { "zpopmin" });
        let mut aof = vec![Bytes::from_static(b"ZREM"), key.clone()];
        aof.extend(popped.iter().map(|(m, _)| m.clone()));
        ctx.propagate(&aof);
    }
    Ok(popped)
}

/// ZMSCORE key member [member …].
pub(crate) fn cmd_zmscore(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let r: Result<Vec<Option<f64>>, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let z = d.zset_mut(&args[1])?;
        Ok(args[2..]
            .iter()
            .map(|m| z.as_ref().and_then(|z| z.score(m)))
            .collect())
    });
    match r {
        Err(e) => err(e),
        Ok(v) => RespValue::array(
            v.into_iter()
                .map(|s| s.map_or_else(RespValue::null_bulk, bulk_f64))
                .collect(),
        ),
    }
}

/// ZRANK / ZREVRANK key member [WITHSCORE].
pub(crate) fn cmd_zrank(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes], rev: bool) -> RespValue {
    let withscore = match args.get(3) {
        None => false,
        Some(o) if args.len() == 4 && upper(o) == "WITHSCORE" => true,
        Some(_) => return err(ERR_SYNTAX),
    };
    let r: Result<Option<(usize, f64)>, &'static str> = ctx.db.with_db(state.db_index, |d| {
        Ok(d.zset_mut(&args[1])?.and_then(|z| {
            let score = z.score(&args[2])?;
            Some((z.rank(&args[2], rev)?, score))
        }))
    });
    match r {
        Err(e) => err(e),
        Ok(None) if withscore => RespValue::null_array(),
        Ok(None) => RespValue::null_bulk(),
        Ok(Some((rank, score))) if withscore => {
            RespValue::array(vec![RespValue::Integer(rank as i64), bulk_f64(score)])
        }
        Ok(Some((rank, _))) => RespValue::Integer(rank as i64),
    }
}

// ----------------------------- opérations ensemblistes -----------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

impl Aggregate {
    /// `zunionInterAggregate` : la somme NaN (inf + -inf) devient 0.
    fn combine(self, target: f64, val: f64) -> f64 {
        match self {
            Self::Sum => {
                let v = target + val;
                if v.is_nan() {
                    0.0
                } else {
                    v
                }
            }
            Self::Min => {
                if val < target {
                    val
                } else {
                    target
                }
            }
            Self::Max => {
                if val > target {
                    val
                } else {
                    target
                }
            }
        }
    }
}

/// Source d'une opération ensembliste : zset, set (score 1) ou clé absente.
enum ZSource<'a> {
    Empty,
    Z(&'a ZSet),
    /// Copie owned (intset → hashtable) pour itérer des `&Bytes` stables.
    S(HashSet<Bytes>),
}

impl<'a> ZSource<'a> {
    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Z(z) => z.len(),
            Self::S(s) => s.len(),
        }
    }

    fn get(&self, m: &Bytes) -> Option<f64> {
        match self {
            Self::Empty => None,
            Self::Z(z) => z.score(m),
            Self::S(s) => s.contains(m).then_some(1.0),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&Bytes, f64)> + '_> {
        match self {
            Self::Empty => Box::new(std::iter::empty()),
            Self::Z(z) => Box::new(z.members.iter().map(|(m, s)| (m, *s))),
            Self::S(s) => Box::new(s.iter().map(|m| (m, 1.0))),
        }
    }
}

/// Lit les sources après purge des clés expirées (WRONGTYPE si ni zset ni set).
fn load_sources<'a>(d: &'a mut DbInner, keys: &[Bytes]) -> Result<Vec<ZSource<'a>>, &'static str> {
    for k in keys {
        d.purge_if_expired(k);
    }
    let d: &'a DbInner = d;
    keys.iter()
        .map(|k| match d.map.get(k).map(|e| &e.value) {
            None => Ok(ZSource::Empty),
            Some(RedisValue::ZSet(z)) => Ok(ZSource::Z(z)),
            Some(RedisValue::Set(s)) => Ok(ZSource::S(s.as_hashset())),
            Some(_) => Err(WRONGTYPE),
        })
        .collect()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SetOp {
    Union,
    Inter,
    Diff,
}

/// Variante de commande : résultat renvoyé, stocké ou seulement compté.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SetOpOutput {
    Reply,
    Store,
    Card,
}

fn weighted(score: f64, w: f64) -> f64 {
    let v = score * w;
    if v.is_nan() {
        0.0
    } else {
        v
    }
}

/// Calcule l'opération (port de `zunionInterDiffGenericCommand`) ; `limit` (> 0) arrête
/// le comptage d'une intersection dès qu'il est atteint.
fn compute_setop(
    srcs: &[ZSource<'_>],
    weights: &[f64],
    op: SetOp,
    agg: Aggregate,
    card_limit: usize,
    count_only: bool,
) -> (HashMap<Bytes, f64>, usize) {
    let mut out: HashMap<Bytes, f64> = HashMap::new();
    match op {
        SetOp::Union => {
            for (src, w) in srcs.iter().zip(weights) {
                for (m, s) in src.iter() {
                    let v = weighted(s, *w);
                    out.entry(m.clone())
                        .and_modify(|cur| *cur = agg.combine(*cur, v))
                        .or_insert(v);
                }
            }
        }
        SetOp::Inter => {
            let mut order: Vec<usize> = (0..srcs.len()).collect();
            order.sort_by_key(|&i| srcs[i].len());
            let (first, rest) = order.split_first().expect("au moins une source");
            let mut card = 0usize;
            'members: for (m, s) in srcs[*first].iter() {
                let mut score = weighted(s, weights[*first]);
                for &j in rest {
                    let Some(v) = srcs[j].get(m) else {
                        continue 'members;
                    };
                    score = agg.combine(score, v * weights[j]);
                }
                if count_only {
                    card += 1;
                    if card_limit > 0 && card >= card_limit {
                        break;
                    }
                } else {
                    out.insert(m.clone(), score);
                }
            }
            if count_only {
                return (out, card);
            }
        }
        SetOp::Diff => {
            for (m, s) in srcs[0].iter() {
                if srcs[1..].iter().all(|o| o.get(m).is_none()) {
                    out.insert(m.clone(), s);
                }
            }
        }
    }
    let n = out.len();
    (out, n)
}

fn sorted_pairs(map: HashMap<Bytes, f64>) -> Vec<(Bytes, f64)> {
    let mut v: Vec<(Bytes, f64)> = map.into_iter().collect();
    v.sort_unstable_by(|a, b| Score::new(a.1).cmp(&Score::new(b.1)).then_with(|| a.0.cmp(&b.0)));
    v
}

/// ZUNION / ZINTER / ZDIFF [WITHSCORES], ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE, ZINTERCARD.
pub(crate) fn cmd_zsetop(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    op: SetOp,
    output: SetOpOutput,
) -> RespValue {
    let store = output == SetOpOutput::Store;
    let card = output == SetOpOutput::Card;
    let numkeys_idx = if store { 2 } else { 1 };
    let Some(setnum) = parse_i64(&args[numkeys_idx]) else {
        return err(ERR_INT);
    };
    if setnum < 1 {
        let name = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
        return err(&format!("ERR at least 1 input key is needed for '{name}' command"));
    }
    if setnum > (args.len() - numkeys_idx - 1) as i64 {
        return err(ERR_SYNTAX);
    }
    let setnum = setnum as usize;
    let keys = &args[numkeys_idx + 1..numkeys_idx + 1 + setnum];
    let opts = &args[numkeys_idx + 1 + setnum..];

    let done = ctx.db.with_db(state.db_index, |d| {
        let srcs = match load_sources(d, keys) {
            Ok(s) => s,
            Err(e) => return SetOpDone::Reply(err(e)),
        };
        let mut weights = vec![1.0f64; setnum];
        let (mut agg, mut withscores, mut limit) = (Aggregate::Sum, false, 0usize);
        let mut j = 0;
        while j < opts.len() {
            let remaining = opts.len() - j;
            let opt = upper(&opts[j]);
            let combinable = op != SetOp::Diff && !card;
            if combinable && remaining > setnum && opt == "WEIGHTS" {
                for (w, raw) in weights.iter_mut().zip(&opts[j + 1..=j + setnum]) {
                    match parse_f64(raw) {
                        Some(v) => *w = v,
                        None => return SetOpDone::Reply(err("ERR weight value is not a float")),
                    }
                }
                j += setnum + 1;
            } else if combinable && remaining >= 2 && opt == "AGGREGATE" {
                agg = match upper(&opts[j + 1]).as_str() {
                    "SUM" => Aggregate::Sum,
                    "MIN" => Aggregate::Min,
                    "MAX" => Aggregate::Max,
                    _ => return SetOpDone::Reply(err(ERR_SYNTAX)),
                };
                j += 2;
            } else if !store && !card && opt == "WITHSCORES" {
                withscores = true;
                j += 1;
            } else if card && remaining >= 2 && opt == "LIMIT" {
                limit = match ranged_long(&opts[j + 1], 0, i64::MAX, ERR_LIMIT_NEGATIVE) {
                    Ok(v) => v as usize,
                    Err(e) => return SetOpDone::Reply(e),
                };
                j += 2;
            } else {
                return SetOpDone::Reply(err(ERR_SYNTAX));
            }
        }
        let (result, n) = compute_setop(&srcs, &weights, op, agg, limit, card);
        match output {
            SetOpOutput::Card => SetOpDone::Reply(RespValue::Integer(n as i64)),
            SetOpOutput::Reply => SetOpDone::Reply(pairs_to_resp(sorted_pairs(result), withscores)),
            SetOpOutput::Store => {
                let dst = &args[1];
                let existed = d.map.remove(dst).is_some_and(|e| !e.is_expired());
                if !result.is_empty() {
                    let mut z = ZSet::default();
                    for (m, s) in result {
                        z.insert(m, s);
                    }
                    d.map.insert(dst.clone(), Entry::new(RedisValue::ZSet(z)));
                }
                SetOpDone::Stored { n: n as i64, existed }
            }
        }
    });
    match done {
        SetOpDone::Reply(r) => r,
        SetOpDone::Stored { n, existed } => {
            let dst = &args[1];
            if n > 0 {
                let event = match op {
                    SetOp::Union => "zunionstore",
                    SetOp::Inter => "zinterstore",
                    SetOp::Diff => "zdiffstore",
                };
                notify_added(ctx, state, dst, event);
                ctx.propagate(args);
            } else if existed {
                ctx.db.touch();
                ctx.notify_key(state.db_index, dst, "del");
                ctx.propagate(&[Bytes::from_static(b"DEL"), dst.clone()]);
            }
            RespValue::Integer(n)
        }
    }
}

/// Issue d'une opération ensembliste calculée sous verrou (effets de bord appliqués après).
enum SetOpDone {
    Reply(RespValue),
    Stored { n: i64, existed: bool },
}

// ----------------------------- tirages aléatoires -----------------------------

/// `[count [WITHSCORES|WITHVALUES]]` commun à ZRANDMEMBER / HRANDFIELD.
/// `None` = pas de `count` (un seul élément ou nil).
pub(crate) fn parse_rand_count(args: &[Bytes], with_flag: &str) -> Result<Option<(i64, bool)>, RespValue> {
    if args.len() < 3 {
        return Ok(None);
    }
    let with = match args.len() {
        3 => false,
        4 if upper(&args[3]) == with_flag => true,
        _ => return Err(err(ERR_SYNTAX)),
    };
    let l = ranged_long(
        &args[2],
        -i64::MAX,
        i64::MAX,
        "ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807",
    )
    .map_err(|e| if parse_i64(&args[2]).is_none() { err(ERR_INT) } else { e })?;
    if with && l < -i64::MAX / 2 {
        return Err(err("ERR value is out of range"));
    }
    Ok(Some((l, with)))
}

/// ZRANDMEMBER key [count [WITHSCORES]].
pub(crate) fn cmd_zrandmember(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let opts = match parse_rand_count(args, "WITHSCORES") {
        Ok(o) => o,
        Err(e) => return e,
    };
    let r: Result<RespValue, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let z = d.zset_mut(&args[1])?;
        Ok(match (z, opts) {
            (None, None) => RespValue::null_bulk(),
            (None, Some(_)) => RespValue::array(vec![]),
            (Some(z), None) => {
                let picked = random_sample(z.members.keys(), z.len(), 1);
                picked.first().map_or_else(RespValue::null_bulk, |m| RespValue::bulk((*m).clone()))
            }
            (Some(z), Some((count, with))) => {
                let picked = random_sample(z.members.iter(), z.len(), count);
                pairs_to_resp(picked.into_iter().map(|(m, s)| (m.clone(), *s)).collect(), with)
            }
        })
    });
    reply(r)
}

// ----------------------------- ZMPOP (moteur bloquant) -----------------------------

/// Réponse ZMPOP / BZMPOP : `[clé, [[membre, score], …]]`.
pub(crate) fn mpop_reply(key: &Bytes, popped: Vec<(Bytes, f64)>) -> RespValue {
    RespValue::array(vec![
        RespValue::bulk(key.clone()),
        RespValue::array(
            popped
                .into_iter()
                .map(|(m, s)| RespValue::array(vec![RespValue::bulk(m), bulk_f64(s)]))
                .collect(),
        ),
    ])
}

/// ZSCAN key cursor [MATCH pattern] [COUNT n].
pub(crate) fn cmd_zscan(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let Some(cursor) = parse_i64(&args[2]).filter(|c| *c >= 0) else {
        return err("ERR invalid cursor");
    };
    let (pattern, count) = match crate::cmd_hashset::parse_scan_opts(args, 3) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let r: Result<Vec<(Bytes, f64)>, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(z) = d.zset_mut(&args[1])? else {
            return Ok(Vec::new());
        };
        // Parcours dans l'ordre (score, membre) : stable entre deux appels.
        Ok(z.by_score
            .iter()
            .filter(|(_, m)| {
                pattern
                    .as_deref()
                    .map_or(true, |p| crate::store::match_glob_bytes(p.as_bytes(), m))
            })
            .map(|(s, m)| (m.clone(), s.0))
            .collect())
    });
    match r {
        Err(e) => err(e),
        Ok(items) => {
            let (next, page) = paginate(items, cursor as u64, count);
            RespValue::array(vec![
                RespValue::bulk(Bytes::from(next.to_string())),
                pairs_to_resp(page, true),
            ])
        }
    }
}
