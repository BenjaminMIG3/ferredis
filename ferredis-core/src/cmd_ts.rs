//! Commandes RedisTimeSeries embarquées (`TS.*`).

use crate::command::{arg_str, wrong_arity, ClientState, CommandContext};
use crate::resp::RespValue;
use crate::store::WRONGTYPE;
use crate::util::*;
use crate::value::{
    Entry, RedisValue, TimeSeries, TsAggregator, TsDuplicatePolicy, TsRule,
};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn after_write(ctx: &CommandContext, state: &ClientState, key: &Bytes, args: &[Bytes]) {
    ctx.db.touch();
    ctx.notify_key(state.db_index, key, "ts.add");
    ctx.propagate(args);
}

fn sample_pair(ts: i64, v: f64) -> RespValue {
    RespValue::array(vec![RespValue::Integer(ts), bulk_f64(v)])
}

fn parse_ts_timestamp(b: &[u8]) -> Result<i64, &'static str> {
    if b == b"*" {
        return Ok(now_ms());
    }
    parse_i64(b).ok_or(ERR_INT)
}

struct CreateOpts {
    retention: u64,
    uncompressed: bool,
    chunk_size: usize,
    duplicate_policy: TsDuplicatePolicy,
    labels: BTreeMap<String, String>,
}

impl Default for CreateOpts {
    fn default() -> Self {
        Self {
            retention: 0,
            uncompressed: false,
            chunk_size: 4096,
            duplicate_policy: TsDuplicatePolicy::Block,
            labels: BTreeMap::new(),
        }
    }
}

fn parse_create_opts(args: &[Bytes], start: usize) -> Result<CreateOpts, &'static str> {
    let mut o = CreateOpts::default();
    let mut i = start;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "RETENTION" => {
                i += 1;
                o.retention = parse_i64(args.get(i).ok_or(ERR_SYNTAX)?)
                    .ok_or(ERR_INT)?
                    .max(0) as u64;
            }
            "UNCOMPRESSED" => o.uncompressed = true,
            "CHUNK_SIZE" => {
                i += 1;
                o.chunk_size = parse_i64(args.get(i).ok_or(ERR_SYNTAX)?)
                    .ok_or(ERR_INT)?
                    .max(1) as usize;
            }
            "DUPLICATE_POLICY" => {
                i += 1;
                let s = arg_str(args, i).ok_or(ERR_SYNTAX)?;
                o.duplicate_policy = TsDuplicatePolicy::parse(s).ok_or(ERR_SYNTAX)?;
            }
            "LABELS" => {
                i += 1;
                while i + 1 < args.len() {
                    let k = String::from_utf8_lossy(&args[i]).into_owned();
                    if k.eq_ignore_ascii_case("RETENTION")
                        || k.eq_ignore_ascii_case("UNCOMPRESSED")
                        || k.eq_ignore_ascii_case("CHUNK_SIZE")
                        || k.eq_ignore_ascii_case("DUPLICATE_POLICY")
                        || k.eq_ignore_ascii_case("LABELS")
                    {
                        break;
                    }
                    let v = String::from_utf8_lossy(&args[i + 1]).into_owned();
                    o.labels.insert(k, v);
                    i += 2;
                }
                continue;
            }
            _ => return Err(ERR_SYNTAX),
        }
        i += 1;
    }
    let _ = o.uncompressed;
    Ok(o)
}

fn apply_opts(ts: &mut TimeSeries, o: &CreateOpts) {
    ts.retention_ms = o.retention;
    ts.chunk_size = o.chunk_size;
    ts.duplicate_policy = o.duplicate_policy;
    ts.labels = o.labels.clone();
}

/// TS.CREATE key [RETENTION …] [LABELS …]
pub(crate) fn cmd_ts_create(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("ts.create");
    }
    let key = &args[1];
    let opts = match parse_create_opts(args, 2) {
        Ok(o) => o,
        Err(e) => return err(e),
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        if d.get_fresh(key).is_some() {
            return Err("ERR TSDB: key already exists");
        }
        let mut ts = TimeSeries::default();
        apply_opts(&mut ts, &opts);
        d.map.insert(key.clone(), Entry::new(RedisValue::TimeSeries(ts)));
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

/// TS.ALTER key …
pub(crate) fn cmd_ts_alter(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("ts.alter");
    }
    let key = &args[1];
    let opts = match parse_create_opts(args, 2) {
        Ok(o) => o,
        Err(e) => return err(e),
    };
    let r = ctx.db.with_db(state.db_index, |d| -> Result<(), &'static str> {
        match d.get_fresh_mut(key).map(|e| &mut e.value) {
            Some(RedisValue::TimeSeries(ts)) => {
                apply_opts(ts, &opts);
                ts.trim_retention();
                Ok(())
            }
            None => Err("ERR TSDB: the key does not exist"),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, key, args);
            RespValue::ok()
        }
    }
}

fn ensure_ts<'a>(
    d: &'a mut crate::store::DbInner,
    key: &Bytes,
    create: bool,
) -> Result<&'a mut TimeSeries, &'static str> {
    let needs_create = match d.get_fresh(key).map(|e| &e.value) {
        Some(RedisValue::TimeSeries(_)) => false,
        Some(_) => return Err(WRONGTYPE),
        None if create => true,
        None => return Err("ERR TSDB: the key does not exist"),
    };
    if needs_create {
        d.map
            .insert(key.clone(), Entry::new(RedisValue::TimeSeries(TimeSeries::default())));
    }
    match d.map.get_mut(key).map(|e| &mut e.value) {
        Some(RedisValue::TimeSeries(ts)) => Ok(ts),
        _ => unreachable!(),
    }
}

fn apply_rules(ctx: &CommandContext, db: usize, src_key: &Bytes, ts: i64, value: f64) {
    let rules: Vec<TsRule> = ctx.db.with_db(db, |d| {
        match d.get_fresh(src_key).map(|e| &e.value) {
            Some(RedisValue::TimeSeries(s)) => s.rules.clone(),
            _ => Vec::new(),
        }
    });
    for rule in rules {
        let bucket = aligned_bucket(ts, rule.bucket_ms, rule.align_timestamp);
        let samples: Vec<(i64, f64)> = ctx.db.with_db(db, |d| {
            match d.get_fresh(src_key).map(|e| &e.value) {
                Some(RedisValue::TimeSeries(s)) => s
                    .samples
                    .range(bucket..bucket + rule.bucket_ms as i64)
                    .map(|(&t, &v)| (t, v))
                    .collect(),
                _ => Vec::new(),
            }
        });
        if samples.is_empty() {
            continue;
        }
        let agg = aggregate(&samples, rule.aggregator);
        let _ = ctx.db.with_db(db, |d| {
            if let Ok(dest) = ensure_ts(d, &rule.dest_key, true) {
                let _ = dest.add(bucket, agg);
                dest.source_key = Some(src_key.clone());
            }
        });
    }
    let _ = (value,);
}

fn aligned_bucket(ts: i64, bucket_ms: u64, align: Option<i64>) -> i64 {
    let b = bucket_ms as i64;
    if b <= 0 {
        return ts;
    }
    let align = align.unwrap_or(0);
    let shifted = ts - align;
    let bucket = if shifted >= 0 {
        (shifted / b) * b
    } else {
        ((shifted - b + 1) / b) * b
    };
    bucket + align
}

fn aggregate(samples: &[(i64, f64)], agg: TsAggregator) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let vals: Vec<f64> = samples.iter().map(|(_, v)| *v).collect();
    match agg {
        TsAggregator::Avg => vals.iter().sum::<f64>() / vals.len() as f64,
        TsAggregator::Sum => vals.iter().sum(),
        TsAggregator::Min => vals.iter().cloned().fold(f64::INFINITY, f64::min),
        TsAggregator::Max => vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        TsAggregator::Range => {
            let min = vals.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            max - min
        }
        TsAggregator::Count => vals.len() as f64,
        TsAggregator::First => vals[0],
        TsAggregator::Last => *vals.last().unwrap(),
        TsAggregator::StdP | TsAggregator::VarP | TsAggregator::StdS | TsAggregator::VarS => {
            let mean = vals.iter().sum::<f64>() / vals.len() as f64;
            let var = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                / if matches!(agg, TsAggregator::StdS | TsAggregator::VarS) {
                    (vals.len() - 1).max(1) as f64
                } else {
                    vals.len() as f64
                };
            match agg {
                TsAggregator::StdP | TsAggregator::StdS => var.sqrt(),
                _ => var,
            }
        }
        TsAggregator::Twa => {
            // approximation : moyenne simple si un seul intervalle
            vals.iter().sum::<f64>() / vals.len() as f64
        }
    }
}

/// TS.ADD key timestamp value [RETENTION …] [LABELS …] [ON_DUPLICATE …]
pub(crate) fn cmd_ts_add(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("ts.add");
    }
    let key = &args[1];
    let ts = match parse_ts_timestamp(&args[2]) {
        Ok(t) => t,
        Err(e) => return err(e),
    };
    let value = match parse_f64(&args[3]) {
        Some(v) => v,
        None => return err(ERR_FLOAT),
    };
    let mut on_dup = None;
    let mut i = 4;
    let mut create_opts = CreateOpts::default();
    let mut has_create = false;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "ON_DUPLICATE" => {
                i += 1;
                let s = match arg_str(args, i) {
                    Some(s) => s,
                    None => return err(ERR_SYNTAX),
                };
                on_dup = Some(match TsDuplicatePolicy::parse(s) {
                    Some(p) => p,
                    None => return err(ERR_SYNTAX),
                });
                i += 1;
            }
            "RETENTION" | "UNCOMPRESSED" | "CHUNK_SIZE" | "DUPLICATE_POLICY" | "LABELS" => {
                has_create = true;
                match parse_create_opts(args, i) {
                    Ok(o) => {
                        create_opts = o;
                        break;
                    }
                    Err(e) => return err(e),
                }
            }
            _ => return err(ERR_SYNTAX),
        }
    }

    let r = ctx.db.with_db(state.db_index, |d| -> Result<i64, &'static str> {
        let exists = d.get_fresh(key).is_some();
        if !exists && !has_create && args.len() == 4 {
            // auto-create empty series (RedisTimeSeries behaviour)
        }
        let series = ensure_ts(d, key, true)?;
        if has_create && !exists {
            apply_opts(series, &create_opts);
        }
        if let Some(p) = on_dup {
            series.duplicate_policy = p;
        }
        series.add(ts, value)?;
        Ok(ts)
    });
    match r {
        Err(e) => err(e),
        Ok(t) => {
            apply_rules(ctx, state.db_index, key, t, value);
            after_write(ctx, state, key, args);
            RespValue::Integer(t)
        }
    }
}

pub(crate) fn cmd_ts_madd(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 || (args.len() - 1) % 3 != 0 {
        return wrong_arity("ts.madd");
    }
    let mut out = Vec::new();
    let mut ok_any = false;
    for chunk in args[1..].chunks(3) {
        let key = &chunk[0];
        let ts = match parse_ts_timestamp(&chunk[1]) {
            Ok(t) => t,
            Err(e) => {
                out.push(err(e));
                continue;
            }
        };
        let value = match parse_f64(&chunk[2]) {
            Some(v) => v,
            None => {
                out.push(err(ERR_FLOAT));
                continue;
            }
        };
        let r = ctx.db.with_db(state.db_index, |d| {
            let series = ensure_ts(d, key, true)?;
            series.add(ts, value)?;
            Ok::<i64, &'static str>(ts)
        });
        match r {
            Ok(t) => {
                apply_rules(ctx, state.db_index, key, t, value);
                ok_any = true;
                out.push(RespValue::Integer(t));
            }
            Err(e) => out.push(err(e)),
        }
    }
    if ok_any {
        ctx.db.touch();
        ctx.propagate(args);
    }
    RespValue::array(out)
}

fn incr_by(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes], name: &str, sign: f64) -> RespValue {
    if args.len() < 3 {
        return wrong_arity(name);
    }
    let key = &args[1];
    let delta = match parse_f64(&args[2]) {
        Some(v) => v * sign,
        None => return err(ERR_FLOAT),
    };
    let mut ts = now_ms();
    let mut i = 3;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "TIMESTAMP" => {
                i += 1;
                ts = match parse_ts_timestamp(args.get(i).map(|b| b.as_ref()).unwrap_or(b"*")) {
                    Ok(t) => t,
                    Err(e) => return err(e),
                };
            }
            _ => break,
        }
        i += 1;
    }
    let r = ctx.db.with_db(state.db_index, |d| -> Result<(i64, f64), &'static str> {
        let series = ensure_ts(d, key, true)?;
        let prev = series.samples.get(&ts).copied().or_else(|| {
            series.samples.iter().next_back().map(|(_, v)| *v)
        }).unwrap_or(0.0);
        // Redis INCRBY adds to last sample if same timestamp policy; use last value + delta at ts
        let base = if series.samples.contains_key(&ts) {
            series.samples[&ts]
        } else {
            series
                .samples
                .iter()
                .next_back()
                .map(|(_, v)| *v)
                .unwrap_or(0.0)
        };
        let _ = prev;
        let new_v = base + delta;
        // force last policy for incr
        let old = series.duplicate_policy;
        series.duplicate_policy = TsDuplicatePolicy::Last;
        series.add(ts, new_v)?;
        series.duplicate_policy = old;
        Ok((ts, new_v))
    });
    match r {
        Err(e) => err(e),
        Ok((t, v)) => {
            apply_rules(ctx, state.db_index, key, t, v);
            after_write(ctx, state, key, args);
            RespValue::Integer(t)
        }
    }
}

pub(crate) fn cmd_ts_incrby(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    incr_by(ctx, state, args, "ts.incrby", 1.0)
}

pub(crate) fn cmd_ts_decrby(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    incr_by(ctx, state, args, "ts.decrby", -1.0)
}

pub(crate) fn cmd_ts_del(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("ts.del");
    }
    let key = &args[1];
    let from = match parse_i64(&args[2]) {
        Some(v) => v,
        None => return err(ERR_INT),
    };
    let to = match parse_i64(&args[3]) {
        Some(v) => v,
        None => return err(ERR_INT),
    };
    let r = ctx.db.with_db(state.db_index, |d| -> Result<i64, &'static str> {
        match d.get_fresh_mut(key).map(|e| &mut e.value) {
            Some(RedisValue::TimeSeries(ts)) => {
                let keys: Vec<i64> = ts.samples.range(from..=to).map(|(&t, _)| t).collect();
                let n = keys.len() as i64;
                for k in keys {
                    ts.samples.remove(&k);
                }
                Ok(n)
            }
            None => Err("ERR TSDB: the key does not exist"),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(n) => {
            if n > 0 {
                after_write(ctx, state, key, args);
            }
            RespValue::Integer(n)
        }
    }
}

pub(crate) fn cmd_ts_get(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("ts.get");
    }
    let key = &args[1];
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(key).map(|e| &e.value) {
            Some(RedisValue::TimeSeries(ts)) => match ts.samples.iter().next_back() {
                Some((&t, &v)) => sample_pair(t, v),
                None => RespValue::array(vec![]),
            },
            None => RespValue::null_array(),
            Some(_) => err(WRONGTYPE),
        }
    })
}

struct RangeOpts {
    from: i64,
    to: i64,
    reverse: bool,
    count: Option<usize>,
    agg: Option<(TsAggregator, u64)>,
    align: Option<i64>,
    filter_by_ts: Option<Vec<i64>>,
    filter_by_value: Option<(f64, f64)>,
}

fn parse_range_bounds(from: &[u8], to: &[u8]) -> Result<(i64, i64), &'static str> {
    let from = if from == b"-" {
        i64::MIN / 4
    } else if from == b"+" {
        i64::MAX / 4
    } else {
        parse_i64(from).ok_or(ERR_INT)?
    };
    let to = if to == b"+" {
        i64::MAX / 4
    } else if to == b"-" {
        i64::MIN / 4
    } else {
        parse_i64(to).ok_or(ERR_INT)?
    };
    Ok((from, to))
}

fn parse_range_opts(args: &[Bytes], from_i: usize, reverse: bool) -> Result<RangeOpts, &'static str> {
    let (from, to) = parse_range_bounds(&args[from_i], &args[from_i + 1])?;
    let mut o = RangeOpts {
        from,
        to,
        reverse,
        count: None,
        agg: None,
        align: None,
        filter_by_ts: None,
        filter_by_value: None,
    };
    let mut i = from_i + 2;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "COUNT" => {
                i += 1;
                o.count = Some(parse_i64(args.get(i).ok_or(ERR_SYNTAX)?).ok_or(ERR_INT)? as usize);
            }
            "AGGREGATION" => {
                i += 1;
                let agg = TsAggregator::parse(arg_str(args, i).ok_or(ERR_SYNTAX)?).ok_or(ERR_SYNTAX)?;
                i += 1;
                let bucket = parse_i64(args.get(i).ok_or(ERR_SYNTAX)?).ok_or(ERR_INT)? as u64;
                o.agg = Some((agg, bucket));
            }
            "ALIGN" => {
                i += 1;
                let raw = args.get(i).ok_or(ERR_SYNTAX)?;
                o.align = Some(if raw.as_ref() == b"start" || raw.as_ref() == b"-" {
                    from
                } else if raw.as_ref() == b"end" || raw.as_ref() == b"+" {
                    to
                } else {
                    parse_i64(raw).ok_or(ERR_INT)?
                });
            }
            "FILTER_BY_TS" => {
                i += 1;
                let mut ts = Vec::new();
                while i < args.len() {
                    let u = upper(&args[i]);
                    if matches!(
                        u.as_str(),
                        "COUNT" | "AGGREGATION" | "ALIGN" | "FILTER_BY_VALUE" | "FILTER_BY_TS"
                            | "FILTER" | "GROUPBY" | "WITHLABELS" | "SELECTED_LABELS"
                    ) {
                        break;
                    }
                    ts.push(parse_i64(&args[i]).ok_or(ERR_INT)?);
                    i += 1;
                }
                o.filter_by_ts = Some(ts);
                continue;
            }
            "FILTER_BY_VALUE" => {
                i += 1;
                let min = parse_f64(args.get(i).ok_or(ERR_SYNTAX)?).ok_or(ERR_FLOAT)?;
                i += 1;
                let max = parse_f64(args.get(i).ok_or(ERR_SYNTAX)?).ok_or(ERR_FLOAT)?;
                o.filter_by_value = Some((min, max));
            }
            _ => return Err(ERR_SYNTAX),
        }
        i += 1;
    }
    Ok(o)
}

fn exec_range(ts: &TimeSeries, o: &RangeOpts) -> Vec<(i64, f64)> {
    let mut samples = ts.range(o.from, o.to, false, None);
    if let Some(ref filter) = o.filter_by_ts {
        let set: std::collections::HashSet<i64> = filter.iter().copied().collect();
        samples.retain(|(t, _)| set.contains(t));
    }
    if let Some((min, max)) = o.filter_by_value {
        samples.retain(|(_, v)| *v >= min && *v <= max);
    }
    let mut out = if let Some((agg, bucket)) = o.agg {
        let align = o.align;
        let mut buckets: BTreeMap<i64, Vec<(i64, f64)>> = BTreeMap::new();
        for (t, v) in samples {
            let b = aligned_bucket(t, bucket, align);
            buckets.entry(b).or_default().push((t, v));
        }
        buckets
            .into_iter()
            .map(|(b, vals)| (b, aggregate(&vals, agg)))
            .collect()
    } else {
        samples
    };
    if o.reverse {
        out.reverse();
    }
    if let Some(n) = o.count {
        out.truncate(n);
    }
    out
}

fn range_reply(samples: Vec<(i64, f64)>) -> RespValue {
    RespValue::array(samples.into_iter().map(|(t, v)| sample_pair(t, v)).collect())
}

pub(crate) fn cmd_ts_range(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    ts_range_cmd(ctx, state, args, false, "ts.range")
}

pub(crate) fn cmd_ts_revrange(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    ts_range_cmd(ctx, state, args, true, "ts.revrange")
}

fn ts_range_cmd(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    reverse: bool,
    name: &str,
) -> RespValue {
    if args.len() < 4 {
        return wrong_arity(name);
    }
    let key = &args[1];
    let opts = match parse_range_opts(args, 2, reverse) {
        Ok(o) => o,
        Err(e) => return err(e),
    };
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(key).map(|e| &e.value) {
            Some(RedisValue::TimeSeries(ts)) => range_reply(exec_range(ts, &opts)),
            None => RespValue::array(vec![]),
            Some(_) => err(WRONGTYPE),
        }
    })
}

fn parse_filters(args: &[Bytes], start: usize) -> Result<(usize, Vec<(String, Option<String>)>), &'static str> {
    // FILTER label=value / label!=value / label= / label=(v1,v2)
    let mut i = start;
    if i >= args.len() || !upper(&args[i]).eq("FILTER") {
        return Err(ERR_SYNTAX);
    }
    i += 1;
    let mut filters = Vec::new();
    while i < args.len() {
        let u = upper(&args[i]);
        if matches!(u.as_str(), "GROUPBY" | "WITHLABELS" | "SELECTED_LABELS" | "COUNT" | "AGGREGATION" | "ALIGN" | "FILTER_BY_TS" | "FILTER_BY_VALUE") {
            break;
        }
        let raw = String::from_utf8_lossy(&args[i]);
        if let Some((k, v)) = raw.split_once("!=") {
            filters.push((k.to_string(), Some(format!("!{v}"))));
        } else if let Some((k, v)) = raw.split_once('=') {
            filters.push((k.to_string(), Some(v.to_string())));
        } else {
            filters.push((raw.into_owned(), None));
        }
        i += 1;
    }
    Ok((i, filters))
}

fn labels_match(labels: &BTreeMap<String, String>, filters: &[(String, Option<String>)]) -> bool {
    for (k, v) in filters {
        match v {
            None => {
                if !labels.contains_key(k) {
                    return false;
                }
            }
            Some(val) if val.starts_with('!') => {
                let want = &val[1..];
                if labels.get(k).map(|x| x.as_str()) == Some(want) {
                    return false;
                }
            }
            Some(val) if val.is_empty() => {
                if labels.contains_key(k) {
                    return false;
                }
            }
            Some(val) if val.starts_with('(') && val.ends_with(')') => {
                let inner = &val[1..val.len() - 1];
                let opts: Vec<&str> = inner.split(',').collect();
                match labels.get(k) {
                    Some(have) if opts.iter().any(|o| *o == have.as_str()) => {}
                    _ => return false,
                }
            }
            Some(val) => {
                if labels.get(k).map(|x| x.as_str()) != Some(val.as_str()) {
                    return false;
                }
            }
        }
    }
    true
}

fn collect_matching_keys(
    ctx: &CommandContext,
    db: usize,
    filters: &[(String, Option<String>)],
) -> Vec<(Bytes, BTreeMap<String, String>)> {
    ctx.db.with_db(db, |d| {
        let mut out = Vec::new();
        for (k, e) in &d.map {
            if e.is_expired() {
                continue;
            }
            if let RedisValue::TimeSeries(ts) = &e.value {
                if labels_match(&ts.labels, filters) {
                    out.push((k.clone(), ts.labels.clone()));
                }
            }
        }
        out
    })
}

pub(crate) fn cmd_ts_mrange(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    ts_mrange(ctx, state, args, false, "ts.mrange")
}

pub(crate) fn cmd_ts_mrevrange(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    ts_mrange(ctx, state, args, true, "ts.mrevrange")
}

fn ts_mrange(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    reverse: bool,
    name: &str,
) -> RespValue {
    if args.len() < 4 {
        return wrong_arity(name);
    }
    let (from, to) = match parse_range_bounds(&args[1], &args[2]) {
        Ok(v) => v,
        Err(e) => return err(e),
    };
    let mut withlabels = false;
    let mut i = 3;
    let mut range_tail = Vec::new();
    // optional COUNT/AGGREGATION before FILTER
    while i < args.len() && !upper(&args[i]).eq("FILTER") {
        if upper(&args[i]).eq("WITHLABELS") {
            withlabels = true;
            i += 1;
            continue;
        }
        range_tail.push(args[i].clone());
        i += 1;
    }
    let (filters, rest_i) = match parse_filters(args, i) {
        Ok((ni, f)) => {
            i = ni;
            (f, i)
        }
        Err(e) => return err(e),
    };
    let _ = rest_i;
    let mut fake = vec![Bytes::from_static(b"TS.RANGE"), Bytes::from_static(b"_")];
    fake.push(Bytes::from(from.to_string()));
    fake.push(Bytes::from(to.to_string()));
    fake.extend(range_tail);
    let opts = match parse_range_opts(&fake, 2, reverse) {
        Ok(mut o) => {
            o.from = from;
            o.to = to;
            o
        }
        Err(e) => return err(e),
    };
    let keys = collect_matching_keys(ctx, state.db_index, &filters);
    let mut out = Vec::with_capacity(keys.len());
    for (key, labels) in keys {
        let samples = ctx.db.with_db(state.db_index, |d| {
            match d.get_fresh(&key).map(|e| &e.value) {
                Some(RedisValue::TimeSeries(ts)) => exec_range(ts, &opts),
                _ => Vec::new(),
            }
        });
        let label_arr = if withlabels {
            let mut pairs = Vec::new();
            for (lk, lv) in &labels {
                pairs.push(RespValue::array(vec![
                    RespValue::bulk(Bytes::from(lk.clone())),
                    RespValue::bulk(Bytes::from(lv.clone())),
                ]));
            }
            RespValue::array(pairs)
        } else {
            RespValue::array(vec![])
        };
        out.push(RespValue::array(vec![
            RespValue::bulk(key),
            label_arr,
            range_reply(samples),
        ]));
    }
    RespValue::array(out)
}

pub(crate) fn cmd_ts_mget(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("ts.mget");
    }
    let mut withlabels = false;
    let mut i = 1;
    if i < args.len() && upper(&args[i]).eq("WITHLABELS") {
        withlabels = true;
        i += 1;
    }
    let (_, filters) = match parse_filters(args, i) {
        Ok(v) => v,
        Err(e) => return err(e),
    };
    let keys = collect_matching_keys(ctx, state.db_index, &filters);
    let mut out = Vec::new();
    for (key, labels) in keys {
        let latest = ctx.db.with_db(state.db_index, |d| {
            match d.get_fresh(&key).map(|e| &e.value) {
                Some(RedisValue::TimeSeries(ts)) => ts
                    .samples
                    .iter()
                    .next_back()
                    .map(|(&t, &v)| sample_pair(t, v))
                    .unwrap_or_else(|| RespValue::array(vec![])),
                _ => RespValue::array(vec![]),
            }
        });
        let label_arr = if withlabels {
            RespValue::array(
                labels
                    .iter()
                    .map(|(lk, lv)| {
                        RespValue::array(vec![
                            RespValue::bulk(Bytes::from(lk.clone())),
                            RespValue::bulk(Bytes::from(lv.clone())),
                        ])
                    })
                    .collect(),
            )
        } else {
            RespValue::array(vec![])
        };
        out.push(RespValue::array(vec![RespValue::bulk(key), label_arr, latest]));
    }
    RespValue::array(out)
}

pub(crate) fn cmd_ts_createrule(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 6 {
        return wrong_arity("ts.createrule");
    }
    let src = &args[1];
    let dest = &args[2];
    if !upper(&args[3]).eq("AGGREGATION") {
        return err(ERR_SYNTAX);
    }
    let agg = match TsAggregator::parse(arg_str(args, 4).unwrap_or("")) {
        Some(a) => a,
        None => return err(ERR_SYNTAX),
    };
    let bucket = match parse_i64(&args[5]) {
        Some(v) if v > 0 => v as u64,
        _ => return err(ERR_INT),
    };
    let align = args.get(6).and_then(|b| parse_i64(b));
    let r = ctx.db.with_db(state.db_index, |d| -> Result<(), &'static str> {
        // dest must exist
        match d.get_fresh(dest).map(|e| &e.value) {
            Some(RedisValue::TimeSeries(_)) => {}
            None => return Err("ERR TSDB: the key does not exist"),
            Some(_) => return Err(WRONGTYPE),
        }
        match d.get_fresh_mut(src).map(|e| &mut e.value) {
            Some(RedisValue::TimeSeries(ts)) => {
                if ts.rules.iter().any(|r| r.dest_key == *dest) {
                    return Err("ERR TSDB: the destination key already has a compaction rule");
                }
                ts.rules.push(TsRule {
                    dest_key: dest.clone(),
                    aggregator: agg,
                    bucket_ms: bucket,
                    align_timestamp: align,
                });
                Ok(())
            }
            None => Err("ERR TSDB: the key does not exist"),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, src, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_ts_deleterule(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("ts.deleterule");
    }
    let src = &args[1];
    let dest = &args[2];
    let r = ctx.db.with_db(state.db_index, |d| -> Result<(), &'static str> {
        match d.get_fresh_mut(src).map(|e| &mut e.value) {
            Some(RedisValue::TimeSeries(ts)) => {
                let before = ts.rules.len();
                ts.rules.retain(|r| r.dest_key != *dest);
                if ts.rules.len() == before {
                    return Err("ERR TSDB: compaction rule does not exist");
                }
                Ok(())
            }
            None => Err("ERR TSDB: the key does not exist"),
            Some(_) => Err(WRONGTYPE),
        }
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_write(ctx, state, src, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_ts_queryindex(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("ts.queryindex");
    }
    let filters: Vec<(String, Option<String>)> = args[1..]
        .iter()
        .map(|b| {
            let raw = String::from_utf8_lossy(b);
            if let Some((k, v)) = raw.split_once("!=") {
                (k.to_string(), Some(format!("!{v}")))
            } else if let Some((k, v)) = raw.split_once('=') {
                (k.to_string(), Some(v.to_string()))
            } else {
                (raw.into_owned(), None)
            }
        })
        .collect();
    let keys = collect_matching_keys(ctx, state.db_index, &filters);
    RespValue::array(keys.into_iter().map(|(k, _)| RespValue::bulk(k)).collect())
}

pub(crate) fn cmd_ts_info(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("ts.info");
    }
    let key = &args[1];
    ctx.db.with_db(state.db_index, |d| {
        match d.get_fresh(key).map(|e| &e.value) {
            Some(RedisValue::TimeSeries(ts)) => {
                let mut items = vec![
                    RespValue::bulk(Bytes::from_static(b"totalSamples")),
                    RespValue::Integer(ts.total_samples() as i64),
                    RespValue::bulk(Bytes::from_static(b"memoryUsage")),
                    RespValue::Integer(ts.approx_bytes() as i64),
                    RespValue::bulk(Bytes::from_static(b"firstTimestamp")),
                    RespValue::Integer(ts.samples.keys().next().copied().unwrap_or(0)),
                    RespValue::bulk(Bytes::from_static(b"lastTimestamp")),
                    RespValue::Integer(ts.samples.keys().next_back().copied().unwrap_or(0)),
                    RespValue::bulk(Bytes::from_static(b"retentionTime")),
                    RespValue::Integer(ts.retention_ms as i64),
                    RespValue::bulk(Bytes::from_static(b"chunkCount")),
                    RespValue::Integer(1),
                    RespValue::bulk(Bytes::from_static(b"chunkSize")),
                    RespValue::Integer(ts.chunk_size as i64),
                    RespValue::bulk(Bytes::from_static(b"chunkType")),
                    RespValue::bulk(Bytes::from_static(b"compressed")),
                    RespValue::bulk(Bytes::from_static(b"duplicatePolicy")),
                    RespValue::bulk(Bytes::from(ts.duplicate_policy.as_str())),
                ];
                let labels: Vec<RespValue> = ts
                    .labels
                    .iter()
                    .map(|(k, v)| {
                        RespValue::array(vec![
                            RespValue::bulk(Bytes::from(k.clone())),
                            RespValue::bulk(Bytes::from(v.clone())),
                        ])
                    })
                    .collect();
                items.push(RespValue::bulk(Bytes::from_static(b"labels")));
                items.push(RespValue::array(labels));
                let rules: Vec<RespValue> = ts
                    .rules
                    .iter()
                    .map(|r| {
                        RespValue::array(vec![
                            RespValue::bulk(r.dest_key.clone()),
                            RespValue::Integer(r.bucket_ms as i64),
                            RespValue::bulk(Bytes::from(r.aggregator.as_str())),
                        ])
                    })
                    .collect();
                items.push(RespValue::bulk(Bytes::from_static(b"rules")));
                items.push(RespValue::array(rules));
                items.push(RespValue::bulk(Bytes::from_static(b"sourceKey")));
                items.push(match &ts.source_key {
                    Some(k) => RespValue::bulk(k.clone()),
                    None => RespValue::null_bulk(),
                });
                RespValue::array(items)
            }
            None => err("ERR TSDB: the key does not exist"),
            Some(_) => err(WRONGTYPE),
        }
    })
}

trait ApproxBytes {
    fn approx_bytes(&self) -> usize;
}
impl ApproxBytes for TimeSeries {
    fn approx_bytes(&self) -> usize {
        self.samples.len() * 24 + self.labels.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
    }
}

// silence unused import warning if HashMap unused in some builds
fn _unused() {
    let _: HashMap<(), ()> = HashMap::new();
}
