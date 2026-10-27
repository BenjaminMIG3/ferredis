//! Commandes géographiques (sorted sets à scores geohash 52 bits) : GEOADD, GEOPOS, GEODIST,
//! GEOHASH, GEORADIUS[_RO], GEORADIUSBYMEMBER[_RO], GEOSEARCH, GEOSEARCHSTORE.

use crate::cmd_zset::{zadd_impl, ZaddOpts};
use crate::command::{ClientState, CommandContext};
use crate::geohash::{self, Shape};
use crate::resp::RespValue;
use crate::util::*;
use crate::value::{Entry, RedisValue, ScoreBound, ZRange, ZSet};
use bytes::Bytes;

/// `(lon, lat)` validés (`extractLongLatOrReply`).
fn lon_lat(lon: &[u8], lat: &[u8]) -> Result<(f64, f64), RespValue> {
    let (Some(x), Some(y)) = (parse_f64(lon), parse_f64(lat)) else {
        return Err(err(ERR_FLOAT));
    };
    if !(geohash::LONG_MIN..=geohash::LONG_MAX).contains(&x) || !(geohash::LAT_MIN..=geohash::LAT_MAX).contains(&y) {
        return Err(RespValue::err(format!("ERR invalid longitude,latitude pair {x:.6},{y:.6}")));
    }
    Ok((x, y))
}

/// Facteur de conversion vers les mètres.
fn unit(raw: &[u8]) -> Result<f64, RespValue> {
    match upper(raw).as_str() {
        "M" => Ok(1.0),
        "KM" => Ok(1000.0),
        "FT" => Ok(0.3048),
        "MI" => Ok(1609.34),
        _ => Err(err("ERR unsupported unit provided. please use M, KM, FT, MI")),
    }
}

fn number(raw: &[u8], msg: &str) -> Result<f64, RespValue> {
    parse_f64(raw).ok_or_else(|| err(msg))
}

/// `radius unit` ? `(rayon, conversion)`.
fn radius_arg(args: &[Bytes]) -> Result<(f64, f64), RespValue> {
    let r = number(&args[0], "ERR need numeric radius")?;
    if r < 0.0 {
        return Err(err("ERR radius cannot be negative"));
    }
    Ok((r, unit(&args[1])?))
}

fn fmt_distance(d: f64) -> RespValue {
    RespValue::bulk(Bytes::from(format!("{d:.4}")))
}

fn coord_pair(lon: f64, lat: f64) -> RespValue {
    RespValue::array(vec![
        RespValue::bulk(Bytes::from(fmt_human(lon))),
        RespValue::bulk(Bytes::from(fmt_human(lat))),
    ])
}

/// GEOADD key [NX|XX] [CH] longitude latitude member [… ].
pub(crate) fn cmd_geoadd(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let mut opts = ZaddOpts::default();
    let mut i = 2;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "NX" => opts.nx = true,
            "XX" => opts.xx = true,
            "CH" => opts.ch = true,
            _ => break,
        }
        i += 1;
    }
    let rest = &args[i..];
    if rest.len() % 3 != 0 || (opts.xx && opts.nx) || rest.is_empty() {
        return err(ERR_SYNTAX);
    }
    let mut pairs = Vec::with_capacity(rest.len() / 3);
    for c in rest.chunks(3) {
        let (lon, lat) = match lon_lat(&c[0], &c[1]) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let bits = geohash::encode_wgs84(lon, lat, geohash::STEP_MAX).unwrap_or_default();
        pairs.push((bits.align52() as f64, c[2].clone()));
    }
    zadd_impl(ctx, state, &args[1], opts, &pairs)
}

/// Scores des membres demandés (`None` = membre absent) ; clé absente ? tous `None`.
fn member_scores(ctx: &CommandContext, state: &ClientState, key: &Bytes, members: &[Bytes]) -> Result<Vec<Option<f64>>, RespValue> {
    ctx.db.with_db(state.db_index, |d| {
        let z = d.zset_mut(key).map_err(err)?;
        Ok(members.iter().map(|m| z.as_ref().and_then(|z| z.score(m))).collect())
    })
}

/// GEOPOS key [member …].
pub(crate) fn cmd_geopos(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    match member_scores(ctx, state, &args[1], &args[2..]) {
        Err(e) => e,
        Ok(scores) => RespValue::array(
            scores
                .into_iter()
                .map(|s| match s.and_then(geohash::decode_score) {
                    Some((lon, lat)) => coord_pair(lon, lat),
                    None => RespValue::null_array(),
                })
                .collect(),
        ),
    }
}

/// GEOHASH key [member …].
pub(crate) fn cmd_geohash(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    match member_scores(ctx, state, &args[1], &args[2..]) {
        Err(e) => e,
        Ok(scores) => RespValue::array(
            scores
                .into_iter()
                .map(|s| match s.and_then(geohash::standard_string) {
                    Some(h) => RespValue::bulk(Bytes::from(h)),
                    None => RespValue::null_bulk(),
                })
                .collect(),
        ),
    }
}

/// GEODIST key member1 member2 [M|KM|FT|MI].
pub(crate) fn cmd_geodist(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let to_meter = match args.len() {
        4 => 1.0,
        5 => match unit(&args[4]) {
            Ok(u) => u,
            Err(e) => return e,
        },
        _ => return err(ERR_SYNTAX),
    };
    match member_scores(ctx, state, &args[1], &args[2..4]) {
        Err(e) => e,
        Ok(s) => match (s[0].and_then(geohash::decode_score), s[1].and_then(geohash::decode_score)) {
            (Some(a), Some(b)) => fmt_distance(geohash::distance(a.0, a.1, b.0, b.1) / to_meter),
            _ => RespValue::null_bulk(),
        },
    }
}

/// Variante de la famille GEORADIUS / GEOSEARCH.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum GeoCmd {
    Radius { ro: bool },
    RadiusByMember { ro: bool },
    Search,
    SearchStore,
}

struct GeoPoint {
    member: Bytes,
    score: f64,
    dist: f64,
    lon: f64,
    lat: f64,
}

#[derive(Default)]
struct SearchOpts {
    withdist: bool,
    withhash: bool,
    withcoord: bool,
    any: bool,
    sort: Option<bool>,
    count: usize,
    store: Option<Bytes>,
    storedist: bool,
}

/// Issue d'une recherche calculée sous verrou.
enum GeoDone {
    Reply(RespValue),
    Stored { dst: Bytes, n: i64, existed: bool },
}

fn points_reply(points: Vec<GeoPoint>, o: &SearchOpts, conversion: f64) -> RespValue {
    let extra = o.withdist as usize + o.withhash as usize + o.withcoord as usize;
    RespValue::array(
        points
            .into_iter()
            .map(|p| {
                if extra == 0 {
                    return RespValue::bulk(p.member);
                }
                let mut item = Vec::with_capacity(extra + 1);
                item.push(RespValue::bulk(p.member));
                if o.withdist {
                    item.push(fmt_distance(p.dist / conversion));
                }
                if o.withhash {
                    item.push(RespValue::Integer(p.score as i64));
                }
                if o.withcoord {
                    item.push(coord_pair(p.lon, p.lat));
                }
                RespValue::array(item)
            })
            .collect(),
    )
}

/// Points de `z` dans la forme, parcourus boîte par boîte (`membersOfAllNeighbors`).
fn collect_points(z: &ZSet, shape: Shape, center: (f64, f64), limit: usize) -> Vec<GeoPoint> {
    let areas = geohash::search_areas(shape, center);
    let mut out = Vec::new();
    let mut last: Option<usize> = None;
    for (i, area) in areas.iter().enumerate() {
        if area.is_zero() {
            continue;
        }
        if let Some(l) = last.filter(|l| *l != 0) {
            if areas[l] == *area {
                continue;
            }
        }
        if limit > 0 && out.len() >= limit {
            break;
        }
        let (min, max) = geohash::box_scores(*area);
        let range = ZRange::Score(
            ScoreBound { value: min, exclusive: false },
            ScoreBound { value: max, exclusive: true },
        );
        for (score, member) in z.iter_range(&range, false) {
            if let Some((lon, lat)) = geohash::decode_score(score.0) {
                if let Some(dist) = geohash::within(shape, center, (lon, lat)) {
                    out.push(GeoPoint { member: member.clone(), score: score.0, dist, lon, lat });
                }
            }
            if limit > 0 && out.len() >= limit {
                break;
            }
        }
        last = Some(i);
    }
    out
}

/// Port de `georadiusGeneric`.
pub(crate) fn cmd_geosearch_generic(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes], which: GeoCmd) -> RespValue {
    let is_search = matches!(which, GeoCmd::Search | GeoCmd::SearchStore);
    let nostore = matches!(which, GeoCmd::Radius { ro: true } | GeoCmd::RadiusByMember { ro: true });
    let src_idx = if which == GeoCmd::SearchStore { 2 } else { 1 };
    let src = &args[src_idx];
    let done = ctx.db.with_db(state.db_index, |d| {
        d.purge_if_expired(src);
        let zobj = match d.map.get(src).map(|e| &e.value) {
            None => None,
            Some(RedisValue::ZSet(z)) => Some(z),
            Some(_) => return Err(err(crate::store::WRONGTYPE)),
        };
        let member_pos = |m: &Bytes| -> Result<(f64, f64), RespValue> {
            zobj.and_then(|z| z.score(m))
                .and_then(geohash::decode_score)
                .ok_or_else(|| err("ERR could not decode requested zset member"))
        };
        let mut center = (0.0, 0.0);
        let (mut shape, mut conversion) = (None, 1.0);
        let base = match which {
            GeoCmd::Radius { .. } => {
                center = lon_lat(&args[2], &args[3])?;
                let (r, c) = radius_arg(&args[4..6])?;
                (shape, conversion) = (Some(Shape::Radius(r * c)), c);
                6
            }
            GeoCmd::RadiusByMember { .. } => {
                if zobj.is_some() {
                    center = member_pos(&args[2])?;
                    let (r, c) = radius_arg(&args[3..5])?;
                    (shape, conversion) = (Some(Shape::Radius(r * c)), c);
                }
                5
            }
            GeoCmd::Search => 2,
            GeoCmd::SearchStore => 3,
        };
        let mut o = SearchOpts {
            store: (which == GeoCmd::SearchStore).then(|| args[1].clone()),
            ..Default::default()
        };
        let (mut frommember, mut fromloc, mut byradius, mut bybox) = (false, false, false, false);
        let rem = args.len() - base;
        let mut i = 0;
        while i < rem {
            let j = base + i;
            let a = |k: usize| &args[j + k];
            let more = rem - i - 1;
            match upper(a(0)).as_str() {
                "WITHDIST" => o.withdist = true,
                "WITHHASH" => o.withhash = true,
                "WITHCOORD" => o.withcoord = true,
                "ANY" => o.any = true,
                "ASC" => o.sort = Some(true),
                "DESC" => o.sort = Some(false),
                "COUNT" if more >= 1 => {
                    let n = parse_i64(a(1)).ok_or_else(|| err(ERR_INT))?;
                    if n <= 0 {
                        return Err(err("ERR COUNT must be > 0"));
                    }
                    o.count = n as usize;
                    i += 1;
                }
                opt @ ("STORE" | "STOREDIST") if more >= 1 && !nostore && !is_search => {
                    o.store = Some(a(1).clone());
                    o.storedist = opt == "STOREDIST";
                    i += 1;
                }
                "STOREDIST" if which == GeoCmd::SearchStore => o.storedist = true,
                "FROMMEMBER" if more >= 1 && is_search && !fromloc => {
                    if zobj.is_some() {
                        center = member_pos(a(1))?;
                    }
                    frommember = true;
                    i += 1;
                }
                "FROMLONLAT" if more >= 2 && is_search && !frommember => {
                    center = lon_lat(a(1), a(2))?;
                    fromloc = true;
                    i += 2;
                }
                "BYRADIUS" if more >= 2 && is_search && !bybox => {
                    let (r, c) = radius_arg(&args[j + 1..j + 3])?;
                    (shape, conversion) = (Some(Shape::Radius(r * c)), c);
                    byradius = true;
                    i += 2;
                }
                "BYBOX" if more >= 3 && is_search && !byradius => {
                    let w = number(a(1), "ERR need numeric width")?;
                    let h = number(a(2), "ERR need numeric height")?;
                    if h < 0.0 || w < 0.0 {
                        return Err(err("ERR height or width cannot be negative"));
                    }
                    let c = unit(a(3))?;
                    (shape, conversion) = (Some(Shape::Box { width: w * c, height: h * c }), c);
                    bybox = true;
                    i += 3;
                }
                _ => return Err(err(ERR_SYNTAX)),
            }
            i += 1;
        }
        if o.store.is_some() && (o.withdist || o.withhash || o.withcoord) {
            let what = if which == GeoCmd::SearchStore { "GEOSEARCHSTORE" } else { "STORE option in GEORADIUS" };
            return Err(RespValue::err(format!(
                "ERR {what} is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
            )));
        }
        let name = String::from_utf8_lossy(&args[0]).into_owned();
        if is_search && !(frommember || fromloc) {
            return Err(RespValue::err(format!(
                "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for {name}"
            )));
        }
        if is_search && !(byradius || bybox) {
            return Err(RespValue::err(format!(
                "ERR exactly one of BYRADIUS and BYBOX can be specified for {name}"
            )));
        }
        if o.any && o.count == 0 {
            return Err(err("ERR the ANY argument requires COUNT argument"));
        }
        let mut points = match (zobj, shape) {
            (Some(z), Some(shape)) => {
                if o.count != 0 && o.sort.is_none() && !o.any {
                    o.sort = Some(true);
                }
                let mut points = collect_points(z, shape, center, if o.any { o.count } else { 0 });
                let returned = if o.count == 0 { points.len() } else { points.len().min(o.count) };
                if let Some(asc) = o.sort {
                    let cmp = |a: &GeoPoint, b: &GeoPoint| {
                        let c = a.dist.partial_cmp(&b.dist).unwrap_or(std::cmp::Ordering::Equal);
                        if asc {
                            c
                        } else {
                            c.reverse()
                        }
                    };
                    if returned > 0 && returned < points.len() {
                        points.select_nth_unstable_by(returned - 1, cmp);
                        points.truncate(returned);
                    }
                    points.sort_by(cmp);
                }
                points.truncate(returned);
                points
            }
            _ => Vec::new(),
        };
        let Some(dst) = o.store.clone() else {
            return Ok(GeoDone::Reply(points_reply(points, &o, conversion)));
        };
        let existed = d.map.remove(&dst).is_some_and(|e| !e.is_expired());
        let n = points.len() as i64;
        if !points.is_empty() {
            let mut z = ZSet::default();
            for p in points.drain(..) {
                z.insert(p.member, if o.storedist { p.dist / conversion } else { p.score });
            }
            d.map.insert(dst.clone(), Entry::new(RedisValue::ZSet(z)));
        }
        Ok(GeoDone::Stored { dst, n, existed })
    });
    let (dst, n, existed) = match done {
        Err(e) | Ok(GeoDone::Reply(e)) => return e,
        Ok(GeoDone::Stored { dst, n, existed }) => (dst, n, existed),
    };
    if n > 0 {
        let event = if matches!(which, GeoCmd::Search | GeoCmd::SearchStore) { "geosearchstore" } else { "georadiusstore" };
        ctx.db.touch();
        ctx.waiters.notify(state.db_index, &dst);
        ctx.notify_key(state.db_index, &dst, event);
        ctx.propagate(args);
    } else if existed {
        ctx.db.touch();
        ctx.notify_key(state.db_index, &dst, "del");
        ctx.propagate(&[Bytes::from_static(b"DEL"), dst.clone()]);
    }
    RespValue::Integer(n)
}
