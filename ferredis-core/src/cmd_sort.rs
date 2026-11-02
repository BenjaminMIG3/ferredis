//! SORT / SORT_RO (port de `sortCommandGeneric`) : BY, LIMIT, GET, ASC|DESC, ALPHA, STORE.

use crate::command::{ClientState, CommandContext};
use crate::resp::RespValue;
use crate::store::{DbInner, WRONGTYPE};
use crate::util::*;
use crate::value::{Entry, RedisValue};
use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::VecDeque;

/// Au-delà, le tri complet est réparti sur plusieurs threads.
const PAR_SORT_THRESHOLD: usize = 1 << 17;

/// Résolution d'un motif BY / GET (`lookupKeyByPattern`) : `#` = l'élément, `*` remplacé,
/// suffixe `->champ` pour un hash. Motif sans `*` ? aucune valeur.
fn lookup_pattern(d: &mut DbInner, pattern: &[u8], elem: &Bytes) -> Option<Bytes> {
    if pattern == b"#" {
        return Some(elem.clone());
    }
    let star = pattern.iter().position(|&c| c == b'*')?;
    let after = &pattern[star + 1..];
    let (post, field) = match after.windows(2).position(|w| w == b"->") {
        Some(f) if f + 2 < after.len() => (&after[..f], Some(&after[f + 2..])),
        _ => (after, None),
    };
    let mut key = Vec::with_capacity(star + elem.len() + post.len());
    key.extend_from_slice(&pattern[..star]);
    key.extend_from_slice(elem);
    key.extend_from_slice(post);
    match (&d.get_fresh(&Bytes::from(key))?.value, field) {
        (RedisValue::String(s), None) => Some(s.clone()),
        (RedisValue::Hash(h), Some(f)) => h.get(f).cloned(),
        _ => None,
    }
}

enum SortKey {
    Num(f64),
    Alpha(Option<Bytes>),
}

struct Item {
    elem: Bytes,
    key: SortKey,
}

fn compare(a: &Item, b: &Item, desc: bool) -> Ordering {
    let cmp = match (&a.key, &b.key) {
        (SortKey::Num(x), SortKey::Num(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal).then_with(|| a.elem.cmp(&b.elem)),
        (SortKey::Alpha(x), SortKey::Alpha(y)) => match (x, y) {
            (Some(x), Some(y)) => x.cmp(y),
            (None, None) => Ordering::Equal,
            (None, _) => Ordering::Less,
            (_, None) => Ordering::Greater,
        },
        _ => Ordering::Equal,
    };
    if desc {
        cmp.reverse()
    } else {
        cmp
    }
}

/// Tri stable parallèle : tranches triées sur des threads puis fusions deux à deux.
fn par_sort<T: Send, F: Fn(&T, &T) -> Ordering + Sync>(v: &mut Vec<T>, cmp: F) {
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get()).min(8);
    if v.len() < PAR_SORT_THRESHOLD || threads < 2 {
        v.sort_by(&cmp);
        return;
    }
    let chunk = v.len().div_ceil(threads);
    std::thread::scope(|s| {
        for part in v.chunks_mut(chunk) {
            let cmp = &cmp;
            s.spawn(move || part.sort_by(cmp));
        }
    });
    let mut runs: Vec<Vec<T>> = Vec::with_capacity(threads);
    let mut rest = std::mem::take(v);
    while !rest.is_empty() {
        let tail = rest.split_off(chunk.min(rest.len()));
        runs.push(rest);
        rest = tail;
    }
    while runs.len() > 1 {
        let mut next = Vec::with_capacity(runs.len().div_ceil(2));
        let mut it = runs.into_iter();
        while let Some(a) = it.next() {
            match it.next() {
                None => next.push(a),
                Some(b) => {
                    let mut out = Vec::with_capacity(a.len() + b.len());
                    let (mut a, mut b) = (a.into_iter().peekable(), b.into_iter().peekable());
                    while let (Some(x), Some(y)) = (a.peek(), b.peek()) {
                        out.push(if cmp(y, x) == Ordering::Less { b.next() } else { a.next() }.unwrap());
                    }
                    out.extend(a);
                    out.extend(b);
                    next.push(out);
                }
            }
        }
        runs = next;
    }
    *v = runs.pop().unwrap_or_default();
}

struct SortOpts {
    by: Option<Bytes>,
    gets: Vec<Bytes>,
    desc: bool,
    alpha: bool,
    limit: Option<(i64, i64)>,
    store: Option<Bytes>,
    dontsort: bool,
}

fn parse_opts(args: &[Bytes], readonly: bool) -> Result<SortOpts, RespValue> {
    let mut o = SortOpts { by: None, gets: Vec::new(), desc: false, alpha: false, limit: None, store: None, dontsort: false };
    let mut j = 2;
    while j < args.len() {
        let left = args.len() - j - 1;
        match upper(&args[j]).as_str() {
            "ASC" => o.desc = false,
            "DESC" => o.desc = true,
            "ALPHA" => o.alpha = true,
            "LIMIT" if left >= 2 => {
                let start = parse_i64(&args[j + 1]).ok_or_else(|| err(ERR_INT))?;
                let count = parse_i64(&args[j + 2]).ok_or_else(|| err(ERR_INT))?;
                o.limit = Some((start, count));
                j += 2;
            }
            "STORE" if left >= 1 && !readonly => {
                o.store = Some(args[j + 1].clone());
                j += 1;
            }
            "BY" if left >= 1 => {
                let p = args[j + 1].clone();
                o.dontsort = !p.contains(&b'*');
                o.by = Some(p);
                j += 1;
            }
            "GET" if left >= 1 => {
                o.gets.push(args[j + 1].clone());
                j += 1;
            }
            _ => return Err(err(ERR_SYNTAX)),
        }
        j += 1;
    }
    Ok(o)
}

/// Bornes `[start, end]` (`end < start` = vide) façon Redis.
fn window(limit: Option<(i64, i64)>, len: i64) -> (i64, i64) {
    let (ls, lc) = limit.unwrap_or((0, -1));
    let mut start = ls.max(0);
    let mut end = if lc < 0 { len - 1 } else { start.saturating_add(lc) - 1 };
    if start >= len {
        start = len - 1;
        end = len - 2;
    }
    if end >= len {
        end = len - 1;
    }
    (start, end)
}

/// Éléments triés et fenêtrés (sans GET).
fn sorted_elements(d: &mut DbInner, key: &Bytes, o: &mut SortOpts) -> Result<Vec<Bytes>, RespValue> {
    let elems: Vec<Bytes> = match d.get_fresh(key).map(|e| &e.value) {
        None => return Ok(Vec::new()),
        Some(RedisValue::List(l)) => {
            let len = l.len() as i64;
            if o.dontsort {
                let (start, end) = window(o.limit, len);
                if end < start {
                    return Ok(Vec::new());
                }
                let n = (end - start + 1) as usize;
                return Ok(if o.desc {
                    l.iter().rev().skip(start as usize).take(n).cloned().collect()
                } else {
                    l.iter().skip(start as usize).take(n).cloned().collect()
                });
            }
            l.iter().cloned().collect()
        }
        Some(RedisValue::Set(s)) => {
            if o.dontsort && o.store.is_some() {
                // Résultat déterministe lorsqu'il est stocké.
                o.dontsort = false;
                o.alpha = true;
                o.by = None;
            }
            s.iter_members()
        }
        Some(RedisValue::ZSet(z)) => {
            if o.dontsort {
                let (start, end) = window(o.limit, z.len() as i64);
                if end < start {
                    return Ok(Vec::new());
                }
                return Ok(z.range_by_rank(start, end, o.desc).into_iter().map(|(m, _)| m).collect());
            }
            z.range_by_rank(0, -1, false).into_iter().map(|(m, _)| m).collect()
        }
        Some(_) => return Err(err(WRONGTYPE)),
    };
    let (start, end) = window(o.limit, elems.len() as i64);
    if end < start {
        return Ok(Vec::new());
    }
    if o.dontsort {
        return Ok(elems[start as usize..=end as usize].to_vec());
    }
    let mut items = Vec::with_capacity(elems.len());
    let mut bad = false;
    for elem in elems {
        let by = match &o.by {
            Some(p) => lookup_pattern(d, p, &elem),
            None => Some(elem.clone()),
        };
        let key = if o.alpha {
            SortKey::Alpha(by)
        } else {
            // Équivalent `strtod` : chaîne vide ? 0, reliquat ou NaN ? erreur.
            SortKey::Num(match by {
                None => 0.0,
                Some(v) if v.is_empty() => 0.0,
                Some(v) => match std::str::from_utf8(&v).ok().and_then(|s| s.trim_start().parse::<f64>().ok()) {
                    Some(f) if !f.is_nan() => f,
                    _ => {
                        bad = true;
                        0.0
                    }
                },
            })
        };
        items.push(Item { elem, key });
    }
    if bad {
        return Err(err("ERR One or more scores can't be converted into double"));
    }
    let desc = o.desc;
    let cmp = |a: &Item, b: &Item| compare(a, b, desc);
    let (s, e) = (start as usize, end as usize);
    if s == 0 && e + 1 == items.len() {
        par_sort(&mut items, cmp);
    } else {
        if s > 0 {
            items.select_nth_unstable_by(s, cmp);
        }
        let tail = &mut items[s..];
        if e - s + 1 < tail.len() {
            tail.select_nth_unstable_by(e - s, cmp);
        }
        tail[..=e - s].sort_unstable_by(cmp);
    }
    Ok(items.drain(s..=e).map(|it| it.elem).collect())
}

/// SORT (`readonly` = SORT_RO).
pub(crate) fn cmd_sort_generic(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes], readonly: bool) -> RespValue {
    let mut o = match parse_opts(args, readonly) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        let elems = sorted_elements(d, &args[1], &mut o)?;
        let values: Vec<Option<Bytes>> = if o.gets.is_empty() {
            elems.into_iter().map(Some).collect()
        } else {
            let mut out = Vec::with_capacity(elems.len() * o.gets.len());
            for e in &elems {
                for g in &o.gets {
                    out.push(lookup_pattern(d, g, e));
                }
            }
            out
        };
        let Some(dst) = &o.store else {
            return Ok((values, false));
        };
        let existed = d.map.remove(dst).is_some_and(|e| !e.is_expired());
        if !values.is_empty() {
            let list: VecDeque<Bytes> = values.iter().map(|v| v.clone().unwrap_or_default()).collect();
            d.map.insert(dst.clone(), Entry::new(RedisValue::List(list)));
        }
        Ok::<_, RespValue>((values, existed))
    });
    let (values, existed) = match r {
        Ok(v) => v,
        Err(e) => return e,
    };
    let Some(dst) = o.store else {
        return RespValue::array(values.into_iter().map(|v| v.map_or_else(RespValue::null_bulk, RespValue::bulk)).collect());
    };
    if !values.is_empty() {
        ctx.db.touch();
        ctx.waiters.notify(state.db_index, &dst);
        ctx.notify_key(state.db_index, &dst, "sortstore");
        ctx.propagate(args);
    } else if existed {
        ctx.db.touch();
        ctx.notify_key(state.db_index, &dst, "del");
        ctx.propagate(&[Bytes::from_static(b"DEL"), dst.clone()]);
    }
    RespValue::Integer(values.len() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_matches_redis() {
        assert_eq!(window(None, 5), (0, 4));
        assert_eq!(window(Some((1, 2)), 5), (1, 2));
        assert_eq!(window(Some((10, 2)), 5), (4, 3));
        assert_eq!(window(Some((-3, 100)), 5), (0, 4));
        assert_eq!(window(Some((0, 0)), 5), (0, -1));
    }

    #[test]
    fn par_sort_is_sorted_and_stable() {
        let mut v: Vec<(u32, usize)> = (0..PAR_SORT_THRESHOLD * 2).map(|i| (((i * 7919) % 1000) as u32, i)).collect();
        par_sort(&mut v, |a, b| a.0.cmp(&b.0));
        assert!(v.windows(2).all(|w| w[0].0 < w[1].0 || (w[0].0 == w[1].0 && w[0].1 < w[1].1)));
    }
}
