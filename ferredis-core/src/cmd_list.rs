//! Commandes sur les listes (hors push/pop de base) : LINDEX, LSET, LREM, LTRIM,
//! LINSERT, LPUSHX/RPUSHX, LMOVE/RPOPLPUSH, LPOS.

use crate::command::{wrong_arity, ClientState, CommandContext};
use crate::resp::RespValue;
use crate::store::WRONGTYPE;
use crate::util::*;
use crate::value::RedisValue;
use bytes::Bytes;

/// Résout un index (négatif = depuis la fin) ; `None` hors bornes.
fn resolve_index(idx: i64, len: usize) -> Option<usize> {
    let i = if idx < 0 { len as i64 + idx } else { idx };
    (i >= 0 && (i as usize) < len).then_some(i as usize)
}

pub(crate) fn cmd_lindex(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("lindex");
    }
    let Some(idx) = parse_i64(&args[2]) else {
        return err(ERR_INT);
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        Ok::<_, &'static str>(
            d.list_mut(&args[1])?
                .and_then(|l| resolve_index(idx, l.len()).map(|i| l[i].clone())),
        )
    });
    match r {
        Err(e) => err(e),
        Ok(Some(v)) => RespValue::bulk(v),
        Ok(None) => RespValue::null_bulk(),
    }
}

pub(crate) fn cmd_lset(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("lset");
    }
    let Some(idx) = parse_i64(&args[2]) else {
        return err(ERR_INT);
    };
    let r: Result<(), &'static str> = ctx.db.with_db(state.db_index, |d| {
        let list = d.list_mut(&args[1])?.ok_or(ERR_NO_SUCH_KEY)?;
        let i = resolve_index(idx, list.len()).ok_or("ERR index out of range")?;
        list[i] = args[3].clone();
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            ctx.db.touch();
            ctx.propagate(args);
            RespValue::ok()
        }
    }
}

/// LREM key count element.
pub(crate) fn cmd_lrem(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("lrem");
    }
    let Some(count) = parse_i64(&args[2]) else {
        return err(ERR_INT);
    };
    let target = &args[3];
    let r: Result<i64, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(list) = d.list_mut(&args[1])? else {
            return Ok(0);
        };
        let limit = if count == 0 { usize::MAX } else { count.unsigned_abs() as usize };
        let mut removed = 0usize;
        if count >= 0 {
            // Tête ? queue : un seul passage, reconstruction en O(n).
            list.retain(|v| {
                if removed < limit && v == target {
                    removed += 1;
                    false
                } else {
                    true
                }
            });
        } else {
            // Queue ? tête : on marque les indices à supprimer en partant de la fin.
            let mut doomed = Vec::new();
            for (i, v) in list.iter().enumerate().rev() {
                if doomed.len() >= limit {
                    break;
                }
                if v == target {
                    doomed.push(i);
                }
            }
            removed = doomed.len();
            let mut skip = doomed.into_iter().rev().peekable();
            let mut i = 0usize;
            list.retain(|_| {
                let drop_it = skip.peek() == Some(&i);
                if drop_it {
                    skip.next();
                }
                i += 1;
                !drop_it
            });
        }
        d.remove_if_empty(&args[1]);
        Ok(removed as i64)
    });
    match r {
        Err(e) => err(e),
        Ok(n) => {
            if n > 0 {
                ctx.db.touch();
                ctx.propagate(args);
            }
            RespValue::Integer(n)
        }
    }
}

/// LTRIM key start stop.
pub(crate) fn cmd_ltrim(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 4 {
        return wrong_arity("ltrim");
    }
    let (Some(start), Some(stop)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
        return err(ERR_INT);
    };
    let r: Result<(), &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(list) = d.list_mut(&args[1])? else {
            return Ok(());
        };
        match normalize_range(start, stop, list.len()) {
            Some((a, b)) => {
                list.truncate(b + 1);
                list.drain(..a);
            }
            None => list.clear(),
        }
        d.remove_if_empty(&args[1]);
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            ctx.db.touch();
            ctx.propagate(args);
            RespValue::ok()
        }
    }
}

/// LINSERT key BEFORE|AFTER pivot element.
pub(crate) fn cmd_linsert(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 5 {
        return wrong_arity("linsert");
    }
    let before = match upper(&args[2]).as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return err(ERR_SYNTAX),
    };
    let r: Result<i64, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(list) = d.list_mut(&args[1])? else {
            return Ok(0);
        };
        match list.iter().position(|v| v == &args[3]) {
            None => Ok(-1),
            Some(pos) => {
                list.insert(if before { pos } else { pos + 1 }, args[4].clone());
                Ok(list.len() as i64)
            }
        }
    });
    match r {
        Err(e) => err(e),
        Ok(n) => {
            if n > 0 {
                ctx.db.touch();
                ctx.propagate(args);
                ctx.waiters.notify(state.db_index, &args[1]);
            }
            RespValue::Integer(n)
        }
    }
}

/// LPUSHX / RPUSHX : pousse uniquement si la liste existe.
pub(crate) fn cmd_pushx(ctx: &CommandContext, state: &ClientState, args: &[Bytes], left: bool) -> RespValue {
    if args.len() < 3 {
        return wrong_arity(if left { "lpushx" } else { "rpushx" });
    }
    let r: Result<i64, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(list) = d.list_mut(&args[1])? else {
            return Ok(0);
        };
        for v in &args[2..] {
            if left {
                list.push_front(v.clone());
            } else {
                list.push_back(v.clone());
            }
        }
        Ok(list.len() as i64)
    });
    match r {
        Err(e) => err(e),
        Ok(n) => {
            if n > 0 {
                ctx.db.touch();
                ctx.propagate(args);
                ctx.waiters.notify(state.db_index, &args[1]);
            }
            RespValue::Integer(n)
        }
    }
}

/// Déplacement atomique d'un élément `src` ? `dst` (sous un seul verrou de base).
pub(crate) fn lmove_impl(
    ctx: &CommandContext,
    db: usize,
    src: &Bytes,
    dst: &Bytes,
    src_left: bool,
    dst_left: bool,
) -> Result<Option<Bytes>, &'static str> {
    let moved = ctx.db.with_db(db, |d| {
        if let Some(e) = d.get_fresh(dst) {
            if !matches!(e.value, RedisValue::List(_)) {
                return Err(WRONGTYPE);
            }
        }
        let Some(list) = d.list_mut(src)? else {
            return Ok(None);
        };
        let v = if src_left { list.pop_front() } else { list.pop_back() };
        let Some(v) = v else {
            return Ok(None);
        };
        let target = d.list_or_create(dst)?;
        if dst_left {
            target.push_front(v.clone());
        } else {
            target.push_back(v.clone());
        }
        d.remove_if_empty(src);
        Ok(Some(v))
    })?;
    if moved.is_some() {
        ctx.db.touch();
        ctx.waiters.notify(db, dst);
    }
    Ok(moved)
}

fn parse_side(raw: &[u8]) -> Option<bool> {
    match upper(raw).as_str() {
        "LEFT" => Some(true),
        "RIGHT" => Some(false),
        _ => None,
    }
}

/// RPOPLPUSH src dst (legacy) ou LMOVE src dst LEFT|RIGHT LEFT|RIGHT.
pub(crate) fn cmd_lmove(ctx: &CommandContext, state: &ClientState, args: &[Bytes], legacy: bool) -> RespValue {
    let (src_left, dst_left) = if legacy {
        if args.len() != 3 {
            return wrong_arity("rpoplpush");
        }
        (false, true)
    } else {
        if args.len() != 5 {
            return wrong_arity("lmove");
        }
        match (parse_side(&args[3]), parse_side(&args[4])) {
            (Some(a), Some(b)) => (a, b),
            _ => return err(ERR_SYNTAX),
        }
    };
    match lmove_impl(ctx, state.db_index, &args[1], &args[2], src_left, dst_left) {
        Err(e) => err(e),
        Ok(Some(v)) => {
            ctx.propagate(args);
            RespValue::bulk(v)
        }
        Ok(None) => RespValue::null_bulk(),
    }
}

/// LPOS key element [RANK r] [COUNT c] [MAXLEN m].
pub(crate) fn cmd_lpos(ctx: &CommandContext, state: &ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("lpos");
    }
    let (mut rank, mut count, mut maxlen) = (1i64, None::<usize>, 0usize);
    let mut i = 3;
    while i < args.len() {
        let Some(n) = args.get(i + 1).and_then(|b| parse_i64(b)) else {
            return err(if args.get(i + 1).is_none() { ERR_SYNTAX } else { ERR_INT });
        };
        match upper(&args[i]).as_str() {
            "RANK" if n != 0 && n != i64::MIN => rank = n,
            "RANK" => return err("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list"),
            "COUNT" if n >= 0 => count = Some(n as usize),
            "COUNT" => return err("ERR COUNT can't be negative"),
            "MAXLEN" if n >= 0 => maxlen = n as usize,
            "MAXLEN" => return err("ERR MAXLEN can't be negative"),
            _ => return err(ERR_SYNTAX),
        }
        i += 2;
    }
    let r: Result<Vec<i64>, &'static str> = ctx.db.with_db(state.db_index, |d| {
        let Some(list) = d.list_mut(&args[1])? else {
            return Ok(Vec::new());
        };
        let want = match count {
            Some(0) => usize::MAX,
            Some(c) => c,
            None => 1,
        };
        let mut skip = rank.unsigned_abs() as usize - 1;
        let mut found = Vec::new();
        let len = list.len();
        let scan = if maxlen == 0 { len } else { maxlen.min(len) };
        let mut visit = |idx: usize, v: &Bytes| -> bool {
            if v == &args[2] {
                if skip > 0 {
                    skip -= 1;
                } else {
                    found.push(idx as i64);
                    if found.len() >= want {
                        return false;
                    }
                }
            }
            true
        };
        if rank > 0 {
            for (idx, v) in list.iter().enumerate().take(scan) {
                if !visit(idx, v) {
                    break;
                }
            }
        } else {
            for (idx, v) in list.iter().enumerate().rev().take(scan) {
                if !visit(idx, v) {
                    break;
                }
            }
        }
        Ok(found)
    });
    match r {
        Err(e) => err(e),
        Ok(found) => match count {
            None => found
                .first()
                .map_or_else(RespValue::null_bulk, |i| RespValue::Integer(*i)),
            Some(_) => RespValue::array(found.into_iter().map(RespValue::Integer).collect()),
        },
    }
}
