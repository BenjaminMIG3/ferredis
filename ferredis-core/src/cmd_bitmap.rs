//! Bitmaps (sémantique Redis 7.2) : SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD, BITFIELD_RO.
//!
//! Les chaînes sont modifiées en place (`Bytes` ? `Vec<u8>` sans copie quand le tampon est
//! unique). BITOP et BITCOUNT sur de gros volumes sont répartis sur plusieurs threads.

use crate::command::{ClientState, CommandContext};
use crate::resp::RespValue;
use crate::store::{DbInner, WRONGTYPE};
use crate::util::*;
use crate::value::{Entry, RedisValue};
use bytes::Bytes;

const ERR_BIT_OFFSET: &str = "ERR bit offset is not an integer or out of range";
const ERR_BIT_VALUE: &str = "ERR bit is not an integer or out of range";
const ERR_BITFIELD_TYPE: &str =
    "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.";
/// `proto-max-bulk-len` par défaut (512 Mo) : borne des offsets en octets.
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;
/// En dessous, le coût de création des threads dépasse le gain.
const PAR_THRESHOLD: usize = 4 << 20;

/// Offset de bit (`#n` = n × largeur pour BITFIELD), borné à 512 Mo.
fn bit_offset(raw: &[u8], hash_bits: Option<u32>) -> Result<u64, RespValue> {
    let (digits, mul) = match (raw.first(), hash_bits) {
        (Some(b'#'), Some(bits)) => (&raw[1..], bits as i64),
        _ => (raw, 1),
    };
    parse_i64(digits)
        .and_then(|v| v.checked_mul(mul))
        .filter(|v| *v >= 0 && (v >> 3) < MAX_BULK_LEN)
        .map(|v| v as u64)
        .ok_or_else(|| err(ERR_BIT_OFFSET))
}

/// Chaîne en lecture (absente ? `None`).
fn read_string<'a>(d: &'a mut DbInner, key: &Bytes) -> Result<Option<&'a Bytes>, &'static str> {
    match d.get_fresh(key) {
        None => Ok(None),
        Some(Entry { value: RedisValue::String(s), .. }) => Ok(Some(s)),
        Some(_) => Err(WRONGTYPE),
    }
}

/// `lookupStringForBitCommand` : crée ou agrandit la chaîne pour couvrir `max_bit`.
/// Retourne la chaîne et `dirty` (créée ou agrandie).
fn string_for_bits<'a>(d: &'a mut DbInner, key: &Bytes, max_bit: u64) -> Result<(&'a mut Bytes, bool), &'static str> {
    let need = (max_bit >> 3) as usize + 1;
    d.purge_if_expired(key);
    let mut created = false;
    let e = d.map.entry(key.clone()).or_insert_with(|| {
        created = true;
        Entry::new(RedisValue::String(Bytes::from(vec![0u8; need])))
    });
    let RedisValue::String(s) = &mut e.value else {
        return Err(WRONGTYPE);
    };
    let grown = s.len() < need;
    if grown {
        edit(s, |v| v.resize(need, 0));
    }
    Ok((s, created || grown))
}

/// Modifie une chaîne en place (pas de copie si le tampon n'est pas partagé).
fn edit<R>(s: &mut Bytes, f: impl FnOnce(&mut Vec<u8>) -> R) -> R {
    let mut v: Vec<u8> = std::mem::take(s).into();
    let r = f(&mut v);
    *s = Bytes::from(v);
    r
}

fn after_write(ctx: &CommandContext, state: &ClientState, args: &[Bytes], event: &str) {
    ctx.db.touch();
    ctx.notify_key(state.db_index, &args[1], event);
    ctx.propagate(args);
}

/// SETBIT key offset value.
pub(crate) fn cmd_setbit(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let offset = match bit_offset(&args[2], None) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let on = match parse_i64(&args[3]) {
        Some(v @ (0 | 1)) => v as u8,
        _ => return err(ERR_BIT_VALUE),
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        let (s, dirty) = string_for_bits(d, &args[1], offset)?;
        let (byte, shift) = ((offset >> 3) as usize, 7 - (offset & 7) as u32);
        let old = (s[byte] >> shift) & 1;
        if old != on {
            edit(s, |v| v[byte] = (v[byte] & !(1 << shift)) | (on << shift));
        }
        Ok::<_, &'static str>((old, dirty || old != on))
    });
    match r {
        Err(e) => err(e),
        Ok((old, changed)) => {
            if changed {
                after_write(ctx, state, args, "setbit");
            }
            RespValue::Integer(old as i64)
        }
    }
}

/// GETBIT key offset.
pub(crate) fn cmd_getbit(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let offset = match bit_offset(&args[2], None) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        Ok::<_, &'static str>(read_string(d, &args[1])?.map_or(0, |s| {
            s.get((offset >> 3) as usize).map_or(0, |b| (b >> (7 - (offset & 7))) & 1)
        }))
    });
    reply(r.map(|v| RespValue::Integer(v as i64)))
}

/// Normalise `[start, end]` (indices négatifs, unité octet ou bit) en bornes **de bits**
/// inclusives. `None` = intervalle vide.
fn bit_range(start: i64, end: i64, strlen: usize, isbit: bool) -> Option<(u64, u64)> {
    let totlen = if isbit { strlen as i64 * 8 } else { strlen as i64 };
    let norm = |v: i64| if v < 0 { (totlen + v).max(0) } else { v };
    let (start, end) = (norm(start), norm(end).min(totlen - 1));
    if start > end || totlen == 0 {
        return None;
    }
    Some(if isbit {
        (start as u64, end as u64)
    } else {
        (start as u64 * 8, end as u64 * 8 + 7)
    })
}

/// Masque des bits de l'octet `idx` compris dans `[bs, be]`.
fn edge_mask(idx: u64, bs: u64, be: u64) -> u8 {
    let mut m = 0xFFu8;
    if idx == bs >> 3 {
        m &= 0xFF >> (bs & 7);
    }
    if idx == be >> 3 {
        m &= 0xFF << (7 - (be & 7));
    }
    m
}

fn popcount(bytes: &[u8]) -> u64 {
    let count = |chunk: &[u8]| -> u64 {
        let mut words = chunk.chunks_exact(8);
        let mut n: u64 = words
            .by_ref()
            .map(|w| u64::from_ne_bytes(w.try_into().expect("8 octets")).count_ones() as u64)
            .sum();
        n += words.remainder().iter().map(|b| b.count_ones() as u64).sum::<u64>();
        n
    };
    if bytes.len() < PAR_THRESHOLD {
        return count(bytes);
    }
    let parts = std::thread::available_parallelism().map_or(1, |n| n.get());
    let size = bytes.len().div_ceil(parts);
    std::thread::scope(|sc| {
        let handles: Vec<_> = bytes.chunks(size).map(|c| sc.spawn(move || count(c))).collect();
        handles.into_iter().map(|h| h.join().unwrap_or(0)).sum()
    })
}

/// Nombre de bits à 1 dans `[bs, be]` (bits inclusifs).
fn count_bits(p: &[u8], bs: u64, be: u64) -> u64 {
    let (first, last) = ((bs >> 3) as usize, (be >> 3) as usize);
    let mut n = popcount(&p[first..=last]);
    for idx in [first, last] {
        let m = edge_mask(idx as u64, bs, be);
        n -= (p[idx] & !m).count_ones() as u64;
        if first == last {
            break;
        }
    }
    n
}

fn parse_unit(raw: &[u8]) -> Result<bool, RespValue> {
    match upper(raw).as_str() {
        "BIT" => Ok(true),
        "BYTE" => Ok(false),
        _ => Err(err(ERR_SYNTAX)),
    }
}

/// BITCOUNT key [start end [BYTE|BIT]].
pub(crate) fn cmd_bitcount(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let range = match args.len() {
        2 => None,
        4 | 5 => {
            let (Some(start), Some(end)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
                return err(ERR_INT);
            };
            let isbit = match args.get(4).map(|u| parse_unit(u)).transpose() {
                Ok(b) => b.unwrap_or(false),
                Err(e) => return e,
            };
            Some((start, end, isbit))
        }
        _ => return err(ERR_SYNTAX),
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        let Some(s) = read_string(d, &args[1])? else {
            return Ok(0);
        };
        let window = match range {
            None => bit_range(0, -1, s.len(), false),
            Some((start, end, _)) if start < 0 && end < 0 && start > end => None,
            Some((start, end, isbit)) => bit_range(start, end, s.len(), isbit),
        };
        Ok::<_, &'static str>(window.map_or(0, |(bs, be)| count_bits(s, bs, be)))
    });
    reply(r.map(|n| RespValue::Integer(n as i64)))
}

/// Premier bit égal à `bit` dans `[bs, be]`, recherche par mots de 64 bits.
fn find_bit(p: &[u8], bit: bool, bs: u64, be: u64) -> Option<u64> {
    let (first, last) = (bs >> 3, be >> 3);
    let probe = |idx: u64| -> Option<u64> {
        let raw = if bit { p[idx as usize] } else { !p[idx as usize] };
        let b = raw & edge_mask(idx, bs, be);
        (b != 0).then(|| idx * 8 + b.leading_zeros() as u64)
    };
    if let Some(pos) = probe(first) {
        return Some(pos);
    }
    if first == last {
        return None;
    }
    let skip = if bit { 0u64 } else { u64::MAX };
    let mut idx = first + 1;
    while idx + 8 <= last {
        let w = u64::from_be_bytes(p[idx as usize..idx as usize + 8].try_into().expect("8 octets"));
        if w != skip {
            break;
        }
        idx += 8;
    }
    (idx..=last).find_map(probe)
}

/// BITPOS key bit [start [end [BYTE|BIT]]].
pub(crate) fn cmd_bitpos(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let bit = match parse_i64(&args[2]) {
        None => return err(ERR_INT),
        Some(b @ (0 | 1)) => b == 1,
        Some(_) => return err("ERR The bit argument must be 1 or 0."),
    };
    if args.len() > 6 {
        return err(ERR_SYNTAX);
    }
    let Some(start) = args.get(3).map_or(Some(0), |a| parse_i64(a)) else {
        return err(ERR_INT);
    };
    let isbit = match args.get(5).map(|u| parse_unit(u)).transpose() {
        Ok(b) => b.unwrap_or(false),
        Err(e) => return e,
    };
    let end_given = args.len() >= 5;
    let end = if end_given {
        match parse_i64(&args[4]) {
            Some(e) => Some(e),
            None => return err(ERR_INT),
        }
    } else {
        None
    };
    let r = ctx.db.with_db(state.db_index, |d| {
        let Some(s) = read_string(d, &args[1])? else {
            return Ok(if bit { -1 } else { 0 });
        };
        let Some((bs, be)) = bit_range(start, end.unwrap_or(-1), s.len(), isbit) else {
            return Ok(-1);
        };
        Ok::<_, &'static str>(match find_bit(s, bit, bs, be) {
            Some(pos) => pos as i64,
            None if bit || end_given => -1,
            None => be as i64 + 1,
        })
    });
    reply(r.map(RespValue::Integer))
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BitOp {
    And,
    Or,
    Xor,
    Not,
}

/// Applique `op` sur `[from, from + out.len())` de toutes les sources (complétées par des zéros).
fn bitop_chunk(op: BitOp, srcs: &[&[u8]], from: usize, out: &mut [u8]) {
    let end = from + out.len();
    let slice = |s: &[u8]| -> (usize, usize) { (from.min(s.len()), end.min(s.len())) };
    match op {
        BitOp::Not => {
            let (a, b) = slice(srcs[0]);
            for (o, x) in out.iter_mut().zip(&srcs[0][a..b]) {
                *o = !x;
            }
            out[b - a..].fill(0xFF);
        }
        BitOp::And => {
            let (a, b) = slice(srcs[0]);
            out[..b - a].copy_from_slice(&srcs[0][a..b]);
            out[b - a..].fill(0);
            for s in &srcs[1..] {
                let (a, b) = slice(s);
                for (o, x) in out.iter_mut().zip(&s[a..b]) {
                    *o &= x;
                }
                out[b - a..].fill(0);
            }
        }
        BitOp::Or | BitOp::Xor => {
            out.fill(0);
            for s in srcs {
                let (a, b) = slice(s);
                if op == BitOp::Or {
                    out.iter_mut().zip(&s[a..b]).for_each(|(o, x)| *o |= x);
                } else {
                    out.iter_mut().zip(&s[a..b]).for_each(|(o, x)| *o ^= x);
                }
            }
        }
    }
}

fn bitop_compute(op: BitOp, srcs: &[&[u8]], maxlen: usize) -> Vec<u8> {
    let mut res = vec![0u8; maxlen];
    if maxlen * srcs.len() < PAR_THRESHOLD {
        bitop_chunk(op, srcs, 0, &mut res);
        return res;
    }
    let parts = std::thread::available_parallelism().map_or(1, |n| n.get());
    let size = maxlen.div_ceil(parts);
    std::thread::scope(|sc| {
        for (i, chunk) in res.chunks_mut(size).enumerate() {
            sc.spawn(move || bitop_chunk(op, srcs, i * size, chunk));
        }
    });
    res
}

/// BITOP AND|OR|XOR|NOT destkey key [key …].
pub(crate) fn cmd_bitop(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let op = match upper(&args[1]).as_str() {
        "AND" => BitOp::And,
        "OR" => BitOp::Or,
        "XOR" => BitOp::Xor,
        "NOT" => BitOp::Not,
        _ => return err(ERR_SYNTAX),
    };
    if op == BitOp::Not && args.len() != 4 {
        return err("ERR BITOP NOT must be called with a single source key.");
    }
    let dst = &args[2];
    let keys = &args[3..];
    let r = ctx.db.with_db(state.db_index, |d| {
        for k in keys {
            d.purge_if_expired(k);
        }
        let mut srcs: Vec<&[u8]> = Vec::with_capacity(keys.len());
        for k in keys {
            match d.map.get(k).map(|e| &e.value) {
                None => srcs.push(&[]),
                Some(RedisValue::String(s)) => srcs.push(s),
                Some(_) => return Err(WRONGTYPE),
            }
        }
        let maxlen = srcs.iter().map(|s| s.len()).max().unwrap_or(0);
        let res = (maxlen > 0).then(|| bitop_compute(op, &srcs, maxlen));
        let existed = d.map.remove(dst).is_some_and(|e| !e.is_expired());
        if let Some(res) = res {
            d.map.insert(dst.clone(), Entry::new(RedisValue::String(Bytes::from(res))));
        }
        Ok((maxlen, existed))
    });
    match r {
        Err(e) => err(e),
        Ok((maxlen, existed)) => {
            if maxlen > 0 {
                ctx.db.touch();
                ctx.notify_key(state.db_index, dst, "set");
                ctx.propagate(args);
            } else if existed {
                ctx.db.touch();
                ctx.notify_key(state.db_index, dst, "del");
                ctx.propagate(&[Bytes::from_static(b"DEL"), dst.clone()]);
            }
            RespValue::Integer(maxlen as i64)
        }
    }
}

// ----------------------------- BITFIELD -----------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FieldOp {
    Get,
    Set(i64),
    IncrBy(i64),
}

#[derive(Clone, Copy, Debug)]
struct Field {
    op: FieldOp,
    offset: u64,
    bits: u32,
    signed: bool,
    overflow: Overflow,
}

/// `i<1..64>` ou `u<1..63>` (préfixe sensible à la casse, comme Redis).
fn field_type(raw: &[u8]) -> Result<(bool, u32), RespValue> {
    let signed = match raw.first() {
        Some(b'i') => true,
        Some(b'u') => false,
        _ => return Err(err(ERR_BITFIELD_TYPE)),
    };
    match parse_i64(&raw[1..]) {
        Some(b) if b >= 1 && (b <= 63 || (signed && b == 64)) => Ok((signed, b as u32)),
        _ => Err(err(ERR_BITFIELD_TYPE)),
    }
}

fn get_unsigned(p: &[u8], offset: u64, bits: u32) -> u64 {
    let mut v = 0u64;
    for o in offset..offset + bits as u64 {
        let byte = p.get((o >> 3) as usize).copied().unwrap_or(0);
        v = (v << 1) | ((byte >> (7 - (o & 7))) & 1) as u64;
    }
    v
}

fn get_signed(p: &[u8], offset: u64, bits: u32) -> i64 {
    let v = get_unsigned(p, offset, bits) as i64;
    if bits < 64 && v & (1i64 << (bits - 1)) != 0 {
        v | (-1i64 << bits)
    } else {
        v
    }
}

fn set_bits(p: &mut [u8], offset: u64, bits: u32, value: u64) {
    for j in 0..bits as u64 {
        let bitval = ((value >> (bits as u64 - 1 - j)) & 1) as u8;
        let o = offset + j;
        let (byte, shift) = ((o >> 3) as usize, 7 - (o & 7));
        p[byte] = (p[byte] & !(1 << shift)) | (bitval << shift);
    }
}

/// `checkUnsignedBitfieldOverflow` : `Some(limite)` si débordement.
fn unsigned_overflow(value: u64, incr: i64, bits: u32, ow: Overflow) -> Option<u64> {
    let max = (1u64 << bits) - 1;
    let maxincr = max.wrapping_sub(value) as i64;
    let minincr = value.wrapping_neg() as i64;
    let wrap = || value.wrapping_add(incr as u64) & max;
    if value > max || (incr > 0 && incr > maxincr) {
        Some(if ow == Overflow::Wrap { wrap() } else { max })
    } else if incr < 0 && incr < minincr {
        Some(if ow == Overflow::Wrap { wrap() } else { 0 })
    } else {
        None
    }
}

/// `checkSignedBitfieldOverflow` (reproduit fidèlement, débordements de calcul compris).
fn signed_overflow(value: i64, incr: i64, bits: u32, ow: Overflow) -> Option<i64> {
    let max = if bits == 64 { i64::MAX } else { (1i64 << (bits - 1)) - 1 };
    let min = -max - 1;
    let maxincr = (max as u64).wrapping_sub(value as u64) as i64;
    let minincr = min.wrapping_sub(value);
    let wrap = || {
        let mut c = (value as u64).wrapping_add(incr as u64);
        if bits < 64 {
            let mask = u64::MAX << bits;
            if c & (1u64 << (bits - 1)) != 0 {
                c |= mask;
            } else {
                c &= !mask;
            }
        }
        c as i64
    };
    if value > max || (bits != 64 && incr > maxincr) || (value >= 0 && incr > 0 && incr > maxincr) {
        Some(if ow == Overflow::Wrap { wrap() } else { max })
    } else if value < min || (bits != 64 && incr < minincr) || (value < 0 && incr < 0 && incr < minincr) {
        Some(if ow == Overflow::Wrap { wrap() } else { min })
    } else {
        None
    }
}

fn parse_fields(args: &[Bytes]) -> Result<Vec<Field>, RespValue> {
    let mut ops = Vec::new();
    let mut overflow = Overflow::Wrap;
    let mut j = 2;
    while j < args.len() {
        let rem = args.len() - j - 1;
        let sub = upper(&args[j]);
        let kind = match sub.as_str() {
            "GET" if rem >= 2 => 0,
            "SET" if rem >= 3 => 1,
            "INCRBY" if rem >= 3 => 2,
            "OVERFLOW" if rem >= 1 => {
                overflow = match upper(&args[j + 1]).as_str() {
                    "WRAP" => Overflow::Wrap,
                    "SAT" => Overflow::Sat,
                    "FAIL" => Overflow::Fail,
                    _ => return Err(err("ERR Invalid OVERFLOW type specified")),
                };
                j += 2;
                continue;
            }
            _ => return Err(err(ERR_SYNTAX)),
        };
        let (signed, bits) = field_type(&args[j + 1])?;
        let offset = bit_offset(&args[j + 2], Some(bits))?;
        let op = if kind == 0 {
            FieldOp::Get
        } else {
            let v = parse_i64(&args[j + 3]).ok_or_else(|| err(ERR_INT))?;
            if kind == 1 {
                FieldOp::Set(v)
            } else {
                FieldOp::IncrBy(v)
            }
        };
        ops.push(Field { op, offset, bits, signed, overflow });
        j += if kind == 0 { 3 } else { 4 };
    }
    Ok(ops)
}

/// Exécute une écriture ; `None` = débordement FAIL (rien n'est écrit). `(réponse, modifié)`.
fn apply_write(p: &mut [u8], f: &Field) -> Option<(i64, bool)> {
    let (incr, set) = match f.op {
        FieldOp::IncrBy(v) => (v, None),
        FieldOp::Set(v) => (0, Some(v)),
        FieldOp::Get => unreachable!("GET traité à part"),
    };
    if f.signed {
        let old = get_signed(p, f.offset, f.bits);
        let base = set.unwrap_or(old);
        let ov = signed_overflow(base, incr, f.bits, f.overflow);
        if ov.is_some() && f.overflow == Overflow::Fail {
            return None;
        }
        let new = ov.unwrap_or_else(|| base.wrapping_add(incr));
        set_bits(p, f.offset, f.bits, new as u64);
        Some((if set.is_some() { old } else { new }, old != new))
    } else {
        let old = get_unsigned(p, f.offset, f.bits);
        let base = set.map_or(old, |v| v as u64);
        let ov = unsigned_overflow(base, incr, f.bits, f.overflow);
        if ov.is_some() && f.overflow == Overflow::Fail {
            return None;
        }
        let new = ov.unwrap_or_else(|| base.wrapping_add(incr as u64));
        set_bits(p, f.offset, f.bits, new);
        Some((if set.is_some() { old as i64 } else { new as i64 }, old != new))
    }
}

fn read_field(p: &[u8], f: &Field) -> i64 {
    if f.signed {
        get_signed(p, f.offset, f.bits)
    } else {
        get_unsigned(p, f.offset, f.bits) as i64
    }
}

fn bitfield_generic(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes], ro: bool) -> RespValue {
    let fields = match parse_fields(args) {
        Ok(f) => f,
        Err(e) => return e,
    };
    let highest = fields
        .iter()
        .filter(|f| f.op != FieldOp::Get)
        .map(|f| f.offset + f.bits as u64 - 1)
        .max();
    if highest.is_some() && ro {
        return err("ERR BITFIELD_RO only supports the GET subcommand");
    }
    let r = ctx.db.with_db(state.db_index, |d| {
        let Some(highest) = highest else {
            let s = read_string(d, &args[1])?;
            let p: &[u8] = s.map_or(&[][..], |s| &s[..]);
            let out = fields.iter().map(|f| RespValue::Integer(read_field(p, f))).collect();
            return Ok((out, false, false));
        };
        let (s, dirty) = string_for_bits(d, &args[1], highest)?;
        let (out, changed) = edit(s, |p| {
            let mut changed = false;
            let out: Vec<RespValue> = fields
                .iter()
                .map(|f| match f.op {
                    FieldOp::Get => RespValue::Integer(read_field(p, f)),
                    _ => match apply_write(p, f) {
                        None => RespValue::null_bulk(),
                        Some((v, ch)) => {
                            changed |= dirty || ch;
                            RespValue::Integer(v)
                        }
                    },
                })
                .collect();
            (out, changed)
        });
        Ok::<_, &'static str>((out, changed, dirty))
    });
    match r {
        Err(e) => err(e),
        Ok((out, changed, dirty)) => {
            if changed {
                after_write(ctx, state, args, "setbit");
            } else if dirty {
                ctx.propagate(args);
            }
            RespValue::array(out)
        }
    }
}

/// BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset incr] [OVERFLOW …].
pub(crate) fn cmd_bitfield(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    bitfield_generic(ctx, state, args, false)
}

/// BITFIELD_RO key [GET type offset …].
pub(crate) fn cmd_bitfield_ro(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    bitfield_generic(ctx, state, args, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_and_counts() {
        let p = b"foobar";
        assert_eq!(count_bits(p, 0, 47), 26);
        let (bs, be) = bit_range(1, 1, 6, false).unwrap();
        assert_eq!(count_bits(p, bs, be), 6);
        let (bs, be) = bit_range(5, 30, 6, true).unwrap();
        assert_eq!(count_bits(p, bs, be), 17);
        assert_eq!(bit_range(5, 2, 6, false), None);
        assert_eq!(bit_range(0, -1, 0, false), None);
    }

    #[test]
    fn positions() {
        let p = [0xFFu8, 0xF0, 0x00];
        assert_eq!(find_bit(&p, false, 0, 23), Some(12));
        assert_eq!(find_bit(&p, true, 12, 23), None);
        assert_eq!(find_bit(&[0xFF; 32], false, 0, 255), None);
        let mut big = vec![0u8; 64];
        big[40] = 0x01;
        assert_eq!(find_bit(&big, true, 3, 511), Some(40 * 8 + 7));
    }

    #[test]
    fn field_overflow_rules() {
        assert_eq!(unsigned_overflow(250, 10, 8, Overflow::Wrap), Some(4));
        assert_eq!(unsigned_overflow(250, 10, 8, Overflow::Sat), Some(255));
        assert_eq!(unsigned_overflow(5, -10, 8, Overflow::Sat), Some(0));
        assert_eq!(unsigned_overflow(5, 3, 8, Overflow::Fail), None);
        assert_eq!(signed_overflow(120, 10, 8, Overflow::Wrap), Some(-126));
        assert_eq!(signed_overflow(-120, -10, 8, Overflow::Sat), Some(-128));
        assert_eq!(signed_overflow(i64::MAX, 1, 64, Overflow::Wrap), Some(i64::MIN));
        let mut p = vec![0u8; 2];
        set_bits(&mut p, 4, 8, 0xAB);
        assert_eq!(get_unsigned(&p, 4, 8), 0xAB);
        assert_eq!(get_signed(&p, 4, 8), 0xABu8 as i8 as i64);
    }

    #[test]
    fn bitop_matches_scalar() {
        let a = vec![0b1100_1010u8; 100];
        let b = vec![0b1010_0110u8; 60];
        let srcs: Vec<&[u8]> = vec![&a, &b];
        let and = bitop_compute(BitOp::And, &srcs, 100);
        assert_eq!(and[0], 0b1000_0010);
        assert_eq!(and[99], 0);
        let xor = bitop_compute(BitOp::Xor, &srcs, 100);
        assert_eq!(xor[0], 0b0110_1100);
        assert_eq!(xor[99], 0b1100_1010);
        let not = bitop_compute(BitOp::Not, &srcs[..1], 100);
        assert_eq!(not[5], !0b1100_1010u8);
    }

    #[test]
    fn parallel_paths_agree() {
        let big: Vec<u8> = (0..(PAR_THRESHOLD + 12345)).map(|i| (i * 31 % 251) as u8).collect();
        let scalar: u64 = big.iter().map(|b| b.count_ones() as u64).sum();
        assert_eq!(popcount(&big), scalar);
        let srcs: Vec<&[u8]> = vec![&big, &big[..1000]];
        let par = bitop_compute(BitOp::Or, &srcs, big.len());
        assert_eq!(par, big);
    }
}
