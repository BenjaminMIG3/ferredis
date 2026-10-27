//! Parseurs d'options communs (DRY) pour plusieurs familles de commandes.

use bytes::Bytes;
use crate::command::arg_str;
use crate::resp::RespValue;
use crate::util::{err, now_unix_ms, parse_i64, upper, ERR_INT, ERR_SYNTAX};
use crate::value::HashExpireCond;

pub(crate) const ERR_NUMKEYS_POSITIVE: &str = "ERR numkeys should be greater than 0";
pub(crate) const ERR_COUNT_POSITIVE: &str = "ERR count should be greater than 0";
pub(crate) const ERR_LIMIT_NEGATIVE: &str = "ERR LIMIT can't be negative";

/// Entier dans `[min, max]`, sinon `msg` (équivalent de `getRangeLongFromObjectOrReply`).
pub(crate) fn ranged_long(raw: &[u8], min: i64, max: i64, msg: &str) -> Result<i64, RespValue> {
    match parse_i64(raw) {
        Some(v) if (min..=max).contains(&v) => Ok(v),
        _ => Err(err(msg)),
    }
}

/// Options `[LIMIT n]` (répétables, la dernière gagne) de SINTERCARD / ZINTERCARD.
/// `0` = pas de limite.
pub(crate) fn parse_card_limit(args: &[Bytes], from: usize) -> Result<usize, RespValue> {
    let mut limit = 0;
    let mut j = from;
    while j < args.len() {
        if upper(&args[j]) == "LIMIT" && j + 1 < args.len() {
            limit = ranged_long(&args[j + 1], 0, i64::MAX, ERR_LIMIT_NEGATIVE)? as usize;
            j += 2;
        } else {
            return Err(err(ERR_SYNTAX));
        }
    }
    Ok(limit)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    #[default]
    Default,
    Sync,
    Async,
}

/// Parse `FIELDS numfields field [field …]` à partir de `from` ; retourne (fields, next_idx).
pub(crate) fn parse_fields_section(args: &[Bytes], from: usize) -> Result<(Vec<Bytes>, usize), RespValue> {
    if from >= args.len() || upper(&args[from]) != "FIELDS" {
        return Err(err(ERR_SYNTAX));
    }
    let n = parse_i64(args.get(from + 1).ok_or_else(|| err(ERR_SYNTAX))?)
        .ok_or_else(|| err(ERR_INT))?;
    if n <= 0 {
        return Err(err("ERR numfields should be > 0"));
    }
    let n = n as usize;
    let start = from + 2;
    if args.len() < start + n {
        return Err(err(ERR_SYNTAX));
    }
    Ok((args[start..start + n].to_vec(), start + n))
}

/// Parse une condition optionnelle `NX|XX|GT|LT` ; avance l'index.
pub(crate) fn parse_expire_cond(args: &[Bytes], i: &mut usize) -> Result<HashExpireCond, RespValue> {
    if *i >= args.len() {
        return Ok(HashExpireCond::None);
    }
    let cond = match upper(&args[*i]).as_str() {
        "NX" => HashExpireCond::Nx,
        "XX" => HashExpireCond::Xx,
        "GT" => HashExpireCond::Gt,
        "LT" => HashExpireCond::Lt,
        "FIELDS" => return Ok(HashExpireCond::None),
        _ => return Err(err(ERR_SYNTAX)),
    };
    *i += 1;
    Ok(cond)
}

/// Convertit EX/PX/EXAT/PXAT (+ valeur relative ou abs) en échéance abs ms.
/// `None` = PERSIST demandé ailleurs ; ici retourne Always Some sauf erreur.
pub(crate) fn abs_expire_ms(opt: &str, raw: &[u8], cmd: &str) -> Result<i64, RespValue> {
    let invalid = || err(&format!("ERR invalid expire time in '{cmd}' command"));
    let n = parse_i64(raw).ok_or_else(|| err(ERR_INT))?;
    let now = now_unix_ms();
    match opt {
        "EX" if n > 0 => Ok(now.saturating_add(n.saturating_mul(1000))),
        "PX" if n > 0 => Ok(now.saturating_add(n)),
        "EXAT" if n > 0 => Ok(n.saturating_mul(1000)),
        "PXAT" if n > 0 => Ok(n),
        "EX" | "PX" | "EXAT" | "PXAT" => Err(invalid()),
        _ => Err(err(ERR_SYNTAX)),
    }
}

/// Échéance abs ms depuis secondes/ms relatives ou timestamps (HEXPIRE*).
pub(crate) enum HashExpireWhen {
    /// Secondes relatives → abs ms
    Seconds(i64),
    /// Millisecondes relatives → abs ms
    Millis(i64),
    /// Unix seconds abs
    AtSeconds(i64),
    /// Unix ms abs
    AtMillis(i64),
}

impl HashExpireWhen {
    pub fn to_abs_ms(self) -> i64 {
        let now = now_unix_ms();
        match self {
            Self::Seconds(s) => now.saturating_add(s.saturating_mul(1000)),
            Self::Millis(m) => now.saturating_add(m),
            Self::AtSeconds(s) => s.saturating_mul(1000),
            Self::AtMillis(m) => m,
        }
    }
}

/// Lit `SYNC` / `ASYNC` en fin d'arguments (FLUSH*, SCRIPT FLUSH, etc.).
pub fn trailing_sync_mode(args: &[Bytes], min_len: usize) -> Result<(SyncMode, usize), &'static str> {
    if args.len() < min_len {
        return Err("arity");
    }
    if args.len() == min_len {
        return Ok((SyncMode::Default, min_len));
    }
    if args.len() != min_len + 1 {
        return Err("syntax");
    }
    match arg_str(args, args.len() - 1).unwrap_or("").to_ascii_uppercase().as_str() {
        "SYNC" => Ok((SyncMode::Sync, min_len)),
        "ASYNC" => Ok((SyncMode::Async, min_len)),
        _ => Err("syntax"),
    }
}
