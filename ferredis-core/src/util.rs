//! Helpers partagés par les modules de commandes (parsing, formatage, erreurs).

use crate::resp::RespValue;
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const ERR_INT: &str = "ERR value is not an integer or out of range";
pub(crate) const ERR_FLOAT: &str = "ERR value is not a valid float";
pub(crate) const ERR_SYNTAX: &str = "ERR syntax error";
pub(crate) const ERR_NO_SUCH_KEY: &str = "ERR no such key";

/// Horodatage Unix en millisecondes (partagé clés / champs hash — DRY).
#[inline]
pub(crate) fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Entier signé strict (comme `string2ll` de Redis : pas d'espaces, pas de `+`).
pub(crate) fn parse_i64(b: &[u8]) -> Option<i64> {
    if b.is_empty() || b[0] == b'+' {
        return None;
    }
    std::str::from_utf8(b).ok()?.parse().ok()
}

/// Flottant fini ou infini ; NaN refusé.
pub(crate) fn parse_f64(b: &[u8]) -> Option<f64> {
    crate::value::parse_score(std::str::from_utf8(b).ok()?)
}

/// Mise en majuscules ASCII d'un argument (options, sous-commandes).
pub(crate) fn upper(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_ascii_uppercase()
}

/// Formatage d'un double à la Redis : entiers sans partie décimale, `inf`/`-inf`.
pub(crate) fn fmt_f64(v: f64) -> String {
    if v.is_infinite() {
        return if v > 0.0 { "inf".into() } else { "-inf".into() };
    }
    if v.fract() == 0.0 && v.abs() < 1e17 {
        format!("{v:.0}")
    } else {
        format!("{v}")
    }
}

pub(crate) fn bulk_f64(v: f64) -> RespValue {
    RespValue::bulk(Bytes::from(fmt_f64(v)))
}

pub(crate) fn err(msg: &str) -> RespValue {
    RespValue::err(msg)
}

/// Convertit `Result<RespValue, &str>` en réponse (erreur → `-ERR…`).
pub(crate) fn reply(r: Result<RespValue, &str>) -> RespValue {
    match r {
        Ok(v) => v,
        Err(e) => RespValue::err(e),
    }
}

/// Liste de paires `(membre, score)` → tableau RESP2 (avec ou sans scores).
pub(crate) fn pairs_to_resp(pairs: Vec<(Bytes, f64)>, withscores: bool) -> RespValue {
    let mut out = Vec::with_capacity(pairs.len() * if withscores { 2 } else { 1 });
    for (m, s) in pairs {
        out.push(RespValue::bulk(m));
        if withscores {
            out.push(bulk_f64(s));
        }
    }
    RespValue::array(out)
}

pub(crate) fn bulk_array(items: impl IntoIterator<Item = Bytes>) -> RespValue {
    RespValue::array(items.into_iter().map(RespValue::bulk).collect())
}

/// Double façon `%.17Lg` de Redis (`addReplyHumanLongDouble`) : zéros finaux retirés.
pub(crate) fn fmt_human(v: f64) -> String {
    if v.is_infinite() {
        return fmt_f64(v);
    }
    let mut s = format!("{v:.17}");
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s == "-0" {
        s = "0".into();
    }
    s
}

/// Échantillon aléatoire façon SRANDMEMBER / HRANDFIELD / ZRANDMEMBER :
/// `count >= 0` → au plus `count` éléments distincts, `count < 0` → `|count|` tirages avec remise.
pub(crate) fn random_sample<X: Copy>(items: impl Iterator<Item = X>, len: usize, count: i64) -> Vec<X> {
    use rand::seq::{IteratorRandom, SliceRandom};
    use rand::Rng;
    let mut rng = rand::thread_rng();
    if count >= 0 {
        let n = count as usize;
        if n >= len {
            return items.collect();
        }
        let mut picked = items.choose_multiple(&mut rng, n);
        picked.shuffle(&mut rng);
        return picked;
    }
    let pool: Vec<X> = items.collect();
    if pool.is_empty() {
        return Vec::new();
    }
    (0..count.unsigned_abs()).map(|_| pool[rng.gen_range(0..pool.len())]).collect()
}

/// Normalise `[start, stop]` (indices négatifs permis) sur une séquence de `len`
/// éléments. Retourne `None` si la fenêtre est vide.
pub(crate) fn normalize_range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let start = if start < 0 { (len + start).max(0) } else { start };
    let stop = if stop < 0 { len + stop } else { stop.min(len - 1) };
    if len == 0 || stop < 0 || start >= len || start > stop {
        None
    } else {
        Some((start as usize, stop as usize))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_formatting() {
        assert_eq!(fmt_f64(3.0), "3");
        assert_eq!(fmt_f64(1.5), "1.5");
        assert_eq!(fmt_f64(f64::NEG_INFINITY), "-inf");
        assert_eq!(fmt_f64(1730000000.123456), "1730000000.123456");
    }

    #[test]
    fn ranges() {
        assert_eq!(normalize_range(0, -1, 5), Some((0, 4)));
        assert_eq!(normalize_range(-2, -1, 5), Some((3, 4)));
        assert_eq!(normalize_range(3, 1, 5), None);
        assert_eq!(normalize_range(0, 10, 0), None);
        assert_eq!(normalize_range(-100, 2, 5), Some((0, 2)));
    }

    #[test]
    fn strict_int() {
        assert_eq!(parse_i64(b"42"), Some(42));
        assert_eq!(parse_i64(b"-7"), Some(-7));
        assert_eq!(parse_i64(b"+7"), None);
        assert_eq!(parse_i64(b""), None);
        assert_eq!(parse_i64(b"1.5"), None);
    }
}
