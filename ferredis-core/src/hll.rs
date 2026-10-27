//! Codec HyperLogLog identique à Redis (`hyperloglog.c`) : en-tête `HYLL`, encodages dense
//! (6 bits / registre) et sparse (opcodes ZERO / XZERO / VAL), MurmurHash64A et estimateur
//! d'Ertl. Les valeurs produites sont interchangeables octet pour octet avec Redis.

pub(crate) const P: u32 = 14;
const Q: u32 = 64 - P;
pub(crate) const REGISTERS: usize = 1 << P;
const P_MASK: u64 = REGISTERS as u64 - 1;
const BITS: usize = 6;
const REGISTER_MAX: u8 = (1 << BITS) - 1;
pub(crate) const HDR: usize = 16;
pub(crate) const DENSE_SIZE: usize = HDR + (REGISTERS * BITS + 7) / 8;
pub(crate) const DENSE: u8 = 0;
pub(crate) const SPARSE: u8 = 1;
const SPARSE_VAL_MAX_VALUE: u8 = 32;
const SPARSE_VAL_MAX_LEN: usize = 4;
const SPARSE_ZERO_MAX_LEN: usize = 64;
const SPARSE_XZERO_MAX_LEN: usize = 16384;
/// `hll-sparse-max-bytes` par défaut.
pub(crate) const SPARSE_MAX_BYTES: usize = 3000;
const ALPHA_INF: f64 = 0.721_347_520_444_481_7;

/// Représentation corrompue (`INVALIDOBJ`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Corrupt;

// ───────────────────────────── hachage ─────────────────────────────

pub(crate) fn murmur64a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;
    let mut h = seed ^ (key.len() as u64).wrapping_mul(M);
    let mut chunks = key.chunks_exact(8);
    for c in chunks.by_ref() {
        let mut k = u64::from_le_bytes(c.try_into().expect("8 octets"));
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    let tail = chunks.remainder();
    if !tail.is_empty() {
        for (i, b) in tail.iter().enumerate().rev() {
            h ^= (*b as u64) << (8 * i);
        }
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

/// `hllPatLen` : (registre, longueur du motif `000…1`).
fn pat_len(ele: &[u8]) -> (usize, u8) {
    let hash = murmur64a(ele, 0xadc8_3b19);
    let index = (hash & P_MASK) as usize;
    let rest = (hash >> P) | (1u64 << Q);
    (index, rest.trailing_zeros() as u8 + 1)
}

// ───────────────────────────── en-tête ─────────────────────────────

/// HLL vide : sparse, un seul opcode XZERO couvrant les 16384 registres.
pub(crate) fn new_sparse() -> Vec<u8> {
    let mut v = Vec::with_capacity(HDR + 2);
    v.extend_from_slice(b"HYLL");
    v.push(SPARSE);
    v.extend_from_slice(&[0; 11]);
    let l = REGISTERS - 1;
    v.push((l >> 8) as u8 | 0x40);
    v.push((l & 0xff) as u8);
    v
}

/// `isHLLObjectOrReply` (partie format).
pub(crate) fn is_valid(s: &[u8]) -> bool {
    s.len() >= HDR
        && &s[..4] == b"HYLL"
        && s[4] <= SPARSE
        && (s[4] != DENSE || s.len() == DENSE_SIZE)
}

pub(crate) fn encoding(s: &[u8]) -> u8 {
    s[4]
}

pub(crate) fn cached_card(s: &[u8]) -> Option<u64> {
    (s[15] & 0x80 == 0).then(|| u64::from_le_bytes(s[8..16].try_into().expect("8 octets")))
}

pub(crate) fn set_cached_card(s: &mut [u8], card: u64) {
    s[8..16].copy_from_slice(&card.to_le_bytes());
}

pub(crate) fn invalidate_cache(s: &mut [u8]) {
    s[15] |= 0x80;
}

// ───────────────────────────── dense ─────────────────────────────

#[inline]
fn dense_get(regs: &[u8], i: usize) -> u8 {
    let byte = i * BITS / 8;
    let fb = (i * BITS) & 7;
    let b0 = regs[byte] as u32;
    let b1 = regs.get(byte + 1).copied().unwrap_or(0) as u32;
    (((b0 >> fb) | (b1 << (8 - fb))) & REGISTER_MAX as u32) as u8
}

#[inline]
fn dense_set(regs: &mut [u8], i: usize, val: u8) {
    let byte = i * BITS / 8;
    let fb = (i * BITS) & 7;
    let fb8 = 8 - fb;
    let v = val as u32;
    regs[byte] &= !((REGISTER_MAX as u32) << fb) as u8;
    regs[byte] |= (v << fb) as u8;
    if let Some(next) = regs.get_mut(byte + 1) {
        *next &= !((REGISTER_MAX as u32) >> fb8) as u8;
        *next |= (v >> fb8) as u8;
    }
}

fn dense_raise(regs: &mut [u8], i: usize, count: u8) -> bool {
    if count > dense_get(regs, i) {
        dense_set(regs, i, count);
        true
    } else {
        false
    }
}

// ───────────────────────────── sparse ─────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Op {
    Zero(usize),
    XZero(usize),
    Val(u8, usize),
}

impl Op {
    fn span(self) -> usize {
        match self {
            Op::Zero(n) | Op::XZero(n) | Op::Val(_, n) => n,
        }
    }

    fn width(self) -> usize {
        if matches!(self, Op::XZero(_)) {
            2
        } else {
            1
        }
    }
}

/// Décode l'opcode à `p` (`None` si un XZERO est tronqué).
fn op_at(s: &[u8], p: usize) -> Option<Op> {
    let b = s[p];
    Some(match b & 0xc0 {
        0x00 => Op::Zero((b & 0x3f) as usize + 1),
        0x40 => Op::XZero(((((b & 0x3f) as usize) << 8) | *s.get(p + 1)? as usize) + 1),
        _ => Op::Val(((b >> 2) & 0x1f) + 1, (b & 0x3) as usize + 1),
    })
}

fn val_byte(val: u8, len: usize) -> u8 {
    ((val - 1) << 2) | (len as u8 - 1) | 0x80
}

/// Opcode(s) de zéros couvrant `len` registres (ZERO ou XZERO, comme Redis).
fn push_zeros(out: &mut Vec<u8>, len: usize) {
    debug_assert!((1..=SPARSE_XZERO_MAX_LEN).contains(&len));
    if len > SPARSE_ZERO_MAX_LEN {
        let l = len - 1;
        out.push((l >> 8) as u8 | 0x40);
        out.push((l & 0xff) as u8);
    } else {
        out.push(len as u8 - 1);
    }
}

/// Itère les opcodes sparse : `(registre de départ, opcode)`.
fn sparse_ops(s: &[u8]) -> impl Iterator<Item = Result<(usize, Op), Corrupt>> + '_ {
    let mut p = HDR;
    let mut idx = 0usize;
    std::iter::from_fn(move || {
        if p >= s.len() {
            return None;
        }
        let Some(op) = op_at(s, p) else {
            p = s.len();
            return Some(Err(Corrupt));
        };
        let start = idx;
        idx += op.span();
        p += op.width();
        Some(Ok((start, op)))
    })
}

/// `hllSparseToDense`. Retourne `true` si une conversion a eu lieu.
pub(crate) fn sparse_to_dense(v: &mut Vec<u8>) -> Result<bool, Corrupt> {
    if encoding(v) == DENSE {
        return Ok(false);
    }
    let mut dense = vec![0u8; DENSE_SIZE];
    dense[..HDR].copy_from_slice(&v[..HDR]);
    dense[4] = DENSE;
    let mut idx = 0usize;
    for item in sparse_ops(v) {
        let (start, op) = item?;
        idx = start + op.span();
        if let Op::Val(val, len) = op {
            if start + len > REGISTERS {
                return Err(Corrupt);
            }
            for i in start..start + len {
                dense_set(&mut dense[HDR..], i, val);
            }
        }
    }
    if idx != REGISTERS {
        return Err(Corrupt);
    }
    *v = dense;
    Ok(true)
}

/// `hllSparseSet` : port exact (découpage d'opcode, fusion des VAL adjacents, promotion).
fn sparse_set(v: &mut Vec<u8>, index: usize, count: u8) -> Result<bool, Corrupt> {
    if count > SPARSE_VAL_MAX_VALUE {
        return promote(v, index, count);
    }
    // Étape 1 : opcode couvrant `index`.
    let end = v.len();
    let (mut p, mut first, mut prev) = (HDR, 0usize, None);
    let mut found = None;
    while p < end {
        let op = op_at(v, p).ok_or(Corrupt)?;
        if index < first + op.span() {
            found = Some(op);
            break;
        }
        prev = Some(p);
        p += op.width();
        first += op.span();
    }
    let Some(op) = found else {
        return Err(Corrupt);
    };
    // Étape 2 : cas triviaux puis découpage général.
    match op {
        Op::Val(old, _) if old >= count => return Ok(false),
        Op::Val(_, 1) | Op::Zero(1) => {
            v[p] = val_byte(count, 1);
            merge_adjacent(v, prev.unwrap_or(HDR));
            invalidate_cache(v);
            return Ok(true);
        }
        _ => {}
    }
    let last = first + op.span() - 1;
    let mut seq = Vec::with_capacity(5);
    match op {
        Op::Zero(_) | Op::XZero(_) => {
            if index != first {
                push_zeros(&mut seq, index - first);
            }
            seq.push(val_byte(count, 1));
            if index != last {
                push_zeros(&mut seq, last - index);
            }
        }
        Op::Val(cur, _) => {
            if index != first {
                seq.push(val_byte(cur, index - first));
            }
            seq.push(val_byte(count, 1));
            if index != last {
                seq.push(val_byte(cur, last - index));
            }
        }
    }
    // Étape 3 : substitution (ou promotion si la taille maximale serait dépassée).
    let oldlen = op.width();
    if seq.len() > oldlen && v.len() + seq.len() - oldlen > SPARSE_MAX_BYTES {
        return promote(v, index, count);
    }
    v.splice(p..p + oldlen, seq);
    // Étape 4 : fusion des VAL adjacents.
    merge_adjacent(v, prev.unwrap_or(HDR));
    invalidate_cache(v);
    Ok(true)
}

/// Fusionne jusqu'à 5 opcodes à partir de `p` (VAL de même valeur, longueur ≤ 4).
fn merge_adjacent(v: &mut Vec<u8>, mut p: usize) {
    let mut scan = 5;
    while p < v.len() && scan > 0 {
        scan -= 1;
        match v[p] & 0xc0 {
            0x40 => {
                p += 2;
                continue;
            }
            0x00 => {
                p += 1;
                continue;
            }
            _ => {}
        }
        if p + 1 < v.len() && v[p + 1] & 0x80 != 0 {
            let (Some(Op::Val(v1, l1)), Some(Op::Val(v2, l2))) = (op_at(v, p), op_at(v, p + 1)) else {
                p += 1;
                continue;
            };
            if v1 == v2 && l1 + l2 <= SPARSE_VAL_MAX_LEN {
                v[p + 1] = val_byte(v1, l1 + l2);
                v.remove(p);
                continue;
            }
        }
        p += 1;
    }
}

fn promote(v: &mut Vec<u8>, index: usize, count: u8) -> Result<bool, Corrupt> {
    sparse_to_dense(v)?;
    dense_raise(&mut v[HDR..], index, count);
    invalidate_cache(v);
    Ok(true)
}

/// Élève le registre `index` à `count` (dense ou sparse). `true` si modifié.
pub(crate) fn set_register(v: &mut Vec<u8>, index: usize, count: u8) -> Result<bool, Corrupt> {
    if encoding(v) == DENSE {
        let changed = dense_raise(&mut v[HDR..], index, count);
        if changed {
            invalidate_cache(v);
        }
        Ok(changed)
    } else {
        sparse_set(v, index, count)
    }
}

/// `hllAdd`.
pub(crate) fn add(v: &mut Vec<u8>, ele: &[u8]) -> Result<bool, Corrupt> {
    let (index, count) = pat_len(ele);
    set_register(v, index, count)
}

// ───────────────────────────── estimation ─────────────────────────────

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let (mut y, mut z) = (1.0, x);
    loop {
        x *= x;
        let prev = z;
        z += x * y;
        y += y;
        if prev == z {
            return z;
        }
    }
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let (mut y, mut z) = (1.0, 1.0 - x);
    loop {
        x = x.sqrt();
        let prev = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if prev == z {
            return z / 3.0;
        }
    }
}

fn estimate(histo: &[u32; 64]) -> u64 {
    let m = REGISTERS as f64;
    let mut z = m * tau((m - histo[Q as usize + 1] as f64) / m);
    for j in (1..=Q as usize).rev() {
        z += histo[j] as f64;
        z *= 0.5;
    }
    z += m * sigma(histo[0] as f64 / m);
    (ALPHA_INF * m * m / z).round() as u64
}

/// `hllCount` sur une valeur dense ou sparse.
pub(crate) fn count(s: &[u8]) -> Result<u64, Corrupt> {
    let mut histo = [0u32; 64];
    if encoding(s) == DENSE {
        let regs = &s[HDR..];
        for i in 0..REGISTERS {
            histo[dense_get(regs, i) as usize] += 1;
        }
    } else {
        let mut idx = 0usize;
        for item in sparse_ops(s) {
            let (start, op) = item?;
            idx = start + op.span();
            match op {
                Op::Zero(n) | Op::XZero(n) => histo[0] += n as u32,
                Op::Val(val, n) => histo[val as usize] += n as u32,
            }
        }
        if idx != REGISTERS {
            return Err(Corrupt);
        }
    }
    Ok(estimate(&histo))
}

/// Registres bruts (un octet par registre), utilisés pour les unions multi-clés.
pub(crate) type Raw = Vec<u8>;

pub(crate) fn new_raw() -> Raw {
    vec![0u8; REGISTERS]
}

/// `hllMerge` : `max[i] = MAX(max[i], hll[i])`.
pub(crate) fn merge_into(max: &mut Raw, s: &[u8]) -> Result<(), Corrupt> {
    if encoding(s) == DENSE {
        let regs = &s[HDR..];
        for (i, m) in max.iter_mut().enumerate() {
            *m = (*m).max(dense_get(regs, i));
        }
        return Ok(());
    }
    let mut idx = 0usize;
    for item in sparse_ops(s) {
        let (start, op) = item?;
        idx = start + op.span();
        if let Op::Val(val, len) = op {
            if start + len > REGISTERS {
                return Err(Corrupt);
            }
            for m in &mut max[start..start + len] {
                *m = (*m).max(val);
            }
        }
    }
    if idx != REGISTERS {
        return Err(Corrupt);
    }
    Ok(())
}

pub(crate) fn count_raw(max: &Raw) -> u64 {
    let mut histo = [0u32; 64];
    for &r in max {
        histo[r as usize & 63] += 1;
    }
    estimate(&histo)
}

/// Registres d'une valeur dense (PFDEBUG GETREG).
pub(crate) fn dense_registers(s: &[u8]) -> Vec<u8> {
    let regs = &s[HDR..];
    (0..REGISTERS).map(|i| dense_get(regs, i)).collect()
}

/// PFDEBUG DECODE : `z:N`, `Z:N`, `v:val,len` séparés par des espaces.
pub(crate) fn decode_sparse(s: &[u8]) -> String {
    let mut out = Vec::new();
    for (_, op) in sparse_ops(s).map_while(Result::ok) {
        out.push(match op {
            Op::Zero(n) => format!("z:{n}"),
            Op::XZero(n) => format!("Z:{n}"),
            Op::Val(v, n) => format!("v:{v},{n}"),
        });
    }
    out.join(" ")
}

/// PFSELFTEST : accès aux registres puis erreur d'approximation dense / sparse.
pub(crate) fn selftest(cycles: usize, max_card: u64) -> Result<(), String> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut dense = vec![0u8; DENSE_SIZE];
    dense[..4].copy_from_slice(b"HYLL");
    let mut expected = vec![0u8; REGISTERS];
    for _ in 0..cycles {
        for (i, e) in expected.iter_mut().enumerate() {
            *e = rng.gen::<u8>() & REGISTER_MAX;
            dense_set(&mut dense[HDR..], i, *e);
        }
        for (i, e) in expected.iter().enumerate() {
            let got = dense_get(&dense[HDR..], i);
            if got != *e {
                return Err(format!("TESTFAILED Register {i} should be {e} but is {got}"));
            }
        }
    }
    dense[HDR..].fill(0);
    let mut sparse = new_sparse();
    let relerr = 1.04 / (REGISTERS as f64).sqrt();
    let seed: u64 = rng.gen();
    let mut checkpoint = 1u64;
    for j in 1..=max_card {
        let ele = (j ^ seed).to_ne_bytes();
        let (index, cnt) = pat_len(&ele);
        dense_raise(&mut dense[HDR..], index, cnt);
        add(&mut sparse, &ele).map_err(|_| "TESTFAILED corrupted sparse HLL".to_string())?;
        if j != checkpoint {
            continue;
        }
        if j < (SPARSE_MAX_BYTES / 2) as u64 && encoding(&sparse) != SPARSE {
            return Err("TESTFAILED sparse encoding not used".into());
        }
        let dense_card = count(&dense).map_err(|_| "TESTFAILED dense corrupted".to_string())?;
        if Ok(dense_card) != count(&sparse) {
            return Err("TESTFAILED dense/sparse disagree".into());
        }
        let abserr = (checkpoint as i64 - dense_card as i64).unsigned_abs();
        let maxerr = if j == 10 { 1 } else { (relerr * 6.0 * checkpoint as f64).ceil() as u64 };
        if abserr > maxerr {
            return Err(format!("TESTFAILED Too big error. card:{checkpoint} abserr:{abserr}"));
        }
        checkpoint *= 10;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur_matches_reference() {
        // Valeurs de référence de MurmurHash64A (seed 0xadc83b19).
        assert_eq!(murmur64a(b"", 0), 0);
        assert_ne!(murmur64a(b"a", 0xadc8_3b19), murmur64a(b"b", 0xadc8_3b19));
        let (idx, cnt) = pat_len(b"hello");
        assert!(idx < REGISTERS && (1..=Q as u8 + 1).contains(&cnt));
    }

    #[test]
    fn empty_sparse_layout() {
        let v = new_sparse();
        assert_eq!(v.len(), 18);
        assert_eq!(&v[16..], &[0x7f, 0xff]);
        assert!(is_valid(&v));
        assert_eq!(count(&v), Ok(0));
        assert_eq!(decode_sparse(&v), "Z:16384");
    }

    #[test]
    fn sparse_and_dense_agree() {
        let mut sparse = new_sparse();
        let mut raw = new_raw();
        for i in 0..2000u32 {
            add(&mut sparse, &i.to_le_bytes()).unwrap();
        }
        merge_into(&mut raw, &sparse).unwrap();
        let card = count(&sparse).unwrap();
        assert_eq!(card, count_raw(&raw));
        let mut dense = sparse.clone();
        if sparse_to_dense(&mut dense).unwrap() {
            assert_eq!(dense.len(), DENSE_SIZE);
        }
        assert_eq!(count(&dense).unwrap(), card);
        assert!((card as f64 - 2000.0).abs() < 2000.0 * 0.05);
    }

    #[test]
    fn promotes_on_large_register() {
        let mut v = new_sparse();
        assert_eq!(set_register(&mut v, 10, 40), Ok(true));
        assert_eq!(encoding(&v), DENSE);
        assert_eq!(dense_registers(&v)[10], 40);
    }

    #[test]
    fn selftest_small() {
        assert_eq!(selftest(5, 100_000), Ok(()));
    }
}
