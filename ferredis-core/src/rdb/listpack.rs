//! Listpack Redis minimal (encode append + decode) pour streams.

use super::io::{corrupt, Result};
use bytes::Bytes;

const LP_EOF: u8 = 0xff;
const LP_HDR: usize = 6;

#[derive(Debug, Clone)]
pub enum LpEl {
    Int(i64),
    Str(Bytes),
}

impl LpEl {
    pub fn as_i64(&self) -> Result<i64> {
        match self {
            Self::Int(v) => Ok(*v),
            Self::Str(s) => std::str::from_utf8(s)
                .ok()
                .and_then(|t| t.parse().ok())
                .ok_or_else(|| corrupt("listpack expected int")),
        }
    }

    pub fn as_bytes(&self) -> Bytes {
        match self {
            Self::Int(v) => Bytes::from(v.to_string()),
            Self::Str(s) => s.clone(),
        }
    }
}

pub fn encode(elements: &[LpEl]) -> Vec<u8> {
    let mut body = Vec::new();
    for el in elements {
        append_el(&mut body, el);
    }
    let total = LP_HDR + body.len() + 1;
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&(total as u32).to_le_bytes());
    let num = if elements.len() < 0xffff {
        elements.len() as u16
    } else {
        0xffff
    };
    out.extend_from_slice(&num.to_le_bytes());
    out.extend_from_slice(&body);
    out.push(LP_EOF);
    debug_assert_eq!(out.len(), total);
    out
}

fn append_el(out: &mut Vec<u8>, el: &LpEl) {
    let start = out.len();
    match el {
        LpEl::Int(v) => encode_int(out, *v),
        LpEl::Str(s) => encode_str(out, s),
    }
    let el_len = out.len() - start;
    encode_backlen(out, el_len);
}

fn encode_int(out: &mut Vec<u8>, v: i64) {
    if (0..128).contains(&v) {
        out.push(v as u8);
    } else if (-4096..4096).contains(&v) {
        let u = (v as i16) as u16;
        out.push(0xc0 | ((u >> 8) as u8 & 0x1f));
        out.push((u & 0xff) as u8);
    } else if (-32768..32768).contains(&v) {
        out.push(0xf1);
        out.extend_from_slice(&(v as i16).to_le_bytes());
    } else if (-8_388_608..8_388_608).contains(&v) {
        out.push(0xf2);
        let b = (v as i32).to_le_bytes();
        out.extend_from_slice(&b[..3]);
    } else if (i32::MIN as i64..=i32::MAX as i64).contains(&v) {
        out.push(0xf3);
        out.extend_from_slice(&(v as i32).to_le_bytes());
    } else {
        out.push(0xf4);
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn encode_str(out: &mut Vec<u8>, s: &[u8]) {
    let len = s.len();
    if len < 64 {
        out.push(0x80 | (len as u8));
        out.extend_from_slice(s);
    } else if len < 4096 {
        out.push(0xe0 | ((len >> 8) as u8));
        out.push((len & 0xff) as u8);
        out.extend_from_slice(s);
    } else {
        out.push(0xf0);
        out.extend_from_slice(&(len as u32).to_le_bytes());
        out.extend_from_slice(s);
    }
}

fn encode_backlen(out: &mut Vec<u8>, mut len: usize) {
    if len <= 127 {
        out.push(len as u8);
        return;
    }
    // multi-byte backlen (Redis): little-endian 7-bit chunks, MSB continue
    let mut buf = [0u8; 5];
    let mut n = 0;
    while len >= 128 {
        buf[n] = (len & 0x7f) as u8 | 0x80;
        len >>= 7;
        n += 1;
    }
    buf[n] = len as u8;
    n += 1;
    out.extend_from_slice(&buf[..n]);
}

pub fn decode(lp: &[u8]) -> Result<Vec<LpEl>> {
    if lp.len() < LP_HDR + 1 {
        return Err(corrupt("listpack too short"));
    }
    let total = u32::from_le_bytes(lp[0..4].try_into().unwrap()) as usize;
    if total != lp.len() {
        return Err(corrupt("listpack size mismatch"));
    }
    if lp[lp.len() - 1] != LP_EOF {
        return Err(corrupt("listpack missing EOF"));
    }
    let mut i = LP_HDR;
    let mut out = Vec::new();
    while i < lp.len() - 1 {
        let (el, next) = decode_el(lp, i)?;
        out.push(el);
        i = next;
    }
    Ok(out)
}

fn decode_el(lp: &[u8], i: usize) -> Result<(LpEl, usize)> {
    let b = *lp.get(i).ok_or_else(|| corrupt("lp eof"))?;
    let (el, after_payload) = if b & 0x80 == 0 {
        (LpEl::Int(b as i64), i + 1)
    } else if b & 0xc0 == 0x80 {
        let len = (b & 0x3f) as usize;
        let start = i + 1;
        if start + len > lp.len() {
            return Err(corrupt("lp 6str oob"));
        }
        (
            LpEl::Str(Bytes::copy_from_slice(&lp[start..start + len])),
            start + len,
        )
    } else if b & 0xe0 == 0xc0 {
        let hi = (b & 0x1f) as i16;
        let lo = *lp.get(i + 1).ok_or_else(|| corrupt("lp 13int"))? as i16;
        let mut v = (hi << 8) | lo;
        if v & 0x1000 != 0 {
            v |= !0x1fff;
        }
        (LpEl::Int(v as i64), i + 2)
    } else if b & 0xf0 == 0xe0 {
        let len = (((b & 0x0f) as usize) << 8) | (*lp.get(i + 1).ok_or_else(|| corrupt("lp 12str"))? as usize);
        let start = i + 2;
        if start + len > lp.len() {
            return Err(corrupt("lp 12str oob"));
        }
        (
            LpEl::Str(Bytes::copy_from_slice(&lp[start..start + len])),
            start + len,
        )
    } else if b == 0xf0 {
        if i + 5 > lp.len() {
            return Err(corrupt("lp 32str hdr"));
        }
        let len = u32::from_le_bytes(lp[i + 1..i + 5].try_into().unwrap()) as usize;
        let start = i + 5;
        if start + len > lp.len() {
            return Err(corrupt("lp 32str oob"));
        }
        (
            LpEl::Str(Bytes::copy_from_slice(&lp[start..start + len])),
            start + len,
        )
    } else if b == 0xf1 {
        if i + 3 > lp.len() {
            return Err(corrupt("lp i16"));
        }
        let v = i16::from_le_bytes(lp[i + 1..i + 3].try_into().unwrap()) as i64;
        (LpEl::Int(v), i + 3)
    } else if b == 0xf2 {
        if i + 4 > lp.len() {
            return Err(corrupt("lp i24"));
        }
        let mut b4 = [0u8; 4];
        b4[..3].copy_from_slice(&lp[i + 1..i + 4]);
        if b4[2] & 0x80 != 0 {
            b4[3] = 0xff;
        }
        let v = i32::from_le_bytes(b4) as i64;
        (LpEl::Int(v), i + 4)
    } else if b == 0xf3 {
        if i + 5 > lp.len() {
            return Err(corrupt("lp i32"));
        }
        let v = i32::from_le_bytes(lp[i + 1..i + 5].try_into().unwrap()) as i64;
        (LpEl::Int(v), i + 5)
    } else if b == 0xf4 {
        if i + 9 > lp.len() {
            return Err(corrupt("lp i64"));
        }
        let v = i64::from_le_bytes(lp[i + 1..i + 9].try_into().unwrap());
        (LpEl::Int(v), i + 9)
    } else {
        return Err(corrupt(format!("lp unknown enc {b:#x}")));
    };
    let next = skip_backlen(lp, after_payload)?;
    Ok((el, next))
}

fn skip_backlen(lp: &[u8], mut i: usize) -> Result<usize> {
    loop {
        let b = *lp.get(i).ok_or_else(|| corrupt("lp backlen"))?;
        i += 1;
        if b & 0x80 == 0 {
            return Ok(i);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_mixed() {
        let els = vec![
            LpEl::Int(1),
            LpEl::Int(0),
            LpEl::Int(2),
            LpEl::Str(Bytes::from_static(b"a")),
            LpEl::Str(Bytes::from_static(b"b")),
            LpEl::Int(0),
        ];
        let lp = encode(&els);
        let got = decode(&lp).unwrap();
        assert_eq!(got.len(), els.len());
        assert_eq!(got[0].as_i64().unwrap(), 1);
        assert_eq!(got[3].as_bytes().as_ref(), b"a");
    }
}
