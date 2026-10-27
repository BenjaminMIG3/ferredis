//! Encodage longueur / chaînes / doubles du format RDB Redis.

use bytes::Bytes;
use std::io;

pub const RDB_VERSION: u16 = 11;
pub const MAGIC: &[u8; 9] = b"REDIS0011";

pub const TYPE_STRING: u8 = 0;
pub const TYPE_LIST: u8 = 1;
pub const TYPE_SET: u8 = 2;
pub const TYPE_HASH: u8 = 4;
pub const TYPE_ZSET_2: u8 = 5;
pub const TYPE_HASH_ZIPMAP: u8 = 9;
pub const TYPE_LIST_ZIPLIST: u8 = 10;
pub const TYPE_SET_INTSET: u8 = 11;
pub const TYPE_ZSET_ZIPLIST: u8 = 12;
pub const TYPE_HASH_ZIPLIST: u8 = 13;
pub const TYPE_LIST_QUICKLIST: u8 = 14;
pub const TYPE_STREAM_LISTPACKS: u8 = 15;
pub const TYPE_HASH_LISTPACK: u8 = 16;
pub const TYPE_ZSET_LISTPACK: u8 = 17;
pub const TYPE_LIST_QUICKLIST_2: u8 = 18;
pub const TYPE_STREAM_LISTPACKS_2: u8 = 19;
pub const TYPE_SET_LISTPACK: u8 = 20;
pub const TYPE_STREAM_LISTPACKS_3: u8 = 21;
/// Types Stack Ferredis (persistés en RDB interne ; hors opcodes Redis officiels).
pub const TYPE_JSON: u8 = 80;
pub const TYPE_TIMESERIES: u8 = 81;
pub const TYPE_BLOOM: u8 = 82;
pub const TYPE_CUCKOO: u8 = 83;
pub const TYPE_CMS: u8 = 84;
pub const TYPE_TOPK: u8 = 85;
pub const TYPE_TDIGEST: u8 = 86;
pub const TYPE_VECTORSET: u8 = 87;
/// Hash avec TTL par champ (extension Ferredis ; Redis 7.4+ field expire).
pub const TYPE_HASH_EX: u8 = 88;

pub const OPCODE_FUNCTION2: u8 = 245;
pub const OPCODE_FUNCTION_PRE_GA: u8 = 246;
pub const OPCODE_IDLE: u8 = 248;
pub const OPCODE_FREQ: u8 = 249;
pub const OPCODE_AUX: u8 = 250;
pub const OPCODE_RESIZEDB: u8 = 251;
pub const OPCODE_EXPIRETIME_MS: u8 = 252;
pub const OPCODE_EXPIRETIME: u8 = 253;
pub const OPCODE_SELECTDB: u8 = 254;
pub const OPCODE_EOF: u8 = 255;

const ENC_INT8: u8 = 0;
const ENC_INT16: u8 = 1;
const ENC_INT32: u8 = 2;
const ENC_LZF: u8 = 3;

#[derive(Debug, thiserror::Error)]
pub enum RdbError {
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("RDB corrupt: {0}")]
    Corrupt(String),
    #[error("unsupported RDB type {0}")]
    UnsupportedType(u8),
    #[error("DUMP checksum mismatch")]
    BadChecksum,
    #[error("DUMP RDB version {0} too new")]
    BadVersion(u16),
}

pub type Result<T> = std::result::Result<T, RdbError>;

pub fn corrupt(msg: impl Into<String>) -> RdbError {
    RdbError::Corrupt(msg.into())
}

pub fn write_len(out: &mut Vec<u8>, len: u64) {
    if len < (1 << 6) {
        out.push(len as u8);
    } else if len < (1 << 14) {
        out.push(((len >> 8) as u8) | 0x40);
        out.push((len & 0xff) as u8);
    } else if len <= u32::MAX as u64 {
        out.push(0x80);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        out.push(0x81);
        out.extend_from_slice(&len.to_be_bytes());
    }
}

pub fn read_len(data: &[u8], i: &mut usize) -> Result<(u64, bool)> {
    let b = *data.get(*i).ok_or_else(|| corrupt("eof len"))?;
    *i += 1;
    match b >> 6 {
        0 => Ok(((b & 0x3f) as u64, false)),
        1 => {
            let b2 = *data.get(*i).ok_or_else(|| corrupt("eof len14"))?;
            *i += 1;
            Ok(((((b as u64) & 0x3f) << 8) | b2 as u64, false))
        }
        2 => {
            if b == 0x80 {
                if *i + 4 > data.len() {
                    return Err(corrupt("eof len32"));
                }
                let v = u32::from_be_bytes(data[*i..*i + 4].try_into().unwrap()) as u64;
                *i += 4;
                Ok((v, false))
            } else if b == 0x81 {
                if *i + 8 > data.len() {
                    return Err(corrupt("eof len64"));
                }
                let v = u64::from_be_bytes(data[*i..*i + 8].try_into().unwrap());
                *i += 8;
                Ok((v, false))
            } else {
                Err(corrupt(format!("bad len prefix {b:#x}")))
            }
        }
        _ => Ok(((b & 0x3f) as u64, true)),
    }
}

pub fn write_raw_string(out: &mut Vec<u8>, s: &[u8]) {
    write_len(out, s.len() as u64);
    out.extend_from_slice(s);
}

pub fn read_string(data: &[u8], i: &mut usize) -> Result<Bytes> {
    let (len_or_enc, is_enc) = read_len(data, i)?;
    if !is_enc {
        let n = len_or_enc as usize;
        if *i + n > data.len() {
            return Err(corrupt("eof string"));
        }
        let b = Bytes::copy_from_slice(&data[*i..*i + n]);
        *i += n;
        return Ok(b);
    }
    match len_or_enc as u8 {
        ENC_INT8 => {
            let v = *data.get(*i).ok_or_else(|| corrupt("eof i8"))? as i8;
            *i += 1;
            Ok(Bytes::from(v.to_string()))
        }
        ENC_INT16 => {
            if *i + 2 > data.len() {
                return Err(corrupt("eof i16"));
            }
            let v = i16::from_le_bytes(data[*i..*i + 2].try_into().unwrap());
            *i += 2;
            Ok(Bytes::from(v.to_string()))
        }
        ENC_INT32 => {
            if *i + 4 > data.len() {
                return Err(corrupt("eof i32"));
            }
            let v = i32::from_le_bytes(data[*i..*i + 4].try_into().unwrap());
            *i += 4;
            Ok(Bytes::from(v.to_string()))
        }
        ENC_LZF => {
            let (clen, _) = read_len(data, i)?;
            let (ulen, _) = read_len(data, i)?;
            let clen = clen as usize;
            if *i + clen > data.len() {
                return Err(corrupt("eof lzf"));
            }
            let compressed = &data[*i..*i + clen];
            *i += clen;
            let decompressed = lzf_decompress(compressed, ulen as usize)?;
            Ok(Bytes::from(decompressed))
        }
        other => Err(corrupt(format!("unknown string enc {other}"))),
    }
}

// typo fix - try_into not try to_into - I'll fix in the write

pub fn write_binary_double(out: &mut Vec<u8>, v: f64) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub fn read_binary_double(data: &[u8], i: &mut usize) -> Result<f64> {
    if *i + 8 > data.len() {
        return Err(corrupt("eof double"));
    }
    let v = f64::from_le_bytes(data[*i..*i + 8].try_into().unwrap());
    *i += 8;
    Ok(v)
}

pub fn write_ms_time(out: &mut Vec<u8>, ms: i64) {
    out.extend_from_slice(&ms.to_le_bytes());
}

pub fn read_ms_time(data: &[u8], i: &mut usize) -> Result<i64> {
    if *i + 8 > data.len() {
        return Err(corrupt("eof mstime"));
    }
    let v = i64::from_le_bytes(data[*i..*i + 8].try_into().unwrap());
    *i += 8;
    Ok(v)
}

pub fn write_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}

pub fn read_u8(data: &[u8], i: &mut usize) -> Result<u8> {
    let v = *data.get(*i).ok_or_else(|| corrupt("eof u8"))?;
    *i += 1;
    Ok(v)
}

/// Décompression LZF minimale (Redis `lzf_d`).
fn lzf_decompress(input: &[u8], expected: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(expected);
    let mut i = 0;
    while i < input.len() {
        let ctrl = input[i] as usize;
        i += 1;
        if ctrl < 32 {
            let len = ctrl + 1;
            if i + len > input.len() {
                return Err(corrupt("lzf literal overflow"));
            }
            out.extend_from_slice(&input[i..i + len]);
            i += len;
        } else {
            let len = ctrl >> 5;
            let mut len = if len == 7 {
                let mut n = 7;
                loop {
                    let b = *input.get(i).ok_or_else(|| corrupt("lzf len"))?;
                    i += 1;
                    n += b as usize;
                    if b != 255 {
                        break;
                    }
                }
                n
            } else {
                len
            };
            len += 2;
            let offset_hi = (ctrl & 0x1f) << 8;
            let offset_lo = *input.get(i).ok_or_else(|| corrupt("lzf off"))? as usize;
            i += 1;
            let offset = offset_hi | offset_lo;
            let start = out
                .len()
                .checked_sub(offset + 1)
                .ok_or_else(|| corrupt("lzf bad ref"))?;
            for j in 0..len {
                let b = *out.get(start + j).ok_or_else(|| corrupt("lzf ref oob"))?;
                out.push(b);
            }
        }
    }
    if out.len() != expected {
        return Err(corrupt(format!(
            "lzf size {} != expected {expected}",
            out.len()
        )));
    }
    Ok(out)
}
