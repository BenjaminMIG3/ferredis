//! Lecteur de transition `FRDB001` → structures mémoire (migration one-shot).

use super::io::{corrupt, Result};
use crate::value::{
    Entry, RedisValue, Stream, StreamConsumer, StreamGroup, StreamNack, ZSet,
    STREAM_ENTRIES_READ_INVALID,
};
use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAGIC: &[u8] = b"FRDB001\n";

/// Décode un fichier FRDB001 ; retourne (dbs, codes function bruts reconstruits depuis FNS1).
pub fn decode_frdb(data: &[u8]) -> Result<(Vec<(usize, Vec<(Bytes, Entry)>)>, Vec<Bytes>)> {
    if !data.starts_with(MAGIC) {
        return Err(corrupt("not FRDB001"));
    }
    let mut i = MAGIC.len();
    let len = read_u64(data, &mut i)? as usize;
    if i + len > data.len() {
        return Err(corrupt("FRDB truncated"));
    }
    let body = &data[i..i + len];
    i += len;
    let dump = bincode_like_decode(body)?;
    let mut fn_codes = Vec::new();
    if i + 4 <= data.len() && &data[i..i + 4] == b"FNS1" {
        i += 4;
        let plen = read_u64(data, &mut i)? as usize;
        if i + plen > data.len() {
            return Err(corrupt("FNS1 truncated"));
        }
        // Convertit le payload FNS1 propriétaire en codes library (un par lib).
        fn_codes = fns1_to_codes(&data[i..i + plen])?;
    }
    Ok((dump, fn_codes))
}

fn fns1_to_codes(data: &[u8]) -> Result<Vec<Bytes>> {
    let mut i = 0;
    let n = read_u32(data, &mut i)? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let name = read_str(data, &mut i)?;
        let engine = read_str(data, &mut i)?;
        let code = read_str(data, &mut i)?;
        // Si le code a déjà un shebang, on le garde ; sinon on en préfixe un minimal.
        if code.starts_with("#!") {
            out.push(Bytes::from(code));
        } else {
            out.push(Bytes::from(format!(
                "#!{engine} name={name}\n{code}"
            )));
        }
    }
    Ok(out)
}

fn bincode_like_decode(data: &[u8]) -> Result<Vec<(usize, Vec<(Bytes, Entry)>)>> {
    let mut i = 0;
    let n_db = read_u32(data, &mut i)? as usize;
    let mut out = Vec::with_capacity(n_db);
    for _ in 0..n_db {
        let db = read_u32(data, &mut i)? as usize;
        let n = read_u32(data, &mut i)? as usize;
        let mut entries = Vec::with_capacity(n);
        for _ in 0..n {
            let k = read_bytes(data, &mut i)?;
            let e = read_entry(data, &mut i)?;
            entries.push((k, e));
        }
        out.push((db, entries));
    }
    Ok(out)
}

fn read_stream_entries(data: &[u8], i: &mut usize) -> Result<Stream> {
    let n = read_u32(data, i)? as usize;
    let mut stream = Stream::default();
    for _ in 0..n {
        let id_s = String::from_utf8_lossy(&read_bytes(data, i)?).into_owned();
        let nf = read_u32(data, i)? as usize;
        let mut fields = Vec::with_capacity(nf);
        for _ in 0..nf {
            fields.push((read_bytes(data, i)?, read_bytes(data, i)?));
        }
        let id = crate::value::parse_stream_id(&id_s).map_err(|e| corrupt(e.to_string()))?;
        if stream.entries.is_empty() {
            stream.first_id = id;
            stream.recorded_first_id = id;
        }
        stream.entries.insert(id, fields);
        stream.last_id = id;
        stream.entries_added = stream.entries_added.saturating_add(1);
    }
    stream.refresh_first_id();
    Ok(stream)
}

fn read_entry(data: &[u8], i: &mut usize) -> Result<Entry> {
    let ttl_unix_ms = read_u64(data, i)?;
    let tag = *data.get(*i).ok_or_else(|| corrupt("eof tag"))?;
    *i += 1;
    let value = match tag {
        1 => RedisValue::String(read_bytes(data, i)?),
        2 => {
            let n = read_u32(data, i)? as usize;
            let mut l = VecDeque::new();
            for _ in 0..n {
                l.push_back(read_bytes(data, i)?);
            }
            RedisValue::List(l)
        }
        3 => {
            let n = read_u32(data, i)? as usize;
            let mut h = HashMap::new();
            for _ in 0..n {
                h.insert(read_bytes(data, i)?, read_bytes(data, i)?);
            }
            RedisValue::Hash(crate::value::HashObject::from_map(h))
        }
        4 => {
            let n = read_u32(data, i)? as usize;
            let mut s = HashSet::new();
            for _ in 0..n {
                s.insert(read_bytes(data, i)?);
            }
            RedisValue::Set(s.into())
        }
        5 => {
            let n = read_u32(data, i)? as usize;
            let mut z = ZSet::default();
            for _ in 0..n {
                let m = read_bytes(data, i)?;
                let score = f64::from_bits(read_u64(data, i)?);
                z.insert(m, score);
            }
            RedisValue::ZSet(z)
        }
        6 => RedisValue::Stream(read_stream_entries(data, i)?),
        7 => {
            let mut stream = read_stream_entries(data, i)?;
            let last = String::from_utf8_lossy(&read_bytes(data, i)?).into_owned();
            stream.last_id = crate::value::parse_stream_id(&last).unwrap_or(stream.last_id);
            stream.entries_added = read_u64(data, i)?;
            let maxdel = String::from_utf8_lossy(&read_bytes(data, i)?).into_owned();
            stream.max_deleted_id =
                crate::value::parse_stream_id(&maxdel).unwrap_or_default();
            let rec = String::from_utf8_lossy(&read_bytes(data, i)?).into_owned();
            stream.recorded_first_id =
                crate::value::parse_stream_id(&rec).unwrap_or(stream.recorded_first_id);
            let ng = read_u32(data, i)? as usize;
            for _ in 0..ng {
                let name = read_bytes(data, i)?;
                let last_s = String::from_utf8_lossy(&read_bytes(data, i)?).into_owned();
                let last_id = crate::value::parse_stream_id(&last_s).unwrap_or_default();
                let er_raw = read_u64(data, i)?;
                let er = if er_raw == u64::MAX {
                    STREAM_ENTRIES_READ_INVALID
                } else {
                    er_raw as i64
                };
                let mut g = StreamGroup::new(name.clone(), last_id, er);
                let nc = read_u32(data, i)? as usize;
                for _ in 0..nc {
                    let cname = read_bytes(data, i)?;
                    let seen = read_u64(data, i)?;
                    let active = read_u64(data, i)? as i64;
                    g.consumers.insert(
                        cname.clone(),
                        StreamConsumer {
                            name: cname,
                            seen_time_ms: seen,
                            active_time_ms: active,
                            pending: Default::default(),
                        },
                    );
                }
                let np = read_u32(data, i)? as usize;
                for _ in 0..np {
                    let id_s = String::from_utf8_lossy(&read_bytes(data, i)?).into_owned();
                    let id = crate::value::parse_stream_id(&id_s).unwrap_or_default();
                    let consumer = read_bytes(data, i)?;
                    let dt = read_u64(data, i)?;
                    let dc = read_u64(data, i)?;
                    g.pel.insert(
                        id,
                        StreamNack {
                            consumer: consumer.clone(),
                            delivery_time_ms: dt,
                            delivery_count: dc,
                        },
                    );
                    if let Some(c) = g.consumers.get_mut(&consumer) {
                        c.pending.insert(id);
                    }
                }
                stream.groups.insert(name, g);
            }
            RedisValue::Stream(stream)
        }
        _ => return Err(corrupt("bad FRDB value tag")),
    };
    let mut entry = Entry::new(value);
    if ttl_unix_ms > 1 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if ttl_unix_ms > now {
            entry.expires_at = Some(Instant::now() + Duration::from_millis(ttl_unix_ms - now));
        } else {
            entry.expires_at = Some(Instant::now());
        }
    }
    Ok(entry)
}

fn read_u32(data: &[u8], i: &mut usize) -> Result<u32> {
    if *i + 4 > data.len() {
        return Err(corrupt("eof u32"));
    }
    let v = u32::from_le_bytes(data[*i..*i + 4].try_into().unwrap());
    *i += 4;
    Ok(v)
}

fn read_u64(data: &[u8], i: &mut usize) -> Result<u64> {
    if *i + 8 > data.len() {
        return Err(corrupt("eof u64"));
    }
    let v = u64::from_le_bytes(data[*i..*i + 8].try_into().unwrap());
    *i += 8;
    Ok(v)
}

fn read_bytes(data: &[u8], i: &mut usize) -> Result<Bytes> {
    let n = read_u32(data, i)? as usize;
    if *i + n > data.len() {
        return Err(corrupt("eof bytes"));
    }
    let b = Bytes::copy_from_slice(&data[*i..*i + n]);
    *i += n;
    Ok(b)
}

fn read_str(data: &[u8], i: &mut usize) -> Result<String> {
    Ok(String::from_utf8_lossy(&read_bytes(data, i)?).into_owned())
}
