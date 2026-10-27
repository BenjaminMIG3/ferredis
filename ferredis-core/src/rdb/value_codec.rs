//! Sérialisation / désérialisation d'une valeur Redis (types simples + stream).

use super::io::{
    corrupt, read_binary_double, read_len, read_string, write_binary_double, write_len,
    write_raw_string, write_u8, Result, TYPE_BLOOM, TYPE_CMS, TYPE_CUCKOO, TYPE_HASH,
    TYPE_HASH_LISTPACK, TYPE_HASH_ZIPLIST, TYPE_HASH_ZIPMAP, TYPE_JSON, TYPE_LIST,
    TYPE_LIST_QUICKLIST, TYPE_LIST_QUICKLIST_2, TYPE_LIST_ZIPLIST, TYPE_SET, TYPE_SET_INTSET,
    TYPE_SET_LISTPACK, TYPE_STREAM_LISTPACKS, TYPE_STREAM_LISTPACKS_2, TYPE_STREAM_LISTPACKS_3,
    TYPE_STRING, TYPE_TDIGEST, TYPE_TIMESERIES, TYPE_TOPK, TYPE_VECTORSET, TYPE_HASH_EX,
    TYPE_ZSET_2, TYPE_ZSET_LISTPACK, TYPE_ZSET_ZIPLIST,
};
use crate::probabilistic::{BloomFilter, CountMinSketch, CuckooFilter, TDigest, TopK};
use crate::util::now_unix_ms;
use crate::value::{HashField, HashObject};
use crate::vector_set::{Quantization, VectorSet};
use super::listpack::{self, LpEl};
use super::stream_codec::{self, stream_rdb_type};
use crate::value::{
    RedisValue, TimeSeries, TsAggregator, TsDuplicatePolicy, TsRule, ZSet,
};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

pub fn encode_value(out: &mut Vec<u8>, value: &RedisValue) {
    match value {
        RedisValue::String(s) => {
            write_u8(out, TYPE_STRING);
            write_raw_string(out, s);
        }
        RedisValue::List(l) => {
            write_u8(out, TYPE_LIST);
            write_len(out, l.len() as u64);
            for v in l {
                write_raw_string(out, v);
            }
        }
        RedisValue::Set(s) => {
            write_u8(out, TYPE_SET);
            write_len(out, s.len() as u64);
            for m in s {
                write_raw_string(out, m);
            }
        }
        RedisValue::Hash(h) => {
            let now = now_unix_ms();
            if h.has_any_field_expire() {
                write_u8(out, TYPE_HASH_EX);
                let live: Vec<_> = h
                    .fields
                    .iter()
                    .filter(|(_, f)| !f.is_expired(now))
                    .collect();
                write_len(out, live.len() as u64);
                for (k, f) in live {
                    write_raw_string(out, k);
                    write_raw_string(out, &f.value);
                    write_len(out, f.expires_at_ms.unwrap_or(0) as u64);
                }
            } else {
                write_u8(out, TYPE_HASH);
                write_len(out, h.live_len() as u64);
                for (k, v) in h.iter_live() {
                    write_raw_string(out, k);
                    write_raw_string(out, v);
                }
            }
        }
        RedisValue::ZSet(z) => {
            write_u8(out, TYPE_ZSET_2);
            write_len(out, z.members.len() as u64);
            for (m, score) in &z.members {
                write_raw_string(out, m);
                write_binary_double(out, *score);
            }
        }
        RedisValue::Stream(s) => {
            write_u8(out, stream_rdb_type());
            stream_codec::encode_stream(out, s);
        }
        RedisValue::Json(v) => {
            write_u8(out, TYPE_JSON);
            write_raw_string(out, v.to_string().as_bytes());
        }
        RedisValue::TimeSeries(ts) => {
            write_u8(out, TYPE_TIMESERIES);
            encode_timeseries(out, ts);
        }
        RedisValue::Bloom(b) => {
            write_u8(out, TYPE_BLOOM);
            write_raw_string(out, &b.scandump());
        }
        RedisValue::Cuckoo(c) => {
            write_u8(out, TYPE_CUCKOO);
            write_raw_string(out, &c.scandump());
        }
        RedisValue::Cms(c) => {
            write_u8(out, TYPE_CMS);
            write_len(out, c.width as u64);
            write_len(out, c.depth as u64);
            write_len(out, c.counters.len() as u64);
            for &v in &c.counters {
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
        RedisValue::TopK(t) => {
            write_u8(out, TYPE_TOPK);
            write_len(out, t.k as u64);
            write_len(out, t.width as u64);
            write_len(out, t.depth as u64);
            write_binary_double(out, t.decay);
            write_len(out, t.heap.len() as u64);
            for (k, v) in &t.heap {
                write_raw_string(out, k);
                out.extend_from_slice(&v.to_le_bytes());
            }
            write_len(out, t.cms.width as u64);
            write_len(out, t.cms.depth as u64);
            write_len(out, t.cms.counters.len() as u64);
            for &v in &t.cms.counters {
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
        RedisValue::TDigest(t) => {
            write_u8(out, TYPE_TDIGEST);
            write_binary_double(out, t.compression);
            write_binary_double(out, t.min);
            write_binary_double(out, t.max);
            write_binary_double(out, t.count);
            write_len(out, t.centroids.len() as u64);
            for &(m, w) in &t.centroids {
                write_binary_double(out, m);
                write_binary_double(out, w);
            }
        }
        RedisValue::VectorSet(v) => {
            write_u8(out, TYPE_VECTORSET);
            write_len(out, v.dim as u64);
            write_u8(
                out,
                match v.quantization {
                    Quantization::F32 => 0,
                    Quantization::Q8 => 1,
                    Quantization::Bin => 2,
                },
            );
            write_len(out, v.m as u64);
            write_len(out, v.ef as u64);
            write_len(out, v.vectors.len() as u64);
            for (name, vals) in &v.vectors {
                write_raw_string(out, name);
                write_len(out, vals.len() as u64);
                for &f in vals {
                    out.extend_from_slice(&f.to_le_bytes());
                }
            }
            write_len(out, v.attrs.len() as u64);
            for (k, a) in &v.attrs {
                write_raw_string(out, k);
                write_raw_string(out, a);
            }
        }
    }
}

fn encode_timeseries(out: &mut Vec<u8>, ts: &TimeSeries) {
    write_len(out, ts.retention_ms);
    write_len(out, ts.chunk_size as u64);
    write_u8(
        out,
        match ts.duplicate_policy {
            TsDuplicatePolicy::Block => 0,
            TsDuplicatePolicy::First => 1,
            TsDuplicatePolicy::Last => 2,
            TsDuplicatePolicy::Min => 3,
            TsDuplicatePolicy::Max => 4,
            TsDuplicatePolicy::Sum => 5,
        },
    );
    write_len(out, ts.labels.len() as u64);
    for (k, v) in &ts.labels {
        write_raw_string(out, k.as_bytes());
        write_raw_string(out, v.as_bytes());
    }
    write_len(out, ts.samples.len() as u64);
    for (&t, &v) in &ts.samples {
        out.extend_from_slice(&t.to_le_bytes());
        write_binary_double(out, v);
    }
    write_len(out, ts.rules.len() as u64);
    for r in &ts.rules {
        write_raw_string(out, &r.dest_key);
        write_raw_string(out, r.aggregator.as_str().as_bytes());
        write_len(out, r.bucket_ms);
        match r.align_timestamp {
            Some(a) => {
                write_u8(out, 1);
                out.extend_from_slice(&a.to_le_bytes());
            }
            None => write_u8(out, 0),
        }
    }
    match &ts.source_key {
        Some(k) => {
            write_u8(out, 1);
            write_raw_string(out, k);
        }
        None => write_u8(out, 0),
    }
}

fn decode_timeseries(data: &[u8], i: &mut usize) -> Result<TimeSeries> {
    let (retention, _) = read_len(data, i)?;
    let (chunk, _) = read_len(data, i)?;
    let dup = match {
        let b = *data.get(*i).ok_or_else(|| corrupt("eof"))?;
        *i += 1;
        b
    } {
        0 => TsDuplicatePolicy::Block,
        1 => TsDuplicatePolicy::First,
        2 => TsDuplicatePolicy::Last,
        3 => TsDuplicatePolicy::Min,
        4 => TsDuplicatePolicy::Max,
        _ => TsDuplicatePolicy::Sum,
    };
    let (nlab, _) = read_len(data, i)?;
    let mut labels = BTreeMap::new();
    for _ in 0..nlab {
        let k = String::from_utf8_lossy(&read_string(data, i)?).into_owned();
        let v = String::from_utf8_lossy(&read_string(data, i)?).into_owned();
        labels.insert(k, v);
    }
    let (ns, _) = read_len(data, i)?;
    let mut samples = BTreeMap::new();
    for _ in 0..ns {
        let t = {
            let b = data.get(*i..*i + 8).ok_or_else(|| corrupt("eof i64"))?;
            *i += 8;
            i64::from_le_bytes(b.try_into().unwrap())
        };
        let v = read_binary_double(data, i)?;
        samples.insert(t, v);
    }
    let (nr, _) = read_len(data, i)?;
    let mut rules = Vec::with_capacity(nr as usize);
    for _ in 0..nr {
        let dest = read_string(data, i)?;
        let agg_s = String::from_utf8_lossy(&read_string(data, i)?).into_owned();
        let agg = TsAggregator::parse(&agg_s).unwrap_or(TsAggregator::Avg);
        let (bucket, _) = read_len(data, i)?;
        let has_align = {
            let b = *data.get(*i).ok_or_else(|| corrupt("eof"))?;
            *i += 1;
            b
        };
        let align = if has_align == 1 {
            let b = data.get(*i..*i + 8).ok_or_else(|| corrupt("eof i64"))?;
            *i += 8;
            Some(i64::from_le_bytes(b.try_into().unwrap()))
        } else {
            None
        };
        rules.push(TsRule {
            dest_key: dest,
            aggregator: agg,
            bucket_ms: bucket,
            align_timestamp: align,
        });
    }
    let has_src = {
        let b = *data.get(*i).ok_or_else(|| corrupt("eof"))?;
        *i += 1;
        b
    };
    let source_key = if has_src == 1 {
        Some(read_string(data, i)?)
    } else {
        None
    };
    Ok(TimeSeries {
        retention_ms: retention,
        chunk_size: chunk as usize,
        duplicate_policy: dup,
        labels,
        samples,
        rules,
        source_key,
        ignore_max_time_diff: 0,
        ignore_max_val_diff: 0.0,
    })
}

pub fn decode_typed_value(data: &[u8], i: &mut usize, rdbtype: u8) -> Result<RedisValue> {
    match rdbtype {
        TYPE_STRING => Ok(RedisValue::String(read_string(data, i)?)),
        TYPE_LIST => {
            let (n, _) = read_len(data, i)?;
            let mut l = VecDeque::with_capacity(n as usize);
            for _ in 0..n {
                l.push_back(read_string(data, i)?);
            }
            Ok(RedisValue::List(l))
        }
        TYPE_SET => {
            let (n, _) = read_len(data, i)?;
            let mut s = HashSet::with_capacity(n as usize);
            for _ in 0..n {
                s.insert(read_string(data, i)?);
            }
            Ok(RedisValue::Set(s))
        }
        TYPE_HASH => {
            let (n, _) = read_len(data, i)?;
            let mut h = HashMap::with_capacity(n as usize);
            for _ in 0..n {
                let k = read_string(data, i)?;
                let v = read_string(data, i)?;
                h.insert(k, v);
            }
            Ok(RedisValue::Hash(HashObject::from_map(h)))
        }
        TYPE_HASH_EX => {
            let (n, _) = read_len(data, i)?;
            let mut obj = HashObject::new();
            for _ in 0..n {
                let k = read_string(data, i)?;
                let v = read_string(data, i)?;
                let (exp, _) = read_len(data, i)?;
                obj.fields.insert(
                    k,
                    HashField {
                        value: v,
                        expires_at_ms: if exp == 0 { None } else { Some(exp as i64) },
                    },
                );
            }
            Ok(RedisValue::Hash(obj))
        }
        TYPE_ZSET_2 => {
            let (n, _) = read_len(data, i)?;
            let mut z = ZSet::default();
            for _ in 0..n {
                let m = read_string(data, i)?;
                let score = read_binary_double(data, i)?;
                z.insert(m, score);
            }
            Ok(RedisValue::ZSet(z))
        }
        3 => {
            // ZSET v1 : score en string
            let (n, _) = read_len(data, i)?;
            let mut z = ZSet::default();
            for _ in 0..n {
                let m = read_string(data, i)?;
                let sc = read_string(data, i)?;
                let score = std::str::from_utf8(&sc)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                z.insert(m, score);
            }
            Ok(RedisValue::ZSet(z))
        }
        TYPE_STREAM_LISTPACKS | TYPE_STREAM_LISTPACKS_2 | TYPE_STREAM_LISTPACKS_3 => {
            Ok(RedisValue::Stream(stream_codec::decode_stream(
                data, i, rdbtype,
            )?))
        }
        TYPE_JSON => {
            let raw = read_string(data, i)?;
            let v: serde_json::Value =
                serde_json::from_slice(&raw).map_err(|e| corrupt(e.to_string()))?;
            Ok(RedisValue::Json(v))
        }
        TYPE_TIMESERIES => Ok(RedisValue::TimeSeries(decode_timeseries(data, i)?)),
        TYPE_BLOOM => {
            let raw = read_string(data, i)?;
            let b = BloomFilter::loadchunk(&raw).ok_or_else(|| corrupt("bloom chunk"))?;
            Ok(RedisValue::Bloom(b))
        }
        TYPE_CUCKOO => {
            let raw = read_string(data, i)?;
            let c = CuckooFilter::loadchunk(&raw).ok_or_else(|| corrupt("cuckoo chunk"))?;
            Ok(RedisValue::Cuckoo(c))
        }
        TYPE_CMS => {
            let (width, _) = read_len(data, i)?;
            let (depth, _) = read_len(data, i)?;
            let (n, _) = read_len(data, i)?;
            let mut counters = Vec::with_capacity(n as usize);
            for _ in 0..n {
                if *i + 8 > data.len() {
                    return Err(corrupt("cms truncated"));
                }
                counters.push(u64::from_le_bytes(data[*i..*i + 8].try_into().unwrap()));
                *i += 8;
            }
            Ok(RedisValue::Cms(CountMinSketch {
                width: width as usize,
                depth: depth as usize,
                counters,
            }))
        }
        TYPE_TOPK => {
            let (k, _) = read_len(data, i)?;
            let (width, _) = read_len(data, i)?;
            let (depth, _) = read_len(data, i)?;
            let decay = read_binary_double(data, i)?;
            let (hn, _) = read_len(data, i)?;
            let mut heap = HashMap::new();
            for _ in 0..hn {
                let key = read_string(data, i)?;
                if *i + 8 > data.len() {
                    return Err(corrupt("topk heap"));
                }
                let v = u64::from_le_bytes(data[*i..*i + 8].try_into().unwrap());
                *i += 8;
                heap.insert(key, v);
            }
            let (cw, _) = read_len(data, i)?;
            let (cd, _) = read_len(data, i)?;
            let (cn, _) = read_len(data, i)?;
            let mut counters = Vec::with_capacity(cn as usize);
            for _ in 0..cn {
                if *i + 8 > data.len() {
                    return Err(corrupt("topk cms"));
                }
                counters.push(u64::from_le_bytes(data[*i..*i + 8].try_into().unwrap()));
                *i += 8;
            }
            Ok(RedisValue::TopK(TopK {
                k: k as usize,
                width: width as usize,
                depth: depth as usize,
                decay,
                heap,
                cms: CountMinSketch {
                    width: cw as usize,
                    depth: cd as usize,
                    counters,
                },
            }))
        }
        TYPE_TDIGEST => {
            let compression = read_binary_double(data, i)?;
            let min = read_binary_double(data, i)?;
            let max = read_binary_double(data, i)?;
            let count = read_binary_double(data, i)?;
            let (n, _) = read_len(data, i)?;
            let mut centroids = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let m = read_binary_double(data, i)?;
                let w = read_binary_double(data, i)?;
                centroids.push((m, w));
            }
            Ok(RedisValue::TDigest(TDigest {
                compression,
                centroids,
                min,
                max,
                count,
            }))
        }
        TYPE_VECTORSET => {
            let (dim, _) = read_len(data, i)?;
            let q = match data.get(*i).copied() {
                Some(0) => Quantization::F32,
                Some(1) => Quantization::Q8,
                Some(2) => Quantization::Bin,
                _ => return Err(corrupt("vector quant")),
            };
            *i += 1;
            let (m, _) = read_len(data, i)?;
            let (ef, _) = read_len(data, i)?;
            let (vn, _) = read_len(data, i)?;
            let mut vectors = HashMap::new();
            for _ in 0..vn {
                let name = read_string(data, i)?;
                let (dn, _) = read_len(data, i)?;
                let mut vals = Vec::with_capacity(dn as usize);
                for _ in 0..dn {
                    if *i + 4 > data.len() {
                        return Err(corrupt("vector vals"));
                    }
                    vals.push(f32::from_le_bytes(data[*i..*i + 4].try_into().unwrap()));
                    *i += 4;
                }
                vectors.insert(name, vals);
            }
            let (an, _) = read_len(data, i)?;
            let mut attrs = HashMap::new();
            for _ in 0..an {
                let k = read_string(data, i)?;
                let a = read_string(data, i)?;
                attrs.insert(k, a);
            }
            let mut vs = VectorSet::new(dim as usize);
            vs.quantization = q;
            vs.m = m as usize;
            vs.ef = ef as usize;
            vs.vectors = vectors;
            vs.attrs = attrs;
            Ok(RedisValue::VectorSet(vs))
        }
        TYPE_HASH_LISTPACK | TYPE_HASH_ZIPLIST => decode_hash_listpack(data, i),
        TYPE_SET_LISTPACK => decode_set_listpack(data, i),
        TYPE_ZSET_LISTPACK | TYPE_ZSET_ZIPLIST => decode_zset_listpack(data, i),
        TYPE_LIST_QUICKLIST | TYPE_LIST_QUICKLIST_2 => decode_quicklist(data, i, rdbtype),
        TYPE_LIST_ZIPLIST => {
            let zl = read_string(data, i)?;
            decode_ziplist_as_list(&zl)
        }
        TYPE_SET_INTSET => decode_intset(data, i),
        TYPE_HASH_ZIPMAP => {
            // Legacy rare : skip via erreur explicite si vraiment rencontré.
            Err(corrupt("hash zipmap not supported"))
        }
        other => Err(super::io::RdbError::UnsupportedType(other)),
    }
}

fn decode_hash_listpack(data: &[u8], i: &mut usize) -> Result<RedisValue> {
    let lp = read_string(data, i)?;
    let els = listpack::decode(&lp)?;
    if els.len() % 2 != 0 {
        return Err(corrupt("hash listpack odd"));
    }
    let mut h = HashMap::new();
    for chunk in els.chunks_exact(2) {
        h.insert(chunk[0].as_bytes(), chunk[1].as_bytes());
    }
    Ok(RedisValue::Hash(crate::value::HashObject::from_map(h)))
}

fn decode_set_listpack(data: &[u8], i: &mut usize) -> Result<RedisValue> {
    let lp = read_string(data, i)?;
    let els = listpack::decode(&lp)?;
    Ok(RedisValue::Set(els.into_iter().map(|e| e.as_bytes()).collect()))
}

fn decode_zset_listpack(data: &[u8], i: &mut usize) -> Result<RedisValue> {
    let lp = read_string(data, i)?;
    let els = listpack::decode(&lp)?;
    if els.len() % 2 != 0 {
        return Err(corrupt("zset listpack odd"));
    }
    let mut z = ZSet::default();
    for chunk in els.chunks_exact(2) {
        let m = chunk[0].as_bytes();
        let score = match &chunk[1] {
            LpEl::Int(v) => *v as f64,
            LpEl::Str(s) => std::str::from_utf8(s)
                .ok()
                .and_then(|t| t.parse().ok())
                .unwrap_or(0.0),
        };
        z.insert(m, score);
    }
    Ok(RedisValue::ZSet(z))
}

fn decode_quicklist(data: &[u8], i: &mut usize, rdbtype: u8) -> Result<RedisValue> {
    let (n, _) = read_len(data, i)?;
    let mut list = VecDeque::new();
    for _ in 0..n {
        if rdbtype == TYPE_LIST_QUICKLIST_2 {
            let (_container, _) = read_len(data, i)?; // node container type
        }
        let node = read_string(data, i)?;
        // ziplist ou listpack
        if let Ok(els) = listpack::decode(&node) {
            for e in els {
                list.push_back(e.as_bytes());
            }
        } else {
            for e in decode_ziplist_elements(&node)? {
                list.push_back(e);
            }
        }
    }
    Ok(RedisValue::List(list))
}

fn decode_ziplist_as_list(zl: &[u8]) -> Result<RedisValue> {
    Ok(RedisValue::List(decode_ziplist_elements(zl)?.into()))
}

/// Décodeur ziplist minimal (entrées string/int).
fn decode_ziplist_elements(zl: &[u8]) -> Result<Vec<Bytes>> {
    if zl.len() < 11 {
        return Err(corrupt("ziplist short"));
    }
    // zlbytes(4) zltail(4) zllen(2) ... zlend(0xff)
    let mut i = 10;
    let mut out = Vec::new();
    while i < zl.len() {
        if zl[i] == 0xff {
            break;
        }
        // prevlen
        if zl[i] < 254 {
            i += 1;
        } else {
            i += 5;
        }
        if i >= zl.len() {
            return Err(corrupt("ziplist enc"));
        }
        let enc = zl[i];
        i += 1;
        if enc >> 6 == 0 {
            let len = (enc & 0x3f) as usize;
            if i + len > zl.len() {
                return Err(corrupt("ziplist str"));
            }
            out.push(Bytes::copy_from_slice(&zl[i..i + len]));
            i += len;
        } else if enc >> 6 == 1 {
            let len = (((enc & 0x3f) as usize) << 8) | (zl[i] as usize);
            i += 1;
            if i + len > zl.len() {
                return Err(corrupt("ziplist str14"));
            }
            out.push(Bytes::copy_from_slice(&zl[i..i + len]));
            i += len;
        } else if enc >> 6 == 2 {
            if i + 4 > zl.len() {
                return Err(corrupt("ziplist str32"));
            }
            let len = u32::from_be_bytes(zl[i..i + 4].try_into().unwrap()) as usize;
            i += 4;
            if i + len > zl.len() {
                return Err(corrupt("ziplist str32b"));
            }
            out.push(Bytes::copy_from_slice(&zl[i..i + len]));
            i += len;
        } else if enc >> 4 == 0xc {
            // 16-bit int
            let v = i16::from_le_bytes(zl[i..i + 2].try_into().unwrap());
            i += 2;
            out.push(Bytes::from(v.to_string()));
        } else if enc >> 4 == 0xd {
            let v = i32::from_le_bytes(zl[i..i + 4].try_into().unwrap());
            i += 4;
            out.push(Bytes::from(v.to_string()));
        } else if enc >> 4 == 0xe {
            let v = i64::from_le_bytes(zl[i..i + 8].try_into().unwrap());
            i += 8;
            out.push(Bytes::from(v.to_string()));
        } else if enc == 0xf0 {
            // 24-bit
            let mut b = [0u8; 4];
            b[..3].copy_from_slice(&zl[i..i + 3]);
            i += 3;
            let v = i32::from_le_bytes(b);
            out.push(Bytes::from(v.to_string()));
        } else if enc == 0xfe {
            let v = zl[i] as i8;
            i += 1;
            out.push(Bytes::from((v as i64).to_string()));
        } else if enc >= 0xf1 && enc <= 0xfd {
            // immediate 4-bit int: enc - 0xf1
            let v = (enc - 0xf1) as i64;
            out.push(Bytes::from(v.to_string()));
        } else {
            return Err(corrupt(format!("ziplist enc {enc:#x}")));
        }
    }
    Ok(out)
}

fn decode_intset(data: &[u8], i: &mut usize) -> Result<RedisValue> {
    let raw = read_string(data, i)?;
    if raw.len() < 8 {
        return Err(corrupt("intset short"));
    }
    let encoding = u32::from_le_bytes(raw[0..4].try_into().unwrap());
    let length = u32::from_le_bytes(raw[4..8].try_into().unwrap()) as usize;
    let mut s = HashSet::with_capacity(length);
    let mut off = 8;
    for _ in 0..length {
        let v = match encoding {
            2 => {
                let x = i16::from_le_bytes(raw[off..off + 2].try_into().unwrap()) as i64;
                off += 2;
                x
            }
            4 => {
                let x = i32::from_le_bytes(raw[off..off + 4].try_into().unwrap()) as i64;
                off += 4;
                x
            }
            8 => {
                let x = i64::from_le_bytes(raw[off..off + 8].try_into().unwrap());
                off += 8;
                x
            }
            _ => return Err(corrupt("intset encoding")),
        };
        s.insert(Bytes::from(v.to_string()));
    }
    Ok(RedisValue::Set(s))
}
