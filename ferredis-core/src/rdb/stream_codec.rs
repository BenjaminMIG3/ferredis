//! Codec stream Redis `RDB_TYPE_STREAM_LISTPACKS_3` (+ lecture v1/v2).

use super::io::{
    corrupt, read_len, read_ms_time, read_string, write_len, write_ms_time, write_raw_string,
    Result, TYPE_STREAM_LISTPACKS, TYPE_STREAM_LISTPACKS_2, TYPE_STREAM_LISTPACKS_3,
};
use super::listpack::{self, LpEl};
use crate::value::{
    Stream, StreamConsumer, StreamGroup, StreamId, StreamNack, STREAM_ENTRIES_READ_INVALID,
};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

const FLAG_DELETED: u64 = 1 << 0;
const FLAG_SAMEFIELDS: u64 = 1 << 1;

pub fn encode_stream(out: &mut Vec<u8>, s: &Stream) {
    // Une listpack par entrée (simple, Redis-lisible).
    write_len(out, s.entries.len() as u64);
    for (id, fields) in &s.entries {
        let mut key = [0u8; 16];
        key[..8].copy_from_slice(&id.ms.to_be_bytes());
        key[8..].copy_from_slice(&id.seq.to_be_bytes());
        write_raw_string(out, &key);
        let lp = encode_entry_listpack(*id, fields);
        write_raw_string(out, &lp);
    }
    write_len(out, s.entries.len() as u64);
    write_len(out, s.last_id.ms);
    write_len(out, s.last_id.seq);
    write_len(out, s.first_id.ms);
    write_len(out, s.first_id.seq);
    write_len(out, s.max_deleted_id.ms);
    write_len(out, s.max_deleted_id.seq);
    write_len(out, s.entries_added);
    write_len(out, s.groups.len() as u64);
    let mut groups: Vec<_> = s.groups.values().collect();
    groups.sort_by(|a, b| a.name.cmp(&b.name));
    for g in groups {
        write_raw_string(out, &g.name);
        write_len(out, g.last_id.ms);
        write_len(out, g.last_id.seq);
        let er = if g.entries_read < 0 {
            u64::MAX
        } else {
            g.entries_read as u64
        };
        write_len(out, er);
        // PEL global
        write_len(out, g.pel.len() as u64);
        for (id, nack) in &g.pel {
            out.extend_from_slice(&id.ms.to_be_bytes());
            out.extend_from_slice(&id.seq.to_be_bytes());
            write_ms_time(out, nack.delivery_time_ms as i64);
            write_len(out, nack.delivery_count);
        }
        // consumers (LISTPACKS_3 : active_time)
        write_len(out, g.consumers.len() as u64);
        let mut consumers: Vec<_> = g.consumers.values().collect();
        consumers.sort_by(|a, b| a.name.cmp(&b.name));
        for c in consumers {
            write_raw_string(out, &c.name);
            write_ms_time(out, c.seen_time_ms as i64);
            write_ms_time(out, c.active_time_ms);
            write_len(out, c.pending.len() as u64);
            for id in &c.pending {
                out.extend_from_slice(&id.ms.to_be_bytes());
                out.extend_from_slice(&id.seq.to_be_bytes());
            }
        }
    }
}

fn encode_entry_listpack(id: StreamId, fields: &[(Bytes, Bytes)]) -> Vec<u8> {
    let mut els = Vec::with_capacity(6 + fields.len() * 2);
    els.push(LpEl::Int(1)); // count
    els.push(LpEl::Int(0)); // deleted
    els.push(LpEl::Int(fields.len() as i64));
    for (f, _) in fields {
        els.push(LpEl::Str(f.clone()));
    }
    els.push(LpEl::Int(0)); // master terminator
    // entry: SAMEFIELDS, delta 0/0, values, lp-count
    els.push(LpEl::Int(FLAG_SAMEFIELDS as i64));
    els.push(LpEl::Int(0)); // ms delta
    els.push(LpEl::Int(0)); // seq delta
    for (_, v) in fields {
        els.push(LpEl::Str(v.clone()));
    }
    // lp-count = number of elements in this entry record
    let entry_items = 3 + fields.len() + 1; // flags+2 deltas + values + lp-count
    els.push(LpEl::Int(entry_items as i64));
    let _ = id; // master key carries the absolute ID
    listpack::encode(&els)
}

pub fn decode_stream(data: &[u8], i: &mut usize, rdbtype: u8) -> Result<Stream> {
    let (n_lp, _) = read_len(data, i)?;
    let mut stream = Stream::default();
    for _ in 0..n_lp {
        let nodekey = read_string(data, i)?;
        if nodekey.len() != 16 {
            return Err(corrupt("stream node key size"));
        }
        let master_ms = u64::from_be_bytes(nodekey[..8].try_into().unwrap());
        let master_seq = u64::from_be_bytes(nodekey[8..].try_into().unwrap());
        let lp_bytes = read_string(data, i)?;
        let entries = decode_listpack_entries(&lp_bytes, master_ms, master_seq)?;
        for (id, fields) in entries {
            if stream.entries.is_empty() {
                stream.first_id = id;
                stream.recorded_first_id = id;
            }
            stream.entries.insert(id, fields);
            stream.last_id = id;
        }
    }
    let (length, _) = read_len(data, i)?;
    let (last_ms, _) = read_len(data, i)?;
    let (last_seq, _) = read_len(data, i)?;
    stream.last_id = StreamId::new(last_ms, last_seq);

    if rdbtype >= TYPE_STREAM_LISTPACKS_2 {
        let (first_ms, _) = read_len(data, i)?;
        let (first_seq, _) = read_len(data, i)?;
        stream.first_id = StreamId::new(first_ms, first_seq);
        let (max_ms, _) = read_len(data, i)?;
        let (max_seq, _) = read_len(data, i)?;
        stream.max_deleted_id = StreamId::new(max_ms, max_seq);
        let (added, _) = read_len(data, i)?;
        stream.entries_added = added;
    } else {
        stream.entries_added = length;
        stream.refresh_first_id();
    }
    if stream.recorded_first_id.is_zero() {
        stream.recorded_first_id = stream.first_id;
    }

    let (ng, _) = read_len(data, i)?;
    for _ in 0..ng {
        let name = read_string(data, i)?;
        let (gms, _) = read_len(data, i)?;
        let (gseq, _) = read_len(data, i)?;
        let last_id = StreamId::new(gms, gseq);
        let er = if rdbtype >= TYPE_STREAM_LISTPACKS_2 {
            let (off, _) = read_len(data, i)?;
            if off == u64::MAX {
                STREAM_ENTRIES_READ_INVALID
            } else {
                off as i64
            }
        } else {
            0
        };
        let mut g = StreamGroup::new(name.clone(), last_id, er);

        let (pel_n, _) = read_len(data, i)?;
        let mut pel_tmp: BTreeMap<StreamId, StreamNack> = BTreeMap::new();
        for _ in 0..pel_n {
            if *i + 16 > data.len() {
                return Err(corrupt("stream pel id"));
            }
            let ms = u64::from_be_bytes(data[*i..*i + 8].try_into().unwrap());
            let seq = u64::from_be_bytes(data[*i + 8..*i + 16].try_into().unwrap());
            *i += 16;
            let dt = read_ms_time(data, i)? as u64;
            let (dc, _) = read_len(data, i)?;
            pel_tmp.insert(
                StreamId::new(ms, seq),
                StreamNack {
                    consumer: Bytes::new(),
                    delivery_time_ms: dt,
                    delivery_count: dc,
                },
            );
        }

        let (nc, _) = read_len(data, i)?;
        for _ in 0..nc {
            let cname = read_string(data, i)?;
            let seen = read_ms_time(data, i)? as u64;
            let active = if rdbtype >= TYPE_STREAM_LISTPACKS_3 {
                read_ms_time(data, i)?
            } else {
                seen as i64
            };
            let mut pending = BTreeSet::new();
            let (cpel, _) = read_len(data, i)?;
            for _ in 0..cpel {
                if *i + 16 > data.len() {
                    return Err(corrupt("stream consumer pel"));
                }
                let ms = u64::from_be_bytes(data[*i..*i + 8].try_into().unwrap());
                let seq = u64::from_be_bytes(data[*i + 8..*i + 16].try_into().unwrap());
                *i += 16;
                let id = StreamId::new(ms, seq);
                if let Some(nack) = pel_tmp.get_mut(&id) {
                    nack.consumer = cname.clone();
                }
                pending.insert(id);
            }
            g.consumers.insert(
                cname.clone(),
                StreamConsumer {
                    name: cname,
                    seen_time_ms: seen,
                    active_time_ms: active,
                    pending,
                },
            );
        }
        g.pel = pel_tmp;
        stream.groups.insert(name, g);
    }

    // Si length dit 0 mais on a des entrées (tolérance), on garde les entrées décodées.
    let _ = (length, TYPE_STREAM_LISTPACKS);
    Ok(stream)
}

fn decode_listpack_entries(
    lp: &[u8],
    master_ms: u64,
    master_seq: u64,
) -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>> {
    let els = listpack::decode(lp)?;
    let mut idx = 0;
    let valid = next_i64(&els, &mut idx)?;
    let deleted = next_i64(&els, &mut idx)?;
    let nfields = next_i64(&els, &mut idx)? as usize;
    let mut master_fields = Vec::with_capacity(nfields);
    for _ in 0..nfields {
        master_fields.push(next_bytes(&els, &mut idx)?);
    }
    let _term = next_i64(&els, &mut idx)?; // 0

    let mut out = Vec::new();
    for _ in 0..(valid + deleted) {
        let flags = next_i64(&els, &mut idx)? as u64;
        let dms = next_i64(&els, &mut idx)? as u64;
        let dseq = next_i64(&els, &mut idx)? as u64;
        let id = StreamId::new(master_ms.wrapping_add(dms), master_seq.wrapping_add(dseq));
        let fields = if flags & FLAG_SAMEFIELDS != 0 {
            let mut f = Vec::with_capacity(nfields);
            for name in &master_fields {
                f.push((name.clone(), next_bytes(&els, &mut idx)?));
            }
            f
        } else {
            let nf = next_i64(&els, &mut idx)? as usize;
            let mut f = Vec::with_capacity(nf);
            for _ in 0..nf {
                f.push((next_bytes(&els, &mut idx)?, next_bytes(&els, &mut idx)?));
            }
            f
        };
        let _lp_count = next_i64(&els, &mut idx)?;
        if flags & FLAG_DELETED == 0 {
            out.push((id, fields));
        }
    }
    Ok(out)
}

fn next_i64(els: &[LpEl], i: &mut usize) -> Result<i64> {
    let el = els.get(*i).ok_or_else(|| corrupt("stream lp short"))?;
    *i += 1;
    el.as_i64()
}

fn next_bytes(els: &[LpEl], i: &mut usize) -> Result<Bytes> {
    let el = els.get(*i).ok_or_else(|| corrupt("stream lp short"))?;
    *i += 1;
    Ok(el.as_bytes())
}

pub fn stream_rdb_type() -> u8 {
    TYPE_STREAM_LISTPACKS_3
}
