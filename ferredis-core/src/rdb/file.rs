//! Fichier RDB Redis complet + payload DUMP/RESTORE.

use super::crc64::crc64;
use super::frdb;
use super::io::{
    corrupt, read_len, read_ms_time, read_string, read_u8, write_len, write_ms_time,
    write_raw_string, write_u8, Result, RdbError, MAGIC, OPCODE_AUX, OPCODE_EOF, OPCODE_EXPIRETIME,
    OPCODE_EXPIRETIME_MS, OPCODE_FREQ, OPCODE_FUNCTION2, OPCODE_FUNCTION_PRE_GA, OPCODE_IDLE,
    OPCODE_RESIZEDB, OPCODE_SELECTDB, RDB_VERSION,
};
use super::value_codec::{decode_typed_value, encode_value};
use crate::functions::FunctionStore;
use crate::store::SharedDb;
use crate::value::{Entry, RedisValue};
use bytes::Bytes;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

/// Snapshot chargé : bases + codes Functions + blob Search (AUX `ferredis-ft`).
pub struct RdbSnapshot {
    pub dbs: Vec<(usize, Vec<(Bytes, Entry)>)>,
    pub function_codes: Vec<Bytes>,
    pub search_blob: Option<Bytes>,
}

pub fn save_rdb_file(
    path: &Path,
    db: &SharedDb,
    functions: Option<&FunctionStore>,
) -> std::io::Result<()> {
    save_rdb_file_full(path, db, functions, None)
}

pub fn save_rdb_file_full(
    path: &Path,
    db: &SharedDb,
    functions: Option<&FunctionStore>,
    search: Option<&crate::search::SearchStore>,
) -> std::io::Result<()> {
    let dump = db.dump_all();
    let mut fn_codes = Vec::new();
    if let Some(fs) = functions {
        for lib in fs.list_libs() {
            fn_codes.push(Bytes::from(lib.code));
        }
    }
    let search_blob = search.map(|s| Bytes::from(s.dump_bytes()));
    let bytes = encode_rdb_bytes_full(&dump, &fn_codes, search_blob.as_deref()).map_err(to_io)?;
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.flush()?;
        let _ = f.sync_all();
    }
    fs::rename(tmp, path)?;
    info!("RDB saved to {}", path.display());
    Ok(())
}

pub fn load_rdb_file(
    path: &Path,
    db: &SharedDb,
    functions: Option<&FunctionStore>,
) -> std::io::Result<()> {
    load_rdb_file_full(path, db, functions, None)
}

pub fn load_rdb_file_full(
    path: &Path,
    db: &SharedDb,
    functions: Option<&FunctionStore>,
    search: Option<&crate::search::SearchStore>,
) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let data = fs::read(path)?;
    let snap = decode_rdb_or_frdb(&data).map_err(to_io)?;
    db.restore_all(snap.dbs);
    if let Some(fs) = functions {
        for code in snap.function_codes {
            if let Err(e) = load_function_code(fs, &code) {
                warn!("function restore: {e}");
            }
        }
    }
    if let (Some(store), Some(blob)) = (search, snap.search_blob) {
        if let Err(e) = store.restore_bytes(&blob) {
            warn!("search restore: {e}");
        }
    }
    info!("RDB loaded from {}", path.display());
    Ok(())
}

fn load_function_code(fs: &FunctionStore, code: &[u8]) -> std::result::Result<(), String> {
    let s = std::str::from_utf8(code).map_err(|e| e.to_string())?;
    fs.load(s, true).map(|_| ())
}

pub fn encode_rdb_bytes(
    dump: &[(usize, Vec<(Bytes, Entry)>)],
    function_codes: &[Bytes],
) -> Result<Vec<u8>> {
    encode_rdb_bytes_full(dump, function_codes, None)
}

pub fn encode_rdb_bytes_full(
    dump: &[(usize, Vec<(Bytes, Entry)>)],
    function_codes: &[Bytes],
    search_blob: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(4096);
    out.extend_from_slice(MAGIC);
    write_aux(&mut out, b"redis-ver", b"7.2.0");
    write_aux(
        &mut out,
        b"redis-bits",
        if cfg!(target_pointer_width = "64") {
            b"64"
        } else {
            b"32"
        },
    );
    let ctime = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string();
    write_aux(&mut out, b"ctime", ctime.as_bytes());
    if let Some(blob) = search_blob {
        write_aux(&mut out, b"ferredis-ft", blob);
    }

    for code in function_codes {
        write_u8(&mut out, OPCODE_FUNCTION2);
        write_raw_string(&mut out, code);
    }

    for (dbidx, entries) in dump {
        if entries.is_empty() {
            continue;
        }
        write_u8(&mut out, OPCODE_SELECTDB);
        write_len(&mut out, *dbidx as u64);
        let expires = entries.iter().filter(|(_, e)| e.expires_at.is_some()).count();
        write_u8(&mut out, OPCODE_RESIZEDB);
        write_len(&mut out, entries.len() as u64);
        write_len(&mut out, expires as u64);
        for (key, entry) in entries {
            if let Some(at) = entry.expires_at {
                let now = Instant::now();
                if at > now {
                    let remain = at.duration_since(now).as_millis() as u64;
                    let unix = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64
                        + remain;
                    write_u8(&mut out, OPCODE_EXPIRETIME_MS);
                    write_ms_time(&mut out, unix as i64);
                }
            }
            let mut typed = Vec::new();
            encode_value(&mut typed, &entry.value);
            out.push(typed[0]);
            write_raw_string(&mut out, key);
            out.extend_from_slice(&typed[1..]);
        }
    }

    write_u8(&mut out, OPCODE_EOF);
    let checksum = crc64(0, &out);
    out.extend_from_slice(&checksum.to_le_bytes());
    Ok(out)
}

fn write_aux(out: &mut Vec<u8>, k: &[u8], v: &[u8]) {
    write_u8(out, OPCODE_AUX);
    write_raw_string(out, k);
    write_raw_string(out, v);
}

pub fn decode_rdb_or_frdb(data: &[u8]) -> Result<RdbSnapshot> {
    if data.starts_with(b"FRDB001\n") {
        let (dbs, fns) = frdb::decode_frdb(data)?;
        return Ok(RdbSnapshot {
            dbs,
            function_codes: fns,
            search_blob: None,
        });
    }
    decode_rdb(data)
}

pub fn decode_rdb(data: &[u8]) -> Result<RdbSnapshot> {
    if data.len() < 9 + 1 + 8 {
        return Err(corrupt("RDB too short"));
    }
    if &data[..5] != b"REDIS" {
        return Err(corrupt("bad RDB magic"));
    }
    let ver: u16 = std::str::from_utf8(&data[5..9])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| corrupt("bad RDB version"))?;
    if ver > RDB_VERSION {
        return Err(RdbError::BadVersion(ver));
    }

    let body_end = data.len() - 8;
    let stored = u64::from_le_bytes(data[body_end..].try_into().unwrap());
    if stored != 0 {
        let got = crc64(0, &data[..body_end]);
        if got != stored {
            return Err(RdbError::BadChecksum);
        }
    }

    let mut i = 9;
    let mut dbs: Vec<(usize, Vec<(Bytes, Entry)>)> = Vec::new();
    let mut cur_db: Option<(usize, Vec<(Bytes, Entry)>)> = None;
    let mut function_codes = Vec::new();
    let mut search_blob: Option<Bytes> = None;
    let mut expire_ms: Option<u64> = None;

    while i < body_end {
        let op = read_u8(data, &mut i)?;
        match op {
            OPCODE_EOF => break,
            OPCODE_SELECTDB => {
                if let Some(prev) = cur_db.take() {
                    dbs.push(prev);
                }
                let (dbid, _) = read_len(data, &mut i)?;
                cur_db = Some((dbid as usize, Vec::new()));
            }
            OPCODE_RESIZEDB => {
                let (_dbhash, _) = read_len(data, &mut i)?;
                let (_exhash, _) = read_len(data, &mut i)?;
            }
            OPCODE_AUX => {
                let k = read_string(data, &mut i)?;
                let v = read_string(data, &mut i)?;
                if k.as_ref() == b"ferredis-ft" {
                    search_blob = Some(v);
                }
            }
            OPCODE_FUNCTION2 => {
                function_codes.push(read_string(data, &mut i)?);
            }
            OPCODE_FUNCTION_PRE_GA => {
                let _ = read_string(data, &mut i)?;
            }
            OPCODE_EXPIRETIME_MS => {
                expire_ms = Some(read_ms_time(data, &mut i)? as u64);
            }
            OPCODE_EXPIRETIME => {
                if i + 4 > body_end {
                    return Err(corrupt("expire sec"));
                }
                let sec = u32::from_le_bytes(data[i..i + 4].try_into().unwrap()) as u64;
                i += 4;
                expire_ms = Some(sec.saturating_mul(1000));
            }
            OPCODE_IDLE => {
                let (_idle, _) = read_len(data, &mut i)?;
            }
            OPCODE_FREQ => {
                let _ = read_u8(data, &mut i)?;
            }
            t if is_object_type(t) => {
                let key = read_string(data, &mut i)?;
                let value = decode_typed_value(data, &mut i, t)?;
                let mut entry = Entry::new(value);
                if let Some(ms) = expire_ms.take() {
                    apply_expire_unix_ms(&mut entry, ms);
                }
                if let Some((_, entries)) = cur_db.as_mut() {
                    entries.push((key, entry));
                } else {
                    cur_db = Some((0, vec![(key, entry)]));
                }
            }
            other => return Err(corrupt(format!("unknown opcode {other}"))),
        }
    }
    if let Some(prev) = cur_db.take() {
        dbs.push(prev);
    }
    Ok(RdbSnapshot {
        dbs,
        function_codes,
        search_blob,
    })
}

fn is_object_type(t: u8) -> bool {
    (t <= 7) || (9..=21).contains(&t) || (80..=88).contains(&t)
}

fn apply_expire_unix_ms(entry: &mut Entry, ttl_unix_ms: u64) {
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

/// Payload DUMP : type + objet + version LE + CRC64 LE.
pub fn create_dump_payload(value: &RedisValue) -> Vec<u8> {
    let mut payload = Vec::new();
    encode_value(&mut payload, value);
    payload.extend_from_slice(&RDB_VERSION.to_le_bytes());
    let c = crc64(0, &payload);
    payload.extend_from_slice(&c.to_le_bytes());
    payload
}

pub fn verify_and_decode_dump(payload: &[u8]) -> Result<RedisValue> {
    if payload.len() < 10 {
        return Err(corrupt("DUMP payload too short"));
    }
    let footer = &payload[payload.len() - 10..];
    let ver = u16::from_le_bytes(footer[..2].try_into().unwrap());
    if ver > RDB_VERSION {
        return Err(RdbError::BadVersion(ver));
    }
    let stored = u64::from_le_bytes(footer[2..].try_into().unwrap());
    if stored != 0 {
        let got = crc64(0, &payload[..payload.len() - 8]);
        if got != stored {
            return Err(RdbError::BadChecksum);
        }
    }
    let body = &payload[..payload.len() - 10];
    let mut i = 0;
    let t = read_u8(body, &mut i)?;
    let v = decode_typed_value(body, &mut i, t)?;
    if i != body.len() {
        return Err(corrupt("DUMP trailing bytes"));
    }
    Ok(v)
}

fn to_io(e: RdbError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{HashObject, RedisValue};
    use std::collections::{HashMap, HashSet, VecDeque};

    #[test]
    fn dump_restore_string() {
        let v = RedisValue::String(Bytes::from_static(b"hello"));
        let p = create_dump_payload(&v);
        let got = verify_and_decode_dump(&p).unwrap();
        assert_eq!(got, v);
    }

    #[test]
    fn dump_restore_hash_list_set() {
        let mut h = HashMap::new();
        h.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        let cases = [
            RedisValue::Hash(HashObject::from_map(h)),
            RedisValue::List(VecDeque::from([Bytes::from_static(b"a")])),
            RedisValue::Set(HashSet::from([Bytes::from_static(b"m")])),
        ];
        for v in cases {
            let p = create_dump_payload(&v);
            assert_eq!(verify_and_decode_dump(&p).unwrap(), v);
        }
    }

    #[test]
    fn rdb_file_roundtrip_memory() {
        let dump = vec![(
            0usize,
            vec![(
                Bytes::from_static(b"k"),
                Entry::new(RedisValue::String(Bytes::from_static(b"v"))),
            )],
        )];
        let bytes = encode_rdb_bytes(&dump, &[]).unwrap();
        let snap = decode_rdb(&bytes).unwrap();
        assert_eq!(snap.dbs.len(), 1);
        assert_eq!(snap.dbs[0].1[0].0, Bytes::from_static(b"k"));
    }
}
