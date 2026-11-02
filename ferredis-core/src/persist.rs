//! Persistence RDB Redis-natif + AOF (fsync ALWAYS|EVERYSEC|NO, rewrite, preamble RDB).

use crate::rdb::{self, encode_rdb_bytes};
use crate::resp::parse_command;
use crate::store::SharedDb;
use crate::value::{Entry, RedisValue};
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct PersistConfig {
    pub dir: PathBuf,
    pub dbfilename: String,
    pub appendonly: bool,
    pub appendfilename: String,
    /// ALWAYS | everysec | no
    pub appendfsync: String,
    /// Si true, BGREWRITEAOF écrit un preamble RDB puis le delta commandes (vide après rewrite complet).
    pub aof_use_rdb_preamble: bool,
}

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("."),
            dbfilename: "dump.rdb".into(),
            appendonly: false,
            appendfilename: "appendonly.aof".into(),
            appendfsync: "everysec".into(),
            aof_use_rdb_preamble: true,
        }
    }
}

pub struct AofLogger {
    inner: Mutex<Option<File>>,
    path: PathBuf,
    enabled: bool,
    fsync_policy: Mutex<String>,
    use_rdb_preamble: bool,
    /// Génération incrémentée après chaque fsync réussi (WAITAOF).
    fsync_gen: AtomicU64,
    /// Génération au moment du dernier append (cible WAITAOF local).
    pending_gen: AtomicU64,
    last_fsync: Mutex<Instant>,
    /// Group commit : retarde le fsync ALWAYS de N ms pour batcher.
    group_commit_ms: AtomicU64,
    /// Buffer de commandes en attente de fsync groupé.
    pending_bytes: Mutex<usize>,
}

impl AofLogger {
    pub fn new(cfg: &PersistConfig) -> Self {
        let path = cfg.dir.join(&cfg.appendfilename);
        let file = if cfg.appendonly {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .ok()
        } else {
            None
        };
        let logger = Self {
            inner: Mutex::new(file),
            path,
            enabled: cfg.appendonly,
            fsync_policy: Mutex::new(cfg.appendfsync.clone()),
            use_rdb_preamble: cfg.aof_use_rdb_preamble,
            fsync_gen: AtomicU64::new(0),
            pending_gen: AtomicU64::new(0),
            last_fsync: Mutex::new(Instant::now()),
            group_commit_ms: AtomicU64::new(0),
            pending_bytes: Mutex::new(0),
        };
        if cfg.appendonly && cfg.appendfsync.eq_ignore_ascii_case("everysec") {
            logger.spawn_everysec_fsync();
        }
        logger
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn set_fsync_policy(&self, policy: &str) {
        *self.fsync_policy.lock() = policy.to_string();
    }

    pub fn set_group_commit_ms(&self, ms: u64) {
        self.group_commit_ms.store(ms, Ordering::Relaxed);
    }

    pub fn group_commit_ms(&self) -> u64 {
        self.group_commit_ms.load(Ordering::Relaxed)
    }

    pub fn fsync_gen(&self) -> u64 {
        self.fsync_gen.load(Ordering::Acquire)
    }

    pub fn pending_gen(&self) -> u64 {
        self.pending_gen.load(Ordering::Acquire)
    }

    fn spawn_everysec_fsync(&self) {
        // Le thread périodique est démarré une fois depuis le serveur ; ici no-op.
        // `maybe_fsync` + `fsync_now` couvrent ALWAYS / WAITAOF / everysec opportuniste.
    }

    pub fn append_command(&self, args: &[Bytes]) {
        if !self.enabled {
            return;
        }
        let mut buf = BytesMut::new();
        buf.put_u8(b'*');
        buf.extend_from_slice(args.len().to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        for a in args {
            buf.put_u8(b'$');
            buf.extend_from_slice(a.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        if let Some(f) = self.inner.lock().as_mut() {
            let _ = f.write_all(&buf);
            let _ = f.flush();
            *self.pending_bytes.lock() += buf.len();
            let next = self.fsync_gen.load(Ordering::Relaxed).saturating_add(1);
            self.pending_gen.store(next, Ordering::Release);
            self.maybe_fsync(f);
        }
    }

    fn maybe_fsync(&self, f: &mut File) {
        let policy = self.fsync_policy.lock().clone();
        if policy.eq_ignore_ascii_case("always") {
            let gc = self.group_commit_ms();
            if gc > 0 {
                let mut last = self.last_fsync.lock();
                if last.elapsed() < Duration::from_millis(gc) {
                    return; // batch : fsync différé
                }
                let _ = f.sync_data();
                *last = Instant::now();
                *self.pending_bytes.lock() = 0;
                self.bump_fsync();
            } else {
                let _ = f.sync_data();
                self.bump_fsync();
            }
        } else if policy.eq_ignore_ascii_case("everysec") {
            let mut last = self.last_fsync.lock();
            if last.elapsed() >= Duration::from_secs(1) {
                let _ = f.sync_data();
                *last = Instant::now();
                *self.pending_bytes.lock() = 0;
                self.bump_fsync();
            }
        }
        // "no" : pas de fsync
    }

    fn bump_fsync(&self) {
        let pending = self.pending_gen.load(Ordering::Acquire);
        let cur = self.fsync_gen.load(Ordering::Acquire);
        if pending > cur {
            self.fsync_gen.store(pending, Ordering::Release);
        } else {
            self.fsync_gen.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Force un fsync (WAITAOF / shutdown).
    pub fn fsync_now(&self) -> std::io::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if let Some(f) = self.inner.lock().as_mut() {
            f.flush()?;
            f.sync_data()?;
            self.bump_fsync();
        }
        Ok(())
    }

    /// Attend que `fsync_gen >= target` ou timeout (ms). Retourne true si atteint.
    pub fn wait_fsync(&self, target: u64, timeout_ms: u64) -> bool {
        if !self.enabled {
            return true;
        }
        let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(1));
        loop {
            let _ = self.fsync_now();
            if self.fsync_gen() >= target {
                return true;
            }
            if Instant::now() >= deadline {
                return self.fsync_gen() >= target;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn rewrite(&self, db: &SharedDb) -> std::io::Result<()> {
        self.rewrite_with_functions(db, None)
    }

    pub fn rewrite_with_functions(
        &self,
        db: &SharedDb,
        functions: Option<&crate::functions::FunctionStore>,
    ) -> std::io::Result<()> {
        let tmp = self.path.with_extension("aof.tmp");
        let mut f = File::create(&tmp)?;
        let dump = db.dump_all();
        let mut fn_codes = Vec::new();
        if let Some(fs) = functions {
            for lib in fs.list_libs() {
                fn_codes.push(Bytes::from(lib.code.clone()));
            }
        }

        if self.use_rdb_preamble {
            let rdb = encode_rdb_bytes(&dump, &fn_codes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            f.write_all(&rdb)?;
        } else {
            for code in &fn_codes {
                write_cmd(&mut f, &[b"FUNCTION", b"LOAD", b"REPLACE", code.as_ref()])?;
            }
            for (dbidx, entries) in &dump {
                if *dbidx != 0 {
                    writeln!(
                        f,
                        "*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n",
                        dbidx.to_string().len(),
                        dbidx
                    )?;
                }
                for (key, entry) in entries {
                    write_entry_as_aof(&mut f, key, entry)?;
                }
            }
        }
        f.flush()?;
        f.sync_data()?;
        drop(f);
        fs::rename(&tmp, &self.path)?;
        *self.inner.lock() = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .ok();
        self.bump_fsync();
        Ok(())
    }
}

fn write_entry_as_aof(f: &mut File, key: &Bytes, entry: &Entry) -> std::io::Result<()> {
    match &entry.value {
        RedisValue::String(v) => {
            write_cmd(f, &[b"SET", key.as_ref(), v.as_ref()])?;
        }
        RedisValue::List(list) => {
            for v in list {
                write_cmd(f, &[b"RPUSH", key.as_ref(), v.as_ref()])?;
            }
        }
        RedisValue::Hash(h) => {
            let now = crate::util::now_unix_ms();
            for (field, hf) in h.fields.iter().filter(|(_, hf)| !hf.is_expired(now)) {
                write_cmd(
                    f,
                    &[b"HSET", key.as_ref(), field.as_ref(), hf.value.as_ref()],
                )?;
                if let Some(exp) = hf.expires_at_ms {
                    let exp_s = exp.to_string();
                    write_cmd(
                        f,
                        &[
                            b"HPEXPIREAT",
                            key.as_ref(),
                            exp_s.as_bytes(),
                            b"FIELDS",
                            b"1",
                            field.as_ref(),
                        ],
                    )?;
                }
            }
        }
        RedisValue::Set(s) => {
            for m in s.iter_members() {
                write_cmd(f, &[b"SADD", key.as_ref(), m.as_ref()])?;
            }
        }
        RedisValue::ZSet(z) => {
            for (m, score) in &z.members {
                let sc = score.to_string();
                write_cmd(f, &[b"ZADD", key.as_ref(), sc.as_bytes(), m.as_ref()])?;
            }
        }
        RedisValue::Stream(stream) => {
            for (id, fields) in &stream.entries {
                let id_s = id.to_string_id();
                let owned: Vec<(Vec<u8>, Vec<u8>)> = fields
                    .iter()
                    .map(|(a, b)| (a.to_vec(), b.to_vec()))
                    .collect();
                let mut flat: Vec<&[u8]> = Vec::new();
                flat.push(b"XADD");
                flat.push(key.as_ref());
                flat.push(id_s.as_bytes());
                for (a, b) in &owned {
                    flat.push(a.as_ref());
                    flat.push(b.as_ref());
                }
                write_cmd(f, &flat)?;
            }
            let last = stream.last_id.to_string_id();
            let added = stream.entries_added.to_string();
            let maxdel = stream.max_deleted_id.to_string_id();
            write_cmd(
                f,
                &[
                    b"XSETID",
                    key.as_ref(),
                    last.as_bytes(),
                    b"ENTRIESADDED",
                    added.as_bytes(),
                    b"MAXDELETEDID",
                    maxdel.as_bytes(),
                ],
            )?;
            for g in stream.groups.values() {
                let gname = g.name.clone();
                let gid = g.last_id.to_string_id();
                let er = g.entries_read.to_string();
                write_cmd(
                    f,
                    &[
                        b"XGROUP",
                        b"CREATE",
                        key.as_ref(),
                        gname.as_ref(),
                        gid.as_bytes(),
                        b"ENTRIESREAD",
                        er.as_bytes(),
                    ],
                )?;
                for c in g.consumers.values() {
                    write_cmd(
                        f,
                        &[
                            b"XGROUP",
                            b"CREATECONSUMER",
                            key.as_ref(),
                            gname.as_ref(),
                            c.name.as_ref(),
                        ],
                    )?;
                }
            }
        }
        RedisValue::Json(v) => {
            let raw = v.to_string();
            write_cmd(f, &[b"JSON.SET", key.as_ref(), b"$", raw.as_bytes()])?;
        }
        RedisValue::TimeSeries(ts) => {
            let ret = ts.retention_ms.to_string();
            let chunk = ts.chunk_size.to_string();
            let dup = ts.duplicate_policy.as_str();
            let label_owned: Vec<(Vec<u8>, Vec<u8>)> = ts
                .labels
                .iter()
                .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                .collect();
            let mut flat: Vec<&[u8]> = vec![
                b"TS.CREATE",
                key.as_ref(),
                b"RETENTION",
                ret.as_bytes(),
                b"CHUNK_SIZE",
                chunk.as_bytes(),
                b"DUPLICATE_POLICY",
                dup.as_bytes(),
            ];
            if !label_owned.is_empty() {
                flat.push(b"LABELS");
                for (k, v) in &label_owned {
                    flat.push(k.as_ref());
                    flat.push(v.as_ref());
                }
            }
            write_cmd(f, &flat)?;
            for (&t, &v) in &ts.samples {
                let ts_s = t.to_string();
                let vs = crate::util::fmt_f64(v);
                write_cmd(
                    f,
                    &[b"TS.ADD", key.as_ref(), ts_s.as_bytes(), vs.as_bytes()],
                )?;
            }
        }
        RedisValue::Bloom(b) => {
            let err = "0.01";
            let cap = (b.size / 8).max(1).to_string();
            let exp = b.expansion.to_string();
            write_cmd(
                f,
                &[
                    b"BF.RESERVE",
                    key.as_ref(),
                    err.as_bytes(),
                    cap.as_bytes(),
                    b"EXPANSION",
                    exp.as_bytes(),
                ],
            )?;
            // Rejouer via LOADCHUNK pour fidélité bits.
            let chunk = b.scandump();
            write_cmd(f, &[b"BF.LOADCHUNK", key.as_ref(), b"1", &chunk])?;
        }
        RedisValue::Cuckoo(c) => {
            let cap = c.items.max(1).to_string();
            let bs = c.bucket_size.to_string();
            let mk = c.max_kicks.to_string();
            write_cmd(
                f,
                &[
                    b"CF.RESERVE",
                    key.as_ref(),
                    cap.as_bytes(),
                    b"BUCKETSIZE",
                    bs.as_bytes(),
                    b"MAXITERATIONS",
                    mk.as_bytes(),
                ],
            )?;
            let chunk = c.scandump();
            write_cmd(f, &[b"CF.LOADCHUNK", key.as_ref(), b"1", &chunk])?;
        }
        RedisValue::Cms(c) => {
            let w = c.width.to_string();
            let d = c.depth.to_string();
            write_cmd(
                f,
                &[b"CMS.INITBYDIM", key.as_ref(), w.as_bytes(), d.as_bytes()],
            )?;
        }
        RedisValue::TopK(t) => {
            let k = t.k.to_string();
            let w = t.width.to_string();
            let d = t.depth.to_string();
            let decay = crate::util::fmt_f64(t.decay);
            write_cmd(
                f,
                &[
                    b"TOPK.RESERVE",
                    key.as_ref(),
                    k.as_bytes(),
                    w.as_bytes(),
                    d.as_bytes(),
                    decay.as_bytes(),
                ],
            )?;
            for (item, _) in t.list() {
                write_cmd(f, &[b"TOPK.ADD", key.as_ref(), item.as_ref()])?;
            }
        }
        RedisValue::TDigest(t) => {
            let comp = crate::util::fmt_f64(t.compression);
            write_cmd(
                f,
                &[
                    b"TDIGEST.CREATE",
                    key.as_ref(),
                    b"COMPRESSION",
                    comp.as_bytes(),
                ],
            )?;
            if !t.centroids.is_empty() {
                let mut flat: Vec<Vec<u8>> = Vec::new();
                for &(m, w) in &t.centroids {
                    // Approx : répéter mean floor(weight) fois (borné).
                    let n = w.min(100.0) as usize;
                    for _ in 0..n.max(1) {
                        flat.push(crate::util::fmt_f64(m).into_bytes());
                    }
                }
                let mut args: Vec<&[u8]> = vec![b"TDIGEST.ADD", key.as_ref()];
                for v in &flat {
                    args.push(v.as_ref());
                }
                write_cmd(f, &args)?;
            }
        }
        RedisValue::VectorSet(v) => {
            for (name, vals) in &v.vectors {
                let n = vals.len().to_string();
                let owned: Vec<String> = vals.iter().map(|f| crate::util::fmt_f64(f64::from(*f))).collect();
                let mut args: Vec<&[u8]> =
                    vec![b"VADD", key.as_ref(), b"VALUES", n.as_bytes()];
                for s in &owned {
                    args.push(s.as_bytes());
                }
                args.push(name.as_ref());
                write_cmd(f, &args)?;
                if let Some(a) = v.attrs.get(name) {
                    write_cmd(f, &[b"VSETATTR", key.as_ref(), name.as_ref(), a.as_ref()])?;
                }
            }
        }
    }
    if let Some(at) = entry.expires_at {
        let now = Instant::now();
        if at > now {
            let ms = at.duration_since(now).as_millis() as i64;
            let ms_s = ms.to_string();
            write_cmd(f, &[b"PEXPIRE", key.as_ref(), ms_s.as_bytes()])?;
        }
    }
    Ok(())
}

fn write_cmd(f: &mut File, args: &[&[u8]]) -> std::io::Result<()> {
    write!(f, "*{}\r\n", args.len())?;
    for a in args {
        write!(f, "${}\r\n", a.len())?;
        f.write_all(a)?;
        f.write_all(b"\r\n")?;
    }
    Ok(())
}

pub fn save_rdb(path: &Path, db: &SharedDb) -> std::io::Result<()> {
    save_rdb_with_functions(path, db, None)
}

pub fn save_rdb_with_functions(
    path: &Path,
    db: &SharedDb,
    functions: Option<&crate::functions::FunctionStore>,
) -> std::io::Result<()> {
    rdb::save_rdb_file(path, db, functions)
}

pub fn save_rdb_with_stack(
    path: &Path,
    db: &SharedDb,
    functions: Option<&crate::functions::FunctionStore>,
    search: Option<&crate::search::SearchStore>,
) -> std::io::Result<()> {
    rdb::save_rdb_file_full(path, db, functions, search)
}

pub fn load_rdb(path: &Path, db: &SharedDb) -> std::io::Result<()> {
    load_rdb_with_functions(path, db, None)
}

pub fn load_rdb_with_functions(
    path: &Path,
    db: &SharedDb,
    functions: Option<&crate::functions::FunctionStore>,
) -> std::io::Result<()> {
    rdb::load_rdb_file(path, db, functions)
}

pub fn load_rdb_with_stack(
    path: &Path,
    db: &SharedDb,
    functions: Option<&crate::functions::FunctionStore>,
    search: Option<&crate::search::SearchStore>,
) -> std::io::Result<()> {
    rdb::load_rdb_file_full(path, db, functions, search)
}

/// Charge AOF : preamble RDB optionnel (restauré dans `db`) puis commandes RESP.
pub async fn load_aof_with_db<F, Fut>(
    path: &Path,
    db: &SharedDb,
    functions: Option<&crate::functions::FunctionStore>,
    mut dispatch: F,
) -> std::io::Result<()>
where
    F: FnMut(Vec<Bytes>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    if !path.exists() {
        return Ok(());
    }
    let data = fs::read(path)?;
    let mut offset = 0usize;
    if data.starts_with(b"REDIS") || data.starts_with(b"FRDB001\n") {
        match find_rdb_end(&data) {
            Some(end) => {
                let snap = rdb::decode_rdb_or_frdb(&data[..end])
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
                db.restore_all(snap.dbs);
                if let Some(fs) = functions {
                    for code in snap.function_codes {
                        if let Ok(s) = std::str::from_utf8(&code) {
                            let _ = fs.load(s, true);
                        }
                    }
                }
                offset = end;
                info!("AOF RDB preamble loaded ({} bytes)", offset);
            }
            None => {
                warn!("AOF preamble RDB incomplete; replaying as commands from start");
            }
        }
    }
    let mut buf = BytesMut::from(&data[offset..]);
    let mut n = 0u64;
    while !buf.is_empty() {
        match parse_command(&mut buf) {
            Ok(args) if !args.is_empty() => {
                dispatch(args).await;
                n += 1;
            }
            Ok(_) => {}
            Err(crate::resp::RespError::Incomplete) => break,
            Err(e) => {
                warn!("AOF parse error: {e}");
                break;
            }
        }
    }
    info!("AOF loaded {n} commands from {}", path.display());
    Ok(())
}

pub async fn load_aof<F, Fut>(path: &Path, dispatch: F) -> std::io::Result<()>
where
    F: FnMut(Vec<Bytes>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    // Sans DB : saute le preamble si présent (compat appels existants).
    load_aof_skip_preamble(path, dispatch).await
}

async fn load_aof_skip_preamble<F, Fut>(path: &Path, mut dispatch: F) -> std::io::Result<()>
where
    F: FnMut(Vec<Bytes>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    if !path.exists() {
        return Ok(());
    }
    let data = fs::read(path)?;
    let offset = if data.starts_with(b"REDIS") || data.starts_with(b"FRDB001\n") {
        find_rdb_end(&data).unwrap_or(0)
    } else {
        0
    };
    let mut buf = BytesMut::from(&data[offset..]);
    while !buf.is_empty() {
        match parse_command(&mut buf) {
            Ok(args) if !args.is_empty() => {
                dispatch(args).await;
            }
            Ok(_) => {}
            Err(crate::resp::RespError::Incomplete) => break,
            Err(e) => {
                warn!("AOF parse error: {e}");
                break;
            }
        }
    }
    Ok(())
}

/// Fin d'un blob RDB Redis (après CRC 8 octets) ou FRDB (magic+len+body[+FNS1]).
fn find_rdb_end(data: &[u8]) -> Option<usize> {
    if data.starts_with(b"REDIS") {
        // Cherche OPCODE_EOF (0xFF) puis 8 octets CRC — parcours via decode.
        match rdb::decode_rdb_or_frdb(data) {
            Ok(_) => {
                // decode consomme jusqu'à EOF+CRC ; taille exacte = fichier si AOF = RDB pur,
                // sinon on calcule : magic…eof+crc. On re-scanne pour EOF.
                let mut i = 9;
                while i < data.len() {
                    if data[i] == 0xff && i + 1 + 8 <= data.len() {
                        // Heuristique : si CRC matche, c'est la fin.
                        let body_end = i + 1;
                        let stored = u64::from_le_bytes(data[body_end..body_end + 8].try_into().ok()?);
                        let got = crate::rdb::crc64(0, &data[..body_end]);
                        if stored == 0 || stored == got {
                            return Some(body_end + 8);
                        }
                    }
                    i += 1;
                }
                None
            }
            Err(_) => None,
        }
    } else if data.starts_with(b"FRDB001\n") {
        let mut i = 8;
        if i + 8 > data.len() {
            return None;
        }
        let len = u64::from_le_bytes(data[i..i + 8].try_into().ok()?) as usize;
        i += 8 + len;
        if i + 4 <= data.len() && &data[i..i + 4] == b"FNS1" {
            i += 4;
            if i + 8 > data.len() {
                return None;
            }
            let plen = u64::from_le_bytes(data[i..i + 8].try_into().ok()?) as usize;
            i += 8 + plen;
        }
        Some(i)
    } else {
        None
    }
}

pub type SharedAof = Arc<AofLogger>;
