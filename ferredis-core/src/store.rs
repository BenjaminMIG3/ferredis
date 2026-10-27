//! Multi-DB key-value store with TTL and SCAN.

use crate::value::{Entry, RedisValue};
use bytes::Bytes;
use parking_lot::RwLock;
use rand::Rng;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub const DEFAULT_DB_COUNT: usize = 16;

#[derive(Debug, Default)]
pub(crate) struct DbInner {
    pub map: HashMap<Bytes, Entry>,
}

impl DbInner {
    pub(crate) fn purge_if_expired(&mut self, key: &Bytes) -> bool {
        if let Some(entry) = self.map.get(key) {
            if entry.is_expired() {
                self.map.remove(key);
                return true;
            }
        }
        false
    }

    pub(crate) fn purge_expired(&mut self, key: &Bytes) -> bool {
        self.purge_if_expired(key)
    }

    pub(crate) fn get_fresh(&mut self, key: &Bytes) -> Option<&Entry> {
        if self.purge_if_expired(key) {
            return None;
        }
        // Touche LRU/LFU via get_mut puis re-emprunt immuable.
        if let Some(e) = self.map.get_mut(key) {
            e.record_access();
        }
        self.map.get(key)
    }

    pub(crate) fn get_fresh_mut(&mut self, key: &Bytes) -> Option<&mut Entry> {
        if self.purge_if_expired(key) {
            return None;
        }
        let e = self.map.get_mut(key)?;
        e.record_access();
        Some(e)
    }

    /// Accès sans mettre à jour LRU (CLIENT NO-TOUCH / OBJECT).
    pub(crate) fn get_fresh_notouch(&mut self, key: &Bytes) -> Option<&Entry> {
        if self.purge_if_expired(key) {
            return None;
        }
        self.map.get(key)
    }

    /// Supprime la clé si son conteneur (liste, hash, set, zset) est vide.
    pub(crate) fn remove_if_empty(&mut self, key: &Bytes) {
        if self
            .map
            .get(key)
            .map(|e| e.value.is_empty_container())
            .unwrap_or(false)
        {
            self.map.remove(key);
        }
    }
}

pub(crate) const WRONGTYPE: &str =
    "WRONGTYPE Operation against a key holding the wrong kind of value";

/// Génère les accesseurs typés `<name>_mut` (lecture/écriture si présent)
/// et `<name>_or_create` (création paresseuse) pour une variante de `RedisValue`.
macro_rules! typed_accessors {
    ($get:ident, $create:ident, $variant:ident, $ty:ty, $ctor:expr) => {
        impl DbInner {
            #[allow(dead_code)]
            pub(crate) fn $get(&mut self, key: &Bytes) -> Result<Option<&mut $ty>, &'static str> {
                match self.get_fresh_mut(key) {
                    None => Ok(None),
                    Some(e) => match &mut e.value {
                        RedisValue::$variant(v) => Ok(Some(v)),
                        _ => Err(WRONGTYPE),
                    },
                }
            }

            #[allow(dead_code)]
            pub(crate) fn $create(&mut self, key: &Bytes) -> Result<&mut $ty, &'static str> {
                self.purge_if_expired(key);
                let e = self
                    .map
                    .entry(key.clone())
                    .or_insert_with(|| Entry::new(RedisValue::$variant($ctor)));
                match &mut e.value {
                    RedisValue::$variant(v) => Ok(v),
                    _ => Err(WRONGTYPE),
                }
            }
        }
    };
}

typed_accessors!(list_mut, list_or_create, List, VecDeque<Bytes>, VecDeque::new());
typed_accessors!(
    hash_mut,
    hash_or_create,
    Hash,
    crate::value::HashObject,
    crate::value::HashObject::new()
);
typed_accessors!(
    set_mut,
    set_or_create,
    Set,
    std::collections::HashSet<Bytes>,
    std::collections::HashSet::new()
);
typed_accessors!(
    zset_mut,
    zset_or_create,
    ZSet,
    crate::value::ZSet,
    crate::value::ZSet::default()
);
typed_accessors!(
    stream_mut,
    stream_or_create,
    Stream,
    crate::value::Stream,
    crate::value::Stream::default()
);

#[derive(Debug)]
pub struct Database {
    dbs: Vec<RwLock<DbInner>>,
    pub dirty: AtomicU64,
}

impl Default for Database {
    fn default() -> Self {
        Self::new(DEFAULT_DB_COUNT)
    }
}

impl Database {
    pub fn new(db_count: usize) -> Self {
        let mut dbs = Vec::with_capacity(db_count);
        for _ in 0..db_count {
            dbs.push(RwLock::new(DbInner::default()));
        }
        Self {
            dbs,
            dirty: AtomicU64::new(0),
        }
    }

    pub fn db_count(&self) -> usize {
        self.dbs.len()
    }

    pub(crate) fn touch(&self) {
        self.dirty.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn touch_dirty(&self) {
        self.touch();
    }

    pub(crate) fn dbs_write(&self, db: usize) -> parking_lot::RwLockWriteGuard<'_, DbInner> {
        self.dbs[db].write()
    }

    /// Échange atomique du contenu de deux bases (verrous pris dans l'ordre des index).
    pub fn swap_dbs(&self, a: usize, b: usize) -> bool {
        if a >= self.dbs.len() || b >= self.dbs.len() {
            return false;
        }
        if a != b {
            let (lo, hi) = (a.min(b), a.max(b));
            let mut g1 = self.dbs[lo].write();
            let mut g2 = self.dbs[hi].write();
            std::mem::swap(&mut *g1, &mut *g2);
            self.touch();
        }
        true
    }

    pub(crate) fn dbs_iter(&self) -> impl Iterator<Item = &RwLock<DbInner>> {
        self.dbs.iter()
    }

    pub fn get_string(&self, db: usize, key: &Bytes) -> Result<Option<Bytes>, &'static str> {
        let mut guard = self.dbs[db].write();
        match guard.get_fresh(key) {
            None => Ok(None),
            Some(e) => match &e.value {
                RedisValue::String(s) => Ok(Some(s.clone())),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    /// Clone l'entrée vivante (DUMP / MIGRATE).
    pub fn get_entry(&self, db: usize, key: &Bytes) -> Option<Entry> {
        let mut guard = self.dbs[db].write();
        guard.get_fresh(key).cloned()
    }

    /// Insère une entrée (RESTORE). `replace=false` → erreur `BUSYKEY` si la clé existe.
    pub fn put_entry(
        &self,
        db: usize,
        key: Bytes,
        entry: Entry,
        replace: bool,
    ) -> Result<(), &'static str> {
        let mut guard = self.dbs[db].write();
        guard.purge_if_expired(&key);
        if !replace && guard.map.contains_key(&key) {
            return Err("BUSYKEY");
        }
        guard.map.insert(key, entry);
        self.touch();
        Ok(())
    }

    pub fn set_string(
        &self,
        db: usize,
        key: Bytes,
        value: Bytes,
        nx: bool,
        xx: bool,
        ex: Option<u64>,
        px: Option<u64>,
    ) -> Result<bool, &'static str> {
        let mut guard = self.dbs[db].write();
        let exists = guard.get_fresh(&key).is_some();
        if nx && exists {
            return Ok(false);
        }
        if xx && !exists {
            return Ok(false);
        }
        let mut entry = Entry::new(RedisValue::String(value));
        if let Some(ms) = px {
            entry.set_expire_ms(ms as i64);
        } else if let Some(secs) = ex {
            entry.set_expire_secs(secs as i64);
        }
        guard.map.insert(key, entry);
        self.touch();
        Ok(true)
    }

    pub fn del(&self, db: usize, keys: &[Bytes]) -> i64 {
        let mut guard = self.dbs[db].write();
        let mut n = 0i64;
        for key in keys {
            // Une clé déjà expirée n'est pas comptée comme supprimée.
            guard.purge_if_expired(key);
            if guard.map.remove(key).is_some() {
                n += 1;
            }
        }
        if n > 0 {
            self.touch();
        }
        n
    }

    pub fn exists(&self, db: usize, keys: &[Bytes]) -> i64 {
        let mut guard = self.dbs[db].write();
        let mut n = 0i64;
        for key in keys {
            if guard.get_fresh(key).is_some() {
                n += 1;
            }
        }
        n
    }

    pub fn key_type(&self, db: usize, key: &Bytes) -> &'static str {
        let mut guard = self.dbs[db].write();
        match guard.get_fresh(key) {
            None => "none",
            Some(e) => e.value.type_name(),
        }
    }

    pub fn ttl(&self, db: usize, key: &Bytes) -> i64 {
        let mut guard = self.dbs[db].write();
        match guard.get_fresh(key) {
            None => -2,
            Some(e) => e.ttl_secs(),
        }
    }

    pub fn pttl(&self, db: usize, key: &Bytes) -> i64 {
        let mut guard = self.dbs[db].write();
        match guard.get_fresh(key) {
            None => -2,
            Some(e) => e.pttl_ms(),
        }
    }

    pub fn expire(&self, db: usize, key: &Bytes, secs: i64) -> i64 {
        let mut guard = self.dbs[db].write();
        match guard.get_fresh_mut(key) {
            None => 0,
            Some(e) => {
                e.set_expire_secs(secs);
                self.touch();
                1
            }
        }
    }

    pub fn pexpire(&self, db: usize, key: &Bytes, ms: i64) -> i64 {
        let mut guard = self.dbs[db].write();
        match guard.get_fresh_mut(key) {
            None => 0,
            Some(e) => {
                e.set_expire_ms(ms);
                self.touch();
                1
            }
        }
    }

    pub fn persist(&self, db: usize, key: &Bytes) -> i64 {
        let mut guard = self.dbs[db].write();
        match guard.get_fresh_mut(key) {
            None => 0,
            Some(e) => {
                if e.expires_at.is_none() {
                    0
                } else {
                    e.persist();
                    self.touch();
                    1
                }
            }
        }
    }

    pub fn incr_by(&self, db: usize, key: &Bytes, delta: i64) -> Result<i64, &'static str> {
        let mut guard = self.dbs[db].write();
        if guard.purge_if_expired(key) {
            // fallthrough to create
        }
        match guard.map.get_mut(key) {
            None => {
                let n = delta;
                guard.map.insert(
                    key.clone(),
                    Entry::new(RedisValue::String(Bytes::from(n.to_string()))),
                );
                self.touch();
                Ok(n)
            }
            Some(entry) => match &mut entry.value {
                RedisValue::String(s) => {
                    let cur: i64 = std::str::from_utf8(s)
                        .ok()
                        .and_then(|t| t.parse().ok())
                        .ok_or("ERR value is not an integer or out of range")?;
                    let n = cur.checked_add(delta).ok_or("ERR overflow")?;
                    *s = Bytes::from(n.to_string());
                    self.touch();
                    Ok(n)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn list_push(
        &self,
        db: usize,
        key: &Bytes,
        values: &[Bytes],
        left: bool,
    ) -> Result<i64, &'static str> {
        let mut guard = self.dbs[db].write();
        if guard.purge_if_expired(key) {
            // create new
        }
        match guard.map.get_mut(key) {
            None => {
                let mut list = VecDeque::new();
                for v in values {
                    if left {
                        list.push_front(v.clone());
                    } else {
                        list.push_back(v.clone());
                    }
                }
                let len = list.len() as i64;
                guard
                    .map
                    .insert(key.clone(), Entry::new(RedisValue::List(list)));
                self.touch();
                Ok(len)
            }
            Some(entry) => match &mut entry.value {
                RedisValue::List(list) => {
                    for v in values {
                        if left {
                            list.push_front(v.clone());
                        } else {
                            list.push_back(v.clone());
                        }
                    }
                    self.touch();
                    Ok(list.len() as i64)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn list_pop(
        &self,
        db: usize,
        key: &Bytes,
        left: bool,
        count: Option<usize>,
    ) -> Result<Option<Vec<Bytes>>, &'static str> {
        let mut guard = self.dbs[db].write();
        match guard.get_fresh_mut(key) {
            None => Ok(None),
            Some(entry) => match &mut entry.value {
                RedisValue::List(list) => {
                    if list.is_empty() {
                        return Ok(None);
                    }
                    let n = count.unwrap_or(1).min(list.len());
                    let mut out = Vec::with_capacity(n);
                    for _ in 0..n {
                        let v = if left {
                            list.pop_front()
                        } else {
                            list.pop_back()
                        };
                        if let Some(v) = v {
                            out.push(v);
                        }
                    }
                    self.touch();
                    // Suppression atomique sous le même verrou (pas de fenêtre de course).
                    guard.remove_if_empty(key);
                    Ok(Some(out))
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn list_len(&self, db: usize, key: &Bytes) -> Result<i64, &'static str> {
        let mut guard = self.dbs[db].write();
        match guard.get_fresh(key) {
            None => Ok(0),
            Some(e) => match &e.value {
                RedisValue::List(l) => Ok(l.len() as i64),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn list_range(
        &self,
        db: usize,
        key: &Bytes,
        start: i64,
        stop: i64,
    ) -> Result<Vec<Bytes>, &'static str> {
        let mut guard = self.dbs[db].write();
        match guard.get_fresh(key) {
            None => Ok(Vec::new()),
            Some(e) => match &e.value {
                RedisValue::List(l) => {
                    let len = l.len() as i64;
                    let mut start = if start < 0 { len + start } else { start };
                    let mut stop = if stop < 0 { len + stop } else { stop };
                    if start < 0 {
                        start = 0;
                    }
                    if stop < 0 {
                        return Ok(Vec::new());
                    }
                    if start >= len || start > stop {
                        return Ok(Vec::new());
                    }
                    if stop >= len {
                        stop = len - 1;
                    }
                    Ok(l
                        .iter()
                        .skip(start as usize)
                        .take((stop - start + 1) as usize)
                        .cloned()
                        .collect())
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    /// Blocking pop: try once; caller handles wait/retry.
    pub fn try_blpop(&self, db: usize, keys: &[Bytes]) -> Option<(Bytes, Bytes)> {
        for key in keys {
            if let Ok(Some(mut vals)) = self.list_pop(db, key, true, Some(1)) {
                if let Some(v) = vals.pop() {
                    return Some((key.clone(), v));
                }
            }
        }
        None
    }

    pub fn try_brpop(&self, db: usize, keys: &[Bytes]) -> Option<(Bytes, Bytes)> {
        for key in keys {
            if let Ok(Some(mut vals)) = self.list_pop(db, key, false, Some(1)) {
                if let Some(v) = vals.pop() {
                    return Some((key.clone(), v));
                }
            }
        }
        None
    }

    /// Exécute `f` sous le verrou d'écriture de la base `db` (point d'entrée des commandes composites).
    pub(crate) fn with_db<R>(&self, db: usize, f: impl FnOnce(&mut DbInner) -> R) -> R {
        let mut guard = self.dbs[db].write();
        f(&mut guard)
    }

    pub fn dbsize(&self, db: usize) -> i64 {
        let mut guard = self.dbs[db].write();
        guard.map.retain(|_, e| !e.is_expired());
        guard.map.len() as i64
    }

    /// `(nombre de clés, nombre de clés avec TTL)` d'une base, après purge des clés expirées.
    pub fn key_stats(&self, db: usize) -> (usize, usize) {
        let mut guard = self.dbs[db].write();
        guard.map.retain(|_, e| !e.is_expired());
        let expires = guard.map.values().filter(|e| e.expires_at.is_some()).count();
        (guard.map.len(), expires)
    }

    /// Estimation de la mémoire utilisée (octets) : clés + valeurs.
    pub fn approx_memory(&self) -> usize {
        self.dbs
            .iter()
            .map(|db| {
                db.read()
                    .map
                    .iter()
                    .map(|(k, e)| k.len() + e.value.approx_bytes() + 64)
                    .sum::<usize>()
            })
            .sum()
    }

    /// Échantillons de clés pour l'éviction : `(key, score)` — score plus petit = meilleur candidat.
    pub fn sample_keys_for_eviction(
        &self,
        db: usize,
        samples: usize,
        policy: crate::eviction::MaxMemoryPolicy,
    ) -> Vec<(Bytes, i64)> {
        use crate::eviction::eviction_score;
        let mut guard = self.dbs[db].write();
        guard.map.retain(|_, e| !e.is_expired());
        let len = guard.map.len();
        if len == 0 {
            return Vec::new();
        }
        let mut rng = rand::thread_rng();
        let offset = rng.gen_range(0..len);
        let volatile_only = matches!(
            policy,
            crate::eviction::MaxMemoryPolicy::VolatileLru
                | crate::eviction::MaxMemoryPolicy::VolatileLfu
                | crate::eviction::MaxMemoryPolicy::VolatileRandom
                | crate::eviction::MaxMemoryPolicy::VolatileTtl
        );
        guard
            .map
            .iter()
            .cycle()
            .skip(offset)
            .take(samples.min(len))
            .filter(|(_, e)| !volatile_only || e.expires_at.is_some())
            .map(|(k, e)| {
                let rnd = rng.gen_range(0..10_000);
                let score = eviction_score(policy, e.idle_secs(), e.lfu_freq, e.pttl_ms(), rnd);
                (k.clone(), score)
            })
            .collect()
    }

    /// Clé aléatoire non expirée (RANDOMKEY).
    pub fn random_key(&self, db: usize) -> Option<Bytes> {
        let mut guard = self.dbs[db].write();
        guard.map.retain(|_, e| !e.is_expired());
        if guard.map.is_empty() {
            return None;
        }
        let idx = rand::thread_rng().gen_range(0..guard.map.len());
        guard.map.keys().nth(idx).cloned()
    }

    pub fn flushdb(&self, db: usize) {
        self.dbs[db].write().map.clear();
        self.touch();
    }

    pub fn flushall(&self) {
        for db in &self.dbs {
            db.write().map.clear();
        }
        self.touch();
    }

    /// SCAN : curseur = index dans la liste triée des clés filtrées (parcours déterministe).
    pub fn scan(
        &self,
        db: usize,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
        type_filter: Option<&str>,
    ) -> (u64, Vec<Bytes>) {
        let mut guard = self.dbs[db].write();
        guard.map.retain(|_, e| !e.is_expired());
        let mut filtered: Vec<Bytes> = guard
            .map
            .iter()
            .filter(|(k, e)| {
                type_filter.map_or(true, |t| e.value.type_name().eq_ignore_ascii_case(t))
                    && pattern.map_or(true, |pat| match_glob_bytes(pat.as_bytes(), k))
            })
            .map(|(k, _)| k.clone())
            .collect();
        drop(guard);
        filtered.sort_unstable();
        paginate(filtered, cursor, count)
    }

    pub fn keys(&self, db: usize, pattern: &str) -> Vec<Bytes> {
        let mut guard = self.dbs[db].write();
        guard.map.retain(|_, e| !e.is_expired());
        let mut out: Vec<Bytes> = guard
            .map
            .keys()
            .filter(|k| match_glob_bytes(pattern.as_bytes(), k))
            .cloned()
            .collect();
        drop(guard);
        out.sort_unstable();
        out
    }

    pub fn random_expire_sample(&self, db: usize, samples: usize) -> usize {
        let mut guard = self.dbs[db].write();
        let len = guard.map.len();
        if len == 0 {
            return 0;
        }
        // Fenêtre aléatoire contiguë : pas de clonage de toutes les clés à chaque tick.
        let offset = rand::thread_rng().gen_range(0..len);
        let keys: Vec<Bytes> = guard
            .map
            .iter()
            .cycle()
            .skip(offset)
            .take(samples.min(len))
            .map(|(k, _)| k.clone())
            .collect();
        let now = crate::util::now_unix_ms();
        let mut removed = 0usize;
        for k in keys {
            let drop_key = match guard.map.get_mut(&k) {
                Some(e) if e.is_expired() => true,
                Some(e) => {
                    if let crate::value::RedisValue::Hash(h) = &mut e.value {
                        removed += h.purge_expired(now);
                        h.is_empty()
                    } else {
                        false
                    }
                }
                None => false,
            };
            if drop_key {
                guard.map.remove(&k);
                removed += 1;
            }
        }
        removed
    }

    pub fn list_keys_preview(&self, db: usize, limit: usize) -> Vec<(Bytes, String, i64)> {
        let mut guard = self.dbs[db].write();
        guard.map.retain(|_, e| !e.is_expired());
        guard
            .map
            .iter()
            .take(limit)
            .map(|(k, e)| (k.clone(), e.value.type_name().to_string(), e.ttl_secs()))
            .collect()
    }

    pub fn set_keep_ttl_string(&self, db: usize, key: Bytes, value: Bytes) {
        let mut guard = self.dbs[db].write();
        let ttl = guard.get_fresh(&key).and_then(|e| e.expires_at);
        let mut entry = Entry::new(RedisValue::String(value));
        entry.expires_at = ttl;
        guard.map.insert(key, entry);
        self.touch();
    }
}

/// Découpe `items` à partir de `cursor` : retourne `(curseur suivant, page)`.
/// Un curseur suivant de 0 signale la fin de l'itération.
pub fn paginate<T>(items: Vec<T>, cursor: u64, count: usize) -> (u64, Vec<T>) {
    let len = items.len();
    let start = (cursor as usize).min(len);
    let end = start.saturating_add(count.max(1)).min(len);
    let next = if end >= len { 0 } else { end as u64 };
    (next, items.into_iter().skip(start).take(end - start).collect())
}

/// Glob Redis complet : `*`, `?`, classes `[abc]` / `[^a-z]` et échappement `\`.
pub fn match_glob_bytes(pattern: &[u8], text: &[u8]) -> bool {
    /// Compare `text[0]` à la classe commençant après `[` ; retourne (match, longueur consommée dans le motif).
    fn class(p: &[u8], c: u8) -> (bool, usize) {
        let mut i = 0;
        let negate = p.first() == Some(&b'^');
        if negate {
            i += 1;
        }
        let mut matched = false;
        while i < p.len() && p[i] != b']' {
            if p[i] == b'\\' && i + 1 < p.len() {
                matched |= p[i + 1] == c;
                i += 2;
            } else if i + 2 < p.len() && p[i + 1] == b'-' && p[i + 2] != b']' {
                let (lo, hi) = (p[i].min(p[i + 2]), p[i].max(p[i + 2]));
                matched |= (lo..=hi).contains(&c);
                i += 3;
            } else {
                matched |= p[i] == c;
                i += 1;
            }
        }
        // `i` pointe sur `]` (ou fin) : on consomme aussi le crochet fermant.
        (matched != negate, (i + 1).min(p.len()))
    }

    let (mut pi, mut ti) = (0usize, 0usize);
    let mut backtrack: Option<(usize, usize)> = None;
    while ti < text.len() {
        let mut advanced = false;
        if pi < pattern.len() {
            match pattern[pi] {
                b'*' => {
                    backtrack = Some((pi, ti));
                    pi += 1;
                    continue;
                }
                b'?' => {
                    pi += 1;
                    ti += 1;
                    advanced = true;
                }
                b'[' => {
                    let (ok, used) = class(&pattern[pi + 1..], text[ti]);
                    if ok {
                        pi += 1 + used;
                        ti += 1;
                        advanced = true;
                    }
                }
                b'\\' if pi + 1 < pattern.len() => {
                    if pattern[pi + 1] == text[ti] {
                        pi += 2;
                        ti += 1;
                        advanced = true;
                    }
                }
                c => {
                    if c == text[ti] {
                        pi += 1;
                        ti += 1;
                        advanced = true;
                    }
                }
            }
        }
        if !advanced {
            match backtrack {
                Some((bp, bt)) => {
                    pi = bp + 1;
                    ti = bt + 1;
                    backtrack = Some((bp, bt + 1));
                }
                None => return false,
            }
        }
    }
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

/// Glob Redis sur des `&str` (voir [`match_glob_bytes`]).
pub fn match_glob(pattern: &str, text: &str) -> bool {
    match_glob_bytes(pattern.as_bytes(), text.as_bytes())
}

pub type SharedDb = Arc<Database>;

pub fn shared_db() -> SharedDb {
    Arc::new(Database::default())
}

/// Registre de clients bloqués (BLPOP, BRPOP, BZPOPMIN, BRPOPLPUSH…).
///
/// Chaque client bloqué possède un `Notify` enregistré sur toutes ses clés.
/// `notify_one` mémorise un permis : aucun réveil n'est perdu même si la
/// donnée arrive entre la tentative de lecture et l'attente.
#[derive(Default)]
pub struct ListWaiters {
    inner: parking_lot::Mutex<HashMap<(usize, Bytes), Vec<Arc<tokio::sync::Notify>>>>,
}

impl ListWaiters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Réveille tous les clients bloqués sur `(db, key)`.
    pub fn notify(&self, db: usize, key: &Bytes) {
        let guard = self.inner.lock();
        if guard.is_empty() {
            return;
        }
        if let Some(waiters) = guard.get(&(db, key.clone())) {
            for n in waiters {
                n.notify_one();
            }
        }
    }

    /// Réveille tous les clients bloqués sur une base (SWAPDB) : ils retentent leur opération.
    pub fn notify_db(&self, db: usize) {
        for ((d, _), waiters) in self.inner.lock().iter() {
            if *d == db {
                waiters.iter().for_each(|n| n.notify_one());
            }
        }
    }

    pub fn register(&self, db: usize, keys: &[Bytes], notify: &Arc<tokio::sync::Notify>) {
        let mut guard = self.inner.lock();
        for key in keys {
            guard
                .entry((db, key.clone()))
                .or_default()
                .push(Arc::clone(notify));
        }
    }

    pub fn unregister(&self, db: usize, keys: &[Bytes], notify: &Arc<tokio::sync::Notify>) {
        let mut guard = self.inner.lock();
        for key in keys {
            let k = (db, key.clone());
            if let Some(v) = guard.get_mut(&k) {
                v.retain(|n| !Arc::ptr_eq(n, notify));
                if v.is_empty() {
                    guard.remove(&k);
                }
            }
        }
    }

    /// Nombre de clients actuellement bloqués (INFO `blocked_clients`).
    pub fn blocked_count(&self) -> usize {
        let guard = self.inner.lock();
        let mut seen: Vec<*const tokio::sync::Notify> = Vec::new();
        for v in guard.values() {
            for n in v {
                let p = Arc::as_ptr(n);
                if !seen.contains(&p) {
                    seen.push(p);
                }
            }
        }
        seen.len()
    }
}

pub type SharedWaiters = Arc<ListWaiters>;

#[cfg(test)]
mod tests {
    use super::match_glob;

    #[test]
    fn glob_classes_and_escapes() {
        assert!(match_glob("h?llo", "hello"));
        assert!(match_glob("h*llo", "heeeello"));
        assert!(match_glob("h[ae]llo", "hallo"));
        assert!(!match_glob("h[^e]llo", "hello"));
        assert!(match_glob("h[a-b]llo", "hbllo"));
        assert!(match_glob(r"h\*llo", "h*llo"));
        assert!(!match_glob(r"h\*llo", "hxllo"));
        assert!(match_glob("__keyspace@*__:*", "__keyspace@0__:foo"));
        assert!(match_glob("*", ""));
    }
}
