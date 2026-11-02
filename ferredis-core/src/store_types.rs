//! Hash / Set / ZSet / bitmap operations on Database (streams → `cmd_stream`).

use crate::store::{Database, WRONGTYPE};
use crate::util::now_unix_ms;
use crate::value::{Entry, HashObject, RedisValue, ZSet};
use bytes::Bytes;
use std::collections::HashSet;

impl Database {
    pub fn hset(
        &self,
        db: usize,
        key: &Bytes,
        fields: &[(Bytes, Bytes)],
    ) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        if guard.purge_expired(key) {}
        match guard.map.get_mut(key) {
            None => {
                let mut h = HashObject::new();
                let mut added = 0i64;
                for (f, v) in fields {
                    if h.insert(f.clone(), v.clone()) {
                        added += 1;
                    }
                }
                guard
                    .map
                    .insert(key.clone(), Entry::new(RedisValue::Hash(h)));
                self.touch_dirty();
                Ok(added)
            }
            Some(e) => {
                let added = match &mut e.value {
                    RedisValue::Hash(h) => {
                        h.purge_expired(now_unix_ms());
                        let mut added = 0i64;
                        for (f, v) in fields {
                            if h.insert(f.clone(), v.clone()) {
                                added += 1;
                            }
                        }
                        added
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        )
                    }
                };
                e.mark_written();
                self.touch_dirty();
                Ok(added)
            }
        }
    }

    pub fn hget(
        &self,
        db: usize,
        key: &Bytes,
        field: &Bytes,
    ) -> Result<Option<Bytes>, &'static str> {
        let mut guard = self.dbs_write(db);
        match guard.get_fresh_mut(key) {
            None => Ok(None),
            Some(e) => match &mut e.value {
                RedisValue::Hash(h) => {
                    h.purge_expired(now_unix_ms());
                    Ok(h.get(field).cloned())
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn hdel(&self, db: usize, key: &Bytes, fields: &[Bytes]) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        match guard.get_fresh_mut(key) {
            None => Ok(0),
            Some(e) => match &mut e.value {
                RedisValue::Hash(h) => {
                    h.purge_expired(now_unix_ms());
                    let mut n = 0i64;
                    for f in fields {
                        if h.remove(f).is_some() {
                            n += 1;
                        }
                    }
                    let empty = h.is_empty();
                    self.touch_dirty();
                    if empty {
                        guard.map.remove(key);
                    }
                    Ok(n)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn hgetall(&self, db: usize, key: &Bytes) -> Result<Vec<(Bytes, Bytes)>, &'static str> {
        let mut guard = self.dbs_write(db);
        match guard.get_fresh_mut(key) {
            None => Ok(Vec::new()),
            Some(e) => match &mut e.value {
                RedisValue::Hash(h) => {
                    h.purge_expired(now_unix_ms());
                    Ok(h.pairs())
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn hexists(&self, db: usize, key: &Bytes, field: &Bytes) -> Result<i64, &'static str> {
        Ok(if self.hget(db, key, field)?.is_some() {
            1
        } else {
            0
        })
    }

    pub fn hlen(&self, db: usize, key: &Bytes) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        match guard.get_fresh_mut(key) {
            None => Ok(0),
            Some(e) => match &mut e.value {
                RedisValue::Hash(h) => {
                    h.purge_expired(now_unix_ms());
                    Ok(h.live_len() as i64)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn hincrby(
        &self,
        db: usize,
        key: &Bytes,
        field: &Bytes,
        delta: i64,
    ) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        if guard.purge_expired(key) {}
        match guard.map.get_mut(key) {
            None => {
                let mut h = HashObject::new();
                h.insert(field.clone(), Bytes::from(delta.to_string()));
                guard
                    .map
                    .insert(key.clone(), Entry::new(RedisValue::Hash(h)));
                self.touch_dirty();
                Ok(delta)
            }
            Some(e) => match &mut e.value {
                RedisValue::Hash(h) => {
                    h.purge_expired(now_unix_ms());
                    let cur = h
                        .get(field)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0);
                    let n = cur.checked_add(delta).ok_or("ERR overflow")?;
                    h.insert(field.clone(), Bytes::from(n.to_string()));
                    self.touch_dirty();
                    Ok(n)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn sadd(&self, db: usize, key: &Bytes, members: &[Bytes]) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        if guard.purge_expired(key) {}
        match guard.map.get_mut(key) {
            None => {
                let mut s = crate::encoding::SetObject::new();
                for m in members {
                    s.insert(m.clone());
                }
                let n = s.len() as i64;
                guard
                    .map
                    .insert(key.clone(), Entry::new(RedisValue::Set(s)));
                self.touch_dirty();
                Ok(n)
            }
            Some(e) => match &mut e.value {
                RedisValue::Set(s) => {
                    let mut n = 0i64;
                    for m in members {
                        if s.insert(m.clone()) {
                            n += 1;
                        }
                    }
                    self.touch_dirty();
                    Ok(n)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn srem(&self, db: usize, key: &Bytes, members: &[Bytes]) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        match guard.get_fresh_mut(key) {
            None => Ok(0),
            Some(e) => match &mut e.value {
                RedisValue::Set(s) => {
                    let mut n = 0i64;
                    for m in members {
                        if s.remove(m) {
                            n += 1;
                        }
                    }
                    let empty = s.is_empty();
                    self.touch_dirty();
                    if empty {
                        guard.map.remove(key);
                    }
                    Ok(n)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn smembers(&self, db: usize, key: &Bytes) -> Result<Vec<Bytes>, &'static str> {
        let mut guard = self.dbs_write(db);
        match guard.get_fresh(key) {
            None => Ok(Vec::new()),
            Some(e) => match &e.value {
                RedisValue::Set(s) => Ok(s.iter_members()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn sismember(&self, db: usize, key: &Bytes, member: &Bytes) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        match guard.get_fresh(key) {
            None => Ok(0),
            Some(e) => match &e.value {
                RedisValue::Set(s) => Ok(if s.contains(member) { 1 } else { 0 }),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn scard(&self, db: usize, key: &Bytes) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        match guard.get_fresh(key) {
            None => Ok(0),
            Some(e) => match &e.value {
                RedisValue::Set(s) => Ok(s.len() as i64),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn zadd(
        &self,
        db: usize,
        key: &Bytes,
        members: &[(f64, Bytes)],
        nx: bool,
        xx: bool,
        ch: bool,
        incr: bool,
    ) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        if guard.purge_expired(key) {}
        match guard.map.get_mut(key) {
            None => {
                if xx {
                    return Ok(0);
                }
                let mut z = ZSet::default();
                let mut added = 0i64;
                for (score, m) in members {
                    if incr {
                        let old = z.score(m).unwrap_or(0.0);
                        let next = old + score;
                        if next.is_nan() {
                            return Err("ERR resulting score is not a number (NaN)");
                        }
                        z.insert(m.clone(), next);
                        added += 1;
                    } else if z.insert(m.clone(), *score) {
                        added += 1;
                    }
                }
                guard
                    .map
                    .insert(key.clone(), Entry::new(RedisValue::ZSet(z)));
                self.touch_dirty();
                Ok(if ch { added } else { added })
            }
            Some(e) => match &mut e.value {
                RedisValue::ZSet(z) => {
                    let mut changed = 0i64;
                    let mut added = 0i64;
                    for (score, m) in members {
                        let exists = z.score(m).is_some();
                        if nx && exists {
                            continue;
                        }
                        if xx && !exists {
                            continue;
                        }
                        if incr {
                            let before = z.score(m).unwrap_or(0.0);
                            let next = before + score;
                            if next.is_nan() {
                                return Err("ERR resulting score is not a number (NaN)");
                            }
                            z.insert(m.clone(), next);
                            if before != next {
                                changed += 1;
                            }
                            if !exists {
                                added += 1;
                            }
                        } else if z.insert(m.clone(), *score) {
                            added += 1;
                            changed += 1;
                        } else if z.score(m) != Some(*score) {
                            z.insert(m.clone(), *score);
                            changed += 1;
                        }
                    }
                    self.touch_dirty();
                    Ok(if ch { changed } else { added })
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn zscore(
        &self,
        db: usize,
        key: &Bytes,
        member: &Bytes,
    ) -> Result<Option<f64>, &'static str> {
        let mut guard = self.dbs_write(db);
        match guard.get_fresh(key) {
            None => Ok(None),
            Some(e) => match &e.value {
                RedisValue::ZSet(z) => Ok(z.score(member)),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn zcard(&self, db: usize, key: &Bytes) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        match guard.get_fresh(key) {
            None => Ok(0),
            Some(e) => match &e.value {
                RedisValue::ZSet(z) => Ok(z.len() as i64),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn zrem(&self, db: usize, key: &Bytes, members: &[Bytes]) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        match guard.get_fresh_mut(key) {
            None => Ok(0),
            Some(e) => match &mut e.value {
                RedisValue::ZSet(z) => {
                    let mut n = 0i64;
                    for m in members {
                        if z.remove(m) {
                            n += 1;
                        }
                    }
                    let empty = z.is_empty();
                    self.touch_dirty();
                    if empty {
                        guard.map.remove(key);
                    }
                    Ok(n)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn getbit(&self, db: usize, key: &Bytes, offset: u64) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        match guard.get_fresh(key) {
            None => Ok(0),
            Some(e) => match &e.value {
                RedisValue::String(s) => {
                    let byte = offset / 8;
                    let bit = 7 - (offset % 8);
                    if byte as usize >= s.len() {
                        Ok(0)
                    } else {
                        Ok(((s[byte as usize] >> bit) & 1) as i64)
                    }
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn setbit(
        &self,
        db: usize,
        key: &Bytes,
        offset: u64,
        value: u8,
    ) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        if guard.purge_expired(key) {}
        let byte_i = (offset / 8) as usize;
        let bit = 7 - (offset % 8);
        match guard.map.get_mut(key) {
            None => {
                let mut buf = vec![0u8; byte_i + 1];
                if value != 0 {
                    buf[byte_i] |= 1 << bit;
                }
                guard.map.insert(
                    key.clone(),
                    Entry::new(RedisValue::String(Bytes::from(buf))),
                );
                self.touch_dirty();
                Ok(0)
            }
            Some(e) => match &mut e.value {
                RedisValue::String(s) => {
                    let mut buf = s.to_vec();
                    if buf.len() <= byte_i {
                        buf.resize(byte_i + 1, 0);
                    }
                    let old = (buf[byte_i] >> bit) & 1;
                    if value != 0 {
                        buf[byte_i] |= 1 << bit;
                    } else {
                        buf[byte_i] &= !(1 << bit);
                    }
                    *s = Bytes::from(buf);
                    self.touch_dirty();
                    Ok(old as i64)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    pub fn bitcount(
        &self,
        db: usize,
        key: &Bytes,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<i64, &'static str> {
        let mut guard = self.dbs_write(db);
        match guard.get_fresh(key) {
            None => Ok(0),
            Some(e) => match &e.value {
                RedisValue::String(s) => {
                    let len = s.len() as i64;
                    let (lo, hi) = match (start, end) {
                        (None, None) => (0, len - 1),
                        (Some(a), Some(b)) => {
                            let a = if a < 0 { len + a } else { a }.clamp(0, len - 1);
                            let b = if b < 0 { len + b } else { b }.clamp(0, len - 1);
                            if a > b {
                                return Ok(0);
                            }
                            (a, b)
                        }
                        _ => return Err("ERR syntax error"),
                    };
                    if len == 0 {
                        return Ok(0);
                    }
                    let slice = &s[lo as usize..=hi as usize];
                    Ok(slice.iter().map(|b| b.count_ones() as i64).sum())
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
        }
    }

    /// Snapshot de toutes les bases (RDB, AOF preamble, réplication).
    pub fn dump_all(&self) -> Vec<(usize, Vec<(Bytes, Entry)>)> {
        self.dbs_iter()
            .enumerate()
            .filter_map(|(idx, db)| {
                let mut guard = db.write();
                guard.map.retain(|_, e| !e.is_expired());
                if guard.map.is_empty() {
                    return None;
                }
                let entries = guard
                    .map
                    .iter()
                    .map(|(k, e)| (k.clone(), e.clone()))
                    .collect();
                Some((idx, entries))
            })
            .collect()
    }

    /// Remplace le contenu des bases par un snapshot (après flush).
    pub fn restore_all(&self, dbs: Vec<(usize, Vec<(Bytes, Entry)>)>) {
        self.flushall();
        for (idx, entries) in dbs {
            if idx >= self.db_count() {
                continue;
            }
            let mut guard = self.dbs_write(idx);
            for (k, e) in entries {
                guard.map.insert(k, e);
            }
        }
        self.touch();
    }

    fn set_members(
        &self,
        guard: &mut crate::store::DbInner,
        key: &Bytes,
    ) -> Result<HashSet<Bytes>, &'static str> {
        match guard.get_fresh(key) {
            None => Ok(HashSet::new()),
            Some(e) => match &e.value {
                RedisValue::Set(s) => Ok(s.as_hashset()),
                _ => Err(WRONGTYPE),
            },
        }
    }

    pub fn sinter(&self, db: usize, keys: &[Bytes]) -> Result<Vec<Bytes>, &'static str> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut guard = self.dbs_write(db);
        let mut acc = self.set_members(&mut guard, &keys[0])?;
        for key in &keys[1..] {
            let s = self.set_members(&mut guard, key)?;
            acc.retain(|m| s.contains(m));
        }
        Ok(acc.into_iter().collect())
    }

    pub fn sunion(&self, db: usize, keys: &[Bytes]) -> Result<Vec<Bytes>, &'static str> {
        let mut guard = self.dbs_write(db);
        let mut acc = HashSet::new();
        for key in keys {
            acc.extend(self.set_members(&mut guard, key)?.into_iter());
        }
        Ok(acc.into_iter().collect())
    }

    pub fn sdiff(&self, db: usize, keys: &[Bytes]) -> Result<Vec<Bytes>, &'static str> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut guard = self.dbs_write(db);
        let mut acc = self.set_members(&mut guard, &keys[0])?;
        for key in &keys[1..] {
            let s = self.set_members(&mut guard, key)?;
            acc.retain(|m| !s.contains(m));
        }
        Ok(acc.into_iter().collect())
    }
}
