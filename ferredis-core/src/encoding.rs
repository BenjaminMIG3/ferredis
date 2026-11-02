//! Encodings Redis compacts réels (intset, listpack thresholds, compression).

use bytes::Bytes;
use std::collections::{HashMap, HashSet};
/// Seuil Redis-like avant conversion intset → hashtable.
pub const INTSET_MAX_ENTRIES: usize = 512;
/// Seuil champs hash listpack.
pub const HASH_LISTPACK_MAX_ENTRIES: usize = 64;
/// Seuil octets par entrée listpack hash.
pub const HASH_LISTPACK_MAX_VALUE: usize = 64;

/// Ensemble d'entiers triés unique (encoding `intset`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IntSet {
    vals: Vec<i64>,
}

impl IntSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.vals.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vals.is_empty()
    }

    pub fn contains_i64(&self, v: i64) -> bool {
        self.vals.binary_search(&v).is_ok()
    }

    pub fn insert_i64(&mut self, v: i64) -> bool {
        match self.vals.binary_search(&v) {
            Ok(_) => false,
            Err(i) => {
                self.vals.insert(i, v);
                true
            }
        }
    }

    pub fn remove_i64(&mut self, v: i64) -> bool {
        if let Ok(i) = self.vals.binary_search(&v) {
            self.vals.remove(i);
            true
        } else {
            false
        }
    }

    pub fn iter_i64(&self) -> impl Iterator<Item = i64> + '_ {
        self.vals.iter().copied()
    }

    pub fn to_hashset(&self) -> HashSet<Bytes> {
        self.vals
            .iter()
            .map(|v| Bytes::from(v.to_string()))
            .collect()
    }

    pub fn approx_bytes(&self) -> usize {
        self.vals.len() * 8 + 16
    }

    pub fn try_parse_member(m: &[u8]) -> Option<i64> {
        let s = std::str::from_utf8(m).ok()?;
        if s.len() > 1 && (s.starts_with('0') || s.starts_with("-0")) {
            return None;
        }
        s.parse().ok()
    }
}

/// Set Redis : stockage intset compact ou hashtable, API compatible `HashSet`.
#[derive(Debug, Clone, PartialEq)]
pub enum SetObject {
    IntSet(IntSet),
    Hash(HashSet<Bytes>),
}

impl Default for SetObject {
    fn default() -> Self {
        Self::IntSet(IntSet::new())
    }
}

impl SetObject {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_hashset(s: HashSet<Bytes>) -> Self {
        let mut is = IntSet::new();
        for m in &s {
            match IntSet::try_parse_member(m) {
                Some(v) if is.len() < INTSET_MAX_ENTRIES => {
                    is.insert_i64(v);
                }
                _ => return Self::Hash(s),
            }
        }
        if is.len() == s.len() {
            Self::IntSet(is)
        } else {
            Self::Hash(s)
        }
    }

    pub fn encoding(&self) -> &'static str {
        match self {
            Self::IntSet(_) => "intset",
            Self::Hash(_) => "hashtable",
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::IntSet(i) => i.len(),
            Self::Hash(h) => h.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, m: &[u8]) -> bool {
        match self {
            Self::IntSet(i) => IntSet::try_parse_member(m)
                .map(|v| i.contains_i64(v))
                .unwrap_or(false),
            Self::Hash(h) => h.contains(m),
        }
    }

    pub fn insert(&mut self, m: Bytes) -> bool {
        match self {
            Self::IntSet(is) => match IntSet::try_parse_member(&m) {
                Some(v) if is.len() < INTSET_MAX_ENTRIES || is.contains_i64(v) => is.insert_i64(v),
                _ => {
                    let mut h = is.to_hashset();
                    let added = h.insert(m);
                    *self = Self::Hash(h);
                    added
                }
            },
            Self::Hash(h) => h.insert(m),
        }
    }

    pub fn remove(&mut self, m: &[u8]) -> bool {
        match self {
            Self::IntSet(is) => IntSet::try_parse_member(m)
                .map(|v| is.remove_i64(v))
                .unwrap_or(false),
            Self::Hash(h) => h.remove(m),
        }
    }

    pub fn iter_members(&self) -> Vec<Bytes> {
        match self {
            Self::IntSet(is) => is.iter_i64().map(|v| Bytes::from(v.to_string())).collect(),
            Self::Hash(h) => h.iter().cloned().collect(),
        }
    }

    pub fn as_hashset(&self) -> HashSet<Bytes> {
        match self {
            Self::IntSet(is) => is.to_hashset(),
            Self::Hash(h) => h.clone(),
        }
    }

    /// Matérialise en hashtable mutable (pour APIs qui exigent `&mut HashSet`).
    pub fn ensure_hash(&mut self) -> &mut HashSet<Bytes> {
        if matches!(self, Self::IntSet(_)) {
            let h = self.as_hashset();
            *self = Self::Hash(h);
        }
        match self {
            Self::Hash(h) => h,
            Self::IntSet(_) => unreachable!(),
        }
    }

    pub fn approx_bytes(&self) -> usize {
        match self {
            Self::IntSet(i) => i.approx_bytes(),
            Self::Hash(h) => h.iter().map(|b| b.len() + 24).sum(),
        }
    }
}

impl From<HashSet<Bytes>> for SetObject {
    fn from(s: HashSet<Bytes>) -> Self {
        Self::from_hashset(s)
    }
}

impl FromIterator<Bytes> for SetObject {
    fn from_iter<T: IntoIterator<Item = Bytes>>(iter: T) -> Self {
        let mut s = SetObject::new();
        for m in iter {
            s.insert(m);
        }
        s
    }
}

/// Hash compact listpack-like (seuils + accounting).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ListPackHash {
    pairs: Vec<(Bytes, Bytes)>,
}

impl ListPackHash {
    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    pub fn get(&self, field: &[u8]) -> Option<&Bytes> {
        self.pairs
            .iter()
            .find(|(k, _)| k.as_ref() == field)
            .map(|(_, v)| v)
    }

    pub fn insert(&mut self, field: Bytes, value: Bytes) -> bool {
        if let Some((_, v)) = self.pairs.iter_mut().find(|(k, _)| k == &field) {
            *v = value;
            false
        } else {
            self.pairs.push((field, value));
            true
        }
    }

    pub fn remove(&mut self, field: &[u8]) -> Option<Bytes> {
        if let Some(i) = self.pairs.iter().position(|(k, _)| k.as_ref() == field) {
            Some(self.pairs.swap_remove(i).1)
        } else {
            None
        }
    }

    pub fn to_map(&self) -> HashMap<Bytes, Bytes> {
        self.pairs.iter().cloned().collect()
    }

    pub fn approx_bytes(&self) -> usize {
        self.pairs
            .iter()
            .map(|(k, v)| k.len() + v.len() + 8)
            .sum::<usize>()
            + 16
    }

    pub fn fits_entry(field: &[u8], value: &[u8]) -> bool {
        field.len() <= HASH_LISTPACK_MAX_VALUE && value.len() <= HASH_LISTPACK_MAX_VALUE
    }

    pub fn can_stay(&self) -> bool {
        self.pairs.len() <= HASH_LISTPACK_MAX_ENTRIES
            && self.pairs.iter().all(|(k, v)| Self::fits_entry(k, v))
    }
}
