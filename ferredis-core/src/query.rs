//! Index secondaires + Ferredis Query (FQ.*) — phase F5.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SecondaryIndex {
    pub name: String,
    pub key_prefix: String,
    pub field: String,
    /// field_value → set of redis keys
    pub map: BTreeMap<String, BTreeSet<Bytes>>,
}

#[derive(Default)]
pub struct QueryStore {
    pub indexes: HashMap<String, SecondaryIndex>,
}

pub type SharedQuery = Arc<Mutex<QueryStore>>;

pub fn shared_query() -> SharedQuery {
    Arc::new(Mutex::new(QueryStore::default()))
}

impl QueryStore {
    pub fn create_index(&mut self, name: String, prefix: String, field: String) -> Result<(), &'static str> {
        if self.indexes.contains_key(&name) {
            return Err("ERR index already exists");
        }
        self.indexes.insert(
            name.clone(),
            SecondaryIndex {
                name,
                key_prefix: prefix,
                field,
                map: BTreeMap::new(),
            },
        );
        Ok(())
    }

    pub fn drop_index(&mut self, name: &str) -> bool {
        self.indexes.remove(name).is_some()
    }

    pub fn list(&self) -> Vec<String> {
        let mut v: Vec<_> = self.indexes.keys().cloned().collect();
        v.sort();
        v
    }

    /// Indexe un hash field si un index matche le préfixe de clé.
    pub fn on_hash_set(&mut self, key: &Bytes, field: &Bytes, value: &Bytes) {
        let key_s = String::from_utf8_lossy(key);
        let field_s = String::from_utf8_lossy(field);
        let val_s = String::from_utf8_lossy(value).into_owned();
        for idx in self.indexes.values_mut() {
            if !key_s.starts_with(&idx.key_prefix) {
                continue;
            }
            if field_s.as_ref() != idx.field {
                continue;
            }
            // Retire l'ancienne entrée pour cette clé.
            for set in idx.map.values_mut() {
                set.remove(key);
            }
            idx.map
                .entry(val_s.clone())
                .or_default()
                .insert(key.clone());
        }
    }

    pub fn lookup(&self, index: &str, value: &str) -> Option<Vec<Bytes>> {
        let idx = self.indexes.get(index)?;
        Some(
            idx.map
                .get(value)
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default(),
        )
    }
}

/// Parse minimal `FQ.SELECT index WHERE field = value [LIMIT n]`.
#[derive(Debug)]
pub struct SelectQuery {
    pub index: String,
    pub value: String,
    pub limit: usize,
}

pub fn parse_select(args: &[Bytes]) -> Result<SelectQuery, &'static str> {
    // FQ.SELECT <index> WHERE <field> = <value> [LIMIT n]
    if args.len() < 6 {
        return Err("ERR wrong number of arguments for 'FQ.SELECT'");
    }
    let index = String::from_utf8_lossy(&args[1]).into_owned();
    if !eq_ignore_ascii(&args[2], b"WHERE") {
        return Err("ERR syntax error: expected WHERE");
    }
    let _field = String::from_utf8_lossy(&args[3]);
    if !eq_ignore_ascii(&args[4], b"=") {
        return Err("ERR syntax error: expected =");
    }
    let value = String::from_utf8_lossy(&args[5]).into_owned();
    let mut limit = 100;
    if args.len() >= 8 && eq_ignore_ascii(&args[6], b"LIMIT") {
        limit = std::str::from_utf8(&args[7])
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
    }
    Ok(SelectQuery {
        index,
        value,
        limit,
    })
}

fn eq_ignore_ascii(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| x.to_ascii_uppercase() == y.to_ascii_uppercase())
}
