//! Moteur RediSearch embarqué : index, schéma, requêtes et agrégations.

use crate::resp::RespValue;
use crate::value::RedisValue;
use bytes::Bytes;
use parking_lot::Mutex;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    Text,
    Tag,
    Numeric,
    Geo,
    Vector,
}

#[derive(Debug, Clone)]
pub struct FieldSchema {
    pub name: String,
    pub field_type: FieldType,
    pub sortable: bool,
    pub weight: f64,
    pub separator: char,
    pub no_index: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexOn {
    Hash,
    Json,
}

#[derive(Debug, Clone)]
pub struct SearchIndex {
    pub name: String,
    pub on: IndexOn,
    pub prefixes: Vec<String>,
    pub schema: Vec<FieldSchema>,
    pub language: String,
    pub stopwords: Vec<String>,
    /// Génération DB au dernier rebuild.
    pub cached_dirty: u64,
    /// Inverted TEXT/TAG : term → doc keys
    pub inverted: HashMap<String, BTreeSet<Bytes>>,
    /// Numeric : field → (value → keys)
    pub numeric: HashMap<String, BTreeMap<i64, BTreeSet<Bytes>>>,
    /// Docs indexés : key → champs extraits
    pub docs: HashMap<Bytes, DocFields>,
    pub num_docs: u64,
    pub num_records: u64,
}

#[derive(Debug, Clone, Default)]
pub struct DocFields {
    pub text: HashMap<String, String>,
    pub tags: HashMap<String, Vec<String>>,
    pub numeric: HashMap<String, f64>,
}

#[derive(Debug, Clone)]
pub struct CursorState {
    pub index: String,
    pub rows: Vec<Vec<(String, String)>>,
    pub offset: usize,
    pub page: usize,
}

pub(crate) struct SearchInner {
    pub indexes: HashMap<String, SearchIndex>,
    pub aliases: HashMap<String, String>,
    pub dicts: HashMap<String, BTreeSet<String>>,
    /// index → group_id → terms
    pub synonyms: HashMap<String, HashMap<String, Vec<String>>>,
    pub cursors: HashMap<u64, CursorState>,
    pub config: HashMap<String, String>,
    pub next_cursor: u64,
}

pub struct SearchStore {
    inner: Mutex<SearchInner>,
}

pub type SharedSearch = Arc<SearchStore>;

pub fn shared_search() -> SharedSearch {
    Arc::new(SearchStore {
        inner: Mutex::new(SearchInner {
            indexes: HashMap::new(),
            aliases: HashMap::new(),
            dicts: HashMap::new(),
            synonyms: HashMap::new(),
            cursors: HashMap::new(),
            config: HashMap::from([
                ("MAXSEARCHRESULTS".into(), "10000".into()),
                ("TIMEOUT".into(), "500".into()),
            ]),
            next_cursor: 1,
        }),
    })
}

impl SearchStore {
    pub fn resolve_name(&self, name: &str) -> String {
        let g = self.inner.lock();
        g.aliases
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(&mut SearchInner) -> R) -> R {
        f(&mut self.inner.lock())
    }

    pub(crate) fn with<R>(&self, f: impl FnOnce(&SearchInner) -> R) -> R {
        f(&self.inner.lock())
    }

    pub fn dump_bytes(&self) -> Vec<u8> {
        let g = self.inner.lock();
        let mut out = Vec::new();
        // format simple : n_indexes, puis pour chaque: name, on, prefixes, schema fields
        write_u32(&mut out, g.indexes.len() as u32);
        for idx in g.indexes.values() {
            write_str(&mut out, &idx.name);
            out.push(match idx.on {
                IndexOn::Hash => 0,
                IndexOn::Json => 1,
            });
            write_u32(&mut out, idx.prefixes.len() as u32);
            for p in &idx.prefixes {
                write_str(&mut out, p);
            }
            write_u32(&mut out, idx.schema.len() as u32);
            for f in &idx.schema {
                write_str(&mut out, &f.name);
                out.push(match f.field_type {
                    FieldType::Text => 0,
                    FieldType::Tag => 1,
                    FieldType::Numeric => 2,
                    FieldType::Geo => 3,
                    FieldType::Vector => 4,
                });
                out.push(u8::from(f.sortable));
                out.extend_from_slice(&f.weight.to_le_bytes());
                out.push(f.separator as u8);
            }
        }
        write_u32(&mut out, g.aliases.len() as u32);
        for (a, t) in &g.aliases {
            write_str(&mut out, a);
            write_str(&mut out, t);
        }
        out
    }

    pub fn restore_bytes(&self, data: &[u8]) -> Result<(), String> {
        let mut i = 0;
        let n = read_u32(data, &mut i)? as usize;
        let mut indexes = HashMap::new();
        for _ in 0..n {
            let name = read_str(data, &mut i)?;
            let on = match data.get(i).copied().ok_or("eof")? {
                0 => IndexOn::Hash,
                _ => IndexOn::Json,
            };
            i += 1;
            let np = read_u32(data, &mut i)? as usize;
            let mut prefixes = Vec::with_capacity(np);
            for _ in 0..np {
                prefixes.push(read_str(data, &mut i)?);
            }
            let nf = read_u32(data, &mut i)? as usize;
            let mut schema = Vec::with_capacity(nf);
            for _ in 0..nf {
                let fname = read_str(data, &mut i)?;
                let ft = match data.get(i).copied().ok_or("eof")? {
                    0 => FieldType::Text,
                    1 => FieldType::Tag,
                    2 => FieldType::Numeric,
                    3 => FieldType::Geo,
                    _ => FieldType::Vector,
                };
                i += 1;
                let sortable = data.get(i).copied().ok_or("eof")? != 0;
                i += 1;
                let weight = f64::from_le_bytes(
                    data.get(i..i + 8)
                        .ok_or("eof")?
                        .try_into()
                        .map_err(|_| "eof")?,
                );
                i += 8;
                let separator = data.get(i).copied().ok_or("eof")? as char;
                i += 1;
                schema.push(FieldSchema {
                    name: fname,
                    field_type: ft,
                    sortable,
                    weight,
                    separator,
                    no_index: false,
                });
            }
            indexes.insert(
                name.clone(),
                SearchIndex {
                    name,
                    on,
                    prefixes,
                    schema,
                    language: "english".into(),
                    stopwords: Vec::new(),
                    cached_dirty: 0,
                    inverted: HashMap::new(),
                    numeric: HashMap::new(),
                    docs: HashMap::new(),
                    num_docs: 0,
                    num_records: 0,
                },
            );
        }
        let na = read_u32(data, &mut i).unwrap_or(0) as usize;
        let mut aliases = HashMap::new();
        for _ in 0..na {
            let a = read_str(data, &mut i)?;
            let t = read_str(data, &mut i)?;
            aliases.insert(a, t);
        }
        let mut g = self.inner.lock();
        g.indexes = indexes;
        g.aliases = aliases;
        Ok(())
    }
}

fn write_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn write_str(out: &mut Vec<u8>, s: &str) {
    write_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}
fn read_u32(data: &[u8], i: &mut usize) -> Result<u32, String> {
    let b = data
        .get(*i..*i + 4)
        .ok_or_else(|| "eof".to_string())?;
    *i += 4;
    Ok(u32::from_le_bytes(b.try_into().unwrap()))
}
fn read_str(data: &[u8], i: &mut usize) -> Result<String, String> {
    let n = read_u32(data, i)? as usize;
    let s = data
        .get(*i..*i + n)
        .ok_or_else(|| "eof".to_string())?;
    *i += n;
    String::from_utf8(s.to_vec()).map_err(|e| e.to_string())
}

pub(crate) fn tokenize(s: &str) -> Vec<String> {
    s.to_ascii_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_string())
        .collect()
}

pub(crate) fn extract_doc(
    on: IndexOn,
    schema: &[FieldSchema],
    value: &RedisValue,
) -> Option<DocFields> {
    let mut doc = DocFields::default();
    match (on, value) {
        (IndexOn::Hash, RedisValue::Hash(h)) => {
            for f in schema {
                let Some(raw) = h.get(f.name.as_bytes()) else {
                    continue;
                };
                let s = String::from_utf8_lossy(raw);
                match f.field_type {
                    FieldType::Text => {
                        doc.text.insert(f.name.clone(), s.into_owned());
                    }
                    FieldType::Tag => {
                        let tags: Vec<String> = s
                            .split(f.separator)
                            .map(|t| t.trim().to_string())
                            .filter(|t| !t.is_empty())
                            .collect();
                        doc.tags.insert(f.name.clone(), tags);
                    }
                    FieldType::Numeric | FieldType::Geo | FieldType::Vector => {
                        if let Ok(n) = s.parse::<f64>() {
                            doc.numeric.insert(f.name.clone(), n);
                        }
                    }
                }
            }
            Some(doc)
        }
        (IndexOn::Json, RedisValue::Json(v)) => {
            for f in schema {
                let path = if f.name.starts_with('$') {
                    f.name.clone()
                } else {
                    format!("$.{}", f.name)
                };
                let Ok(p) = crate::json_path::Path::parse(&path) else {
                    continue;
                };
                let hits = crate::json_path::get_all(v, &p);
                let Some(hit) = hits.first() else {
                    continue;
                };
                match f.field_type {
                    FieldType::Text => {
                        let s = match hit {
                            JsonValue::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        doc.text.insert(f.name.clone(), s);
                    }
                    FieldType::Tag => {
                        let s = match hit {
                            JsonValue::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        let tags: Vec<String> = s
                            .split(f.separator)
                            .map(|t| t.trim().to_string())
                            .filter(|t| !t.is_empty())
                            .collect();
                        doc.tags.insert(f.name.clone(), tags);
                    }
                    FieldType::Numeric | FieldType::Geo | FieldType::Vector => {
                        if let Some(n) = hit.as_f64() {
                            doc.numeric.insert(f.name.clone(), n);
                        } else if let Some(s) = hit.as_str() {
                            if let Ok(n) = s.parse::<f64>() {
                                doc.numeric.insert(f.name.clone(), n);
                            }
                        }
                    }
                }
            }
            Some(doc)
        }
        _ => None,
    }
}

pub(crate) fn rebuild_index(
    idx: &mut SearchIndex,
    keys: impl Iterator<Item = (Bytes, RedisValue)>,
    dirty: u64,
) {
    idx.inverted.clear();
    idx.numeric.clear();
    idx.docs.clear();
    idx.num_docs = 0;
    idx.num_records = 0;
    for (key, value) in keys {
        let key_str = String::from_utf8_lossy(&key);
        if !idx.prefixes.is_empty()
            && !idx
                .prefixes
                .iter()
                .any(|p| p.is_empty() || key_str.starts_with(p))
        {
            continue;
        }
        let Some(doc) = extract_doc(idx.on, &idx.schema, &value) else {
            continue;
        };
        index_doc(idx, key, doc);
    }
    idx.cached_dirty = dirty;
}

fn index_doc(idx: &mut SearchIndex, key: Bytes, doc: DocFields) {
    for (field, text) in &doc.text {
        let weight = idx
            .schema
            .iter()
            .find(|f| f.name == *field)
            .map(|f| f.weight)
            .unwrap_or(1.0);
        let _ = weight;
        for term in tokenize(text) {
            let k = format!("{field}:{term}");
            idx.inverted.entry(k).or_default().insert(key.clone());
            idx.inverted
                .entry(term.clone())
                .or_default()
                .insert(key.clone());
            idx.num_records += 1;
        }
    }
    for (field, tags) in &doc.tags {
        for tag in tags {
            let k = format!("{field}:{}", tag.to_ascii_lowercase());
            idx.inverted.entry(k).or_default().insert(key.clone());
            idx.num_records += 1;
        }
    }
    for (field, val) in &doc.numeric {
        // stocke en micro-unités pour ordonner
        let key_i = (*val * 1000.0).round() as i64;
        idx.numeric
            .entry(field.clone())
            .or_default()
            .entry(key_i)
            .or_default()
            .insert(key.clone());
    }
    idx.docs.insert(key, doc);
    idx.num_docs += 1;
}

#[derive(Debug, Clone)]
pub enum QueryNode {
    All,
    Term(String),
    FieldTerm(String, String),
    Tag(String, Vec<String>),
    Numeric {
        field: String,
        min: f64,
        max: f64,
        min_excl: bool,
        max_excl: bool,
    },
    And(Vec<QueryNode>),
    Or(Vec<QueryNode>),
    Not(Box<QueryNode>),
}

pub fn parse_query(q: &str) -> QueryNode {
    let q = q.trim();
    if q.is_empty() || q == "*" {
        return QueryNode::All;
    }
    // split OR at top level
    let parts: Vec<&str> = split_top(q, '|');
    if parts.len() > 1 {
        return QueryNode::Or(parts.into_iter().map(parse_query).collect());
    }
    let parts: Vec<&str> = split_ws_and(q);
    if parts.len() > 1 {
        return QueryNode::And(parts.into_iter().map(parse_atom).collect());
    }
    parse_atom(q)
}

fn split_top(s: &str, sep: char) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            c if c == sep && depth == 0 => {
                out.push(s[start..i].trim());
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    out.push(s[start..].trim());
    out.into_iter().filter(|p| !p.is_empty()).collect()
}

fn split_ws_and(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    let mut in_braces = false;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            '{' => in_braces = true,
            '}' => in_braces = false,
            ' ' | '\t' if depth == 0 && !in_braces => {
                let part = s[start..i].trim();
                if !part.is_empty() {
                    out.push(part);
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let part = s[start..].trim();
    if !part.is_empty() {
        out.push(part);
    }
    out
}

fn parse_atom(s: &str) -> QueryNode {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix('-') {
        return QueryNode::Not(Box::new(parse_atom(rest)));
    }
    if s.starts_with('(') && s.ends_with(')') {
        return parse_query(&s[1..s.len() - 1]);
    }
    if let Some(rest) = s.strip_prefix('@') {
        if let Some((field, body)) = rest.split_once(':') {
            if let Some(inner) = body.strip_prefix('[').and_then(|x| x.strip_suffix(']')) {
                let mut parts = inner.split_whitespace();
                let min_s = parts.next().unwrap_or("-inf");
                let max_s = parts.next().unwrap_or("+inf");
                let (min, min_excl) = parse_num_bound(min_s, true);
                let (max, max_excl) = parse_num_bound(max_s, false);
                return QueryNode::Numeric {
                    field: field.to_string(),
                    min,
                    max,
                    min_excl,
                    max_excl,
                };
            }
            if let Some(inner) = body.strip_prefix('{').and_then(|x| x.strip_suffix('}')) {
                let tags: Vec<String> = inner
                    .split('|')
                    .map(|t| t.trim().to_ascii_lowercase())
                    .filter(|t| !t.is_empty())
                    .collect();
                return QueryNode::Tag(field.to_string(), tags);
            }
            return QueryNode::FieldTerm(field.to_string(), body.to_ascii_lowercase());
        }
    }
    QueryNode::Term(s.to_ascii_lowercase())
}

fn parse_num_bound(s: &str, is_min: bool) -> (f64, bool) {
    let excl = s.starts_with('(');
    let body = if excl { &s[1..] } else { s };
    let v = if body.eq_ignore_ascii_case("-inf") {
        f64::NEG_INFINITY
    } else if body.eq_ignore_ascii_case("+inf") || body.eq_ignore_ascii_case("inf") {
        f64::INFINITY
    } else {
        body.parse().unwrap_or(if is_min {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        })
    };
    (v, excl)
}

pub fn eval_query(idx: &SearchIndex, q: &QueryNode) -> BTreeSet<Bytes> {
    match q {
        QueryNode::All => idx.docs.keys().cloned().collect(),
        QueryNode::Term(t) => idx.inverted.get(t).cloned().unwrap_or_default(),
        QueryNode::FieldTerm(f, t) => {
            let k = format!("{f}:{t}");
            idx.inverted.get(&k).cloned().unwrap_or_default()
        }
        QueryNode::Tag(f, tags) => {
            let mut out = BTreeSet::new();
            for tag in tags {
                let k = format!("{f}:{tag}");
                if let Some(set) = idx.inverted.get(&k) {
                    out.extend(set.iter().cloned());
                }
            }
            out
        }
        QueryNode::Numeric {
            field,
            min,
            max,
            min_excl,
            max_excl,
        } => {
            let mut out = BTreeSet::new();
            if let Some(tree) = idx.numeric.get(field) {
                let lo = (*min * 1000.0).floor() as i64;
                let hi = (*max * 1000.0).ceil() as i64;
                for (&k, set) in tree.range(lo..=hi) {
                    let v = k as f64 / 1000.0;
                    if (*min_excl && v <= *min) || (!min_excl && v < *min) {
                        continue;
                    }
                    if (*max_excl && v >= *max) || (!max_excl && v > *max) {
                        continue;
                    }
                    out.extend(set.iter().cloned());
                }
            }
            out
        }
        QueryNode::And(nodes) => {
            let mut iter = nodes.iter().map(|n| eval_query(idx, n));
            let Some(mut acc) = iter.next() else {
                return BTreeSet::new();
            };
            for s in iter {
                acc = acc.intersection(&s).cloned().collect();
            }
            acc
        }
        QueryNode::Or(nodes) => {
            let mut acc = BTreeSet::new();
            for n in nodes {
                acc.extend(eval_query(idx, n));
            }
            acc
        }
        QueryNode::Not(n) => {
            let all: BTreeSet<_> = idx.docs.keys().cloned().collect();
            let sub = eval_query(idx, n);
            all.difference(&sub).cloned().collect()
        }
    }
}

pub fn info_array(idx: &SearchIndex) -> RespValue {
    let mut schema_arr = Vec::new();
    for f in &idx.schema {
        schema_arr.push(RespValue::bulk(Bytes::from(f.name.clone())));
        schema_arr.push(RespValue::bulk(Bytes::from(
            match f.field_type {
                FieldType::Text => "TEXT",
                FieldType::Tag => "TAG",
                FieldType::Numeric => "NUMERIC",
                FieldType::Geo => "GEO",
                FieldType::Vector => "VECTOR",
            }
            .to_string(),
        )));
    }
    RespValue::array(vec![
        RespValue::bulk(Bytes::from_static(b"index_name")),
        RespValue::bulk(Bytes::from(idx.name.clone())),
        RespValue::bulk(Bytes::from_static(b"index_options")),
        RespValue::array(vec![]),
        RespValue::bulk(Bytes::from_static(b"index_definition")),
        RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(b"key_type")),
            RespValue::bulk(Bytes::from(match idx.on {
                IndexOn::Hash => "HASH",
                IndexOn::Json => "JSON",
            })),
            RespValue::bulk(Bytes::from_static(b"prefixes")),
            RespValue::array(
                idx.prefixes
                    .iter()
                    .map(|p| RespValue::bulk(Bytes::from(p.clone())))
                    .collect(),
            ),
        ]),
        RespValue::bulk(Bytes::from_static(b"attributes")),
        RespValue::array(schema_arr),
        RespValue::bulk(Bytes::from_static(b"num_docs")),
        RespValue::Integer(idx.num_docs as i64),
        RespValue::bulk(Bytes::from_static(b"num_records")),
        RespValue::Integer(idx.num_records as i64),
        RespValue::bulk(Bytes::from_static(b"num_terms")),
        RespValue::Integer(idx.inverted.len() as i64),
    ])
}

static _DIRTY_HINT: AtomicU64 = AtomicU64::new(0);
pub fn bump_hint() {
    _DIRTY_HINT.fetch_add(1, Ordering::Relaxed);
}
