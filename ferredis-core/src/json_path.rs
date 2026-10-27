//! Sous-ensemble JSONPath RedisJSON : `$`, `.key`, `["key"]`, `[n]`, `[*]`, `.*`, `$..`.

use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum Segment {
    Key(String),
    Index(i64),
    Wildcard,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    pub recursive: bool,
    pub segs: Vec<Segment>,
}

impl Path {
    pub fn parse(raw: &str) -> Result<Self, &'static str> {
        let s = raw.trim();
        if s.is_empty() {
            return Err("ERR wrong path syntax");
        }
        let (recursive, body) = if s == "$" || s == "." {
            return Ok(Self {
                recursive: false,
                segs: Vec::new(),
            });
        } else if let Some(rest) = s.strip_prefix("$..") {
            (true, rest)
        } else if let Some(rest) = s.strip_prefix("$") {
            (false, rest)
        } else if s.starts_with('.') || s.starts_with('[') {
            (false, s)
        } else {
            // chemin relatif style RedisJSON legacy `foo.bar`
            return Ok(Self {
                recursive: false,
                segs: parse_segments(s)?,
            });
        };
        Ok(Self {
            recursive,
            segs: if body.is_empty() {
                Vec::new()
            } else {
                parse_segments(body)?
            },
        })
    }

    pub fn is_root(&self) -> bool {
        !self.recursive && self.segs.is_empty()
    }
}

fn parse_segments(mut s: &str) -> Result<Vec<Segment>, &'static str> {
    let mut segs = Vec::new();
    while !s.is_empty() {
        if s.starts_with('.') {
            s = &s[1..];
            if s.starts_with('*') {
                segs.push(Segment::Wildcard);
                s = &s[1..];
                continue;
            }
            let end = s
                .find(['.', '['])
                .unwrap_or(s.len());
            if end == 0 {
                return Err("ERR wrong path syntax");
            }
            segs.push(Segment::Key(s[..end].to_string()));
            s = &s[end..];
        } else if s.starts_with('[') {
            let close = s.find(']').ok_or("ERR wrong path syntax")?;
            let inner = &s[1..close];
            s = &s[close + 1..];
            if inner == "*" {
                segs.push(Segment::Wildcard);
            } else if let Some(q) = inner.strip_prefix('\'').and_then(|x| x.strip_suffix('\''))
                .or_else(|| inner.strip_prefix('"').and_then(|x| x.strip_suffix('"')))
            {
                segs.push(Segment::Key(q.to_string()));
            } else {
                let idx: i64 = inner.parse().map_err(|_| "ERR wrong path syntax")?;
                segs.push(Segment::Index(idx));
            }
        } else {
            // premier segment sans point
            let end = s.find(['.', '[']).unwrap_or(s.len());
            segs.push(Segment::Key(s[..end].to_string()));
            s = &s[end..];
        }
    }
    Ok(segs)
}

/// Pointeur mutable vers un nœud JSON (pour SET/DEL).
pub enum NodeMut<'a> {
    Value(&'a mut Value),
}

/// Résout toutes les cibles d'un chemin (lecture).
pub fn get_all<'a>(root: &'a Value, path: &Path) -> Vec<&'a Value> {
    if path.is_root() {
        return vec![root];
    }
    let mut cur: Vec<&Value> = vec![root];
    if path.recursive && path.segs.is_empty() {
        let mut out = Vec::new();
        collect_all(root, &mut out);
        return out;
    }
    if path.recursive {
        let mut seeds = Vec::new();
        collect_all(root, &mut seeds);
        cur = seeds;
    }
    for seg in &path.segs {
        let mut next = Vec::new();
        for v in cur {
            match seg {
                Segment::Key(k) => {
                    if let Value::Object(m) = v {
                        if let Some(c) = m.get(k) {
                            next.push(c);
                        }
                    }
                }
                Segment::Index(i) => {
                    if let Value::Array(a) = v {
                        if let Some(idx) = norm_index(*i, a.len()) {
                            next.push(&a[idx]);
                        }
                    }
                }
                Segment::Wildcard => match v {
                    Value::Object(m) => next.extend(m.values()),
                    Value::Array(a) => next.extend(a.iter()),
                    _ => {}
                },
            }
        }
        cur = next;
        if cur.is_empty() {
            break;
        }
    }
    cur
}

fn collect_all<'a>(v: &'a Value, out: &mut Vec<&'a Value>) {
    out.push(v);
    match v {
        Value::Object(m) => {
            for c in m.values() {
                collect_all(c, out);
            }
        }
        Value::Array(a) => {
            for c in a {
                collect_all(c, out);
            }
        }
        _ => {}
    }
}

fn norm_index(i: i64, len: usize) -> Option<usize> {
    if len == 0 {
        return None;
    }
    if i >= 0 {
        let u = i as usize;
        if u < len {
            Some(u)
        } else {
            None
        }
    } else {
        let u = (-i) as usize;
        if u <= len {
            Some(len - u)
        } else {
            None
        }
    }
}

/// Applique une mutation sur le premier match (chemins non-wildcard).
pub fn with_mut<R>(
    root: &mut Value,
    path: &Path,
    f: impl FnOnce(&mut Value) -> R,
) -> Result<R, &'static str> {
    if path.recursive {
        return Err("ERR wrong path syntax");
    }
    if path.is_root() {
        return Ok(f(root));
    }
    let mut cur = root;
    let last = path.segs.len() - 1;
    for (i, seg) in path.segs.iter().enumerate() {
        let is_last = i == last;
        match seg {
            Segment::Key(k) => {
                let obj = cur
                    .as_object_mut()
                    .ok_or("ERR path does not exist or does not lead to object")?;
                if is_last {
                    let entry = obj.entry(k.clone()).or_insert(Value::Null);
                    return Ok(f(entry));
                }
                if !obj.contains_key(k) {
                    return Err("ERR path does not exist");
                }
                cur = obj.get_mut(k).unwrap();
            }
            Segment::Index(idx) => {
                let arr = cur
                    .as_array_mut()
                    .ok_or("ERR path does not exist or does not lead to array")?;
                let i = norm_index(*idx, arr.len()).ok_or("ERR index out of bounds")?;
                if is_last {
                    return Ok(f(&mut arr[i]));
                }
                cur = &mut arr[i];
            }
            Segment::Wildcard => return Err("ERR wrong path syntax"),
        }
    }
    Err("ERR path does not exist")
}

/// Supprime les nœuds matchés ; retourne le nombre de suppressions.
pub fn delete_path(root: &mut Value, path: &Path) -> usize {
    if path.is_root() {
        *root = Value::Null;
        return 1;
    }
    if path.recursive || path.segs.is_empty() {
        return 0;
    }
    delete_at(root, &path.segs)
}

fn delete_at(cur: &mut Value, segs: &[Segment]) -> usize {
    if segs.is_empty() {
        return 0;
    }
    let (head, tail) = segs.split_first().unwrap();
    if tail.is_empty() {
        match head {
            Segment::Key(k) => {
                if let Value::Object(m) = cur {
                    return if m.remove(k).is_some() { 1 } else { 0 };
                }
            }
            Segment::Index(i) => {
                if let Value::Array(a) = cur {
                    if let Some(idx) = norm_index(*i, a.len()) {
                        a.remove(idx);
                        return 1;
                    }
                }
            }
            Segment::Wildcard => {
                return match cur {
                    Value::Object(m) => {
                        let n = m.len();
                        m.clear();
                        n
                    }
                    Value::Array(a) => {
                        let n = a.len();
                        a.clear();
                        n
                    }
                    _ => 0,
                };
            }
        }
        return 0;
    }
    match head {
        Segment::Key(k) => {
            if let Value::Object(m) = cur {
                if let Some(c) = m.get_mut(k) {
                    return delete_at(c, tail);
                }
            }
        }
        Segment::Index(i) => {
            if let Value::Array(a) = cur {
                if let Some(idx) = norm_index(*i, a.len()) {
                    return delete_at(&mut a[idx], tail);
                }
            }
        }
        Segment::Wildcard => {
            let mut n = 0;
            match cur {
                Value::Object(m) => {
                    for c in m.values_mut() {
                        n += delete_at(c, tail);
                    }
                }
                Value::Array(a) => {
                    for c in a.iter_mut() {
                        n += delete_at(c, tail);
                    }
                }
                _ => {}
            }
            return n;
        }
    }
    0
}

/// Crée les objets/tableaux parents manquants puis pose `val` (JSON.SET).
pub fn set_path(root: &mut Value, path: &Path, val: Value, nx: bool, xx: bool) -> Result<bool, &'static str> {
    if path.recursive {
        return Err("ERR wrong path syntax");
    }
    if path.is_root() {
        if nx && !root.is_null() && *root != Value::Null {
            // clé existante au root : pour RedisJSON, NX sur $ refuse si doc existe — géré à l'extérieur
        }
        if xx && matches!(root, Value::Null) {
            return Ok(false);
        }
        *root = val;
        return Ok(true);
    }
    ensure_and_set(root, &path.segs, val, nx, xx)
}

fn ensure_and_set(
    cur: &mut Value,
    segs: &[Segment],
    val: Value,
    nx: bool,
    xx: bool,
) -> Result<bool, &'static str> {
    let (head, tail) = segs.split_first().ok_or("ERR wrong path syntax")?;
    if tail.is_empty() {
        match head {
            Segment::Key(k) => {
                let obj = match cur {
                    Value::Object(m) => m,
                    Value::Null => {
                        *cur = Value::Object(Map::new());
                        cur.as_object_mut().unwrap()
                    }
                    _ => return Err("ERR path already exists as a different type"),
                };
                let exists = obj.contains_key(k);
                if nx && exists {
                    return Ok(false);
                }
                if xx && !exists {
                    return Ok(false);
                }
                obj.insert(k.clone(), val);
                Ok(true)
            }
            Segment::Index(i) => {
                let arr = match cur {
                    Value::Array(a) => a,
                    Value::Null => {
                        *cur = Value::Array(Vec::new());
                        cur.as_array_mut().unwrap()
                    }
                    _ => return Err("ERR path already exists as a different type"),
                };
                let idx = if *i < 0 {
                    return Err("ERR index out of bounds");
                } else {
                    *i as usize
                };
                while arr.len() < idx {
                    arr.push(Value::Null);
                }
                let exists = idx < arr.len();
                if nx && exists {
                    return Ok(false);
                }
                if xx && !exists {
                    return Ok(false);
                }
                if idx == arr.len() {
                    arr.push(val);
                } else {
                    arr[idx] = val;
                }
                Ok(true)
            }
            Segment::Wildcard => Err("ERR wrong path syntax"),
        }
    } else {
        match head {
            Segment::Key(k) => {
                if matches!(cur, Value::Null) {
                    *cur = Value::Object(Map::new());
                }
                let obj = cur
                    .as_object_mut()
                    .ok_or("ERR path already exists as a different type")?;
                if !obj.contains_key(k) {
                    let next_is_index = matches!(tail.first(), Some(Segment::Index(_)));
                    obj.insert(
                        k.clone(),
                        if next_is_index {
                            Value::Array(Vec::new())
                        } else {
                            Value::Object(Map::new())
                        },
                    );
                }
                ensure_and_set(obj.get_mut(k).unwrap(), tail, val, nx, xx)
            }
            Segment::Index(i) => {
                if matches!(cur, Value::Null) {
                    *cur = Value::Array(Vec::new());
                }
                let arr = cur
                    .as_array_mut()
                    .ok_or("ERR path already exists as a different type")?;
                let idx = norm_index(*i, arr.len().max((*i as usize).saturating_add(1)))
                    .or_else(|| {
                        if *i >= 0 {
                            let u = *i as usize;
                            while arr.len() <= u {
                                arr.push(Value::Null);
                            }
                            Some(u)
                        } else {
                            None
                        }
                    })
                    .ok_or("ERR index out of bounds")?;
                if matches!(arr[idx], Value::Null) {
                    let next_is_index = matches!(tail.first(), Some(Segment::Index(_)));
                    arr[idx] = if next_is_index {
                        Value::Array(Vec::new())
                    } else {
                        Value::Object(Map::new())
                    };
                }
                ensure_and_set(&mut arr[idx], tail, val, nx, xx)
            }
            Segment::Wildcard => Err("ERR wrong path syntax"),
        }
    }
}

pub fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

pub fn to_resp_json(v: &Value) -> crate::resp::RespValue {
    use crate::resp::RespValue;
    use bytes::Bytes;
    match v {
        Value::Null => RespValue::null_bulk(),
        Value::Bool(b) => RespValue::bulk(if *b { Bytes::from_static(b"true") } else { Bytes::from_static(b"false") }),
        Value::Number(n) => RespValue::bulk(Bytes::from(n.to_string())),
        Value::String(s) => RespValue::bulk(Bytes::from(s.clone())),
        Value::Array(a) => {
            RespValue::array(a.iter().map(to_resp_json).collect())
        }
        Value::Object(m) => {
            let mut items = Vec::with_capacity(m.len() * 2);
            for (k, v) in m {
                items.push(RespValue::bulk(Bytes::from(k.clone())));
                items.push(to_resp_json(v));
            }
            RespValue::array(items)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn get_nested() {
        let v = json!({"a":{"b":[1,2,3]}});
        let p = Path::parse("$.a.b[1]").unwrap();
        let got = get_all(&v, &p);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], &json!(2));
    }

    #[test]
    fn set_creates_parents() {
        let mut v = Value::Null;
        let p = Path::parse("$.x.y").unwrap();
        set_path(&mut v, &p, json!(42), false, false).unwrap();
        assert_eq!(v, json!({"x":{"y":42}}));
    }
}
