//! Commandes RediSearch embarquées (`FT.*`).

use crate::command::{arg_str, wrong_arity, ClientState, CommandContext};
use crate::resp::RespValue;
use crate::search::{
    self, eval_query, extract_doc, info_array, parse_query, rebuild_index, CursorState, FieldSchema,
    FieldType, IndexOn, QueryNode, SearchIndex,
};
use crate::util::*;
use crate::value::RedisValue;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashMap};

fn after_meta(ctx: &CommandContext, args: &[Bytes]) {
    ctx.db.touch();
    ctx.aof.append_command(args);
}

fn collect_keys_for_index(
    ctx: &CommandContext,
    db: usize,
    idx: &SearchIndex,
) -> Vec<(Bytes, RedisValue)> {
    ctx.db.with_db(db, |d| {
        d.map
            .iter()
            .filter(|(_, e)| !e.is_expired())
            .filter(|(k, _)| {
                let ks = String::from_utf8_lossy(k);
                idx.prefixes.is_empty()
                    || idx
                        .prefixes
                        .iter()
                        .any(|p| p.is_empty() || ks.starts_with(p))
            })
            .filter(|(_, e)| match (idx.on, &e.value) {
                (IndexOn::Hash, RedisValue::Hash(_)) => true,
                (IndexOn::Json, RedisValue::Json(_)) => true,
                _ => false,
            })
            .map(|(k, e)| (k.clone(), e.value.clone()))
            .collect()
    })
}

fn ensure_fresh(ctx: &CommandContext, state: &ClientState, name: &str) -> Result<(), String> {
    let dirty = ctx.db.dirty.load(std::sync::atomic::Ordering::Relaxed);
    let resolved = ctx.search.resolve_name(name);
    ctx.search.with_mut(|g| {
        let idx = g
            .indexes
            .get_mut(&resolved)
            .ok_or_else(|| format!("Unknown index name '{name}'"))?;
        if idx.cached_dirty != dirty {
            let keys = collect_keys_for_index(ctx, state.db_index, idx);
            rebuild_index(idx, keys.into_iter(), dirty);
        }
        Ok(())
    })
}

fn parse_schema(args: &[Bytes], mut i: usize) -> Result<(usize, Vec<FieldSchema>), &'static str> {
    if i >= args.len() || !upper(&args[i]).eq("SCHEMA") {
        return Err(ERR_SYNTAX);
    }
    i += 1;
    let mut schema = Vec::new();
    while i < args.len() {
        let name = String::from_utf8_lossy(&args[i]).into_owned();
        i += 1;
        if i >= args.len() {
            return Err(ERR_SYNTAX);
        }
        let ty = match upper(&args[i]).as_str() {
            "TEXT" => FieldType::Text,
            "TAG" => FieldType::Tag,
            "NUMERIC" => FieldType::Numeric,
            "GEO" => FieldType::Geo,
            "VECTOR" => FieldType::Vector,
            _ => return Err(ERR_SYNTAX),
        };
        i += 1;
        let mut sortable = false;
        let mut weight = 1.0;
        let mut separator = ',';
        let mut no_index = false;
        while i < args.len() {
            match upper(&args[i]).as_str() {
                "SORTABLE" => {
                    sortable = true;
                    i += 1;
                }
                "NOINDEX" => {
                    no_index = true;
                    i += 1;
                }
                "WEIGHT" => {
                    i += 1;
                    weight = parse_f64(args.get(i).ok_or(ERR_SYNTAX)?).ok_or(ERR_FLOAT)?;
                    i += 1;
                }
                "SEPARATOR" => {
                    i += 1;
                    let s = arg_str(args, i).ok_or(ERR_SYNTAX)?;
                    separator = s.chars().next().unwrap_or(',');
                    i += 1;
                }
                "AS" => {
                    // AS alias — ignore alias name for indexing key but skip
                    i += 2;
                }
                // next field name (not a type keyword) → break
                "TEXT" | "TAG" | "NUMERIC" | "GEO" | "VECTOR" => break,
                other if matches!(
                    other,
                    "SCHEMA" | "PREFIX" | "ON" | "LANGUAGE" | "STOPWORDS" | "FILTER"
                ) =>
                {
                    break;
                }
                _ => {
                    // could be next field name
                    break;
                }
            }
        }
        schema.push(FieldSchema {
            name,
            field_type: ty,
            sortable,
            weight,
            separator,
            no_index,
        });
    }
    if schema.is_empty() {
        return Err(ERR_SYNTAX);
    }
    Ok((i, schema))
}

/// FT.CREATE index [ON HASH|JSON] [PREFIX count prefix…] SCHEMA …
pub(crate) fn cmd_ft_create(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("ft.create");
    }
    let name = arg_str(args, 1).unwrap_or("").to_string();
    let mut on = IndexOn::Hash;
    let mut prefixes = vec!["".to_string()];
    let mut language = "english".to_string();
    let mut i = 2;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "ON" => {
                i += 1;
                match upper(args.get(i).map(|b| b.as_ref()).unwrap_or(b"")).as_str() {
                    "HASH" => on = IndexOn::Hash,
                    "JSON" => on = IndexOn::Json,
                    _ => return err(ERR_SYNTAX),
                }
                i += 1;
            }
            "PREFIX" => {
                i += 1;
                let n = match parse_i64(args.get(i).ok_or(ERR_SYNTAX).unwrap_or(&Bytes::from_static(b"0"))) {
                    Some(v) => v as usize,
                    None => return err(ERR_INT),
                };
                i += 1;
                prefixes.clear();
                for _ in 0..n {
                    prefixes.push(String::from_utf8_lossy(args.get(i).map(|b| b.as_ref()).unwrap_or(b"")).into_owned());
                    i += 1;
                }
            }
            "LANGUAGE" => {
                i += 1;
                language = arg_str(args, i).unwrap_or("english").to_string();
                i += 1;
            }
            "SCHEMA" => break,
            "STOPWORDS" | "FILTER" | "SCORE" | "PAYLOAD_FIELD" | "TEMPORARY" | "NOOFFSETS"
            | "NOHL" | "NOFIELDS" | "NOFREQS" | "SKIPINITIALSCAN" | "MAXTEXTFIELDS" => {
                // options reconnues : skip value(s) best-effort
                i += 1;
                if upper(&args[i.saturating_sub(1)]).eq("STOPWORDS") {
                    if let Some(n) = args.get(i).and_then(|b| parse_i64(b)) {
                        i += 1 + n as usize;
                    }
                } else {
                    i += 1;
                }
            }
            _ => return err(ERR_SYNTAX),
        }
    }
    let (_, schema) = match parse_schema(args, i) {
        Ok(v) => v,
        Err(e) => return err(e),
    };

    let created = ctx.search.with_mut(|g| {
        if g.indexes.contains_key(&name) {
            return Err("Index already exists");
        }
        let mut idx = SearchIndex {
            name: name.clone(),
            on,
            prefixes,
            schema,
            language,
            stopwords: Vec::new(),
            cached_dirty: 0,
            inverted: HashMap::new(),
            numeric: HashMap::new(),
            docs: HashMap::new(),
            num_docs: 0,
            num_records: 0,
        };
        let dirty = ctx.db.dirty.load(std::sync::atomic::Ordering::Relaxed);
        let keys = collect_keys_for_index(ctx, state.db_index, &idx);
        rebuild_index(&mut idx, keys.into_iter(), dirty);
        g.indexes.insert(name.clone(), idx);
        Ok(())
    });
    match created {
        Err(e) => err(e),
        Ok(()) => {
            after_meta(ctx, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_ft_dropindex(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("ft.dropindex");
    }
    let name = arg_str(args, 1).unwrap_or("").to_string();
    let r = ctx.search.with_mut(|g| {
        if g.indexes.remove(&name).is_none() {
            return Err(format!("Unknown Index name"));
        }
        g.aliases.retain(|_, v| v != &name);
        Ok(())
    });
    match r {
        Err(e) => err(&e),
        Ok(()) => {
            after_meta(ctx, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_ft_info(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("ft.info");
    }
    let name = arg_str(args, 1).unwrap_or("");
    if let Err(e) = ensure_fresh(ctx, state, name) {
        return err(&e);
    }
    let resolved = ctx.search.resolve_name(name);
    ctx.search.with(|g| match g.indexes.get(&resolved) {
        Some(idx) => info_array(idx),
        None => err("Unknown Index name"),
    })
}

pub(crate) fn cmd_ft_list(ctx: &CommandContext, _state: &mut ClientState, _args: &[Bytes]) -> RespValue {
    ctx.search.with(|g| {
        let mut names: Vec<_> = g.indexes.keys().cloned().collect();
        names.sort();
        RespValue::array(
            names
                .into_iter()
                .map(|n| RespValue::bulk(Bytes::from(n)))
                .collect(),
        )
    })
}

struct SearchOpts {
    query: String,
    limit_off: usize,
    limit_num: usize,
    return_fields: Option<Vec<String>>,
    sortby: Option<(String, bool)>,
    withscores: bool,
    nocontent: bool,
}

fn parse_search_opts(args: &[Bytes], q_i: usize) -> Result<SearchOpts, &'static str> {
    let query = arg_str(args, q_i).ok_or(ERR_SYNTAX)?.to_string();
    let mut o = SearchOpts {
        query,
        limit_off: 0,
        limit_num: 10,
        return_fields: None,
        sortby: None,
        withscores: false,
        nocontent: false,
    };
    let mut i = q_i + 1;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "LIMIT" => {
                i += 1;
                o.limit_off = parse_i64(args.get(i).ok_or(ERR_SYNTAX)?).ok_or(ERR_INT)? as usize;
                i += 1;
                o.limit_num = parse_i64(args.get(i).ok_or(ERR_SYNTAX)?).ok_or(ERR_INT)? as usize;
            }
            "RETURN" => {
                i += 1;
                let n = parse_i64(args.get(i).ok_or(ERR_SYNTAX)?).ok_or(ERR_INT)? as usize;
                i += 1;
                let mut fields = Vec::with_capacity(n);
                for _ in 0..n {
                    fields.push(arg_str(args, i).ok_or(ERR_SYNTAX)?.to_string());
                    i += 1;
                }
                o.return_fields = Some(fields);
                continue;
            }
            "SORTBY" => {
                i += 1;
                let field = arg_str(args, i).ok_or(ERR_SYNTAX)?.to_string();
                i += 1;
                let desc = if i < args.len() && upper(&args[i]).eq("DESC") {
                    i += 1;
                    true
                } else {
                    if i < args.len() && upper(&args[i]).eq("ASC") {
                        i += 1;
                    }
                    false
                };
                o.sortby = Some((field, desc));
                continue;
            }
            "WITHSCORES" => o.withscores = true,
            "NOCONTENT" => o.nocontent = true,
            "TIMEOUT" | "PARAMS" | "DIALECT" | "SLOP" | "LANGUAGE" | "EXPANDER" | "SCORER"
            | "EXPLAINSCORE" | "PAYLOAD" | "SORTKEYS" | "INKEYS" | "INFIELDS" | "FILTER"
            | "GEOFILTER" | "HIGHLIGHT" | "SUMMARIZE" => {
                // skip best-effort
                i += 1;
                while i < args.len() && !is_search_opt(&args[i]) {
                    i += 1;
                }
                continue;
            }
            _ => return Err(ERR_SYNTAX),
        }
        i += 1;
    }
    Ok(o)
}

fn is_search_opt(b: &[u8]) -> bool {
    matches!(
        upper(b).as_str(),
        "LIMIT" | "RETURN" | "SORTBY" | "WITHSCORES" | "NOCONTENT" | "TIMEOUT" | "PARAMS"
            | "DIALECT" | "LOAD" | "GROUPBY" | "REDUCE" | "APPLY" | "FILTER" | "CURSOR"
    )
}

fn doc_payload(
    ctx: &CommandContext,
    db: usize,
    key: &Bytes,
    idx: &SearchIndex,
    fields: &Option<Vec<String>>,
    nocontent: bool,
) -> Vec<RespValue> {
    if nocontent {
        return vec![];
    }
    let value = ctx.db.with_db(db, |d| d.get_fresh(key).map(|e| e.value.clone()));
    let Some(value) = value else {
        return vec![];
    };
    let mut out = Vec::new();
    match (&idx.on, &value) {
        (IndexOn::Hash, RedisValue::Hash(h)) => {
            let wanted: Vec<String> = fields.clone().unwrap_or_else(|| {
                idx.schema.iter().map(|f| f.name.clone()).collect()
            });
            for f in wanted {
                out.push(RespValue::bulk(Bytes::from(f.clone())));
                match h.get(f.as_bytes()) {
                    Some(v) => out.push(RespValue::bulk(v.clone())),
                    None => out.push(RespValue::null_bulk()),
                }
            }
        }
        (IndexOn::Json, RedisValue::Json(v)) => {
            out.push(RespValue::bulk(Bytes::from_static(b"$")));
            out.push(RespValue::bulk(Bytes::from(v.to_string())));
        }
        _ => {}
    }
    out
}

/// FT.SEARCH index query …
pub(crate) fn cmd_ft_search(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("ft.search");
    }
    let name = arg_str(args, 1).unwrap_or("");
    let opts = match parse_search_opts(args, 2) {
        Ok(o) => o,
        Err(e) => return err(e),
    };
    if let Err(e) = ensure_fresh(ctx, state, name) {
        return err(&e);
    }
    let resolved = ctx.search.resolve_name(name);
    let q = parse_query(&opts.query);
    let mut keys: Vec<Bytes> = ctx.search.with(|g| {
        let Some(idx) = g.indexes.get(&resolved) else {
            return Vec::new();
        };
        eval_query(idx, &q).into_iter().collect()
    });
    if let Some((ref field, desc)) = opts.sortby {
        keys.sort_by(|a, b| {
            let va = ctx.search.with(|g| {
                g.indexes
                    .get(&resolved)
                    .and_then(|idx| idx.docs.get(a))
                    .and_then(|d| d.numeric.get(field).copied().or_else(|| {
                        d.text.get(field).and_then(|s| s.parse().ok())
                    }))
                    .unwrap_or(0.0)
            });
            let vb = ctx.search.with(|g| {
                g.indexes
                    .get(&resolved)
                    .and_then(|idx| idx.docs.get(b))
                    .and_then(|d| d.numeric.get(field).copied().or_else(|| {
                        d.text.get(field).and_then(|s| s.parse().ok())
                    }))
                    .unwrap_or(0.0)
            });
            if desc {
                vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
            }
        });
    } else {
        keys.sort();
    }
    let total = keys.len();
    let page: Vec<Bytes> = keys
        .into_iter()
        .skip(opts.limit_off)
        .take(opts.limit_num)
        .collect();

    let mut out = vec![RespValue::Integer(total as i64)];
    let idx_on_schema = ctx.search.with(|g| g.indexes.get(&resolved).cloned());
    let Some(idx) = idx_on_schema else {
        return err("Unknown Index name");
    };
    for key in page {
        out.push(RespValue::bulk(key.clone()));
        if opts.withscores {
            out.push(RespValue::bulk(Bytes::from_static(b"1")));
        }
        let payload = doc_payload(ctx, state.db_index, &key, &idx, &opts.return_fields, opts.nocontent);
        if !opts.nocontent {
            out.push(RespValue::array(payload));
        }
    }
    RespValue::array(out)
}

/// FT.AGGREGATE index query …
pub(crate) fn cmd_ft_aggregate(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("ft.aggregate");
    }
    let name = arg_str(args, 1).unwrap_or("");
    let query = arg_str(args, 2).unwrap_or("*").to_string();
    if let Err(e) = ensure_fresh(ctx, state, name) {
        return err(&e);
    }
    let resolved = ctx.search.resolve_name(name);

    let mut load_fields: Vec<String> = Vec::new();
    let mut groupby: Vec<String> = Vec::new();
    let mut reduces: Vec<(String, String, String)> = Vec::new(); // op, field, as
    let mut sortby: Option<(String, bool)> = None;
    let mut limit_off = 0usize;
    let mut limit_num = 10usize;
    let mut cursor_count: Option<usize> = None;

    let mut i = 3;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "LOAD" => {
                i += 1;
                if arg_str(args, i) == Some("*") {
                    i += 1;
                    load_fields.clear(); // all
                } else {
                    let n = parse_i64(args.get(i).ok_or(ERR_SYNTAX).unwrap_or(&Bytes::new()))
                        .unwrap_or(0) as usize;
                    i += 1;
                    for _ in 0..n {
                        load_fields.push(arg_str(args, i).unwrap_or("").to_string());
                        i += 1;
                    }
                }
            }
            "GROUPBY" => {
                i += 1;
                let n = parse_i64(args.get(i).unwrap_or(&Bytes::from_static(b"0"))).unwrap_or(0) as usize;
                i += 1;
                groupby.clear();
                for _ in 0..n {
                    let f = arg_str(args, i).unwrap_or("").trim_start_matches('@').to_string();
                    groupby.push(f);
                    i += 1;
                }
            }
            "REDUCE" => {
                i += 1;
                let op = upper(args.get(i).map(|b| b.as_ref()).unwrap_or(b""));
                i += 1;
                let nargs = parse_i64(args.get(i).unwrap_or(&Bytes::from_static(b"0"))).unwrap_or(0) as usize;
                i += 1;
                let field = if nargs > 0 {
                    arg_str(args, i).unwrap_or("").trim_start_matches('@').to_string()
                } else {
                    String::new()
                };
                i += nargs;
                let as_name = if i < args.len() && upper(&args[i]).eq("AS") {
                    i += 1;
                    let n = arg_str(args, i).unwrap_or("count").to_string();
                    i += 1;
                    n
                } else {
                    op.to_ascii_lowercase()
                };
                reduces.push((op, field, as_name));
                continue;
            }
            "SORTBY" => {
                i += 1;
                let _n = parse_i64(args.get(i).unwrap_or(&Bytes::from_static(b"1")));
                i += 1;
                let field = arg_str(args, i).unwrap_or("").trim_start_matches('@').to_string();
                i += 1;
                let desc = if i < args.len() && upper(&args[i]).eq("DESC") {
                    i += 1;
                    true
                } else {
                    if i < args.len() && upper(&args[i]).eq("ASC") {
                        i += 1;
                    }
                    false
                };
                sortby = Some((field, desc));
                continue;
            }
            "LIMIT" => {
                i += 1;
                limit_off = parse_i64(args.get(i).unwrap_or(&Bytes::from_static(b"0"))).unwrap_or(0) as usize;
                i += 1;
                limit_num = parse_i64(args.get(i).unwrap_or(&Bytes::from_static(b"10"))).unwrap_or(10) as usize;
            }
            "CURSOR" => {
                i += 1;
                if i < args.len() && upper(&args[i]).eq("COUNT") {
                    i += 1;
                    cursor_count =
                        Some(parse_i64(args.get(i).unwrap_or(&Bytes::from_static(b"10"))).unwrap_or(10) as usize);
                }
            }
            "APPLY" | "FILTER" | "TIMEOUT" | "PARAMS" | "DIALECT" => {
                i += 1;
                while i < args.len() && !is_search_opt(&args[i]) {
                    i += 1;
                }
                continue;
            }
            _ => return err(ERR_SYNTAX),
        }
        i += 1;
    }

    let q = parse_query(&query);
    let docs: Vec<(Bytes, search::DocFields)> = ctx.search.with(|g| {
        let Some(idx) = g.indexes.get(&resolved) else {
            return Vec::new();
        };
        eval_query(idx, &q)
            .into_iter()
            .filter_map(|k| idx.docs.get(&k).cloned().map(|d| (k, d)))
            .collect()
    });

    // rows as map field→string
    let mut rows: Vec<BTreeMap<String, String>> = docs
        .into_iter()
        .map(|(key, doc)| {
            let mut m = BTreeMap::new();
            m.insert("__key".into(), String::from_utf8_lossy(&key).into_owned());
            for (f, v) in &doc.text {
                m.insert(f.clone(), v.clone());
            }
            for (f, tags) in &doc.tags {
                m.insert(f.clone(), tags.join(","));
            }
            for (f, v) in &doc.numeric {
                m.insert(f.clone(), crate::util::fmt_f64(*v));
            }
            if !load_fields.is_empty() {
                m.retain(|k, _| k == "__key" || load_fields.iter().any(|f| f == k));
            }
            m
        })
        .collect();

    if !groupby.is_empty() {
        let mut groups: BTreeMap<Vec<String>, Vec<BTreeMap<String, String>>> = BTreeMap::new();
        for row in rows {
            let gk: Vec<String> = groupby
                .iter()
                .map(|f| row.get(f).cloned().unwrap_or_default())
                .collect();
            groups.entry(gk).or_default().push(row);
        }
        rows = groups
            .into_iter()
            .map(|(gk, members)| {
                let mut out = BTreeMap::new();
                for (i, f) in groupby.iter().enumerate() {
                    out.insert(f.clone(), gk[i].clone());
                }
                for (op, field, as_name) in &reduces {
                    let val = match op.as_str() {
                        "COUNT" => members.len().to_string(),
                        "SUM" => {
                            let s: f64 = members
                                .iter()
                                .filter_map(|m| m.get(field).and_then(|s| s.parse::<f64>().ok()))
                                .sum();
                            fmt_f64(s)
                        }
                        "AVG" => {
                            let vals: Vec<f64> = members
                                .iter()
                                .filter_map(|m| m.get(field).and_then(|s| s.parse::<f64>().ok()))
                                .collect();
                            if vals.is_empty() {
                                "nan".into()
                            } else {
                                fmt_f64(vals.iter().sum::<f64>() / vals.len() as f64)
                            }
                        }
                        "MIN" => members
                            .iter()
                            .filter_map(|m| m.get(field).and_then(|s| s.parse::<f64>().ok()))
                            .fold(f64::INFINITY, f64::min)
                            .to_string(),
                        "MAX" => members
                            .iter()
                            .filter_map(|m| m.get(field).and_then(|s| s.parse::<f64>().ok()))
                            .fold(f64::NEG_INFINITY, f64::max)
                            .to_string(),
                        "COUNT_DISTINCT" => {
                            let set: BTreeSet<_> =
                                members.iter().filter_map(|m| m.get(field)).collect();
                            set.len().to_string()
                        }
                        "TOLIST" => {
                            let list: Vec<_> = members
                                .iter()
                                .filter_map(|m| m.get(field).cloned())
                                .collect();
                            format!("[{}]", list.join(", "))
                        }
                        "FIRST_VALUE" => members
                            .first()
                            .and_then(|m| m.get(field))
                            .cloned()
                            .unwrap_or_default(),
                        _ => "0".into(),
                    };
                    out.insert(as_name.clone(), val);
                }
                out
            })
            .collect();
    }

    if let Some((ref field, desc)) = sortby {
        rows.sort_by(|a, b| {
            let va = a.get(field).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
            let vb = b.get(field).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
            if desc {
                vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
            }
        });
    }

    let total = rows.len();
    let page: Vec<_> = rows.into_iter().skip(limit_off).take(limit_num).collect();

    if let Some(count) = cursor_count {
        let all_rows: Vec<Vec<(String, String)>> = page
            .iter()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .collect();
        // store remaining in cursor — simplified: store all page and paginate
        let id = ctx.search.with_mut(|g| {
            let id = g.next_cursor;
            g.next_cursor += 1;
            g.cursors.insert(
                id,
                CursorState {
                    index: resolved.clone(),
                    rows: all_rows,
                    offset: count.min(limit_num),
                    page: count,
                },
            );
            id
        });
        let first: Vec<_> = page.into_iter().take(count).collect();
        let mut out = vec![RespValue::Integer(total as i64)];
        for row in first {
            let flat: Vec<RespValue> = row
                .into_iter()
                .flat_map(|(k, v)| {
                    [
                        RespValue::bulk(Bytes::from(k)),
                        RespValue::bulk(Bytes::from(v)),
                    ]
                })
                .collect();
            out.push(RespValue::array(flat));
        }
        return RespValue::array(vec![RespValue::array(out), RespValue::Integer(id as i64)]);
    }

    let mut out = vec![RespValue::Integer(total as i64)];
    for row in page {
        let flat: Vec<RespValue> = row
            .into_iter()
            .flat_map(|(k, v)| {
                [
                    RespValue::bulk(Bytes::from(k)),
                    RespValue::bulk(Bytes::from(v)),
                ]
            })
            .collect();
        out.push(RespValue::array(flat));
    }
    RespValue::array(out)
}

pub(crate) fn cmd_ft_cursor(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("ft.cursor");
    }
    match upper(&args[1]).as_str() {
        "READ" => {
            if args.len() < 4 {
                return wrong_arity("ft.cursor");
            }
            let id = match parse_i64(&args[3]) {
                Some(v) => v as u64,
                None => return err(ERR_INT),
            };
            let count = args
                .get(5)
                .and_then(|b| parse_i64(b))
                .unwrap_or(10) as usize;
            ctx.search.with_mut(|g| {
                let Some(cur) = g.cursors.get_mut(&id) else {
                    return err("Cursor does not exist");
                };
                let slice: Vec<_> = cur.rows.iter().skip(cur.offset).take(count).cloned().collect();
                cur.offset += slice.len();
                let done = cur.offset >= cur.rows.len();
                let mut out = vec![RespValue::Integer(slice.len() as i64)];
                for row in slice {
                    let flat: Vec<RespValue> = row
                        .into_iter()
                        .flat_map(|(k, v)| {
                            [
                                RespValue::bulk(Bytes::from(k)),
                                RespValue::bulk(Bytes::from(v)),
                            ]
                        })
                        .collect();
                    out.push(RespValue::array(flat));
                }
                let cid = if done {
                    g.cursors.remove(&id);
                    0
                } else {
                    id as i64
                };
                RespValue::array(vec![RespValue::array(out), RespValue::Integer(cid)])
            })
        }
        "DEL" => {
            if args.len() < 4 {
                return wrong_arity("ft.cursor");
            }
            let id = match parse_i64(&args[3]) {
                Some(v) => v as u64,
                None => return err(ERR_INT),
            };
            ctx.search.with_mut(|g| {
                g.cursors.remove(&id);
            });
            RespValue::ok()
        }
        _ => err("ERR unknown subcommand for FT.CURSOR"),
    }
}

pub(crate) fn cmd_ft_alter(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("ft.alter");
    }
    let name = arg_str(args, 1).unwrap_or("").to_string();
    let schema_i = args.iter().position(|a| upper(a).eq("SCHEMA")).unwrap_or(2);
    // SCHEMA ADD field type …
    let mut i = schema_i;
    if upper(&args[i]).eq("SCHEMA") {
        i += 1;
    }
    if i < args.len() && upper(&args[i]).eq("ADD") {
        i += 1;
    }
    let fake: Vec<Bytes> = {
        let mut v = vec![Bytes::from_static(b"FT.ALTER"), Bytes::from_static(b"_"), Bytes::from_static(b"SCHEMA")];
        v.extend(args[i..].iter().cloned());
        v
    };
    let (_, added) = match parse_schema(&fake, 2) {
        Ok(v) => v,
        Err(e) => return err(e),
    };
    let r = ctx.search.with_mut(|g| {
        let idx = g
            .indexes
            .get_mut(&name)
            .ok_or("Unknown Index name")?;
        idx.schema.extend(added);
        idx.cached_dirty = 0;
        Ok::<(), &str>(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            let _ = ensure_fresh(ctx, state, &name);
            after_meta(ctx, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_ft_aliasadd(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("ft.aliasadd");
    }
    let alias = arg_str(args, 1).unwrap_or("").to_string();
    let index = arg_str(args, 2).unwrap_or("").to_string();
    let r = ctx.search.with_mut(|g| {
        if !g.indexes.contains_key(&index) {
            return Err("Unknown Index name");
        }
        if g.aliases.contains_key(&alias) || g.indexes.contains_key(&alias) {
            return Err("Alias already exists");
        }
        g.aliases.insert(alias, index);
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_meta(ctx, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_ft_aliasdel(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("ft.aliasdel");
    }
    let alias = arg_str(args, 1).unwrap_or("").to_string();
    let r = ctx.search.with_mut(|g| {
        if g.aliases.remove(&alias).is_none() {
            return Err("Alias does not exist");
        }
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_meta(ctx, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_ft_aliasupdate(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("ft.aliasupdate");
    }
    let alias = arg_str(args, 1).unwrap_or("").to_string();
    let index = arg_str(args, 2).unwrap_or("").to_string();
    let r = ctx.search.with_mut(|g| {
        if !g.indexes.contains_key(&index) {
            return Err("Unknown Index name");
        }
        g.aliases.insert(alias, index);
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_meta(ctx, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_ft_tagvals(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return wrong_arity("ft.tagvals");
    }
    let name = arg_str(args, 1).unwrap_or("");
    let field = arg_str(args, 2).unwrap_or("");
    if let Err(e) = ensure_fresh(ctx, state, name) {
        return err(&e);
    }
    let resolved = ctx.search.resolve_name(name);
    ctx.search.with(|g| {
        let Some(idx) = g.indexes.get(&resolved) else {
            return err("Unknown Index name");
        };
        let mut tags: BTreeSet<String> = BTreeSet::new();
        for doc in idx.docs.values() {
            if let Some(t) = doc.tags.get(field) {
                tags.extend(t.iter().cloned());
            }
        }
        RespValue::array(
            tags.into_iter()
                .map(|t| RespValue::bulk(Bytes::from(t)))
                .collect(),
        )
    })
}

pub(crate) fn cmd_ft_explain(_ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("ft.explain");
    }
    let q = arg_str(args, 2).unwrap_or("*");
    let node = parse_query(q);
    RespValue::bulk(Bytes::from(format!("{node:?}")))
}

pub(crate) fn cmd_ft_explaincli(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    let r = cmd_ft_explain(ctx, state, args);
    match r {
        RespValue::BulkString(Some(b)) => RespValue::array(vec![RespValue::bulk(b)]),
        other => other,
    }
}

pub(crate) fn cmd_ft_profile(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    // FT.PROFILE index SEARCH|AGGREGATE QUERY …
    if args.len() < 5 {
        return wrong_arity("ft.profile");
    }
    let kind = upper(&args[2]);
    let mut fake = vec![
        if kind == "SEARCH" {
            Bytes::from_static(b"FT.SEARCH")
        } else {
            Bytes::from_static(b"FT.AGGREGATE")
        },
        args[1].clone(),
    ];
    // skip QUERY keyword if present
    let start = if upper(&args[3]).eq("QUERY") { 4 } else { 3 };
    fake.extend(args[start..].iter().cloned());
    let result = if kind == "SEARCH" {
        cmd_ft_search(ctx, state, &fake)
    } else {
        cmd_ft_aggregate(ctx, state, &fake)
    };
    RespValue::array(vec![
        result,
        RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(b"Total profile time")),
            RespValue::bulk(Bytes::from_static(b"0")),
        ]),
    ])
}

pub(crate) fn cmd_ft_spellcheck(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("ft.spellcheck");
    }
    let name = arg_str(args, 1).unwrap_or("");
    let query = arg_str(args, 2).unwrap_or("");
    if let Err(e) = ensure_fresh(ctx, state, name) {
        return err(&e);
    }
    let resolved = ctx.search.resolve_name(name);
    let terms = search::tokenize(query);
    let mut out = Vec::new();
    ctx.search.with(|g| {
        let Some(idx) = g.indexes.get(&resolved) else {
            return;
        };
        let dict_terms: BTreeSet<String> = idx.inverted.keys().cloned().collect();
        for t in terms {
            if idx.inverted.contains_key(&t) {
                continue;
            }
            let suggestions: Vec<RespValue> = dict_terms
                .iter()
                .filter(|d| levenshtein(d, &t) <= 2)
                .take(5)
                .map(|d| {
                    RespValue::array(vec![
                        RespValue::bulk(Bytes::from_static(b"0.5")),
                        RespValue::bulk(Bytes::from(d.clone())),
                    ])
                })
                .collect();
            out.push(RespValue::array(vec![
                RespValue::bulk(Bytes::from_static(b"TERM")),
                RespValue::bulk(Bytes::from(t)),
                RespValue::array(suggestions),
            ]));
        }
    });
    RespValue::array(out)
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut cur = vec![i + 1];
        for (j, cb) in b.iter().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            cur.push((prev[j + 1] + 1).min(cur[j] + 1).min(prev[j] + cost));
        }
        prev = cur;
    }
    prev[b.len()]
}

pub(crate) fn cmd_ft_dictadd(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("ft.dictadd");
    }
    let dict = arg_str(args, 1).unwrap_or("").to_string();
    let mut n = 0i64;
    ctx.search.with_mut(|g| {
        let set = g.dicts.entry(dict).or_default();
        for t in &args[2..] {
            if set.insert(String::from_utf8_lossy(t).into_owned()) {
                n += 1;
            }
        }
    });
    after_meta(ctx, args);
    RespValue::Integer(n)
}

pub(crate) fn cmd_ft_dictdel(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("ft.dictdel");
    }
    let dict = arg_str(args, 1).unwrap_or("").to_string();
    let mut n = 0i64;
    ctx.search.with_mut(|g| {
        if let Some(set) = g.dicts.get_mut(&dict) {
            for t in &args[2..] {
                if set.remove(std::str::from_utf8(t).unwrap_or("")) {
                    n += 1;
                }
            }
        }
    });
    after_meta(ctx, args);
    RespValue::Integer(n)
}

pub(crate) fn cmd_ft_dictdump(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("ft.dictdump");
    }
    let dict = arg_str(args, 1).unwrap_or("");
    ctx.search.with(|g| match g.dicts.get(dict) {
        Some(set) => RespValue::array(
            set.iter()
                .map(|t| RespValue::bulk(Bytes::from(t.clone())))
                .collect(),
        ),
        None => err("ERR could not find dictionary with given name"),
    })
}

pub(crate) fn cmd_ft_synupdate(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return wrong_arity("ft.synupdate");
    }
    let index = arg_str(args, 1).unwrap_or("").to_string();
    let group = arg_str(args, 2).unwrap_or("").to_string();
    let terms: Vec<String> = args[3..]
        .iter()
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .collect();
    let r = ctx.search.with_mut(|g| {
        if !g.indexes.contains_key(&index) {
            return Err("Unknown Index name");
        }
        g.synonyms
            .entry(index)
            .or_default()
            .insert(group, terms);
        Ok(())
    });
    match r {
        Err(e) => err(e),
        Ok(()) => {
            after_meta(ctx, args);
            RespValue::ok()
        }
    }
}

pub(crate) fn cmd_ft_syndump(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("ft.syndump");
    }
    let index = arg_str(args, 1).unwrap_or("");
    ctx.search.with(|g| {
        let Some(map) = g.synonyms.get(index) else {
            return RespValue::array(vec![]);
        };
        let mut out = Vec::new();
        for (gid, terms) in map {
            out.push(RespValue::bulk(Bytes::from(gid.clone())));
            out.push(RespValue::array(
                terms
                    .iter()
                    .map(|t| RespValue::bulk(Bytes::from(t.clone())))
                    .collect(),
            ));
        }
        RespValue::array(out)
    })
}

pub(crate) fn cmd_ft_config(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 3 {
        return wrong_arity("ft.config");
    }
    match upper(&args[1]).as_str() {
        "GET" => {
            let name = arg_str(args, 2).unwrap_or("*");
            ctx.search.with(|g| {
                if name == "*" {
                    let mut out = Vec::new();
                    for (k, v) in &g.config {
                        out.push(RespValue::array(vec![
                            RespValue::bulk(Bytes::from(k.clone())),
                            RespValue::bulk(Bytes::from(v.clone())),
                        ]));
                    }
                    RespValue::array(out)
                } else {
                    match g.config.get(name) {
                        Some(v) => RespValue::array(vec![RespValue::array(vec![
                            RespValue::bulk(Bytes::from(name.to_string())),
                            RespValue::bulk(Bytes::from(v.clone())),
                        ])]),
                        None => RespValue::array(vec![]),
                    }
                }
            })
        }
        "SET" => {
            if args.len() != 4 {
                return wrong_arity("ft.config");
            }
            let name = arg_str(args, 2).unwrap_or("").to_string();
            let val = arg_str(args, 3).unwrap_or("").to_string();
            ctx.search.with_mut(|g| {
                g.config.insert(name, val);
            });
            RespValue::ok()
        }
        _ => err("ERR unknown subcommand for FT.CONFIG"),
    }
}

// silence unused
fn _use_extract() {
    let _ = extract_doc;
    let _ = QueryNode::All;
}
