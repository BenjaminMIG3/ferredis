//! Hints / complétions d'arguments style redis-cli (patterns, syntaxe).
//!
//! Source unique des gabarits : utilisée par le Hinter (gris) et le Completer (Tab).

/// Gabarit de syntaxe par commande (noms UPPER). Placeholders spéciaux :
/// - `pattern` → Tab propose `*`
/// - tokens en MAJUSCULES (`MATCH`, `ASYNC`…) → complétables
const HINTS: &[(&str, &str)] = &[
    ("APPEND", "key value"),
    ("AUTH", "password"),
    ("BGREWRITEAOF", ""),
    ("BGSAVE", "[SCHEDULE]"),
    ("BITCOUNT", "key [start end [BYTE|BIT]]"),
    ("BITOP", "operation destkey key [key ...]"),
    ("BLPOP", "key [key ...] timeout"),
    ("BRPOP", "key [key ...] timeout"),
    ("BZPOPMAX", "key [key ...] timeout"),
    ("BZPOPMIN", "key [key ...] timeout"),
    ("CLIENT", "subcommand [arg ...]"),
    ("CLUSTER", "subcommand [arg ...]"),
    ("COMMAND", "[subcommand ...]"),
    ("CONFIG", "GET|SET|RESETSTAT|REWRITE [arg ...]"),
    ("COPY", "source destination [DB destination-db] [REPLACE]"),
    ("DBSIZE", ""),
    ("DECR", "key"),
    ("DECRBY", "key decrement"),
    ("DEL", "key [key ...]"),
    ("DISCARD", ""),
    ("DUMP", "key"),
    ("ECHO", "message"),
    ("EVAL", "script numkeys [key ...] [arg ...]"),
    ("EVALSHA", "sha1 numkeys [key ...] [arg ...]"),
    ("EXISTS", "key [key ...]"),
    ("EXPIRE", "key seconds [NX|XX|GT|LT]"),
    ("EXPIREAT", "key timestamp [NX|XX|GT|LT]"),
    ("FLUSHALL", "[ASYNC|SYNC]"),
    ("FLUSHDB", "[ASYNC|SYNC]"),
    ("GEOADD", "key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]"),
    ("GEODIST", "key member1 member2 [M|KM|FT|MI]"),
    ("GEOHASH", "key member [member ...]"),
    ("GEOPOS", "key member [member ...]"),
    ("GEORADIUS", "key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key|STOREDIST key]"),
    ("GET", "key"),
    ("GETDEL", "key"),
    ("GETEX", "key [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|PERSIST]"),
    ("GETRANGE", "key start end"),
    ("GETSET", "key value"),
    ("HDEL", "key field [field ...]"),
    ("HELLO", "[protover [AUTH username password] [SETNAME clientname]]"),
    ("HEXISTS", "key field"),
    ("HGET", "key field"),
    ("HGETALL", "key"),
    ("HINCRBY", "key field increment"),
    ("HINCRBYFLOAT", "key field increment"),
    ("HKEYS", "key"),
    ("HLEN", "key"),
    ("HMGET", "key field [field ...]"),
    ("HMSET", "key field value [field value ...]"),
    ("HSCAN", "key cursor [MATCH pattern] [COUNT count]"),
    ("HSET", "key field value [field value ...]"),
    ("HSETNX", "key field value"),
    ("HSTRLEN", "key field"),
    ("HVALS", "key"),
    ("INCR", "key"),
    ("INCRBY", "key increment"),
    ("INCRBYFLOAT", "key increment"),
    ("INFO", "[section ...]"),
    ("KEYS", "pattern"),
    ("LASTSAVE", ""),
    ("LINDEX", "key index"),
    ("LINSERT", "key BEFORE|AFTER pivot element"),
    ("LLEN", "key"),
    ("LPOP", "key [count]"),
    ("LPOS", "key element [RANK rank] [COUNT num-matches] [MAXLEN len]"),
    ("LPUSH", "key element [element ...]"),
    ("LPUSHX", "key element [element ...]"),
    ("LRANGE", "key start stop"),
    ("LREM", "key count element"),
    ("LSET", "key index element"),
    ("LTRIM", "key start stop"),
    ("MEMORY", "USAGE key [SAMPLES count]|STATS|DOCTOR|PURGE|MALLOC-STATS"),
    ("MGET", "key [key ...]"),
    ("MIGRATE", "host port key|\"\" destination-db timeout [COPY] [REPLACE] [AUTH password] [KEYS key [key ...]]"),
    ("MONITOR", ""),
    ("MOVE", "key db"),
    ("MSET", "key value [key value ...]"),
    ("MSETNX", "key value [key value ...]"),
    ("MULTI", ""),
    ("OBJECT", "ENCODING|FREQ|IDLETIME|REFCOUNT key"),
    ("PERSIST", "key"),
    ("PEXPIRE", "key milliseconds [NX|XX|GT|LT]"),
    ("PEXPIREAT", "key milliseconds-timestamp [NX|XX|GT|LT]"),
    ("PFADD", "key element [element ...]"),
    ("PFCOUNT", "key [key ...]"),
    ("PFMERGE", "destkey sourcekey [sourcekey ...]"),
    ("PING", "[message]"),
    ("PSETEX", "key milliseconds value"),
    ("PSUBSCRIBE", "pattern [pattern ...]"),
    ("PTTL", "key"),
    ("PUBLISH", "channel message"),
    ("PUBSUB", "CHANNELS [pattern]|NUMSUB [channel ...]|NUMPAT"),
    ("PUNSUBSCRIBE", "[pattern ...]"),
    ("QUIT", ""),
    ("RANDOMKEY", ""),
    ("RENAME", "key newkey"),
    ("RENAMENX", "key newkey"),
    ("RESET", ""),
    ("RESTORE", "key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]"),
    ("ROLE", ""),
    ("RPOP", "key [count]"),
    ("RPOPLPUSH", "source destination"),
    ("RPUSH", "key element [element ...]"),
    ("RPUSHX", "key element [element ...]"),
    ("SADD", "key member [member ...]"),
    ("SAVE", ""),
    ("SCAN", "cursor [MATCH pattern] [COUNT count] [TYPE type]"),
    ("SCARD", "key"),
    ("SCRIPT", "EXISTS|FLUSH|KILL|LOAD [arg ...]"),
    ("SDIFF", "key [key ...]"),
    ("SDIFFSTORE", "destination key [key ...]"),
    ("SELECT", "index"),
    ("SET", "key value [NX|XX] [GET] [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|KEEPTTL]"),
    ("SETBIT", "key offset value"),
    ("SETEX", "key seconds value"),
    ("SETNX", "key value"),
    ("SETRANGE", "key offset value"),
    ("SHUTDOWN", "[NOSAVE|SAVE] [NOW] [FORCE] [ABORT]"),
    ("SINTER", "key [key ...]"),
    ("SINTERSTORE", "destination key [key ...]"),
    ("SISMEMBER", "key member"),
    ("SLOWLOG", "GET|LEN|RESET [count]"),
    ("SMEMBERS", "key"),
    ("SMOVE", "source destination member"),
    ("SORT", "key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA] [STORE destination]"),
    ("SPOP", "key [count]"),
    ("SRANDMEMBER", "key [count]"),
    ("SREM", "key member [member ...]"),
    ("SSCAN", "key cursor [MATCH pattern] [COUNT count]"),
    ("STRLEN", "key"),
    ("SUBSCRIBE", "channel [channel ...]"),
    ("SUBSTR", "key start end"),
    ("SUNION", "key [key ...]"),
    ("SUNIONSTORE", "destination key [key ...]"),
    ("SWAPDB", "index1 index2"),
    ("TIME", ""),
    ("TOUCH", "key [key ...]"),
    ("TTL", "key"),
    ("TYPE", "key"),
    ("UNLINK", "key [key ...]"),
    ("UNSUBSCRIBE", "[channel ...]"),
    ("UNWATCH", ""),
    ("WATCH", "key [key ...]"),
    ("XADD", "key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|id field value [field value ...]"),
    ("XLEN", "key"),
    ("XRANGE", "key start end [COUNT count]"),
    ("XREAD", "[COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]"),
    ("XREVRANGE", "key end start [COUNT count]"),
    ("XTRIM", "key MAXLEN|MINID [=|~] threshold [LIMIT count]"),
    ("ZADD", "key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]"),
    ("ZCARD", "key"),
    ("ZCOUNT", "key min max"),
    ("ZINCRBY", "key increment member"),
    ("ZINTERSTORE", "destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]"),
    ("ZLEXCOUNT", "key min max"),
    ("ZPOPMAX", "key [count]"),
    ("ZPOPMIN", "key [count]"),
    ("ZRANGE", "key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]"),
    ("ZRANGEBYLEX", "key min max [LIMIT offset count]"),
    ("ZRANGEBYSCORE", "key min max [WITHSCORES] [LIMIT offset count]"),
    ("ZRANK", "key member"),
    ("ZREM", "key member [member ...]"),
    ("ZREMRANGEBYLEX", "key min max"),
    ("ZREMRANGEBYRANK", "key start stop"),
    ("ZREMRANGEBYSCORE", "key min max"),
    ("ZREVRANGE", "key start stop [WITHSCORES]"),
    ("ZREVRANGEBYLEX", "key max min [LIMIT offset count]"),
    ("ZREVRANGEBYSCORE", "key max min [WITHSCORES] [LIMIT offset count]"),
    ("ZREVRANK", "key member"),
    ("ZSCAN", "key cursor [MATCH pattern] [COUNT count]"),
    ("ZSCORE", "key member"),
    ("ZUNIONSTORE", "destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]"),
];

/// Tokens Tab proposés pour un slot `pattern`.
const PATTERN_SUGGESTIONS: &[&str] = &["*"];

fn hint_template(cmd: &str) -> Option<&'static str> {
    let u = cmd.to_ascii_uppercase();
    HINTS
        .binary_search_by_key(&u.as_str(), |(n, _)| *n)
        .ok()
        .map(|i| HINTS[i].1)
        .or_else(|| HINTS.iter().find(|(n, _)| *n == u).map(|(_, h)| *h))
}

/// Hint gris restant après les arguments déjà saisis (comportement redis-cli).
pub fn syntax_hint(line: &str) -> Option<String> {
    let trimmed_end_space = line.ends_with(|c: char| c.is_whitespace());
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }
    // En cours de frappe du nom de commande → pas de hint d'args
    if parts.len() == 1 && !trimmed_end_space {
        return None;
    }
    let tmpl = hint_template(parts[0])?;
    if tmpl.is_empty() {
        return None;
    }
    let args_typed = if trimmed_end_space {
        parts.len() - 1
    } else {
        parts.len().saturating_sub(2) // dernier token partiel : on montre le slot courant
    };
    let partial = if trimmed_end_space {
        None
    } else {
        parts.last().copied()
    };
    let remaining = consume_hint(tmpl, args_typed, partial)?;
    if remaining.is_empty() {
        None
    } else if trimmed_end_space {
        Some(remaining)
    } else {
        let token = parts.last().unwrap_or(&"");
        hint_suffix_for_partial(&remaining, token)
    }
}

fn hint_suffix_for_partial(remaining: &str, token: &str) -> Option<String> {
    if token.is_empty() {
        return Some(remaining.to_string());
    }
    let first = remaining.split_whitespace().next()?;
    let bare = first.trim_matches(|c| c == '[' || c == ']' || c == '.' || c == '"');
    // ONEOF : NX|XX → si le token matche un préfixe
    for alt in bare.split('|') {
        let a = alt.trim_matches(|c| c == '[' || c == ']');
        if a.to_ascii_uppercase().starts_with(&token.to_ascii_uppercase()) && a.len() > token.len() {
            let rest = remaining.strip_prefix(first).unwrap_or("");
            return Some(format!("{}{}", &a[token.len()..], rest));
        }
    }
    // Placeholders (pattern, key…) : pas de suffixe inline, on garde le hint complet
    if bare.chars().all(|c| c.is_ascii_lowercase() || c == '-' || c == '_') {
        None
    } else if bare.to_ascii_uppercase().starts_with(&token.to_ascii_uppercase())
        && bare.len() > token.len()
    {
        let rest = remaining.strip_prefix(first).unwrap_or("");
        Some(format!("{}{}", &bare[token.len()..], rest))
    } else {
        None
    }
}

/// Avance dans le gabarit de `n` arguments déjà fournis.
fn consume_hint(tmpl: &str, args_done: usize, partial: Option<&str>) -> Option<String> {
    let tokens = tokenize_hint(tmpl);
    if tokens.is_empty() {
        return Some(String::new());
    }
    let mut idx = 0usize;
    let mut consumed = 0usize;
    while consumed < args_done && idx < tokens.len() {
        idx += 1;
        consumed += 1;
    }
    if idx >= tokens.len() {
        return Some(String::new());
    }
    let slice = &tokens[idx..];
    let mut out = join_hint_tokens(slice);
    if let Some(p) = partial {
        if !p.is_empty() {
            // Ne pas consommer : le Completer gère ; hint reste sur le slot courant
            let _ = &out;
        }
    }
    // Nettoyage crochets orphelins en tête
    out = tidy_brackets(&out);
    Some(out)
}

fn tokenize_hint(tmpl: &str) -> Vec<String> {
    // Découpe naive sur espaces en respectant [...] de premier niveau
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut depth = 0i32;
    for ch in tmpl.chars() {
        match ch {
            '[' => {
                depth += 1;
                cur.push(ch);
            }
            ']' => {
                depth -= 1;
                cur.push(ch);
            }
            ' ' if depth == 0 => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            _ => cur.push(ch),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn join_hint_tokens(tokens: &[String]) -> String {
    tokens.join(" ")
}

fn tidy_brackets(s: &str) -> String {
    s.trim().to_string()
}

/// Complétions Tab pour l'argument en cours.
pub fn arg_completions(parts: &[&str], token: &str, ends_with_space: bool) -> Vec<PairLike> {
    if parts.is_empty() {
        return Vec::new();
    }
    let cmd = parts[0].to_ascii_uppercase();
    let arg_idx = if ends_with_space {
        parts.len() - 1
    } else {
        parts.len().saturating_sub(2)
    };
    // Token en cours (vide si espace final)
    let tok = if ends_with_space { "" } else { token };
    let tok_u = tok.to_ascii_uppercase();

    let mut out: Vec<PairLike> = Vec::new();

    // Position pattern directe (KEYS, PSUBSCRIBE, …)
    if is_pattern_arg(&cmd, arg_idx, parts, ends_with_space) {
        for p in PATTERN_SUGGESTIONS {
            if p.starts_with(tok) || tok.is_empty() {
                out.push(PairLike {
                    display: (*p).into(),
                    replacement: (*p).into(),
                });
            }
        }
    }

    // Après MATCH → pattern
    if arg_idx > 0 {
        let prev = if ends_with_space {
            parts.last().copied()
        } else if parts.len() >= 2 {
            Some(parts[parts.len() - 2])
        } else {
            None
        };
        if prev.is_some_and(|p| p.eq_ignore_ascii_case("MATCH")) {
            for p in PATTERN_SUGGESTIONS {
                if p.starts_with(tok) || tok.is_empty() {
                    out.push(PairLike {
                        display: (*p).into(),
                        replacement: (*p).into(),
                    });
                }
            }
        }
    }

    // Tokens optionnels du gabarit restant
    if let Some(tmpl) = hint_template(&cmd) {
        let remaining = consume_hint(tmpl, arg_idx, None).unwrap_or_default();
        for cand in extract_completable_tokens(&remaining) {
            if cand.to_ascii_uppercase().starts_with(&tok_u) {
                out.push(PairLike {
                    display: cand.to_ascii_lowercase(),
                    replacement: cand.to_ascii_lowercase(),
                });
            }
        }
    }

    // Dédup
    out.sort_by(|a, b| a.replacement.cmp(&b.replacement));
    out.dedup_by(|a, b| a.replacement == b.replacement);
    out
}

fn is_pattern_arg(cmd: &str, arg_idx: usize, parts: &[&str], ends_with_space: bool) -> bool {
    match cmd {
        "KEYS" | "PSUBSCRIBE" | "PUNSUBSCRIBE" => arg_idx == 0,
        "PUBSUB" => {
            // PUBSUB CHANNELS [pattern]
            let sub = parts.get(1).map(|s| s.to_ascii_uppercase());
            (ends_with_space && parts.len() == 2 || !ends_with_space && parts.len() == 3)
                && sub.as_deref() == Some("CHANNELS")
                && arg_idx >= 1
        }
        "SORT" => {
            // Après BY ou GET
            let prev_i = if ends_with_space {
                parts.len() - 1
            } else {
                parts.len().saturating_sub(2)
            };
            parts
                .get(prev_i)
                .is_some_and(|p| p.eq_ignore_ascii_case("BY") || p.eq_ignore_ascii_case("GET"))
        }
        _ => false,
    }
}

fn extract_completable_tokens(remaining: &str) -> Vec<String> {
    let mut cands = Vec::new();
    let mut buf = String::new();
    let push_word = |w: &str, cands: &mut Vec<String>| {
        let w = w.trim_matches(|c| matches!(c, '[' | ']' | '(' | ')' | '.' | '"' | '=' | '~'));
        if w.is_empty() {
            return;
        }
        // ONEOF
        if w.contains('|') {
            for alt in w.split('|') {
                let a = alt.trim_matches(|c| matches!(c, '[' | ']'));
                if is_token_name(a) {
                    cands.push(a.to_string());
                }
            }
            return;
        }
        if is_token_name(w) {
            cands.push(w.to_string());
        }
    };
    for ch in remaining.chars() {
        match ch {
            ' ' | ']' => {
                if !buf.is_empty() {
                    push_word(&buf, &mut cands);
                    buf.clear();
                }
            }
            '[' => {
                if !buf.is_empty() {
                    push_word(&buf, &mut cands);
                    buf.clear();
                }
            }
            _ => buf.push(ch),
        }
    }
    if !buf.is_empty() {
        push_word(&buf, &mut cands);
    }
    cands
}

fn is_token_name(s: &str) -> bool {
    !s.is_empty()
        && s.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '-' || c == '_')
        && s.chars().any(|c| c.is_ascii_alphabetic())
}

#[derive(Clone, Debug)]
pub struct PairLike {
    pub display: String,
    pub replacement: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_hint() {
        assert_eq!(syntax_hint("KEYS ").as_deref(), Some("pattern"));
        assert_eq!(syntax_hint("keys ").as_deref(), Some("pattern"));
    }

    #[test]
    fn keys_tab_star() {
        let c = arg_completions(&["KEYS"], "", true);
        assert!(c.iter().any(|p| p.replacement == "*"));
    }

    #[test]
    fn scan_match_star() {
        let c = arg_completions(&["SCAN", "0", "MATCH"], "", true);
        assert!(c.iter().any(|p| p.replacement == "*"));
    }

    #[test]
    fn get_hint_consumed() {
        assert_eq!(syntax_hint("GET ").as_deref(), Some("key"));
        assert_eq!(syntax_hint("GET foo ").as_deref(), None);
    }

    #[test]
    fn hints_sorted() {
        let names: Vec<_> = HINTS.iter().map(|(n, _)| *n).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        assert_eq!(names, sorted, "HINTS must stay sorted for binary_search");
    }
}
