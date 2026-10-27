//! ACL Redis 7.2 : utilisateurs, catégories, patterns clés/canaux, persistance fichier.

use crate::command_registry;
use crate::resp::RespValue;
use crate::store::match_glob;
use bytes::Bytes;
use parking_lot::RwLock;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

const LOG_CAP: usize = 128;

/// Catégories ACL Redis (sous-ensemble OSS + scripting/connection).
pub const ACL_CATEGORIES: &[&str] = &[
    "keyspace", "read", "write", "set", "sortedset", "list", "hash", "string", "bitmap",
    "hyperloglog", "geo", "stream", "pubsub", "admin", "fast", "slow", "blocking", "dangerous",
    "connection", "transaction", "scripting",
];

#[derive(Debug, Clone, Default)]
struct UserRules {
    /// `None` = hérité de `all_commands` ; sinon allow/deny explicite.
    allow_cmds: HashSet<String>,
    deny_cmds: HashSet<String>,
    allow_cats: HashSet<String>,
    deny_cats: HashSet<String>,
    all_commands: bool,
    key_patterns: Vec<String>,
    all_keys: bool,
    channel_patterns: Vec<String>,
    all_channels: bool,
    /// Mots de passe en clair (tests / `>`).
    passwords: HashSet<String>,
    /// SHA256 hex lowercase (`#hash`).
    hash_passwords: HashSet<String>,
    nopass: bool,
    enabled: bool,
}

impl UserRules {
    fn default_user() -> Self {
        Self {
            all_commands: true,
            all_keys: true,
            all_channels: true,
            nopass: true,
            enabled: true,
            ..Default::default()
        }
    }

    fn reset(&mut self) {
        *self = Self {
            enabled: self.enabled,
            ..Self::default()
        };
    }
}

#[derive(Debug, Clone)]
pub struct AclLogEntry {
    pub count: u64,
    pub reason: String,
    pub context: String,
    pub object: String,
    pub username: String,
    pub age_seconds: f64,
    pub client_info: String,
    created: SystemTime,
}

#[derive(Default)]
struct AclInner {
    users: HashMap<String, UserRules>,
    log: Vec<AclLogEntry>,
    aclfile: Option<PathBuf>,
}

pub struct AclStore {
    inner: RwLock<AclInner>,
}

pub type SharedAcl = Arc<AclStore>;

pub fn shared_acl() -> SharedAcl {
    Arc::new(AclStore::new())
}

impl AclStore {
    pub fn new() -> Self {
        let mut users = HashMap::new();
        users.insert("default".into(), UserRules::default_user());
        Self {
            inner: RwLock::new(AclInner {
                users,
                log: Vec::new(),
                aclfile: None,
            }),
        }
    }

    pub fn set_aclfile(&self, path: Option<PathBuf>) {
        self.inner.write().aclfile = path;
    }

    /// Aligne l'utilisateur `default` sur `requirepass` (démarrage / CONFIG SET).
    pub fn sync_requirepass(&self, requirepass: Option<&str>) {
        let mut g = self.inner.write();
        let u = g.users.entry("default".into()).or_insert_with(UserRules::default_user);
        u.passwords.clear();
        u.hash_passwords.clear();
        match requirepass {
            Some(p) if !p.is_empty() => {
                u.nopass = false;
                u.passwords.insert(p.to_string());
                u.enabled = true;
            }
            _ => {
                u.nopass = true;
            }
        }
        if !u.all_commands && u.allow_cats.is_empty() && u.allow_cmds.is_empty() {
            u.all_commands = true;
        }
        if !u.all_keys && u.key_patterns.is_empty() {
            u.all_keys = true;
        }
        if !u.all_channels && u.channel_patterns.is_empty() {
            u.all_channels = true;
        }
    }

    pub fn authenticate(&self, username: &str, password: &str) -> Result<(), &'static str> {
        let g = self.inner.read();
        let Some(u) = g.users.get(username) else {
            return Err("WRONGPASS invalid username-password pair or user is disabled.");
        };
        if !u.enabled {
            return Err("WRONGPASS invalid username-password pair or user is disabled.");
        }
        if u.nopass {
            return Ok(());
        }
        if u.passwords.contains(password) {
            return Ok(());
        }
        let hash = sha256_hex(password.as_bytes());
        if u.hash_passwords.contains(&hash) {
            return Ok(());
        }
        Err("WRONGPASS invalid username-password pair or user is disabled.")
    }

    /// Vérifie commande (+ clés si fournies). `keys` = arguments clé de la commande.
    pub fn check_command(
        &self,
        username: &str,
        cmd: &str,
        keys: &[Bytes],
        channels: &[Bytes],
        client_info: &str,
    ) -> Result<(), String> {
        let g = self.inner.read();
        let Some(u) = g.users.get(username) else {
            return Err(format!("NOPERM User {username} has no permissions."));
        };
        if !u.enabled {
            return Err(format!("NOPERM User {username} is disabled."));
        }
        let upper = cmd.to_ascii_uppercase();
        if !command_allowed(u, &upper) {
            drop(g);
            self.push_log("command", &upper, username, client_info);
            return Err(format!(
                "NOPERM this user has no permissions to run the '{cmd}' command"
            ));
        }
        if !keys.is_empty() && !keys_allowed(u, keys) {
            drop(g);
            self.push_log("key", &String::from_utf8_lossy(keys.first().map(|k| k.as_ref()).unwrap_or_default()), username, client_info);
            return Err("NOPERM this user has no permissions to access one of the keys used as arguments".into());
        }
        if !channels.is_empty() && !channels_allowed(u, channels) {
            drop(g);
            self.push_log("channel", "pub/sub", username, client_info);
            return Err("NOPERM this user has no permissions to access one of the channels used as arguments".into());
        }
        Ok(())
    }

    fn push_log(&self, reason: &str, object: &str, username: &str, client_info: &str) {
        let mut g = self.inner.write();
        if let Some(last) = g.log.last_mut() {
            if last.reason == reason && last.object == object && last.username == username {
                last.count += 1;
                last.created = SystemTime::now();
                return;
            }
        }
        if g.log.len() >= LOG_CAP {
            g.log.remove(0);
        }
        g.log.push(AclLogEntry {
            count: 1,
            reason: reason.into(),
            context: "toplevel".into(),
            object: object.into(),
            username: username.into(),
            age_seconds: 0.0,
            client_info: client_info.into(),
            created: SystemTime::now(),
        });
    }

    pub fn whoami_ok(&self, username: &str) -> bool {
        self.inner.read().users.contains_key(username)
    }
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

fn command_allowed(u: &UserRules, cmd: &str) -> bool {
    let lower = cmd.to_ascii_lowercase();
    if u.deny_cmds.contains(&lower) {
        return false;
    }
    if u.allow_cmds.contains(&lower) {
        return true;
    }
    let cats = categories_for(cmd);
    if cats.iter().any(|c| u.deny_cats.contains(*c)) {
        return false;
    }
    if u.all_commands {
        return true;
    }
    cats.iter().any(|c| u.allow_cats.contains(*c))
}

fn keys_allowed(u: &UserRules, keys: &[Bytes]) -> bool {
    if u.all_keys {
        return true;
    }
    if u.key_patterns.is_empty() {
        return false;
    }
    keys.iter().all(|k| {
        let s = String::from_utf8_lossy(k);
        u.key_patterns.iter().any(|p| match_glob(p, &s))
    })
}

fn channels_allowed(u: &UserRules, channels: &[Bytes]) -> bool {
    if u.all_channels {
        return true;
    }
    if u.channel_patterns.is_empty() {
        return false;
    }
    channels.iter().all(|c| {
        let s = String::from_utf8_lossy(c);
        u.channel_patterns.iter().any(|p| match_glob(p, &s))
    })
}

/// Catégories ACL d'une commande (flags registre + famille).
pub fn categories_for(cmd: &str) -> Vec<&'static str> {
    let mut out = Vec::new();
    if let Some(spec) = command_registry::lookup(cmd) {
        for f in spec.flags {
            match *f {
                "write" => out.push("write"),
                "readonly" => out.push("read"),
                "admin" => {
                    out.push("admin");
                    out.push("dangerous");
                }
                "pubsub" => out.push("pubsub"),
                "blocking" => out.push("blocking"),
                "fast" => out.push("fast"),
                _ => {}
            }
        }
        if !spec.has_flag("fast") {
            out.push("slow");
        }
    }
    let u = cmd.to_ascii_uppercase();
    let family = match u.as_str() {
        "DEL" | "UNLINK" | "EXISTS" | "KEYS" | "SCAN" | "TYPE" | "RENAME" | "RENAMENX" | "MOVE"
        | "COPY" | "RANDOMKEY" | "DUMP" | "RESTORE" | "MIGRATE" | "OBJECT" | "TOUCH" | "SORT"
        | "SORT_RO" | "WAIT" | "SWAPDB" => "keyspace",
        c if c.starts_with('X') => "stream",
        c if c.starts_with('Z') => "sortedset",
        c if matches!(c.chars().next(), Some('S'))
            && matches!(
                c,
                "SADD" | "SREM" | "SMEMBERS" | "SISMEMBER" | "SCARD" | "SINTER" | "SUNION"
                    | "SDIFF" | "SSCAN" | "SPOP" | "SMOVE" | "SMISMEMBER" | "SINTERSTORE"
                    | "SUNIONSTORE" | "SDIFFSTORE" | "SINTERCARD" | "SRANDMEMBER"
            ) =>
        {
            "set"
        }
        c if c.starts_with('H') && c != "HELLO" => "hash",
        c if c.starts_with('L')
            || matches!(c, "RPUSH" | "RPOP" | "RPOPLPUSH" | "BRPOP" | "BLPOP" | "BRPOPLPUSH" | "BLMOVE" | "BLMPOP") =>
        {
            "list"
        }
        c if matches!(
            c,
            "SETBIT" | "GETBIT" | "BITCOUNT" | "BITPOS" | "BITOP" | "BITFIELD" | "BITFIELD_RO"
        ) =>
        {
            "bitmap"
        }
        c if c.starts_with("PF") => "hyperloglog",
        c if c.starts_with("GEO") => "geo",
        c if c.starts_with("BF.") => "bloom",
        c if c.starts_with("CF.") => "cuckoo",
        c if c.starts_with("CMS.") => "cms",
        c if c.starts_with("TOPK.") => "topk",
        c if c.starts_with("TDIGEST.") => "tdigest",
        c if matches!(
            c,
            "VADD" | "VCARD" | "VDIM" | "VEMB" | "VGETATTR" | "VINFO" | "VISMEMBER" | "VLINKS"
                | "VRANDMEMBER" | "VREM" | "VSETATTR" | "VSIM"
        ) =>
        {
            "search"
        }
        c if c.starts_with("JSON.") => "json",
        c if c.starts_with("TS.") => "timeseries",
        c if c.starts_with("FT.") => "search",
        c if matches!(
            c,
            "GET" | "SET" | "SETEX" | "PSETEX" | "SETNX" | "GETSET" | "GETDEL" | "GETEX" | "MGET"
                | "MSET" | "MSETNX" | "APPEND" | "STRLEN" | "INCR" | "DECR" | "INCRBY" | "DECRBY"
                | "INCRBYFLOAT" | "SETRANGE" | "GETRANGE" | "SUBSTR" | "LCS"
        ) =>
        {
            "string"
        }
        c if matches!(
            c,
            "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH"
        ) =>
        {
            "transaction"
        }
        c if matches!(
            c,
            "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "SCRIPT" | "FUNCTION" | "FCALL"
                | "FCALL_RO"
        ) =>
        {
            "scripting"
        }
        c if matches!(
            c,
            "AUTH" | "HELLO" | "PING" | "ECHO" | "SELECT" | "QUIT" | "RESET" | "CLIENT"
        ) =>
        {
            "connection"
        }
        _ => "",
    };
    if !family.is_empty() {
        out.push(family);
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn describe_user(name: &str, u: &UserRules) -> String {
    let mut parts = vec![format!("user {name}")];
    parts.push(if u.enabled { "on".into() } else { "off".into() });
    if u.nopass {
        parts.push("nopass".into());
    }
    for p in &u.passwords {
        parts.push(format!(">{p}"));
    }
    for h in &u.hash_passwords {
        parts.push(format!("#{h}"));
    }
    if u.all_keys {
        parts.push("~*".into());
    } else {
        for p in &u.key_patterns {
            parts.push(format!("~{p}"));
        }
    }
    if u.all_channels {
        parts.push("&*".into());
    } else {
        for p in &u.channel_patterns {
            parts.push(format!("&{p}"));
        }
    }
    if u.all_commands {
        parts.push("+@all".into());
    } else {
        for c in &u.allow_cats {
            parts.push(format!("+@{c}"));
        }
        for c in &u.allow_cmds {
            parts.push(format!("+{c}"));
        }
    }
    for c in &u.deny_cats {
        parts.push(format!("-@{c}"));
    }
    for c in &u.deny_cmds {
        parts.push(format!("-{c}"));
    }
    parts.join(" ")
}

fn apply_selector(u: &mut UserRules, tok: &str) -> Result<(), String> {
    if tok.is_empty() {
        return Ok(());
    }
    match tok {
        "on" => u.enabled = true,
        "off" => u.enabled = false,
        "nopass" => {
            u.nopass = true;
            u.passwords.clear();
            u.hash_passwords.clear();
        }
        "reset" => u.reset(),
        "resetpass" => {
            u.passwords.clear();
            u.hash_passwords.clear();
            u.nopass = false;
        }
        "resetkeys" => {
            u.key_patterns.clear();
            u.all_keys = false;
        }
        "resetchannels" => {
            u.channel_patterns.clear();
            u.all_channels = false;
        }
        "allkeys" | "~*" => {
            u.all_keys = true;
            u.key_patterns.clear();
        }
        "allchannels" | "&*" => {
            u.all_channels = true;
            u.channel_patterns.clear();
        }
        "allcommands" | "+@all" => {
            u.all_commands = true;
            u.allow_cmds.clear();
            u.allow_cats.clear();
            u.deny_cmds.clear();
            u.deny_cats.clear();
        }
        "nocommands" | "-@all" => {
            u.all_commands = false;
            u.allow_cmds.clear();
            u.allow_cats.clear();
        }
        t if t.starts_with('>') => {
            u.nopass = false;
            u.passwords.insert(t[1..].to_string());
        }
        t if t.starts_with('#') => {
            u.nopass = false;
            u.hash_passwords.insert(t[1..].to_ascii_lowercase());
        }
        t if t.starts_with("!<") || t.starts_with('!') => {
            // reset a password hash — ignore unknown hashes quietly
            let h = t.trim_start_matches(['!', '<', '>']).to_ascii_lowercase();
            u.hash_passwords.remove(&h);
            u.passwords.remove(t.trim_start_matches(['!', '<', '>']));
        }
        t if t.starts_with('~') => {
            u.all_keys = false;
            u.key_patterns.push(t[1..].to_string());
        }
        t if t.starts_with('&') => {
            u.all_channels = false;
            u.channel_patterns.push(t[1..].to_string());
        }
        t if t.starts_with("+@") => {
            let cat = t[2..].to_ascii_lowercase();
            if cat == "all" {
                u.all_commands = true;
            } else {
                u.all_commands = false;
                u.deny_cats.remove(&cat);
                u.allow_cats.insert(cat);
            }
        }
        t if t.starts_with("-@") => {
            let cat = t[2..].to_ascii_lowercase();
            if cat == "all" {
                u.all_commands = false;
                u.allow_cats.clear();
                u.allow_cmds.clear();
            } else {
                u.allow_cats.remove(&cat);
                u.deny_cats.insert(cat);
            }
        }
        t if t.starts_with('+') => {
            let cmd = t[1..].to_ascii_lowercase();
            u.deny_cmds.remove(&cmd);
            u.allow_cmds.insert(cmd);
            u.all_commands = false;
        }
        t if t.starts_with('-') => {
            let cmd = t[1..].to_ascii_lowercase();
            u.allow_cmds.remove(&cmd);
            u.deny_cmds.insert(cmd);
        }
        _ => return Err(format!("ERR Error in ACL SETUSER modifier '{tok}'")),
    }
    Ok(())
}

/// Extrait les clés « typiques » pour le contrôle ACL (approximation Redis GETKEYS).
pub fn keys_for_command(cmd: &str, args: &[Bytes]) -> Vec<Bytes> {
    if args.len() < 2 {
        return Vec::new();
    }
    let u = cmd.to_ascii_uppercase();
    match u.as_str() {
        "MSET" | "MSETNX" => args.iter().skip(1).step_by(2).cloned().collect(),
        "MGET" | "DEL" | "UNLINK" | "EXISTS" | "TOUCH" | "WATCH" => args[1..].to_vec(),
        "BLPOP" | "BRPOP" | "BZPOPMIN" | "BZPOPMAX" => {
            if args.len() >= 3 {
                args[1..args.len() - 1].to_vec()
            } else {
                Vec::new()
            }
        }
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO" => {
            let n = args
                .get(2)
                .and_then(|b| std::str::from_utf8(b).ok())
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
            args.iter().skip(3).take(n).cloned().collect()
        }
        "XREAD" | "XREADGROUP" => {
            let mut i = 1;
            while i < args.len() {
                let t = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
                if t == "STREAMS" {
                    let rest = &args[i + 1..];
                    let half = rest.len() / 2;
                    return rest.iter().take(half).cloned().collect();
                }
                i += 1;
            }
            Vec::new()
        }
        _ if args.len() >= 2 => vec![args[1].clone()],
        _ => Vec::new(),
    }
}

pub fn channels_for_command(cmd: &str, args: &[Bytes]) -> Vec<Bytes> {
    let u = cmd.to_ascii_uppercase();
    match u.as_str() {
        "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" | "PUBLISH" | "SPUBLISH" => args[1..].to_vec(),
        "UNSUBSCRIBE" | "PUNSUBSCRIBE" | "SUNSUBSCRIBE" => args[1..].to_vec(),
        _ => Vec::new(),
    }
}

use crate::command::{arg_str, wrong_arity, ClientState, CommandContext};

pub(crate) fn cmd_acl(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("acl");
    }
    let sub = arg_str(args, 1).unwrap_or("").to_ascii_uppercase();
    match sub.as_str() {
        "WHOAMI" => RespValue::bulk(Bytes::from(state.acl_user.clone())),
        "USERS" => {
            let g = ctx.acl.inner.read();
            let mut names: Vec<_> = g.users.keys().cloned().collect();
            names.sort();
            RespValue::array(names.into_iter().map(|n| RespValue::bulk(Bytes::from(n))).collect())
        }
        "LIST" => {
            let g = ctx.acl.inner.read();
            let mut names: Vec<_> = g.users.keys().cloned().collect();
            names.sort();
            RespValue::array(
                names
                    .into_iter()
                    .map(|n| {
                        let line = describe_user(&n, &g.users[&n]);
                        RespValue::bulk(Bytes::from(line))
                    })
                    .collect(),
            )
        }
        "GETUSER" => {
            if args.len() != 3 {
                return wrong_arity("acl|getuser");
            }
            let name = arg_str(args, 2).unwrap_or("");
            let g = ctx.acl.inner.read();
            let Some(u) = g.users.get(name) else {
                return RespValue::null_array();
            };
            getuser_reply(u)
        }
        "SETUSER" => {
            if args.len() < 3 {
                return wrong_arity("acl|setuser");
            }
            let name = arg_str(args, 2).unwrap_or("").to_string();
            if name.is_empty() {
                return RespValue::err("ERR Username can't be empty");
            }
            let mut g = ctx.acl.inner.write();
            let u = g.users.entry(name).or_insert_with(|| UserRules {
                enabled: false,
                ..UserRules::default()
            });
            for tok in args.iter().skip(3) {
                let s = String::from_utf8_lossy(tok);
                if let Err(e) = apply_selector(u, &s) {
                    return RespValue::err(e);
                }
            }
            RespValue::ok()
        }
        "DELUSER" => {
            if args.len() < 3 {
                return wrong_arity("acl|deluser");
            }
            let mut g = ctx.acl.inner.write();
            let mut n = 0i64;
            for a in &args[2..] {
                let name = String::from_utf8_lossy(a);
                if name == "default" {
                    return RespValue::err("ERR The 'default' user cannot be removed");
                }
                if g.users.remove(name.as_ref()).is_some() {
                    n += 1;
                }
            }
            RespValue::Integer(n)
        }
        "CAT" => {
            if args.len() == 2 {
                return RespValue::array(
                    ACL_CATEGORIES
                        .iter()
                        .map(|c| RespValue::bulk(Bytes::from_static(c.as_bytes())))
                        .collect(),
                );
            }
            let cat = arg_str(args, 2).unwrap_or("").to_ascii_lowercase();
            let mut cmds: Vec<String> = command_registry::supported_command_names()
                .iter()
                .filter(|n| categories_for(n).iter().any(|c| *c == cat))
                .map(|n| n.to_ascii_lowercase())
                .collect();
            cmds.sort();
            RespValue::array(cmds.into_iter().map(|c| RespValue::bulk(Bytes::from(c))).collect())
        }
        "GENPASS" => {
            let bits = if args.len() >= 3 {
                match arg_str(args, 2).and_then(|s| s.parse::<usize>().ok()) {
                    Some(b) if (1..=4096).contains(&b) => b,
                    _ => return RespValue::err("ERR Number of bits for GENPASS should be between 1 and 4096"),
                }
            } else {
                256
            };
            let nbytes = bits.div_ceil(8);
            let mut rng = rand::thread_rng();
            let bytes: Vec<u8> = (0..nbytes).map(|_| rng.gen()).collect();
            RespValue::bulk(Bytes::from(hex::encode(bytes)))
        }
        "DRYRUN" => {
            if args.len() < 4 {
                return wrong_arity("acl|dryrun");
            }
            let user = arg_str(args, 2).unwrap_or("default");
            let cmd = arg_str(args, 3).unwrap_or("").to_ascii_uppercase();
            let rest = &args[3..];
            let keys = keys_for_command(&cmd, rest);
            let chans = channels_for_command(&cmd, rest);
            match ctx.acl.check_command(user, &cmd, &keys, &chans, "dryrun") {
                Ok(()) => RespValue::ok(),
                Err(e) => RespValue::err(e),
            }
        }
        "LOG" => acl_log(ctx, args),
        "SAVE" => acl_save(ctx),
        "LOAD" => acl_load(ctx),
        "HELP" => RespValue::array(
            [
                "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "CAT [<category>]",
                "DELUSER <username> [<username> ...]",
                "DRYRUN <username> <command> [<arg> ...]",
                "GENPASS [<bits>]",
                "GETUSER <username>",
                "LIST",
                "LOAD",
                "LOG [<count> | RESET]",
                "SAVE",
                "SETUSER <username> <rules ...>",
                "USERS",
                "WHOAMI",
                "HELP",
            ]
            .iter()
            .map(|s| RespValue::bulk(Bytes::from_static(s.as_bytes())))
            .collect(),
        ),
        _ => RespValue::err(format!(
            "ERR unknown subcommand or wrong number of arguments for 'ACL|{sub}'. Try ACL HELP."
        )),
    }
}

fn getuser_reply(u: &UserRules) -> RespValue {
    let mut flags = vec![RespValue::bulk(Bytes::from_static(if u.enabled {
        b"on"
    } else {
        b"off"
    }))];
    if u.nopass {
        flags.push(RespValue::bulk(Bytes::from_static(b"nopass")));
    }
    let mut passwords: Vec<RespValue> = u
        .passwords
        .iter()
        .map(|p| RespValue::bulk(Bytes::from(format!("#{}" , sha256_hex(p.as_bytes())))))
        .collect();
    passwords.extend(
        u.hash_passwords
            .iter()
            .map(|h| RespValue::bulk(Bytes::from(format!("#{h}")))),
    );
    let mut commands = Vec::new();
    if u.all_commands {
        commands.push(RespValue::bulk(Bytes::from_static(b"+@all")));
    } else {
        for c in &u.allow_cats {
            commands.push(RespValue::bulk(Bytes::from(format!("+@{c}"))));
        }
        for c in &u.allow_cmds {
            commands.push(RespValue::bulk(Bytes::from(format!("+{c}"))));
        }
    }
    for c in &u.deny_cats {
        commands.push(RespValue::bulk(Bytes::from(format!("-@{c}"))));
    }
    for c in &u.deny_cmds {
        commands.push(RespValue::bulk(Bytes::from(format!("-{c}"))));
    }
    let keys: Vec<RespValue> = if u.all_keys {
        vec![RespValue::bulk(Bytes::from_static(b"~*"))]
    } else {
        u.key_patterns
            .iter()
            .map(|p| RespValue::bulk(Bytes::from(format!("~{p}"))))
            .collect()
    };
    let channels: Vec<RespValue> = if u.all_channels {
        vec![RespValue::bulk(Bytes::from_static(b"&*"))]
    } else {
        u.channel_patterns
            .iter()
            .map(|p| RespValue::bulk(Bytes::from(format!("&{p}"))))
            .collect()
    };
    RespValue::array(vec![
        RespValue::bulk(Bytes::from_static(b"flags")),
        RespValue::array(flags),
        RespValue::bulk(Bytes::from_static(b"passwords")),
        RespValue::array(passwords),
        RespValue::bulk(Bytes::from_static(b"commands")),
        RespValue::bulk(Bytes::from(
            commands
                .iter()
                .filter_map(|c| match c {
                    RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(" "),
        )),
        RespValue::bulk(Bytes::from_static(b"keys")),
        RespValue::array(keys),
        RespValue::bulk(Bytes::from_static(b"channels")),
        RespValue::array(channels),
    ])
}

fn acl_log(ctx: &CommandContext, args: &[Bytes]) -> RespValue {
    if args.len() >= 3 && arg_str(args, 2).unwrap_or("").eq_ignore_ascii_case("RESET") {
        ctx.acl.inner.write().log.clear();
        return RespValue::ok();
    }
    let count = args
        .get(2)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10);
    let g = ctx.acl.inner.read();
    let now = SystemTime::now();
    let items: Vec<_> = g
        .log
        .iter()
        .rev()
        .take(count)
        .map(|e| {
            let age = now
                .duration_since(e.created)
                .unwrap_or_default()
                .as_secs_f64();
            RespValue::array(vec![
                RespValue::bulk(Bytes::from_static(b"count")),
                RespValue::Integer(e.count as i64),
                RespValue::bulk(Bytes::from_static(b"reason")),
                RespValue::bulk(Bytes::from(e.reason.clone())),
                RespValue::bulk(Bytes::from_static(b"context")),
                RespValue::bulk(Bytes::from(e.context.clone())),
                RespValue::bulk(Bytes::from_static(b"object")),
                RespValue::bulk(Bytes::from(e.object.clone())),
                RespValue::bulk(Bytes::from_static(b"username")),
                RespValue::bulk(Bytes::from(e.username.clone())),
                RespValue::bulk(Bytes::from_static(b"age-seconds")),
                RespValue::bulk(Bytes::from(format!("{age:.3}"))),
                RespValue::bulk(Bytes::from_static(b"client-info")),
                RespValue::bulk(Bytes::from(e.client_info.clone())),
            ])
        })
        .collect();
    RespValue::array(items)
}

fn acl_save(ctx: &CommandContext) -> RespValue {
    let g = ctx.acl.inner.read();
    let path = g
        .aclfile
        .clone()
        .unwrap_or_else(|| ctx.config.read().persist.dir.join("users.acl"));
    let mut body = String::new();
    let mut names: Vec<_> = g.users.keys().cloned().collect();
    names.sort();
    for n in names {
        body.push_str(&describe_user(&n, &g.users[&n]));
        body.push('\n');
    }
    drop(g);
    match fs::write(&path, body) {
        Ok(()) => {
            ctx.acl.inner.write().aclfile = Some(path);
            RespValue::ok()
        }
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

fn acl_load(ctx: &CommandContext) -> RespValue {
    let path = ctx
        .acl
        .inner
        .read()
        .aclfile
        .clone()
        .unwrap_or_else(|| ctx.config.read().persist.dir.join("users.acl"));
    let data = match fs::read_to_string(&path) {
        Ok(d) => d,
        Err(e) => return RespValue::err(format!("ERR {e}")),
    };
    let mut users = HashMap::new();
    users.insert("default".into(), UserRules::default_user());
    for line in data.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        if parts.next() != Some("user") {
            return RespValue::err("ERR Error in ACL LOAD: missing 'user' prefix");
        }
        let Some(name) = parts.next() else {
            return RespValue::err("ERR Error in ACL LOAD: missing username");
        };
        let mut u = UserRules {
            enabled: false,
            ..UserRules::default()
        };
        for tok in parts {
            if let Err(e) = apply_selector(&mut u, tok) {
                return RespValue::err(e);
            }
        }
        users.insert(name.to_string(), u);
    }
    let mut g = ctx.acl.inner.write();
    g.users = users;
    g.aclfile = Some(path);
    RespValue::ok()
}

pub fn load_acl_file_on_boot(acl: &AclStore, path: &Path) {
    if !path.exists() {
        return;
    }
    acl.set_aclfile(Some(path.to_path_buf()));
    let data = match fs::read_to_string(path) {
        Ok(d) => d,
        Err(_) => return,
    };
    let mut users = HashMap::new();
    for line in data.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        if parts.next() != Some("user") {
            continue;
        }
        let Some(name) = parts.next() else { continue };
        let mut u = UserRules {
            enabled: false,
            ..UserRules::default()
        };
        for tok in parts {
            let _ = apply_selector(&mut u, tok);
        }
        users.insert(name.to_string(), u);
    }
    if !users.contains_key("default") {
        users.insert("default".into(), UserRules::default_user());
    }
    let mut g = acl.inner.write();
    g.users = users;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_known_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn default_allows_all() {
        let acl = AclStore::new();
        assert!(acl.check_command("default", "GET", &[], &[], "").is_ok());
    }

    #[test]
    fn deny_write_category() {
        let acl = AclStore::new();
        {
            let mut g = acl.inner.write();
            let u = g.users.get_mut("default").unwrap();
            u.all_commands = false;
            u.allow_cats.insert("read".into());
            u.deny_cats.insert("write".into());
        }
        assert!(acl.check_command("default", "GET", &[Bytes::from("k")], &[], "").is_ok());
        assert!(acl.check_command("default", "SET", &[Bytes::from("k")], &[], "").is_err());
    }
}
