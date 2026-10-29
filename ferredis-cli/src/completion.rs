//! Autocomplétion Tab + hints (commandes, sous-commandes, patterns d'args).

use crate::hints;
use ferredis_core::command_registry;
use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Helper, Result};
use std::borrow::Cow;

/// Sous-commandes fréquentes (DRY : table locale, pas de duplication serveur).
const SUBCOMMANDS: &[(&str, &[&str])] = &[
    (
        "ACL",
        &[
            "CAT", "DELUSER", "DRYRUN", "GENPASS", "GETUSER", "LIST", "LOAD", "LOG", "SAVE",
            "SETUSER", "USERS", "WHOAMI",
        ],
    ),
    (
        "CLIENT",
        &[
            "CACHING", "GETNAME", "GETREDIR", "ID", "INFO", "KILL", "LIST", "NO-EVICT", "NO-TOUCH",
            "PAUSE", "REPLY", "SETINFO", "SETNAME", "TRACKING", "TRACKINGINFO", "UNBLOCK",
            "UNPAUSE",
        ],
    ),
    (
        "CLUSTER",
        &[
            "ADDSLOTS", "BUMPEPOCH", "COUNTKEYSINSLOT", "DELSLOTS", "FAILOVER", "FORGET",
            "GETKEYSINSLOT", "INFO", "KEYSLOT", "MEET", "MYID", "NODES", "REPLICAS", "REPLICATE",
            "RESET", "SAVECONFIG", "SET-CONFIG-EPOCH", "SETSLOT", "SLOTS",
        ],
    ),
    ("CONFIG", &["GET", "RESETSTAT", "REWRITE", "SET"]),
    (
        "COMMAND",
        &["COUNT", "DOCS", "GETKEYS", "GETKEYSANDFLAGS", "INFO", "LIST"],
    ),
    (
        "FUNCTION",
        &["DELETE", "DUMP", "FLUSH", "KILL", "LIST", "LOAD", "RESTORE", "STATS"],
    ),
    ("LATENCY", &["DOCTOR", "GRAPH", "HISTORY", "LATEST", "RESET"]),
    (
        "MEMORY",
        &["DOCTOR", "HELP", "MALLOC-STATS", "PURGE", "STATS", "USAGE"],
    ),
    ("MODULE", &["LIST", "LOAD", "LOADEX", "UNLOAD"]),
    ("OBJECT", &["ENCODING", "FREQ", "HELP", "IDLETIME", "REFCOUNT"]),
    (
        "PUBSUB",
        &["CHANNELS", "HELP", "NUMPAT", "NUMSUB", "SHARDCHANNELS", "SHARDNUMSUB"],
    ),
    ("SCRIPT", &["DEBUG", "EXISTS", "FLUSH", "KILL", "LOAD"]),
    ("SLOWLOG", &["GET", "LEN", "RESET"]),
    (
        "XGROUP",
        &["CREATE", "CREATECONSUMER", "DELCONSUMER", "DESTROY", "HELP", "SETID"],
    ),
    ("XINFO", &["CONSUMERS", "GROUPS", "HELP", "STREAM"]),
];

pub struct RedisHelper {
    commands: Vec<&'static str>,
}

impl RedisHelper {
    pub fn new() -> Self {
        let mut commands: Vec<&'static str> = command_registry::supported_command_names().to_vec();
        for extra in ["HELP", "CLEAR", "EXIT", "QUIT"] {
            if !commands.iter().any(|c| c.eq_ignore_ascii_case(extra)) {
                commands.push(extra);
            }
        }
        commands.sort_unstable();
        commands.dedup();
        Self { commands }
    }

    fn complete_token(&self, token: &str) -> Vec<Pair> {
        let upper = token.to_ascii_uppercase();
        self.commands
            .iter()
            .filter(|c| c.starts_with(&upper))
            .map(|c| Pair {
                display: c.to_ascii_lowercase(),
                replacement: c.to_ascii_lowercase(),
            })
            .collect()
    }

    fn complete_sub(&self, cmd: &str, token: &str) -> Vec<Pair> {
        let cmd_u = cmd.to_ascii_uppercase();
        let tok_u = token.to_ascii_uppercase();
        SUBCOMMANDS
            .iter()
            .find(|(name, _)| *name == cmd_u)
            .map(|(_, subs)| {
                subs.iter()
                    .filter(|s| s.starts_with(tok_u.as_str()))
                    .map(|s| Pair {
                        display: s.to_ascii_lowercase(),
                        replacement: s.to_ascii_lowercase(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl Default for RedisHelper {
    fn default() -> Self {
        Self::new()
    }
}

impl Completer for RedisHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Pair>)> {
        let before = &line[..pos];
        let start = before
            .rfind(|c: char| c.is_whitespace())
            .map(|i| i + 1)
            .unwrap_or(0);
        let token = &before[start..];
        let ends_space = before.ends_with(|c: char| c.is_whitespace());
        let parts: Vec<&str> = before.split_whitespace().collect();

        // 1) Nom de commande
        if parts.is_empty() || (parts.len() == 1 && !ends_space) {
            return Ok((start, self.complete_token(token)));
        }

        let mut candidates: Vec<Pair> = Vec::new();

        // 2) Sous-commandes
        let sub_tok = if ends_space { "" } else { token };
        if parts.len() == 1 && ends_space {
            candidates.extend(self.complete_sub(parts[0], ""));
        } else if parts.len() >= 2 {
            candidates.extend(self.complete_sub(parts[0], sub_tok));
        }

        // 3) Patterns / tokens d'arguments (KEYS → *, MATCH → *, ASYNC, …)
        for p in hints::arg_completions(&parts, token, ends_space) {
            candidates.push(Pair {
                display: p.display,
                replacement: p.replacement,
            });
        }

        candidates.sort_by(|a, b| a.replacement.cmp(&b.replacement));
        candidates.dedup_by(|a, b| a.replacement == b.replacement);

        // Si espace final, insertion après l'espace (start == pos)
        Ok((start, candidates))
    }
}

impl Hinter for RedisHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<String> {
        if pos < line.len() {
            return None;
        }

        // Hint syntaxe args (ex. "KEYS " → "pattern")
        if let Some(h) = hints::syntax_hint(line) {
            return Some(h);
        }

        // Sinon suffixe du nom de commande
        let token = line
            .rsplit_once(char::is_whitespace)
            .map(|(_, t)| t)
            .unwrap_or(line);
        if token.is_empty() {
            return None;
        }
        let upper = token.to_ascii_uppercase();
        self.commands
            .iter()
            .find(|c| c.starts_with(&upper) && c.len() > upper.len())
            .map(|c| c[upper.len()..].to_ascii_lowercase())
    }
}

impl Highlighter for RedisHelper {
    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        // Style discret façon linenoise / redis-cli
        Cow::Owned(format!("\x1b[90m{hint}\x1b[0m"))
    }
}

impl Validator for RedisHelper {}

impl Helper for RedisHelper {}
