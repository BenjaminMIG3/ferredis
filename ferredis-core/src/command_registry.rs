//! Source unique des commandes : métadonnées (COMMAND LIST/COUNT/INFO, arité) et, pour les
//! commandes migrées, le handler appelé par le dispatch après contrôle générique de l'arité.

use crate::cmd_bitmap as bitmap;
use crate::cmd_geo::{self as geo, GeoCmd};
use crate::cmd_hashset as hashset;
use crate::cmd_hash_expire as hexpire;
use crate::cmd_hll as hll;
use crate::cmd_json as json;
use crate::cmd_key as key;
use crate::cmd_pubsub as pubsub;
use crate::cmd_search as search;
use crate::cmd_server as server;
use crate::cmd_sort as sort;
use crate::cmd_stream as stream;
use crate::cmd_string as string;
use crate::cmd_ts as ts;
use crate::cmd_probabilistic as prob;
use crate::cmd_vector as vector;
use crate::cmd_zset::{self as zset, RangeKind, SetOp, SetOpOutput, ZRangeCmd};
use crate::modules;
use crate::command::{ClientState, CommandContext};
use crate::commands_extra as extra;
use crate::pubsub::SubKind;
use crate::resp::RespValue;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::OnceLock;

pub type Handler = fn(&CommandContext, &mut ClientState, &[Bytes]) -> RespValue;
pub type MultiHandler = fn(&CommandContext, &mut ClientState, &[Bytes]) -> Vec<RespValue>;

/// Handler d'une commande migrée : une réponse, ou plusieurs (SUBSCRIBE…).
#[derive(Clone, Copy)]
pub enum Exec {
    One(Handler),
    Many(MultiHandler),
}

/// Métadonnées minimales compatibles Redis `COMMAND INFO`.
#[derive(Clone, Copy)]
pub struct CommandSpec {
    pub name: &'static str,
    /// Arity Redis : `-N` = au moins N args ; `N` = exactement N args (incl. nom).
    pub arity: i8,
    pub flags: &'static [&'static str],
    pub exec: Option<Exec>,
}

impl std::fmt::Debug for CommandSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandSpec").field("name", &self.name).field("arity", &self.arity).finish()
    }
}

impl CommandSpec {
    pub fn arity_ok(&self, argc: usize) -> bool {
        let a = self.arity as i64;
        let n = argc as i64;
        if a >= 0 {
            n == a
        } else {
            n >= -a
        }
    }

    pub fn has_flag(&self, flag: &str) -> bool {
        self.flags.contains(&flag)
    }
}

macro_rules! commands {
    ($($name:literal, $arity:expr, flags: [$($flag:literal),* $(,)?] $(, exec: $kind:ident($exec:expr))? ;)+) => {
        pub const COMMAND_SPECS: &[CommandSpec] = &[
            $(CommandSpec {
                name: $name,
                arity: $arity,
                flags: &[$($flag),*],
                exec: commands!(@exec $($kind($exec))?),
            },)+
        ];
    };
    (@exec) => { None };
    (@exec $kind:ident($e:expr)) => { Some(Exec::$kind($e)) };
}

commands! {
    "ACL", -2, flags: ["admin", "noscript", "loading", "stale", "fast"];
    "APPEND", 3, flags: ["write", "denyoom", "fast"];
    "AUTH", -2, flags: ["noscript", "loading", "stale", "fast", "no_auth"];
    "BGREWRITEAOF", 1, flags: ["admin", "noscript"];
    "BGSAVE", 1, flags: ["admin", "noscript"];
    "BF.ADD", 3, flags: ["write", "denyoom", "fast"], exec: One(prob::cmd_bf_add);
    "BF.CARD", 2, flags: ["readonly", "fast"], exec: One(prob::cmd_bf_card);
    "BF.DEBUG", 2, flags: ["readonly"], exec: One(prob::cmd_bf_debug);
    "BF.EXISTS", 3, flags: ["readonly", "fast"], exec: One(prob::cmd_bf_exists);
    "BF.INFO", 2, flags: ["readonly"], exec: One(prob::cmd_bf_info);
    "BF.INSERT", -4, flags: ["write", "denyoom"], exec: One(prob::cmd_bf_insert);
    "BF.LOADCHUNK", 4, flags: ["write", "denyoom"], exec: One(prob::cmd_bf_loadchunk);
    "BF.MADD", -3, flags: ["write", "denyoom"], exec: One(prob::cmd_bf_madd);
    "BF.MEXISTS", -3, flags: ["readonly", "fast"], exec: One(prob::cmd_bf_mexists);
    "BF.RESERVE", -4, flags: ["write", "denyoom"], exec: One(prob::cmd_bf_reserve);
    "BF.SCANDUMP", 3, flags: ["readonly"], exec: One(prob::cmd_bf_scandump);
    "BITCOUNT", -2, flags: ["readonly"], exec: One(bitmap::cmd_bitcount);
    "BITFIELD", -2, flags: ["write", "denyoom"], exec: One(bitmap::cmd_bitfield);
    "BITFIELD_RO", -2, flags: ["readonly", "fast"], exec: One(bitmap::cmd_bitfield_ro);
    "BITOP", -4, flags: ["write", "denyoom"], exec: One(bitmap::cmd_bitop);
    "BITPOS", -3, flags: ["readonly"], exec: One(bitmap::cmd_bitpos);
    "BLMOVE", 6, flags: ["write", "denyoom", "noscript", "blocking"];
    "BLMPOP", -5, flags: ["write", "blocking"];
    "BLPOP", -3, flags: ["write", "noscript", "blocking"];
    "BRPOP", -3, flags: ["write", "noscript", "blocking"];
    "BRPOPLPUSH", 4, flags: ["write", "denyoom", "noscript", "blocking"];
    "BZMPOP", -5, flags: ["write", "blocking"];
    "BZPOPMAX", -3, flags: ["write", "noscript", "blocking", "fast"];
    "BZPOPMIN", -3, flags: ["write", "noscript", "blocking", "fast"];
    "CF.ADD", 3, flags: ["write", "denyoom", "fast"], exec: One(prob::cmd_cf_add);
    "CF.ADDNX", 3, flags: ["write", "denyoom", "fast"], exec: One(prob::cmd_cf_addnx);
    "CF.COUNT", 3, flags: ["readonly", "fast"], exec: One(prob::cmd_cf_count);
    "CF.DEBUG", 2, flags: ["readonly"], exec: One(prob::cmd_cf_debug);
    "CF.DEL", 3, flags: ["write", "fast"], exec: One(prob::cmd_cf_del);
    "CF.EXISTS", 3, flags: ["readonly", "fast"], exec: One(prob::cmd_cf_exists);
    "CF.INFO", 2, flags: ["readonly"], exec: One(prob::cmd_cf_info);
    "CF.INSERT", -4, flags: ["write", "denyoom"], exec: One(prob::cmd_cf_insert);
    "CF.INSERTNX", -4, flags: ["write", "denyoom"], exec: One(prob::cmd_cf_insertnx);
    "CF.LOADCHUNK", 4, flags: ["write", "denyoom"], exec: One(prob::cmd_cf_loadchunk);
    "CF.MEXISTS", -3, flags: ["readonly", "fast"], exec: One(prob::cmd_cf_mexists);
    "CF.RESERVE", -3, flags: ["write", "denyoom"], exec: One(prob::cmd_cf_reserve);
    "CF.SCANDUMP", 3, flags: ["readonly"], exec: One(prob::cmd_cf_scandump);
    "CLIENT", -2, flags: ["admin", "noscript", "loading", "stale"];
    "CMS.INCRBY", -4, flags: ["write", "denyoom"], exec: One(prob::cmd_cms_incrby);
    "CMS.INFO", 2, flags: ["readonly"], exec: One(prob::cmd_cms_info);
    "CMS.INITBYDIM", 4, flags: ["write", "denyoom"], exec: One(prob::cmd_cms_initbydim);
    "CMS.INITBYPROB", 4, flags: ["write", "denyoom"], exec: One(prob::cmd_cms_initbyprob);
    "CMS.MERGE", -4, flags: ["write", "denyoom"], exec: One(prob::cmd_cms_merge);
    "CMS.QUERY", -3, flags: ["readonly"], exec: One(prob::cmd_cms_query);
    "COMMAND", -1, flags: ["loading", "stale"];
    "CONFIG", -2, flags: ["admin", "noscript", "loading", "stale"];
    "COPY", -3, flags: ["write", "denyoom"];
    "DATABASES", 1, flags: ["readonly", "admin", "fast"], exec: One(server::cmd_databases);
    "DBSIZE", 1, flags: ["readonly", "fast"];
    "DECR", 2, flags: ["write", "denyoom", "fast"];
    "DECRBY", 3, flags: ["write", "denyoom", "fast"];
    "DEBUG", -2, flags: ["admin", "noscript", "loading", "stale"], exec: One(server::cmd_debug);
    "DEL", -2, flags: ["write"];
    "DISCARD", 1, flags: ["noscript", "loading", "stale", "fast", "allow_busy"];
    "DUMP", 2, flags: ["readonly"], exec: One(crate::cmd_dump::cmd_dump);
    "ECHO", 2, flags: ["fast"];
    "EVAL", -3, flags: ["noscript", "stale", "skip_monitor", "no_mandatory_keys"];
    "EVALSHA", -3, flags: ["noscript", "stale", "skip_monitor", "no_mandatory_keys"];
    "EVALSHA_RO", -3, flags: ["readonly", "noscript", "stale", "skip_monitor"];
    "EVAL_RO", -3, flags: ["readonly", "noscript", "stale", "skip_monitor"];
    "EXEC", 1, flags: ["noscript", "loading", "stale", "skip_slowlog"];
    "EXISTS", -2, flags: ["readonly", "fast"];
    "EXPIRE", -3, flags: ["write", "fast"];
    "EXPIREAT", -3, flags: ["write", "fast"];
    "EXPIRETIME", 2, flags: ["readonly", "fast"];
    "FAILOVER", -1, flags: ["admin", "noscript", "stale"], exec: One(crate::repl::cmd_failover);
    "FERREDIS", -2, flags: ["admin", "fast"];
    "FLUSHALL", -1, flags: ["write"];
    "FLUSHDB", -1, flags: ["write"];
    "FQ.DROPINDEX", 2, flags: ["write", "fast"], exec: One(crate::cmd_query::cmd_fq);
    "FQ.INDEX", -7, flags: ["write", "denyoom"], exec: One(crate::cmd_query::cmd_fq);
    "FQ.LIST", 1, flags: ["readonly", "fast"], exec: One(crate::cmd_query::cmd_fq);
    "FQ.SELECT", -6, flags: ["readonly"], exec: One(crate::cmd_query::cmd_fq);
    "FCALL", -3, flags: ["noscript", "stale", "no_mandatory_keys"], exec: One(|c, s, a| crate::functions::cmd_fcall(c, s, a, false));
    "FCALL_RO", -3, flags: ["readonly", "noscript", "stale", "no_mandatory_keys"], exec: One(|c, s, a| crate::functions::cmd_fcall(c, s, a, true));
    "FT._LIST", 1, flags: ["readonly"], exec: One(search::cmd_ft_list);
    "FT.AGGREGATE", -3, flags: ["readonly"], exec: One(search::cmd_ft_aggregate);
    "FT.ALIASADD", 3, flags: ["write", "denyoom"], exec: One(search::cmd_ft_aliasadd);
    "FT.ALIASDEL", 2, flags: ["write"], exec: One(search::cmd_ft_aliasdel);
    "FT.ALIASUPDATE", 3, flags: ["write", "denyoom"], exec: One(search::cmd_ft_aliasupdate);
    "FT.ALTER", -3, flags: ["write", "denyoom"], exec: One(search::cmd_ft_alter);
    "FT.CONFIG", -3, flags: ["admin"], exec: One(search::cmd_ft_config);
    "FT.CREATE", -6, flags: ["write", "denyoom"], exec: One(search::cmd_ft_create);
    "FT.CURSOR", -3, flags: ["write"], exec: One(search::cmd_ft_cursor);
    "FT.DICTADD", -3, flags: ["write", "denyoom"], exec: One(search::cmd_ft_dictadd);
    "FT.DICTDEL", -3, flags: ["write"], exec: One(search::cmd_ft_dictdel);
    "FT.DICTDUMP", 2, flags: ["readonly"], exec: One(search::cmd_ft_dictdump);
    "FT.DROPINDEX", -2, flags: ["write"], exec: One(search::cmd_ft_dropindex);
    "FT.EXPLAIN", -3, flags: ["readonly"], exec: One(search::cmd_ft_explain);
    "FT.EXPLAINCLI", -3, flags: ["readonly"], exec: One(search::cmd_ft_explaincli);
    "FT.INFO", 2, flags: ["readonly"], exec: One(search::cmd_ft_info);
    "FT.PROFILE", -5, flags: ["readonly"], exec: One(search::cmd_ft_profile);
    "FT.SEARCH", -3, flags: ["readonly"], exec: One(search::cmd_ft_search);
    "FT.SPELLCHECK", -3, flags: ["readonly"], exec: One(search::cmd_ft_spellcheck);
    "FT.SYNDUMP", 2, flags: ["readonly"], exec: One(search::cmd_ft_syndump);
    "FT.SYNUPDATE", -4, flags: ["write", "denyoom"], exec: One(search::cmd_ft_synupdate);
    "FT.TAGVALS", 3, flags: ["readonly"], exec: One(search::cmd_ft_tagvals);
    "FUNCTION", -2, flags: ["noscript", "stale"], exec: One(crate::functions::cmd_function);
    "GEOADD", -5, flags: ["write", "denyoom"], exec: One(geo::cmd_geoadd);
    "GEODIST", -4, flags: ["readonly"], exec: One(geo::cmd_geodist);
    "GEOHASH", -2, flags: ["readonly"], exec: One(geo::cmd_geohash);
    "GEOPOS", -2, flags: ["readonly"], exec: One(geo::cmd_geopos);
    "GEORADIUS", -6, flags: ["write", "denyoom"], exec: One(|c, s, a| geo::cmd_geosearch_generic(c, s, a, GeoCmd::Radius { ro: false }));
    "GEORADIUSBYMEMBER", -5, flags: ["write", "denyoom"], exec: One(|c, s, a| geo::cmd_geosearch_generic(c, s, a, GeoCmd::RadiusByMember { ro: false }));
    "GEORADIUSBYMEMBER_RO", -5, flags: ["readonly"], exec: One(|c, s, a| geo::cmd_geosearch_generic(c, s, a, GeoCmd::RadiusByMember { ro: true }));
    "GEORADIUS_RO", -6, flags: ["readonly"], exec: One(|c, s, a| geo::cmd_geosearch_generic(c, s, a, GeoCmd::Radius { ro: true }));
    "GEOSEARCH", -7, flags: ["readonly"], exec: One(|c, s, a| geo::cmd_geosearch_generic(c, s, a, GeoCmd::Search));
    "GEOSEARCHSTORE", -8, flags: ["write", "denyoom"], exec: One(|c, s, a| geo::cmd_geosearch_generic(c, s, a, GeoCmd::SearchStore));
    "GET", 2, flags: ["readonly", "fast"];
    "GETBIT", 3, flags: ["readonly", "fast"], exec: One(bitmap::cmd_getbit);
    "GETDEL", 2, flags: ["write", "fast"];
    "GETEX", -2, flags: ["write", "fast"];
    "GETRANGE", 4, flags: ["readonly"];
    "GETSET", 3, flags: ["write", "denyoom", "fast"];
    "HDEL", -3, flags: ["write", "fast"];
    "HELLO", -1, flags: ["noscript", "loading", "stale", "fast", "no_auth"];
    "HEXISTS", 3, flags: ["readonly", "fast"];
    "HEXPIRE", -6, flags: ["write", "fast"], exec: One(hexpire::cmd_hexpire);
    "HEXPIREAT", -6, flags: ["write", "fast"], exec: One(hexpire::cmd_hexpireat);
    "HEXPIRETIME", -4, flags: ["readonly", "fast"], exec: One(hexpire::cmd_hexpiretime);
    "HGET", 3, flags: ["readonly", "fast"];
    "HGETALL", 2, flags: ["readonly"];
    "HGETDEL", -4, flags: ["write", "fast"], exec: One(hexpire::cmd_hgetdel);
    "HGETEX", -5, flags: ["write", "fast"], exec: One(hexpire::cmd_hgetex);
    "HINCRBY", 4, flags: ["write", "denyoom", "fast"];
    "HINCRBYFLOAT", 4, flags: ["write", "denyoom", "fast"];
    "HKEYS", 2, flags: ["readonly"];
    "HLEN", 2, flags: ["readonly", "fast"];
    "HMGET", -3, flags: ["readonly", "fast"];
    "HMSET", -4, flags: ["write", "denyoom", "fast"];
    "HPERSIST", -4, flags: ["write", "fast"], exec: One(hexpire::cmd_hpersist);
    "HPEXPIRE", -6, flags: ["write", "fast"], exec: One(hexpire::cmd_hpexpire);
    "HPEXPIREAT", -6, flags: ["write", "fast"], exec: One(hexpire::cmd_hpexpireat);
    "HPEXPIRETIME", -4, flags: ["readonly", "fast"], exec: One(hexpire::cmd_hpexpiretime);
    "HPTTL", -4, flags: ["readonly", "fast"], exec: One(hexpire::cmd_hpttl);
    "HRANDFIELD", -2, flags: ["readonly"], exec: One(hashset::cmd_hrandfield);
    "HSCAN", -3, flags: ["readonly"];
    "HSET", -4, flags: ["write", "denyoom", "fast"];
    "HSETEX", -5, flags: ["write", "denyoom"], exec: One(hexpire::cmd_hsetex);
    "HSETNX", 4, flags: ["write", "denyoom", "fast"];
    "HSTRLEN", 3, flags: ["readonly", "fast"];
    "HTTL", -4, flags: ["readonly", "fast"], exec: One(hexpire::cmd_httl);
    "HVALS", 2, flags: ["readonly"];
    "INCR", 2, flags: ["write", "denyoom", "fast"];
    "INCRBY", 3, flags: ["write", "denyoom", "fast"];
    "INCRBYFLOAT", 3, flags: ["write", "denyoom", "fast"];
    "INDEX", -2, flags: ["write", "denyoom"], exec: One(crate::cmd_query::cmd_index);
    "INFO", -1, flags: ["loading", "stale"];
    "JSON.ARRAPPEND", -4, flags: ["write", "denyoom"], exec: One(json::cmd_json_arrappend);
    "JSON.ARRINDEX", -4, flags: ["readonly"], exec: One(json::cmd_json_arrindex);
    "JSON.ARRINSERT", -5, flags: ["write", "denyoom"], exec: One(json::cmd_json_arrinsert);
    "JSON.ARRLEN", -2, flags: ["readonly", "fast"], exec: One(json::cmd_json_arrlen);
    "JSON.ARRPOP", -2, flags: ["write"], exec: One(json::cmd_json_arrpop);
    "JSON.ARRTRIM", 5, flags: ["write"], exec: One(json::cmd_json_arrtrim);
    "JSON.CLEAR", -2, flags: ["write"], exec: One(json::cmd_json_clear);
    "JSON.DEBUG", -2, flags: ["readonly"], exec: One(json::cmd_json_debug);
    "JSON.DEL", -2, flags: ["write"], exec: One(json::cmd_json_del);
    "JSON.FORGET", -2, flags: ["write"], exec: One(json::cmd_json_forget);
    "JSON.GET", -2, flags: ["readonly", "fast"], exec: One(json::cmd_json_get);
    "JSON.MERGE", 4, flags: ["write", "denyoom"], exec: One(json::cmd_json_merge);
    "JSON.MGET", -3, flags: ["readonly"], exec: One(json::cmd_json_mget);
    "JSON.MSET", -4, flags: ["write", "denyoom"], exec: One(json::cmd_json_mset);
    "JSON.NUMINCRBY", 4, flags: ["write", "denyoom"], exec: One(json::cmd_json_numincrby);
    "JSON.NUMMULTBY", 4, flags: ["write", "denyoom"], exec: One(json::cmd_json_nummultby);
    "JSON.OBJKEYS", -2, flags: ["readonly"], exec: One(json::cmd_json_objkeys);
    "JSON.OBJLEN", -2, flags: ["readonly", "fast"], exec: One(json::cmd_json_objlen);
    "JSON.RESP", -2, flags: ["readonly"], exec: One(json::cmd_json_resp);
    "JSON.SET", -4, flags: ["write", "denyoom"], exec: One(json::cmd_json_set);
    "JSON.STRAPPEND", -3, flags: ["write", "denyoom"], exec: One(json::cmd_json_strappend);
    "JSON.STRLEN", -2, flags: ["readonly", "fast"], exec: One(json::cmd_json_strlen);
    "JSON.TOGGLE", -2, flags: ["write"], exec: One(json::cmd_json_toggle);
    "JSON.TYPE", -2, flags: ["readonly", "fast"], exec: One(json::cmd_json_type);
    "KEYS", 2, flags: ["readonly"];
    "LASTSAVE", 1, flags: ["loading", "stale", "fast"];
    "LATENCY", -2, flags: ["admin", "noscript", "loading", "stale"], exec: One(server::cmd_latency);
    "LCS", -3, flags: ["readonly"], exec: One(string::cmd_lcs);
    "LINDEX", 3, flags: ["readonly"];
    "LINSERT", 5, flags: ["write", "denyoom"];
    "LLEN", 2, flags: ["readonly", "fast"];
    "LMOVE", 5, flags: ["write", "denyoom"];
    "LMPOP", -4, flags: ["write"], exec: One(|c, s, a| crate::blocking::run_once(c, s, "LMPOP", a));
    "LOLWUT", -1, flags: ["readonly", "fast"];
    "LPOP", -2, flags: ["write", "fast"];
    "LPOS", -3, flags: ["readonly"];
    "LPUSH", -3, flags: ["write", "denyoom", "fast"];
    "LPUSHX", -3, flags: ["write", "denyoom", "fast"];
    "LRANGE", 4, flags: ["readonly"];
    "LREM", 4, flags: ["write"];
    "LSET", 4, flags: ["write", "denyoom"];
    "LTRIM", 4, flags: ["write"];
    "MEMORY", -2, flags: ["readonly"];
    "MIGRATE", -6, flags: ["write", "denyoom"], exec: One(crate::cmd_dump::cmd_migrate);
    "MGET", -2, flags: ["readonly", "fast"];
    "MODULE", -2, flags: ["admin", "noscript", "loading", "stale"], exec: One(modules::cmd_module);
    "MONITOR", 1, flags: ["admin", "noscript", "loading", "stale"];
    "MOVE", 3, flags: ["write", "fast"];
    "MSET", -3, flags: ["write", "denyoom"];
    "MSETNX", -3, flags: ["write", "denyoom"];
    "MULTI", 1, flags: ["noscript", "loading", "stale", "fast", "allow_busy"];
    "OBJECT", -2, flags: ["readonly"];
    "PERSIST", 2, flags: ["write", "fast"];
    "PEXPIRE", -3, flags: ["write", "fast"];
    "PEXPIREAT", -3, flags: ["write", "fast"];
    "PEXPIRETIME", 2, flags: ["readonly", "fast"];
    "PFADD", -2, flags: ["write", "denyoom", "fast"], exec: One(hll::cmd_pfadd);
    "PFCOUNT", -2, flags: ["readonly"], exec: One(hll::cmd_pfcount);
    "PFDEBUG", 3, flags: ["write", "denyoom", "admin"], exec: One(hll::cmd_pfdebug);
    "PFMERGE", -2, flags: ["write", "denyoom"], exec: One(hll::cmd_pfmerge);
    "PFSELFTEST", 1, flags: ["admin"], exec: One(hll::cmd_pfselftest);
    "PING", -1, flags: ["fast"];
    "PSETEX", 4, flags: ["write", "denyoom"];
    "PSUBSCRIBE", -2, flags: ["pubsub", "noscript", "loading", "stale"], exec: Many(|c, s, a| pubsub::subscribe(c, s, a, SubKind::Pattern));
    "PSYNC", -3, flags: ["admin", "noscript"], exec: Many(crate::repl::cmd_psync);
    "PTTL", 2, flags: ["readonly", "fast"];
    "PUBLISH", 3, flags: ["pubsub", "loading", "stale", "fast"], exec: One(pubsub::cmd_publish);
    "PUBSUB", -2, flags: ["pubsub", "loading", "stale"], exec: One(pubsub::cmd_pubsub);
    "PUNSUBSCRIBE", -1, flags: ["pubsub", "noscript", "loading", "stale"], exec: Many(|c, s, a| pubsub::unsubscribe(c, s, a, SubKind::Pattern));
    "QUIT", -1, flags: ["noscript", "loading", "stale", "fast", "no_auth"];
    "RANDOMKEY", 1, flags: ["readonly"];
    "RENAME", 3, flags: ["write"];
    "RENAMENX", 3, flags: ["write", "fast"];
    "RESET", 1, flags: ["noscript", "loading", "stale", "fast", "no_auth"];
    "RESTORE", -4, flags: ["write", "denyoom"], exec: One(crate::cmd_dump::cmd_restore);
    "RESTORE-ASKING", -4, flags: ["write", "denyoom"], exec: One(crate::cmd_dump::cmd_restore_asking);
    "READONLY", 1, flags: ["fast", "loading", "stale"], exec: One(crate::repl::cmd_readonly);
    "READWRITE", 1, flags: ["fast", "loading", "stale"], exec: One(crate::repl::cmd_readwrite);
    "REPLCONF", -3, flags: ["admin", "noscript", "loading", "stale"], exec: Many(crate::repl::cmd_replconf_multi);
    "REPLICAOF", 3, flags: ["admin", "noscript", "stale"], exec: One(crate::repl::cmd_replicaof);
    "ROLE", 1, flags: ["noscript", "loading", "stale", "fast"], exec: One(crate::repl::cmd_role);
    "SENTINEL", -2, flags: ["admin", "noscript", "stale"], exec: One(crate::sentinel::cmd_sentinel);
    "CLUSTER", -2, flags: ["admin", "noscript", "stale"], exec: One(crate::cluster::cmd_cluster);
    "ASKING", 1, flags: ["fast"], exec: One(crate::cluster::cmd_asking);
    "RPOP", -2, flags: ["write", "fast"];
    "RPOPLPUSH", 3, flags: ["write", "denyoom"];
    "RPUSH", -3, flags: ["write", "denyoom", "fast"];
    "RPUSHX", -3, flags: ["write", "denyoom", "fast"];
    "SADD", -3, flags: ["write", "denyoom", "fast"];
    "SAVE", 1, flags: ["admin", "noscript"];
    "SCAN", -2, flags: ["readonly"];
    "SCARD", 2, flags: ["readonly", "fast"];
    "SCRIPT", -2, flags: ["noscript"];
    "SDIFF", -2, flags: ["readonly"];
    "SDIFFSTORE", -3, flags: ["write", "denyoom"];
    "SELECT", 2, flags: ["loading", "stale", "fast"];
    "SET", -3, flags: ["write", "denyoom"];
    "SETBIT", 4, flags: ["write", "denyoom"], exec: One(bitmap::cmd_setbit);
    "SETEX", 4, flags: ["write", "denyoom"];
    "SETNX", 3, flags: ["write", "denyoom", "fast"];
    "SETRANGE", 4, flags: ["write", "denyoom"];
    "SHUTDOWN", -1, flags: ["admin", "noscript", "loading", "stale"];
    "SINTER", -2, flags: ["readonly"];
    "SINTERCARD", -3, flags: ["readonly"], exec: One(hashset::cmd_sintercard);
    "SINTERSTORE", -3, flags: ["write", "denyoom"];
    "SISMEMBER", 3, flags: ["readonly", "fast"];
    "SLAVEOF", 3, flags: ["admin", "noscript", "stale"], exec: One(crate::repl::cmd_slaveof);
    "SLOWLOG", -2, flags: ["admin", "loading", "stale"];
    "SMEMBERS", 2, flags: ["readonly"];
    "SMISMEMBER", -3, flags: ["readonly", "fast"];
    "SMOVE", 4, flags: ["write", "fast"];
    "SORT", -2, flags: ["write", "denyoom"], exec: One(|c, s, a| sort::cmd_sort_generic(c, s, a, false));
    "SORT_RO", -2, flags: ["readonly"], exec: One(|c, s, a| sort::cmd_sort_generic(c, s, a, true));
    "SPOP", -2, flags: ["write", "fast"];
    "SPUBLISH", 3, flags: ["pubsub", "loading", "stale", "fast"], exec: One(pubsub::cmd_spublish);
    "SRANDMEMBER", -2, flags: ["readonly"], exec: One(hashset::cmd_srandmember);
    "SREM", -3, flags: ["write", "fast"];
    "SSCAN", -3, flags: ["readonly"];
    "SSUBSCRIBE", -2, flags: ["pubsub", "noscript", "loading", "stale"], exec: Many(|c, s, a| pubsub::subscribe(c, s, a, SubKind::Shard));
    "STRLEN", 2, flags: ["readonly", "fast"];
    "SUBSCRIBE", -2, flags: ["pubsub", "noscript", "loading", "stale"], exec: Many(|c, s, a| pubsub::subscribe(c, s, a, SubKind::Channel));
    "SUBSTR", 4, flags: ["readonly"];
    "SUNION", -2, flags: ["readonly"];
    "SUNIONSTORE", -3, flags: ["write", "denyoom"];
    "SUNSUBSCRIBE", -1, flags: ["pubsub", "noscript", "loading", "stale"], exec: Many(|c, s, a| pubsub::unsubscribe(c, s, a, SubKind::Shard));
    "SWAPDB", 3, flags: ["write", "fast"], exec: One(key::cmd_swapdb);
    "SYNC", 1, flags: ["admin", "noscript"], exec: Many(crate::repl::cmd_sync);
    "TIME", 1, flags: ["loading", "stale", "fast"];
    "TDIGEST.ADD", -3, flags: ["write", "denyoom"], exec: One(prob::cmd_tdigest_add);
    "TDIGEST.BYRANK", -3, flags: ["readonly"], exec: One(prob::cmd_tdigest_byrank);
    "TDIGEST.BYREVRANK", -3, flags: ["readonly"], exec: One(prob::cmd_tdigest_byrevrank);
    "TDIGEST.CDF", -3, flags: ["readonly"], exec: One(prob::cmd_tdigest_cdf);
    "TDIGEST.CREATE", -2, flags: ["write", "denyoom"], exec: One(prob::cmd_tdigest_create);
    "TDIGEST.INFO", 2, flags: ["readonly"], exec: One(prob::cmd_tdigest_info);
    "TDIGEST.MAX", 2, flags: ["readonly", "fast"], exec: One(prob::cmd_tdigest_max);
    "TDIGEST.MERGE", -4, flags: ["write", "denyoom"], exec: One(prob::cmd_tdigest_merge);
    "TDIGEST.MIN", 2, flags: ["readonly", "fast"], exec: One(prob::cmd_tdigest_min);
    "TDIGEST.QUANTILE", -3, flags: ["readonly"], exec: One(prob::cmd_tdigest_quantile);
    "TDIGEST.RANK", -3, flags: ["readonly"], exec: One(prob::cmd_tdigest_rank);
    "TDIGEST.RESET", 2, flags: ["write"], exec: One(prob::cmd_tdigest_reset);
    "TDIGEST.REVRANK", -3, flags: ["readonly"], exec: One(prob::cmd_tdigest_revrank);
    "TDIGEST.TRIMMED_MEAN", 4, flags: ["readonly"], exec: One(prob::cmd_tdigest_trimmed_mean);
    "TOPK.ADD", -3, flags: ["write", "denyoom"], exec: One(prob::cmd_topk_add);
    "TOPK.COUNT", -3, flags: ["readonly"], exec: One(prob::cmd_topk_count);
    "TOPK.INCRBY", -4, flags: ["write", "denyoom"], exec: One(prob::cmd_topk_incrby);
    "TOPK.INFO", 2, flags: ["readonly"], exec: One(prob::cmd_topk_info);
    "TOPK.LIST", -2, flags: ["readonly"], exec: One(prob::cmd_topk_list);
    "TOPK.QUERY", -3, flags: ["readonly"], exec: One(prob::cmd_topk_query);
    "TOPK.RESERVE", -3, flags: ["write", "denyoom"], exec: One(prob::cmd_topk_reserve);
    "TOUCH", -2, flags: ["readonly", "fast"];
    "TS.ADD", -4, flags: ["write", "denyoom"], exec: One(ts::cmd_ts_add);
    "TS.ALTER", -2, flags: ["write", "denyoom"], exec: One(ts::cmd_ts_alter);
    "TS.CREATE", -2, flags: ["write", "denyoom"], exec: One(ts::cmd_ts_create);
    "TS.CREATERULE", -6, flags: ["write", "denyoom"], exec: One(ts::cmd_ts_createrule);
    "TS.DECRBY", -3, flags: ["write", "denyoom"], exec: One(ts::cmd_ts_decrby);
    "TS.DEL", 4, flags: ["write"], exec: One(ts::cmd_ts_del);
    "TS.DELETERULE", 3, flags: ["write"], exec: One(ts::cmd_ts_deleterule);
    "TS.GET", -2, flags: ["readonly", "fast"], exec: One(ts::cmd_ts_get);
    "TS.INCRBY", -3, flags: ["write", "denyoom"], exec: One(ts::cmd_ts_incrby);
    "TS.INFO", -2, flags: ["readonly"], exec: One(ts::cmd_ts_info);
    "TS.MADD", -4, flags: ["write", "denyoom"], exec: One(ts::cmd_ts_madd);
    "TS.MGET", -3, flags: ["readonly"], exec: One(ts::cmd_ts_mget);
    "TS.MRANGE", -4, flags: ["readonly"], exec: One(ts::cmd_ts_mrange);
    "TS.MREVRANGE", -4, flags: ["readonly"], exec: One(ts::cmd_ts_mrevrange);
    "TS.QUERYINDEX", -2, flags: ["readonly"], exec: One(ts::cmd_ts_queryindex);
    "TS.RANGE", -4, flags: ["readonly"], exec: One(ts::cmd_ts_range);
    "TS.REVRANGE", -4, flags: ["readonly"], exec: One(ts::cmd_ts_revrange);
    "TTL", 2, flags: ["readonly", "fast"];
    "TYPE", 2, flags: ["readonly", "fast"];
    "UNLINK", -2, flags: ["write", "fast"];
    "UNSUBSCRIBE", -1, flags: ["pubsub", "noscript", "loading", "stale"], exec: Many(|c, s, a| pubsub::unsubscribe(c, s, a, SubKind::Channel));
    "UNWATCH", 1, flags: ["noscript", "loading", "stale", "fast", "allow_busy"];
    "VADD", -4, flags: ["write", "denyoom"], exec: One(vector::cmd_vadd);
    "VCARD", 2, flags: ["readonly", "fast"], exec: One(vector::cmd_vcard);
    "VDIM", 2, flags: ["readonly", "fast"], exec: One(vector::cmd_vdim);
    "VEMB", 3, flags: ["readonly"], exec: One(vector::cmd_vemb);
    "VGETATTR", 3, flags: ["readonly"], exec: One(vector::cmd_vgetattr);
    "VINFO", 2, flags: ["readonly"], exec: One(vector::cmd_vinfo);
    "VISMEMBER", 3, flags: ["readonly", "fast"], exec: One(vector::cmd_vismember);
    "VLINKS", -3, flags: ["readonly"], exec: One(vector::cmd_vlinks);
    "VRANDMEMBER", -2, flags: ["readonly"], exec: One(vector::cmd_vrandmember);
    "VREM", 3, flags: ["write", "fast"], exec: One(vector::cmd_vrem);
    "VSETATTR", 4, flags: ["write", "denyoom"], exec: One(vector::cmd_vsetattr);
    "VSIM", -3, flags: ["readonly"], exec: One(vector::cmd_vsim);
    "WAIT", 3, flags: ["noscript"], exec: One(crate::repl::cmd_wait);
    "WAITAOF", 4, flags: ["noscript"], exec: One(crate::cmd_dump::cmd_waitaof);
    "WATCH", -2, flags: ["noscript", "loading", "stale", "fast", "allow_busy"];
    "XACK", -4, flags: ["write", "fast"], exec: One(stream::cmd_xack);
    "XADD", -5, flags: ["write", "denyoom", "fast"], exec: One(stream::cmd_xadd);
    "XAUTOCLAIM", -6, flags: ["write", "fast"], exec: One(stream::cmd_xautoclaim);
    "XCLAIM", -6, flags: ["write", "fast"], exec: One(stream::cmd_xclaim);
    "XDEL", -3, flags: ["write", "fast"], exec: One(stream::cmd_xdel);
    "XGROUP", -2, flags: ["write", "denyoom"], exec: One(stream::cmd_xgroup);
    "XINFO", -2, flags: ["readonly"], exec: One(stream::cmd_xinfo);
    "XLEN", 2, flags: ["readonly", "fast"], exec: One(stream::cmd_xlen);
    "XPENDING", -3, flags: ["readonly"], exec: One(stream::cmd_xpending);
    "XRANGE", -4, flags: ["readonly"], exec: One(stream::cmd_xrange);
    "XREAD", -4, flags: ["readonly", "blocking"];
    "XREADGROUP", -7, flags: ["write", "blocking"];
    "XREVRANGE", -4, flags: ["readonly"], exec: One(stream::cmd_xrevrange);
    "XSETID", -3, flags: ["write", "denyoom", "fast"], exec: One(stream::cmd_xsetid);
    "XTRIM", -2, flags: ["write"], exec: One(stream::cmd_xtrim);
    "ZADD", -4, flags: ["write", "denyoom", "fast"], exec: One(zset::cmd_zadd);
    "ZCARD", 2, flags: ["readonly", "fast"], exec: One(|c, s, a| extra::cmd_zcard(c, s, a));
    "ZCOUNT", 4, flags: ["readonly", "fast"], exec: One(|c, s, a| zset::cmd_zcount_generic(c, s, a, RangeKind::Score));
    "ZDIFF", -3, flags: ["readonly"], exec: One(|c, s, a| zset::cmd_zsetop(c, s, a, SetOp::Diff, SetOpOutput::Reply));
    "ZDIFFSTORE", -4, flags: ["write", "denyoom"], exec: One(|c, s, a| zset::cmd_zsetop(c, s, a, SetOp::Diff, SetOpOutput::Store));
    "ZINCRBY", 4, flags: ["write", "denyoom", "fast"], exec: One(zset::cmd_zincrby);
    "ZINTER", -3, flags: ["readonly"], exec: One(|c, s, a| zset::cmd_zsetop(c, s, a, SetOp::Inter, SetOpOutput::Reply));
    "ZINTERCARD", -3, flags: ["readonly"], exec: One(|c, s, a| zset::cmd_zsetop(c, s, a, SetOp::Inter, SetOpOutput::Card));
    "ZINTERSTORE", -4, flags: ["write", "denyoom"], exec: One(|c, s, a| zset::cmd_zsetop(c, s, a, SetOp::Inter, SetOpOutput::Store));
    "ZLEXCOUNT", 4, flags: ["readonly", "fast"], exec: One(|c, s, a| zset::cmd_zcount_generic(c, s, a, RangeKind::Lex));
    "ZMPOP", -4, flags: ["write"], exec: One(|c, s, a| crate::blocking::run_once(c, s, "ZMPOP", a));
    "ZMSCORE", -3, flags: ["readonly", "fast"], exec: One(zset::cmd_zmscore);
    "ZPOPMAX", -2, flags: ["write", "fast"], exec: One(|c, s, a| zset::cmd_zpop(c, s, a, true));
    "ZPOPMIN", -2, flags: ["write", "fast"], exec: One(|c, s, a| zset::cmd_zpop(c, s, a, false));
    "ZRANDMEMBER", -2, flags: ["readonly"], exec: One(zset::cmd_zrandmember);
    "ZRANGE", -4, flags: ["readonly"], exec: One(|c, s, a| zset::cmd_zrange_generic(c, s, a, ZRangeCmd::Range));
    "ZRANGEBYLEX", -4, flags: ["readonly"], exec: One(|c, s, a| zset::cmd_zrange_generic(c, s, a, ZRangeCmd::RangeByLex));
    "ZRANGEBYSCORE", -4, flags: ["readonly"], exec: One(|c, s, a| zset::cmd_zrange_generic(c, s, a, ZRangeCmd::RangeByScore));
    "ZRANGESTORE", -5, flags: ["write", "denyoom"], exec: One(|c, s, a| zset::cmd_zrange_generic(c, s, a, ZRangeCmd::RangeStore));
    "ZRANK", -3, flags: ["readonly", "fast"], exec: One(|c, s, a| zset::cmd_zrank(c, s, a, false));
    "ZREM", -3, flags: ["write", "fast"], exec: One(|c, s, a| extra::cmd_zrem(c, s, a));
    "ZREMRANGEBYLEX", 4, flags: ["write"], exec: One(|c, s, a| zset::cmd_zremrange(c, s, a, RangeKind::Lex));
    "ZREMRANGEBYRANK", 4, flags: ["write"], exec: One(|c, s, a| zset::cmd_zremrange(c, s, a, RangeKind::Rank));
    "ZREMRANGEBYSCORE", 4, flags: ["write"], exec: One(|c, s, a| zset::cmd_zremrange(c, s, a, RangeKind::Score));
    "ZREVRANGE", -4, flags: ["readonly"], exec: One(|c, s, a| zset::cmd_zrange_generic(c, s, a, ZRangeCmd::RevRange));
    "ZREVRANGEBYLEX", -4, flags: ["readonly"], exec: One(|c, s, a| zset::cmd_zrange_generic(c, s, a, ZRangeCmd::RevRangeByLex));
    "ZREVRANGEBYSCORE", -4, flags: ["readonly"], exec: One(|c, s, a| zset::cmd_zrange_generic(c, s, a, ZRangeCmd::RevRangeByScore));
    "ZREVRANK", -3, flags: ["readonly", "fast"], exec: One(|c, s, a| zset::cmd_zrank(c, s, a, true));
    "ZSCAN", -3, flags: ["readonly"], exec: One(zset::cmd_zscan);
    "ZSCORE", 3, flags: ["readonly", "fast"], exec: One(|c, s, a| extra::cmd_zscore(c, s, a));
    "ZUNION", -3, flags: ["readonly"], exec: One(|c, s, a| zset::cmd_zsetop(c, s, a, SetOp::Union, SetOpOutput::Reply));
    "ZUNIONSTORE", -4, flags: ["write", "denyoom"], exec: One(|c, s, a| zset::cmd_zsetop(c, s, a, SetOp::Union, SetOpOutput::Store));
}

/// Noms pour compatibilité (`COMMAND LIST` historique).
pub fn supported_command_names() -> &'static [&'static str] {
    static NAMES: OnceLock<Vec<&'static str>> = OnceLock::new();
    NAMES.get_or_init(|| COMMAND_SPECS.iter().map(|s| s.name).collect()).as_slice()
}

/// Index nom (majuscules) → spec, construit une fois.
fn index() -> &'static HashMap<&'static str, &'static CommandSpec> {
    static INDEX: OnceLock<HashMap<&'static str, &'static CommandSpec>> = OnceLock::new();
    INDEX.get_or_init(|| COMMAND_SPECS.iter().map(|s| (s.name, s)).collect())
}

/// Recherche O(1) d'un nom déjà en majuscules.
pub fn lookup(upper: &str) -> Option<&'static CommandSpec> {
    index().get(upper).copied()
}

pub fn spec_by_name(name: &str) -> Option<&'static CommandSpec> {
    lookup(&name.to_ascii_uppercase())
}

/// Arité valide pour `cmd` (majuscules) ; commande inconnue → vrai (contrôle laissé au handler).
pub fn arity_ok(cmd: &str, argc: usize) -> bool {
    lookup(cmd).is_none_or(|s| s.arity_ok(argc))
}

pub fn command_count() -> usize {
    COMMAND_SPECS.len()
}

/// Entrée `COMMAND INFO` pour une commande (format Redis 7).
pub fn command_info_array(spec: &CommandSpec) -> RespValue {
    let flags: Vec<RespValue> = spec
        .flags
        .iter()
        .map(|f| RespValue::bulk(Bytes::from_static(f.as_bytes())))
        .collect();
    RespValue::array(vec![
        RespValue::bulk(Bytes::from(spec.name.to_ascii_lowercase())),
        RespValue::Integer(spec.arity as i64),
        RespValue::array(flags),
        RespValue::Integer(0),
        RespValue::Integer(0),
        RespValue::Integer(0),
        RespValue::array(vec![]),
        RespValue::array(vec![]),
        RespValue::array(vec![]),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_matches_count() {
        assert_eq!(command_count(), supported_command_names().len());
        assert_eq!(index().len(), command_count(), "noms dupliqués dans COMMAND_SPECS");
    }

    #[test]
    fn get_has_spec() {
        assert!(spec_by_name("GET").is_some());
        assert!(spec_by_name("monitor").is_some());
    }

    #[test]
    fn arity_rules() {
        let zadd = lookup("ZADD").unwrap();
        assert!(!zadd.arity_ok(3) && zadd.arity_ok(4) && zadd.arity_ok(10));
        let swap = lookup("SWAPDB").unwrap();
        assert!(swap.arity_ok(3) && !swap.arity_ok(4));
        assert!(COMMAND_SPECS.iter().all(|s| s.flags.iter().all(|f| *f != "read")));
    }
}
