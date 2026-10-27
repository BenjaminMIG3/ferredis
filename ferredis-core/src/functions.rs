//! Redis Functions (engine LUA) : FUNCTION LOAD/LIST/… et FCALL / FCALL_RO.

use crate::command::{arg_str, wrong_arity, ClientState, CommandContext};
use crate::opts::{trailing_sync_mode, SyncMode};
use crate::resp::RespValue;
use crate::script::{load_function_library, run_function, ShebangMeta};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct RegisteredFunction {
    pub name: String,
    pub lib: String,
    pub code: String,
    pub no_writes: bool,
}

#[derive(Clone)]
pub struct FunctionLib {
    pub name: String,
    pub engine: String,
    pub code: String,
    pub functions: Vec<String>,
}

struct FunctionInner {
    libs: HashMap<String, FunctionLib>,
    /// function name ? registration
    fns: HashMap<String, RegisteredFunction>,
}

pub struct FunctionStore {
    inner: Mutex<FunctionInner>,
    pub kill: AtomicBool,
    pub running: AtomicBool,
    pub calls: AtomicU64,
}

pub type SharedFunctions = Arc<FunctionStore>;

pub fn shared_functions() -> SharedFunctions {
    Arc::new(FunctionStore {
        inner: Mutex::new(FunctionInner {
            libs: HashMap::new(),
            fns: HashMap::new(),
        }),
        kill: AtomicBool::new(false),
        running: AtomicBool::new(false),
        calls: AtomicU64::new(0),
    })
}

impl FunctionStore {
    pub fn list_libs(&self) -> Vec<FunctionLib> {
        let g = self.inner.lock();
        let mut v: Vec<_> = g.libs.values().cloned().collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    pub fn dump_payload(&self) -> Vec<u8> {
        // Format simple FRDB-aux : u32 n, puis (name, engine, code) en longueur-préfixée.
        let g = self.inner.lock();
        let mut out = Vec::new();
        out.extend_from_slice(&(g.libs.len() as u32).to_le_bytes());
        for lib in g.libs.values() {
            write_str(&mut out, &lib.name);
            write_str(&mut out, &lib.engine);
            write_str(&mut out, &lib.code);
        }
        out
    }

    pub fn restore_payload(&self, data: &[u8]) -> Result<(), String> {
        let mut i = 0;
        let n = read_u32(data, &mut i)? as usize;
        let mut libs = HashMap::new();
        let mut fns = HashMap::new();
        for _ in 0..n {
            let name = read_str(data, &mut i)?;
            let engine = read_str(data, &mut i)?;
            let code = read_str(data, &mut i)?;
            let loaded = compile_library(&name, &engine, &code)?;
            for f in &loaded.functions {
                fns.insert(
                    f.name.clone(),
                    RegisteredFunction {
                        name: f.name.clone(),
                        lib: name.clone(),
                        code: f.body.clone(),
                        no_writes: f.no_writes,
                    },
                );
            }
            let fn_names: Vec<String> = loaded.functions.iter().map(|f| f.name.clone()).collect();
            libs.insert(
                name.clone(),
                FunctionLib {
                    name,
                    engine,
                    code,
                    functions: fn_names,
                },
            );
        }
        let mut g = self.inner.lock();
        g.libs = libs;
        g.fns = fns;
        Ok(())
    }

    pub fn get_fn(&self, name: &str) -> Option<RegisteredFunction> {
        self.inner.lock().fns.get(name).cloned()
    }

    pub fn load(&self, code: &str, replace: bool) -> Result<String, String> {
        let meta = parse_shebang(code)?;
        let name = meta
            .lib_name
            .clone()
            .ok_or_else(|| "ERR Library name was not provided".to_string())?;
        let engine = meta.engine.clone();
        if engine != "LUA" {
            return Err(format!("ERR Engine '{engine}' not supported"));
        }
        {
            let g = self.inner.lock();
            if g.libs.contains_key(&name) && !replace {
                return Err(format!("ERR Library '{name}' already exists"));
            }
        }
        let loaded = compile_library(&name, &engine, code)?;
        let mut g = self.inner.lock();
        if let Some(old) = g.libs.remove(&name) {
            for f in &old.functions {
                g.fns.remove(f);
            }
        }
        for f in &loaded.functions {
            if let Some(existing) = g.fns.get(&f.name) {
                if existing.lib != name {
                    return Err(format!(
                        "ERR Function {} already exists in library {}",
                        f.name, existing.lib
                    ));
                }
            }
        }
        for f in &loaded.functions {
            g.fns.insert(
                f.name.clone(),
                RegisteredFunction {
                    name: f.name.clone(),
                    lib: name.clone(),
                    code: f.body.clone(),
                    no_writes: f.no_writes,
                },
            );
        }
        let fn_names: Vec<String> = loaded.functions.iter().map(|f| f.name.clone()).collect();
        g.libs.insert(
            name.clone(),
            FunctionLib {
                name: name.clone(),
                engine,
                code: code.to_string(),
                functions: fn_names,
            },
        );
        Ok(name)
    }

    pub fn delete(&self, names: &[String]) -> i64 {
        let mut g = self.inner.lock();
        let mut n = 0i64;
        for name in names {
            if let Some(lib) = g.libs.remove(name) {
                for f in &lib.functions {
                    g.fns.remove(f);
                }
                n += 1;
            }
        }
        n
    }

    pub fn flush(&self) {
        let mut g = self.inner.lock();
        g.libs.clear();
        g.fns.clear();
    }
}

fn write_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn read_u32(data: &[u8], i: &mut usize) -> Result<u32, String> {
    if *i + 4 > data.len() {
        return Err("eof".into());
    }
    let v = u32::from_le_bytes(data[*i..*i + 4].try_into().unwrap());
    *i += 4;
    Ok(v)
}

fn read_str(data: &[u8], i: &mut usize) -> Result<String, String> {
    let n = read_u32(data, i)? as usize;
    if *i + n > data.len() {
        return Err("eof".into());
    }
    let s = String::from_utf8_lossy(&data[*i..*i + n]).into_owned();
    *i += n;
    Ok(s)
}

fn parse_shebang(code: &str) -> Result<ShebangMeta, String> {
    let first = code.lines().next().unwrap_or("").trim();
    if !first.starts_with("#!") {
        return Err("ERR Missing library metadata".into());
    }
    let rest = first.trim_start_matches("#!").trim();
    let mut parts = rest.split_whitespace();
    let engine = parts
        .next()
        .unwrap_or("lua")
        .to_ascii_uppercase();
    let mut meta = ShebangMeta {
        engine,
        lib_name: None,
        no_writes: false,
        allow_oom: false,
    };
    for p in parts {
        if let Some(v) = p.strip_prefix("name=") {
            meta.lib_name = Some(v.to_string());
        } else if let Some(v) = p.strip_prefix("flags=") {
            for f in v.split(',') {
                match f.trim() {
                    "no-writes" => meta.no_writes = true,
                    "allow-oom" => meta.allow_oom = true,
                    _ => {}
                }
            }
        }
    }
    Ok(meta)
}

/// Charge la bibliothèque dans une VM temporaire pour enregistrer les fonctions.
fn compile_library(
    lib_name: &str,
    engine: &str,
    code: &str,
) -> Result<crate::script::CompiledLibFns, String> {
    if engine != "LUA" {
        return Err(format!("ERR Engine '{engine}' not supported"));
    }
    load_function_library(lib_name, code)
}

pub(crate) fn cmd_function(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return wrong_arity("function");
    }
    let sub = arg_str(args, 1).unwrap_or("").to_ascii_uppercase();
    match sub.as_str() {
        "LOAD" => {
            let mut replace = false;
            let mut idx = 2;
            while idx < args.len() {
                let t = arg_str(args, idx).unwrap_or("").to_ascii_uppercase();
                if t == "REPLACE" {
                    replace = true;
                    idx += 1;
                } else {
                    break;
                }
            }
            if idx >= args.len() {
                return wrong_arity("function|load");
            }
            let code = String::from_utf8_lossy(&args[idx]);
            match ctx.functions.load(&code, replace) {
                Ok(name) => {
                    ctx.propagate(args);
                    RespValue::bulk(Bytes::from(name))
                }
                Err(e) => RespValue::err(e),
            }
        }
        "DELETE" => {
            if args.len() < 3 {
                return wrong_arity("function|delete");
            }
            let names: Vec<String> = args[2..]
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect();
            let n = ctx.functions.delete(&names);
            if n > 0 {
                ctx.propagate(args);
            }
            RespValue::ok()
        }
        "FLUSH" => {
            let _ = trailing_sync_mode(args, 2);
            let _ = SyncMode::Sync;
            ctx.functions.flush();
            ctx.propagate(args);
            RespValue::ok()
        }
        "LIST" => function_list(ctx, args),
        "STATS" => function_stats(ctx),
        "KILL" => {
            if !ctx.functions.running.load(Ordering::SeqCst) {
                return RespValue::err("ERR Not busy running a function");
            }
            ctx.functions.kill.store(true, Ordering::SeqCst);
            RespValue::ok()
        }
        "DUMP" => {
            let payload = ctx.functions.dump_payload();
            RespValue::bulk(Bytes::from(payload))
        }
        "RESTORE" => {
            let mut replace = false;
            let mut flush = false;
            let mut idx = 2;
            while idx < args.len() {
                match arg_str(args, idx).unwrap_or("").to_ascii_uppercase().as_str() {
                    "REPLACE" => {
                        replace = true;
                        idx += 1;
                    }
                    "FLUSH" => {
                        flush = true;
                        idx += 1;
                    }
                    "APPEND" => idx += 1,
                    _ => break,
                }
            }
            if idx >= args.len() {
                return wrong_arity("function|restore");
            }
            if flush {
                ctx.functions.flush();
            }
            if !replace && !ctx.functions.list_libs().is_empty() && !flush {
                // MERGE semantics: try restore; conflicts error inside
            }
            match ctx.functions.restore_payload(&args[idx]) {
                Ok(()) => {
                    ctx.propagate(args);
                    RespValue::ok()
                }
                Err(e) => RespValue::err(format!("ERR {e}")),
            }
        }
        "HELP" => RespValue::array(
            [
                "FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "LOAD [REPLACE] <library-code>",
                "DELETE <library-name>",
                "DUMP",
                "FLUSH [ASYNC|SYNC]",
                "KILL",
                "LIST [LIBRARYNAME pattern] [WITHCODE]",
                "RESTORE <payload> [FLUSH|APPEND|REPLACE]",
                "STATS",
                "HELP",
            ]
            .iter()
            .map(|s| RespValue::bulk(Bytes::from_static(s.as_bytes())))
            .collect(),
        ),
        _ => RespValue::err(format!(
            "ERR unknown subcommand or wrong number of arguments for 'FUNCTION|{sub}'. Try FUNCTION HELP."
        )),
    }
}

fn function_list(ctx: &CommandContext, args: &[Bytes]) -> RespValue {
    let mut withcode = false;
    let mut pattern = "*".to_string();
    let mut i = 2;
    while i < args.len() {
        match arg_str(args, i).unwrap_or("").to_ascii_uppercase().as_str() {
            "WITHCODE" => {
                withcode = true;
                i += 1;
            }
            "LIBRARYNAME" if i + 1 < args.len() => {
                pattern = String::from_utf8_lossy(&args[i + 1]).into_owned();
                i += 2;
            }
            _ => return RespValue::err("ERR Unknown subcommand or wrong number of arguments for FUNCTION LIST"),
        }
    }
    let libs = ctx.functions.list_libs();
    let mut out = Vec::new();
    for lib in libs {
        if !crate::store::match_glob(&pattern, &lib.name) {
            continue;
        }
        let mut entry = vec![
            RespValue::bulk(Bytes::from_static(b"library_name")),
            RespValue::bulk(Bytes::from(lib.name.clone())),
            RespValue::bulk(Bytes::from_static(b"engine")),
            RespValue::bulk(Bytes::from(lib.engine.to_ascii_lowercase())),
            RespValue::bulk(Bytes::from_static(b"functions")),
        ];
        let fns: Vec<RespValue> = lib
            .functions
            .iter()
            .map(|n| {
                let no_writes = ctx
                    .functions
                    .get_fn(n)
                    .map(|f| f.no_writes)
                    .unwrap_or(false);
                let mut fentry = vec![
                    RespValue::bulk(Bytes::from_static(b"name")),
                    RespValue::bulk(Bytes::from(n.clone())),
                    RespValue::bulk(Bytes::from_static(b"description")),
                    RespValue::null_bulk(),
                    RespValue::bulk(Bytes::from_static(b"flags")),
                ];
                let flags = if no_writes {
                    vec![RespValue::bulk(Bytes::from_static(b"no-writes"))]
                } else {
                    vec![]
                };
                fentry.push(RespValue::array(flags));
                RespValue::array(fentry)
            })
            .collect();
        entry.push(RespValue::array(fns));
        if withcode {
            entry.push(RespValue::bulk(Bytes::from_static(b"library_code")));
            entry.push(RespValue::bulk(Bytes::from(lib.code)));
        }
        out.push(RespValue::array(entry));
    }
    RespValue::array(out)
}

fn function_stats(ctx: &CommandContext) -> RespValue {
    let running = if ctx.functions.running.load(Ordering::SeqCst) {
        RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(b"name")),
            RespValue::bulk(Bytes::from_static(b"?")),
            RespValue::bulk(Bytes::from_static(b"command")),
            RespValue::bulk(Bytes::from_static(b"FCALL")),
            RespValue::bulk(Bytes::from_static(b"duration_ms")),
            RespValue::Integer(0),
        ])
    } else {
        RespValue::null_bulk()
    };
    let libs = ctx.functions.list_libs();
    let n_fn: usize = libs.iter().map(|l| l.functions.len()).sum();
    RespValue::array(vec![
        RespValue::bulk(Bytes::from_static(b"running_script")),
        running,
        RespValue::bulk(Bytes::from_static(b"engines")),
        RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(b"LUA")),
            RespValue::array(vec![
                RespValue::bulk(Bytes::from_static(b"libraries_count")),
                RespValue::Integer(libs.len() as i64),
                RespValue::bulk(Bytes::from_static(b"functions_count")),
                RespValue::Integer(n_fn as i64),
            ]),
        ]),
    ])
}

pub(crate) fn cmd_fcall(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    read_only: bool,
) -> RespValue {
    let name_cmd = if read_only { "fcall_ro" } else { "fcall" };
    if args.len() < 3 {
        return wrong_arity(name_cmd);
    }
    let fname = arg_str(args, 1).unwrap_or("").to_string();
    let numkeys = match arg_str(args, 2).and_then(|s| s.parse::<i64>().ok()) {
        Some(n) if n < 0 => return RespValue::err("ERR Number of keys can't be negative"),
        Some(n) => n as usize,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    if args.len() < 3 + numkeys {
        return RespValue::err("ERR Number of keys can't be greater than number of args");
    }
    let Some(f) = ctx.functions.get_fn(&fname) else {
        return RespValue::err(format!("ERR Function not found"));
    };
    let ro = read_only || f.no_writes;
    ctx.functions.running.store(true, Ordering::SeqCst);
    ctx.functions.kill.store(false, Ordering::SeqCst);
    ctx.functions.calls.fetch_add(1, Ordering::Relaxed);
    let _start = Instant::now();
    let keys = &args[3..3 + numkeys];
    let argv = &args[3 + numkeys..];
    let result = run_function(ctx, state, &f.name, &f.code, keys, argv, ro);
    ctx.functions.running.store(false, Ordering::SeqCst);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::ServerConfig;

    #[tokio::test]
    async fn load_and_fcall() {
        let ctx = CommandContext::new(ServerConfig::default());
        let mut st = ClientState {
            authenticated: true,
            ..Default::default()
        };
        let code = "#!lua name=mylib\nredis.register_function('echo', function(k,a) return a[1] end)";
        let r = cmd_function(
            &ctx,
            &mut st,
            &[
                Bytes::from("FUNCTION"),
                Bytes::from("LOAD"),
                Bytes::from(code),
            ],
        );
        assert_eq!(r, RespValue::bulk(Bytes::from_static(b"mylib")));
        let r = cmd_fcall(
            &ctx,
            &mut st,
            &[
                Bytes::from("FCALL"),
                Bytes::from("echo"),
                Bytes::from("0"),
                Bytes::from("hi"),
            ],
            false,
        );
        assert_eq!(r, RespValue::bulk(Bytes::from_static(b"hi")));
    }
}
