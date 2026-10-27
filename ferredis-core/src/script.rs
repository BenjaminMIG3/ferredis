//! Scripting Lua (EVAL / EVALSHA / SCRIPT / Functions) via mlua 5.1.
//!
//! Bibliothèques Redis : `cjson`, `cmsgpack`, `bit` ; `redis.call`/`pcall`/`log`/`setresp`.

use crate::command::{execute, ClientState, CommandContext};
use crate::command_registry;
use crate::resp::RespValue;
use bytes::Bytes;
use dashmap::DashMap;
use mlua::{HookTriggers, Lua, LuaOptions, StdLib, Value as LuaValue, VmState};
use parking_lot::Mutex;
use sha1::{Digest, Sha1};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

const SCRIPT_TIME_LIMIT: Duration = Duration::from_secs(5);
const MAX_DEPTH: usize = 64;

/// DEBUG : 0=NO, 1=YES, 2=SYNC.
static SCRIPT_DEBUG: AtomicU8 = AtomicU8::new(0);
static SCRIPT_BUSY: AtomicBool = AtomicBool::new(false);
static SCRIPT_KILL: AtomicBool = AtomicBool::new(false);
static SCRIPT_HAS_WRITES: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Default)]
pub struct ShebangMeta {
    pub engine: String,
    pub lib_name: Option<String>,
    pub no_writes: bool,
    pub allow_oom: bool,
}

#[derive(Default)]
pub struct ScriptCache {
    scripts: DashMap<String, String>,
}

pub fn sha1_hex(source: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(source);
    hex::encode(hasher.finalize())
}

impl ScriptCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load(&self, source: &str) -> String {
        let sha = sha1_hex(source.as_bytes());
        self.scripts
            .entry(sha.clone())
            .or_insert_with(|| source.to_string());
        sha
    }

    pub fn get(&self, sha: &str) -> Option<String> {
        self.scripts
            .get(&sha.to_ascii_lowercase())
            .map(|v| v.clone())
    }

    pub fn exists(&self, shas: &[String]) -> Vec<i64> {
        shas.iter()
            .map(|s| self.scripts.contains_key(&s.to_ascii_lowercase()) as i64)
            .collect()
    }

    pub fn flush(&self) {
        self.scripts.clear();
    }
}

pub type SharedScripts = Arc<ScriptCache>;

pub fn shared_scripts() -> SharedScripts {
    Arc::new(ScriptCache::new())
}

pub fn set_script_debug(mode: &str) -> Result<(), &'static str> {
    let v = match mode.to_ascii_uppercase().as_str() {
        "NO" => 0,
        "YES" => 1,
        "SYNC" => 2,
        _ => return Err("ERR Only YES|SYNC|NO debug modes are supported"),
    };
    SCRIPT_DEBUG.store(v, Ordering::SeqCst);
    Ok(())
}

pub fn script_debug_mode() -> u8 {
    SCRIPT_DEBUG.load(Ordering::SeqCst)
}

pub fn request_script_kill() -> Result<(), &'static str> {
    if !SCRIPT_BUSY.load(Ordering::SeqCst) {
        return Err("ERR No scripts in execution right now.");
    }
    if SCRIPT_HAS_WRITES.load(Ordering::SeqCst) {
        return Err(
            "ERR Sorry the script already executed write commands against the dataset. You can only call SCRIPT KILL from now on if you are sure that SCRIPT KILL cannot kill a script with writes (use SHUTDOWN NOSAVE and restart the server).",
        );
    }
    SCRIPT_KILL.store(true, Ordering::SeqCst);
    Ok(())
}

fn denied_in_script(cmd: &str) -> bool {
    matches!(
        cmd,
        "MULTI"
            | "EXEC"
            | "DISCARD"
            | "WATCH"
            | "UNWATCH"
            | "EVAL"
            | "EVALSHA"
            | "EVAL_RO"
            | "EVALSHA_RO"
            | "SCRIPT"
            | "FUNCTION"
            | "FCALL"
            | "FCALL_RO"
            | "SUBSCRIBE"
            | "PSUBSCRIBE"
            | "UNSUBSCRIBE"
            | "PUNSUBSCRIBE"
            | "SSUBSCRIBE"
            | "SUNSUBSCRIBE"
            | "AUTH"
            | "HELLO"
            | "QUIT"
            | "RESET"
    )
}

fn is_write_command(cmd: &str) -> bool {
    command_registry::lookup(cmd).is_some_and(|s| s.has_flag("write"))
}

/// Parse shebang Redis 7 (`#!lua flags=no-writes`).
pub fn parse_eval_shebang(source: &str) -> (ShebangMeta, &str) {
    let mut meta = ShebangMeta {
        engine: "LUA".into(),
        ..Default::default()
    };
    let trimmed = source.trim_start();
    if !trimmed.starts_with("#!") {
        return (meta, source);
    }
    let (first, rest) = match trimmed.find('\n') {
        Some(i) => (&trimmed[..i], &trimmed[i + 1..]),
        None => return (meta, ""),
    };
    let body = first.trim_start_matches("#!").trim();
    let mut parts = body.split_whitespace();
    if let Some(eng) = parts.next() {
        meta.engine = eng.to_ascii_uppercase();
    }
    for p in parts {
        if let Some(v) = p.strip_prefix("flags=") {
            for f in v.split(',') {
                match f.trim() {
                    "no-writes" => meta.no_writes = true,
                    "allow-oom" => meta.allow_oom = true,
                    _ => {}
                }
            }
        } else if let Some(v) = p.strip_prefix("name=") {
            meta.lib_name = Some(v.to_string());
        }
    }
    (meta, rest)
}

pub fn run_script(
    ctx: &CommandContext,
    state: &mut ClientState,
    source: &str,
    keys: &[Bytes],
    args: &[Bytes],
    read_only: bool,
) -> RespValue {
    let (shebang, body) = parse_eval_shebang(source);
    let ro = read_only || shebang.no_writes;
    let lua = match Lua::new_with(
        StdLib::TABLE | StdLib::STRING | StdLib::MATH,
        LuaOptions::new().catch_rust_panics(true),
    ) {
        Ok(l) => l,
        Err(e) => return RespValue::err(format!("ERR failed to create Lua VM: {e}")),
    };

    let shared = Arc::new(Mutex::new(std::mem::take(state)));
    SCRIPT_BUSY.store(true, Ordering::SeqCst);
    SCRIPT_KILL.store(false, Ordering::SeqCst);
    SCRIPT_HAS_WRITES.store(false, Ordering::SeqCst);
    let outcome = execute_in_vm(&lua, ctx, &shared, body, keys, args, ro, None);
    SCRIPT_BUSY.store(false, Ordering::SeqCst);
    *state = std::mem::take(&mut *shared.lock());

    match outcome {
        Ok(v) => v,
        Err(e) => script_error(&e),
    }
}

/// Exécute le corps d'une fonction Redis (KEYS/ARGV déjà injectés via convention Functions).
pub fn run_function(
    ctx: &CommandContext,
    state: &mut ClientState,
    fname: &str,
    body: &str,
    keys: &[Bytes],
    args: &[Bytes],
    read_only: bool,
) -> RespValue {
    let lua = match Lua::new_with(
        StdLib::TABLE | StdLib::STRING | StdLib::MATH,
        LuaOptions::new().catch_rust_panics(true),
    ) {
        Ok(l) => l,
        Err(e) => return RespValue::err(format!("ERR failed to create Lua VM: {e}")),
    };
    let shared = Arc::new(Mutex::new(std::mem::take(state)));
    SCRIPT_BUSY.store(true, Ordering::SeqCst);
    SCRIPT_KILL.store(false, Ordering::SeqCst);
    SCRIPT_HAS_WRITES.store(false, Ordering::SeqCst);
    // Corps = function(keys, args) ... end  → on l'appelle.
    let wrapper = format!("return ({body})(KEYS, ARGV)");
    let outcome = execute_in_vm(&lua, ctx, &shared, &wrapper, keys, args, read_only, Some(fname));
    SCRIPT_BUSY.store(false, Ordering::SeqCst);
    *state = std::mem::take(&mut *shared.lock());
    match outcome {
        Ok(v) => v,
        Err(e) => script_error(&e),
    }
}

#[derive(Clone)]
pub struct LoadedFn {
    pub name: String,
    pub body: String,
    pub no_writes: bool,
}

#[derive(Clone)]
pub struct CompiledLibFns {
    pub functions: Vec<LoadedFn>,
}

/// Charge une bibliothèque Functions et récupère les `redis.register_function`.
pub fn load_function_library(lib_name: &str, code: &str) -> Result<CompiledLibFns, String> {
    let (_meta, body) = parse_eval_shebang(code);
    let lua = Lua::new_with(
        StdLib::TABLE | StdLib::STRING | StdLib::MATH,
        LuaOptions::new().catch_rust_panics(true),
    )
    .map_err(|e| format!("ERR {e}"))?;

    let registered: Arc<Mutex<Vec<LoadedFn>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let globals = lua.globals();
        install_stdlib_libs(&lua, &globals).map_err(|e| format!("ERR {e}"))?;
        let redis = lua.create_table().map_err(|e| format!("ERR {e}"))?;
        let reg2 = Arc::clone(&registered);
        let register_var = lua
            .create_function(move |_lua, args: mlua::Variadic<LuaValue>| {
                if args.len() == 1 {
                    if let LuaValue::Table(t) = &args[0] {
                        let name: String = t
                            .get::<String>("function_name")
                            .or_else(|_| t.get::<String>("name"))
                            .map_err(|_| mlua::Error::runtime("ERR function_name missing"))?;
                        let callback: mlua::Function = t.get("callback")?;
                        let flags: Option<mlua::Table> = t.get("flags").ok();
                        let mut no_writes = false;
                        if let Some(f) = flags {
                            for pair in f.sequence_values::<String>() {
                                if pair? == "no-writes" {
                                    no_writes = true;
                                }
                            }
                        }
                        let dumped = callback.dump(false);
                        let body = format!("assert(loadstring({}))", format_lua_byte_string(&dumped));
                        reg2.lock().push(LoadedFn {
                            name,
                            body,
                            no_writes,
                        });
                        return Ok(());
                    }
                }
                if args.len() >= 2 {
                    let name = match &args[0] {
                        LuaValue::String(s) => String::from_utf8_lossy(&s.as_bytes()).into_owned(),
                        _ => {
                            return Err(mlua::Error::runtime("ERR function name must be a string"));
                        }
                    };
                    let callback = match &args[1] {
                        LuaValue::Function(f) => f.clone(),
                        _ => return Err(mlua::Error::runtime("ERR callback must be a function")),
                    };
                    let dumped = callback.dump(false);
                    let body = format!("assert(loadstring({}))", format_lua_byte_string(&dumped));
                    reg2.lock().push(LoadedFn {
                        name,
                        body,
                        no_writes: false,
                    });
                    return Ok(());
                }
                Err(mlua::Error::runtime("ERR wrong number of arguments to register_function"))
            })
            .map_err(|e| format!("ERR {e}"))?;

        redis.set("register_function", register_var).map_err(|e| format!("ERR {e}"))?;
        redis
            .set(
                "set_repl",
                lua.create_function(|_, _: LuaValue| Ok(())).unwrap(),
            )
            .ok();
        for (name, level) in [
            ("LOG_DEBUG", 0),
            ("LOG_VERBOSE", 1),
            ("LOG_NOTICE", 2),
            ("LOG_WARNING", 3),
        ] {
            redis.set(name, level).ok();
        }
        globals.set("redis", redis).map_err(|e| format!("ERR {e}"))?;
        let loadstring = lua
            .create_function(|lua, s: mlua::String| {
                let bytes = s.as_bytes().to_vec();
                lua.load(&bytes).into_function()
            })
            .map_err(|e| format!("ERR {e}"))?;
        globals.set("loadstring", loadstring).map_err(|e| format!("ERR {e}"))?;

        lua.load(body)
            .set_name(lib_name)
            .exec()
            .map_err(|e| format!("ERR Error compiling function library: {e}"))?;
    }

    let fns = registered.lock().clone();
    if fns.is_empty() {
        return Err("ERR No functions registered".into());
    }
    Ok(CompiledLibFns { functions: fns })
}

fn format_lua_byte_string(bytes: &[u8]) -> String {
    let mut s = String::from("\"");
    for b in bytes {
        s.push_str(&format!("\\{b:03}"));
    }
    s.push('"');
    s
}

fn execute_in_vm(
    lua: &Lua,
    ctx: &CommandContext,
    shared: &Arc<Mutex<ClientState>>,
    source: &str,
    keys: &[Bytes],
    args: &[Bytes],
    read_only: bool,
    _fname: Option<&str>,
) -> Result<RespValue, mlua::Error> {
    let globals = lua.globals();
    for banned in ["dofile", "loadfile", "collectgarbage"] {
        globals.set(banned, LuaValue::Nil)?;
    }
    install_stdlib_libs(lua, &globals)?;
    // loadstring for function bytecode
    let loadstring = lua.create_function(|lua, s: mlua::String| {
        let bytes = s.as_bytes();
        lua.load(&*bytes).into_function()
    })?;
    globals.set("loadstring", loadstring)?;

    globals.set("KEYS", bytes_table(lua, keys)?)?;
    globals.set("ARGV", bytes_table(lua, args)?)?;
    globals.set("redis", build_redis_table(lua, ctx, shared, read_only)?)?;

    let started = Instant::now();
    let kill_fns = Arc::clone(&ctx.functions);
    lua.set_hook(
        HookTriggers::new().every_nth_instruction(5_000),
        move |_lua, _dbg| {
            if SCRIPT_KILL.load(Ordering::SeqCst) || kill_fns.kill.load(Ordering::SeqCst) {
                return Err(mlua::Error::runtime(
                    "ERR Script killed by user with SCRIPT|FUNCTION KILL.",
                ));
            }
            if started.elapsed() > SCRIPT_TIME_LIMIT {
                Err(mlua::Error::runtime(
                    "ERR Script killed by user with SCRIPT KILL... (time limit exceeded)",
                ))
            } else {
                Ok(VmState::Continue)
            }
        },
    );

    if script_debug_mode() != 0 {
        debug!(target: "ferredis::lua", "executing script (debug on)");
    }

    let value = lua.load(source).set_name("user_script").eval::<LuaValue>()?;
    let proto = shared.lock().resp_version;
    Ok(lua_to_resp(&value, 0, proto))
}

fn install_stdlib_libs(lua: &Lua, globals: &mlua::Table) -> Result<(), mlua::Error> {
    globals.set("cjson", build_cjson(lua)?)?;
    globals.set("cmsgpack", build_cmsgpack(lua)?)?;
    globals.set("bit", build_bit(lua)?)?;
    Ok(())
}

fn build_cjson(lua: &Lua) -> Result<mlua::Table, mlua::Error> {
    let t = lua.create_table()?;
    t.set(
        "encode",
        lua.create_function(|lua, v: LuaValue| {
            let json = lua_to_json(&v, 0)?;
            let s = serde_json::to_string(&json).map_err(mlua::Error::external)?;
            lua.create_string(&s)
        })?,
    )?;
    t.set(
        "decode",
        lua.create_function(|lua, s: mlua::String| {
            let raw = s.as_bytes().to_vec();
            let text = String::from_utf8_lossy(&raw);
            let v: serde_json::Value =
                serde_json::from_str(&text).map_err(|e| mlua::Error::runtime(format!("Invalid JSON: {e}")))?;
            json_to_lua(lua, &v)
        })?,
    )?;
    t.set("null", lua.create_table()?)?; // sentinel table like lua-cjson
    Ok(t)
}

fn lua_to_json(v: &LuaValue, depth: usize) -> Result<serde_json::Value, mlua::Error> {
    if depth > MAX_DEPTH {
        return Err(mlua::Error::runtime("json encode depth exceeded"));
    }
    Ok(match v {
        LuaValue::Nil => serde_json::Value::Null,
        LuaValue::Boolean(b) => serde_json::Value::Bool(*b),
        LuaValue::Integer(i) => serde_json::json!(*i),
        LuaValue::Number(n) => serde_json::json!(*n),
        LuaValue::String(s) => serde_json::Value::String(String::from_utf8_lossy(&s.as_bytes()).into_owned()),
        LuaValue::Table(t) => {
            // array if consecutive integer keys from 1
            let mut is_array = true;
            let mut max = 0i64;
            let mut pairs_vec: Vec<(LuaValue, LuaValue)> = Vec::new();
            for pair in t.pairs::<LuaValue, LuaValue>() {
                let (k, v) = pair?;
                if let LuaValue::Integer(i) = k {
                    if i > 0 {
                        max = max.max(i);
                    } else {
                        is_array = false;
                    }
                } else {
                    is_array = false;
                }
                pairs_vec.push((k, v));
            }
            if is_array && !pairs_vec.is_empty() {
                let mut arr = vec![serde_json::Value::Null; max as usize];
                for (k, v) in pairs_vec {
                    if let LuaValue::Integer(i) = k {
                        arr[(i - 1) as usize] = lua_to_json(&v, depth + 1)?;
                    }
                }
                serde_json::Value::Array(arr)
            } else {
                let mut map = serde_json::Map::new();
                for (k, v) in pairs_vec {
                    let key = match k {
                        LuaValue::String(s) => String::from_utf8_lossy(&s.as_bytes()).into_owned(),
                        LuaValue::Integer(i) => i.to_string(),
                        LuaValue::Number(n) => n.to_string(),
                        _ => continue,
                    };
                    map.insert(key, lua_to_json(&v, depth + 1)?);
                }
                serde_json::Value::Object(map)
            }
        }
        _ => serde_json::Value::Null,
    })
}

fn json_to_lua(lua: &Lua, v: &serde_json::Value) -> Result<LuaValue, mlua::Error> {
    Ok(match v {
        serde_json::Value::Null => LuaValue::Nil,
        serde_json::Value::Bool(b) => LuaValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                LuaValue::Integer(i)
            } else {
                LuaValue::Number(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => LuaValue::String(lua.create_string(s)?),
        serde_json::Value::Array(a) => {
            let t = lua.create_table_with_capacity(a.len(), 0)?;
            for (i, item) in a.iter().enumerate() {
                t.raw_set(i + 1, json_to_lua(lua, item)?)?;
            }
            LuaValue::Table(t)
        }
        serde_json::Value::Object(o) => {
            let t = lua.create_table_with_capacity(0, o.len())?;
            for (k, v) in o {
                t.set(k.as_str(), json_to_lua(lua, v)?)?;
            }
            LuaValue::Table(t)
        }
    })
}

fn build_cmsgpack(lua: &Lua) -> Result<mlua::Table, mlua::Error> {
    let t = lua.create_table()?;
    t.set(
        "pack",
        lua.create_function(|lua, args: mlua::Variadic<LuaValue>| {
            let mut buf = Vec::new();
            if args.len() == 1 {
                msgpack_encode(&args[0], &mut buf, 0)?;
            } else {
                // pack multiple as array
                msgpack_encode_array_header(&mut buf, args.len());
                for a in args.iter() {
                    msgpack_encode(a, &mut buf, 0)?;
                }
            }
            lua.create_string(&buf)
        })?,
    )?;
    t.set(
        "unpack",
        lua.create_function(|lua, s: mlua::String| {
            let bytes = s.as_bytes();
            let mut i = 0;
            msgpack_decode(lua, &bytes, &mut i)
        })?,
    )?;
    Ok(t)
}

fn msgpack_encode_array_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x90 | (len as u8));
    } else if len <= u16::MAX as usize {
        buf.push(0xdc);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xdd);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

fn msgpack_encode(v: &LuaValue, buf: &mut Vec<u8>, depth: usize) -> Result<(), mlua::Error> {
    if depth > MAX_DEPTH {
        return Err(mlua::Error::runtime("cmsgpack depth"));
    }
    match v {
        LuaValue::Nil => buf.push(0xc0),
        LuaValue::Boolean(false) => buf.push(0xc2),
        LuaValue::Boolean(true) => buf.push(0xc3),
        LuaValue::Integer(i) => {
            let i = *i;
            if (0..128).contains(&i) {
                buf.push(i as u8);
            } else if (-32..0).contains(&i) {
                buf.push(i as u8);
            } else if (i32::MIN as i64..=i32::MAX as i64).contains(&i) {
                buf.push(0xd2);
                buf.extend_from_slice(&(i as i32).to_be_bytes());
            } else {
                buf.push(0xd3);
                buf.extend_from_slice(&i.to_be_bytes());
            }
        }
        LuaValue::Number(n) => {
            buf.push(0xcb);
            buf.extend_from_slice(&n.to_be_bytes());
        }
        LuaValue::String(s) => {
            let b = s.as_bytes();
            if b.len() < 32 {
                buf.push(0xa0 | (b.len() as u8));
            } else if b.len() <= u8::MAX as usize {
                buf.push(0xd9);
                buf.push(b.len() as u8);
            } else {
                buf.push(0xda);
                buf.extend_from_slice(&(b.len() as u16).to_be_bytes());
            }
            buf.extend_from_slice(&b);
        }
        LuaValue::Table(t) => {
            let mut pairs_vec: Vec<(LuaValue, LuaValue)> = Vec::new();
            let mut max = 0i64;
            let mut is_array = true;
            for pair in t.pairs::<LuaValue, LuaValue>() {
                let (k, v) = pair?;
                if let LuaValue::Integer(i) = k {
                    if i > 0 {
                        max = max.max(i);
                    } else {
                        is_array = false;
                    }
                } else {
                    is_array = false;
                }
                pairs_vec.push((k, v));
            }
            if is_array {
                msgpack_encode_array_header(buf, max as usize);
                for i in 1..=max {
                    let item = t.raw_get::<LuaValue>(i).unwrap_or(LuaValue::Nil);
                    msgpack_encode(&item, buf, depth + 1)?;
                }
            } else {
                let len = pairs_vec.len();
                if len < 16 {
                    buf.push(0x80 | (len as u8));
                } else {
                    buf.push(0xde);
                    buf.extend_from_slice(&(len as u16).to_be_bytes());
                }
                for (k, v) in pairs_vec {
                    msgpack_encode(&k, buf, depth + 1)?;
                    msgpack_encode(&v, buf, depth + 1)?;
                }
            }
        }
        _ => buf.push(0xc0),
    }
    Ok(())
}

fn msgpack_decode(lua: &Lua, data: &[u8], i: &mut usize) -> Result<LuaValue, mlua::Error> {
    if *i >= data.len() {
        return Err(mlua::Error::runtime("cmsgpack truncated"));
    }
    let b = data[*i];
    *i += 1;
    Ok(match b {
        0xc0 => LuaValue::Nil,
        0xc2 => LuaValue::Boolean(false),
        0xc3 => LuaValue::Boolean(true),
        0xcb => {
            let mut a = [0u8; 8];
            a.copy_from_slice(&data[*i..*i + 8]);
            *i += 8;
            LuaValue::Number(f64::from_be_bytes(a))
        }
        0xd2 => {
            let mut a = [0u8; 4];
            a.copy_from_slice(&data[*i..*i + 4]);
            *i += 4;
            LuaValue::Integer(i32::from_be_bytes(a) as i64)
        }
        0xd3 => {
            let mut a = [0u8; 8];
            a.copy_from_slice(&data[*i..*i + 8]);
            *i += 8;
            LuaValue::Integer(i64::from_be_bytes(a))
        }
        b if b < 0x80 => LuaValue::Integer(b as i64),
        b if b >= 0xe0 => LuaValue::Integer((b as i8) as i64),
        b if (0xa0..0xc0).contains(&b) => {
            let len = (b & 0x1f) as usize;
            let s = lua.create_string(&data[*i..*i + len])?;
            *i += len;
            LuaValue::String(s)
        }
        0xd9 => {
            let len = data[*i] as usize;
            *i += 1;
            let s = lua.create_string(&data[*i..*i + len])?;
            *i += len;
            LuaValue::String(s)
        }
        b if (0x90..0xa0).contains(&b) => {
            let len = (b & 0x0f) as usize;
            let t = lua.create_table_with_capacity(len, 0)?;
            for n in 0..len {
                t.raw_set(n + 1, msgpack_decode(lua, data, i)?)?;
            }
            LuaValue::Table(t)
        }
        b if (0x80..0x90).contains(&b) => {
            let len = (b & 0x0f) as usize;
            let t = lua.create_table_with_capacity(0, len)?;
            for _ in 0..len {
                let k = msgpack_decode(lua, data, i)?;
                let v = msgpack_decode(lua, data, i)?;
                t.set(k, v)?;
            }
            LuaValue::Table(t)
        }
        _ => return Err(mlua::Error::runtime("cmsgpack unsupported type")),
    })
}

fn build_bit(lua: &Lua) -> Result<mlua::Table, mlua::Error> {
    let t = lua.create_table()?;
    let tobit = |n: mlua::Number| -> i32 { n as i64 as i32 };
    t.set(
        "tobit",
        lua.create_function(move |_, n: f64| Ok(tobit(n) as i64))?,
    )?;
    t.set(
        "bnot",
        lua.create_function(move |_, n: f64| Ok((!tobit(n)) as i64))?,
    )?;
    t.set(
        "band",
        lua.create_function(move |_, args: mlua::Variadic<f64>| {
            let mut r = !0i32;
            for a in args.iter() {
                r &= tobit(*a);
            }
            Ok(r as i64)
        })?,
    )?;
    t.set(
        "bor",
        lua.create_function(move |_, args: mlua::Variadic<f64>| {
            let mut r = 0i32;
            for a in args.iter() {
                r |= tobit(*a);
            }
            Ok(r as i64)
        })?,
    )?;
    t.set(
        "bxor",
        lua.create_function(move |_, args: mlua::Variadic<f64>| {
            let mut r = 0i32;
            for a in args.iter() {
                r ^= tobit(*a);
            }
            Ok(r as i64)
        })?,
    )?;
    t.set(
        "lshift",
        lua.create_function(move |_, (n, s): (f64, f64)| {
            Ok((tobit(n).wrapping_shl((s as i32 as u32) & 31)) as i64)
        })?,
    )?;
    t.set(
        "rshift",
        lua.create_function(move |_, (n, s): (f64, f64)| {
            Ok(((tobit(n) as u32) >> ((s as i32 as u32) & 31)) as i32 as i64)
        })?,
    )?;
    t.set(
        "arshift",
        lua.create_function(move |_, (n, s): (f64, f64)| {
            Ok((tobit(n) >> ((s as i32 as u32) & 31)) as i64)
        })?,
    )?;
    t.set(
        "rol",
        lua.create_function(move |_, (n, s): (f64, f64)| {
            Ok(tobit(n).rotate_left((s as i32 as u32) & 31) as i64)
        })?,
    )?;
    t.set(
        "ror",
        lua.create_function(move |_, (n, s): (f64, f64)| {
            Ok(tobit(n).rotate_right((s as i32 as u32) & 31) as i64)
        })?,
    )?;
    t.set(
        "bswap",
        lua.create_function(move |_, n: f64| Ok(tobit(n).swap_bytes() as i64))?,
    )?;
    t.set(
        "tohex",
        lua.create_function(move |_, (n, nhex): (f64, Option<f64>)| {
            let v = tobit(n) as u32;
            let w = nhex.map(|x| x as i32).unwrap_or(8).unsigned_abs().clamp(1, 8) as usize;
            Ok(format!("{v:0width$x}", width = w))
        })?,
    )?;
    Ok(t)
}

fn bytes_table(lua: &Lua, items: &[Bytes]) -> Result<mlua::Table, mlua::Error> {
    let t = lua.create_table_with_capacity(items.len(), 0)?;
    for (i, b) in items.iter().enumerate() {
        t.raw_set(i + 1, lua.create_string(b.as_ref())?)?;
    }
    Ok(t)
}

fn build_redis_table(
    lua: &Lua,
    ctx: &CommandContext,
    shared: &Arc<Mutex<ClientState>>,
    read_only: bool,
) -> Result<mlua::Table, mlua::Error> {
    let redis = lua.create_table()?;
    let initial = shared.lock().resp_version;
    let resp_ver = Arc::new(AtomicU8::new(initial));

    let call = {
        let (ctx, shared, ro) = (ctx.clone(), Arc::clone(shared), read_only);
        lua.create_function(move |lua, cmd_args: mlua::Variadic<LuaValue>| {
            match call_redis(lua, &ctx, &shared, cmd_args, ro)? {
                RespValue::Error(e) => Err(mlua::Error::runtime(String::from_utf8_lossy(&e).into_owned())),
                other => resp_to_lua(lua, &other),
            }
        })?
    };
    let pcall = {
        let (ctx, shared, ro) = (ctx.clone(), Arc::clone(shared), read_only);
        lua.create_function(move |lua, cmd_args: mlua::Variadic<LuaValue>| {
            match call_redis(lua, &ctx, &shared, cmd_args, ro) {
                Ok(resp) => resp_to_lua(lua, &resp),
                Err(e) => resp_to_lua(lua, &RespValue::err(root_message(&e))),
            }
        })?
    };
    redis.set("call", call)?;
    redis.set("pcall", pcall)?;

    redis.set(
        "error_reply",
        lua.create_function(|lua, msg: mlua::String| {
            let t = lua.create_table()?;
            t.set("err", msg)?;
            Ok(t)
        })?,
    )?;
    redis.set(
        "status_reply",
        lua.create_function(|lua, msg: mlua::String| {
            let t = lua.create_table()?;
            t.set("ok", msg)?;
            Ok(t)
        })?,
    )?;
    redis.set(
        "sha1hex",
        lua.create_function(|_, data: mlua::String| Ok(sha1_hex(&data.as_bytes())))?,
    )?;

    let resp_set = Arc::clone(&resp_ver);
    let shared_resp = Arc::clone(shared);
    redis.set(
        "setresp",
        lua.create_function(move |_, v: i64| {
            if v != 2 && v != 3 {
                return Err(mlua::Error::runtime("ERR RESP version must be 2 or 3"));
            }
            let ver = v as u8;
            resp_set.store(ver, Ordering::SeqCst);
            shared_resp.lock().resp_version = ver;
            if let Some(info) = &shared_resp.lock().info {
                info.set_resp(ver);
            }
            Ok(())
        })?,
    )?;

    redis.set(
        "log",
        lua.create_function(|_, args: mlua::Variadic<LuaValue>| {
            let level = match args.first() {
                Some(LuaValue::Integer(i)) => *i,
                Some(LuaValue::Number(n)) => *n as i64,
                _ => 2,
            };
            let msg = args
                .iter()
                .skip(1)
                .map(|v| match v {
                    LuaValue::String(s) => String::from_utf8_lossy(&s.as_bytes()).into_owned(),
                    LuaValue::Integer(i) => i.to_string(),
                    LuaValue::Number(n) => n.to_string(),
                    LuaValue::Boolean(b) => b.to_string(),
                    _ => String::from("?"),
                })
                .collect::<Vec<_>>()
                .join(" ");
            match level {
                0 => debug!(target: "ferredis::lua", "{msg}"),
                1 => info!(target: "ferredis::lua", "{msg}"),
                2 => info!(target: "ferredis::lua", "{msg}"),
                _ => {
                    warn!(target: "ferredis::lua", "{msg}");
                    if level >= 3 {
                        error!(target: "ferredis::lua", "{msg}");
                    }
                }
            }
            Ok(())
        })?,
    )?;

    redis.set("set_repl", lua.create_function(|_, _: LuaValue| Ok(()))?)?;
    redis.set("replicate_commands", lua.create_function(|_, ()| Ok(true))?)?;
    for (name, level) in [
        ("LOG_DEBUG", 0),
        ("LOG_VERBOSE", 1),
        ("LOG_NOTICE", 2),
        ("LOG_WARNING", 3),
    ] {
        redis.set(name, level)?;
    }
    redis.set("REPL_NONE", 0)?;
    redis.set("REPL_AOF", 1)?;
    redis.set("REPL_SLAVE", 2)?;
    redis.set("REPL_REPLICA", 2)?;
    redis.set("REPL_ALL", 3)?;
    redis.set("REDIS_VERSION", "7.2.0")?;
    redis.set("REDIS_VERSION_NUM", 0x0007_0200)?;
    Ok(redis)
}

fn call_redis(
    _lua: &Lua,
    ctx: &CommandContext,
    shared: &Arc<Mutex<ClientState>>,
    cmd_args: mlua::Variadic<LuaValue>,
    read_only: bool,
) -> Result<RespValue, mlua::Error> {
    let mut argv: Vec<Bytes> = Vec::with_capacity(cmd_args.len());
    for v in cmd_args.iter() {
        argv.push(lua_arg_to_bytes(v)?);
    }
    if argv.is_empty() {
        return Err(mlua::Error::runtime(
            "ERR Please specify at least one argument for this redis lib call",
        ));
    }
    let name = String::from_utf8_lossy(&argv[0]).to_ascii_uppercase();
    if denied_in_script(&name) {
        return Err(mlua::Error::runtime(
            "ERR This Redis command is not allowed from script",
        ));
    }
    if read_only && is_write_command(&name) {
        return Err(mlua::Error::runtime(
            "ERR Write commands are not allowed from read-only scripts",
        ));
    }
    if is_write_command(&name) {
        SCRIPT_HAS_WRITES.store(true, Ordering::SeqCst);
    }
    let mut st = shared.lock();
    let reply = execute(ctx, &mut st, &name, &argv);
    Ok(reply.into_iter().next().unwrap_or_else(RespValue::null_bulk))
}

fn lua_arg_to_bytes(v: &LuaValue) -> Result<Bytes, mlua::Error> {
    match v {
        LuaValue::String(s) => Ok(Bytes::copy_from_slice(&s.as_bytes())),
        LuaValue::Integer(i) => Ok(Bytes::from(i.to_string())),
        LuaValue::Number(n) => Ok(Bytes::from(crate::util::fmt_f64(*n))),
        _ => Err(mlua::Error::runtime(
            "ERR Lua redis lib command arguments must be strings or integers",
        )),
    }
}

fn resp_to_lua(lua: &Lua, resp: &RespValue) -> Result<LuaValue, mlua::Error> {
    Ok(match resp {
        RespValue::BulkString(Some(s)) => LuaValue::String(lua.create_string(s.as_ref())?),
        RespValue::BulkString(None) | RespValue::Array(None) => LuaValue::Boolean(false),
        RespValue::Integer(n) => LuaValue::Number(*n as f64),
        RespValue::SimpleString(s) => {
            let t = lua.create_table()?;
            t.set("ok", lua.create_string(s.as_ref())?)?;
            LuaValue::Table(t)
        }
        RespValue::Error(e) => {
            let t = lua.create_table()?;
            t.set("err", lua.create_string(e.as_ref())?)?;
            LuaValue::Table(t)
        }
        RespValue::Array(Some(items)) => {
            let t = lua.create_table_with_capacity(items.len(), 0)?;
            for (i, item) in items.iter().enumerate() {
                t.raw_set(i + 1, resp_to_lua(lua, item)?)?;
            }
            LuaValue::Table(t)
        }
        RespValue::Null => LuaValue::Nil,
        RespValue::Bool(b) => LuaValue::Boolean(*b),
        RespValue::Double(n) => LuaValue::Number(*n),
        RespValue::BigNumber(b) | RespValue::Verbatim { data: b, .. } => {
            LuaValue::String(lua.create_string(b.as_ref())?)
        }
        RespValue::Map(entries) => {
            let t = lua.create_table_with_capacity(0, entries.len())?;
            for (k, v) in entries {
                let key = match k {
                    RespValue::BulkString(Some(b)) | RespValue::SimpleString(b) => {
                        lua.create_string(b.as_ref())?
                    }
                    other => lua.create_string(format!("{other}").as_bytes())?,
                };
                t.raw_set(key, resp_to_lua(lua, v)?)?;
            }
            LuaValue::Table(t)
        }
        RespValue::Set(items) | RespValue::Push(items) => {
            let t = lua.create_table_with_capacity(items.len(), 0)?;
            for (i, item) in items.iter().enumerate() {
                t.raw_set(i + 1, resp_to_lua(lua, item)?)?;
            }
            LuaValue::Table(t)
        }
        RespValue::Attribute { value, .. } => resp_to_lua(lua, value)?,
    })
}

fn lua_to_resp(v: &LuaValue, depth: usize, proto: u8) -> RespValue {
    match v {
        LuaValue::Nil => {
            if proto >= 3 {
                RespValue::Null
            } else {
                RespValue::null_bulk()
            }
        }
        LuaValue::Boolean(false) => {
            if proto >= 3 {
                RespValue::Bool(false)
            } else {
                RespValue::null_bulk()
            }
        }
        LuaValue::Boolean(true) => {
            if proto >= 3 {
                RespValue::Bool(true)
            } else {
                RespValue::Integer(1)
            }
        }
        LuaValue::Integer(i) => RespValue::Integer(*i as i64),
        LuaValue::Number(n) => {
            if proto >= 3 && n.fract() != 0.0 {
                RespValue::Double(*n)
            } else {
                RespValue::Integer(*n as i64)
            }
        }
        LuaValue::String(s) => RespValue::bulk(Bytes::copy_from_slice(&s.as_bytes())),
        LuaValue::Table(t) if depth < MAX_DEPTH => {
            if let Ok(LuaValue::String(e)) = t.raw_get::<LuaValue>("err") {
                return RespValue::Error(Bytes::copy_from_slice(&e.as_bytes()));
            }
            if let Ok(LuaValue::String(s)) = t.raw_get::<LuaValue>("ok") {
                return RespValue::SimpleString(Bytes::copy_from_slice(&s.as_bytes()));
            }
            let mut items = Vec::new();
            let mut i = 1i64;
            while let Ok(item) = t.raw_get::<LuaValue>(i) {
                if matches!(item, LuaValue::Nil) {
                    break;
                }
                items.push(lua_to_resp(&item, depth + 1, proto));
                i += 1;
            }
            RespValue::array(items)
        }
        _ => {
            if proto >= 3 {
                RespValue::Null
            } else {
                RespValue::null_bulk()
            }
        }
    }
}

fn root_message(e: &mlua::Error) -> String {
    match e {
        mlua::Error::CallbackError { cause, .. } => root_message(cause),
        mlua::Error::RuntimeError(s) => s.clone(),
        mlua::Error::SyntaxError { message, .. } => message.clone(),
        other => other.to_string(),
    }
}

fn script_error(e: &mlua::Error) -> RespValue {
    let msg = root_message(e);
    let first_line = msg.lines().next().unwrap_or("").trim();
    let code_len = first_line.find(' ').unwrap_or(first_line.len());
    let has_code = code_len >= 3
        && first_line[..code_len]
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b == b'_');
    if matches!(e, mlua::Error::SyntaxError { .. }) {
        RespValue::err(format!("ERR Error compiling script (new function): {first_line}"))
    } else if has_code {
        RespValue::err(first_line)
    } else {
        RespValue::err(format!("ERR Error running script: {first_line}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::dispatch;

    async fn eval(ctx: &CommandContext, state: &mut ClientState, script: &str, rest: &[&str]) -> RespValue {
        let mut args = vec![Bytes::from("EVAL"), Bytes::from(script.to_string())];
        args.extend(rest.iter().map(|s| Bytes::from(s.to_string())));
        dispatch(ctx, state, &args).await.remove(0)
    }

    fn setup() -> (CommandContext, ClientState) {
        (
            CommandContext::new(Default::default()),
            ClientState {
                authenticated: true,
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn keys_argv_and_conversions() {
        let (ctx, mut st) = setup();
        let r = eval(&ctx, &mut st, "return {KEYS[1], ARGV[1], 3.9, true, false}", &["1", "k", "a"]).await;
        assert_eq!(
            r,
            RespValue::array(vec![
                RespValue::bulk("k"),
                RespValue::bulk("a"),
                RespValue::Integer(3),
                RespValue::Integer(1),
                RespValue::null_bulk(),
            ])
        );
    }

    #[tokio::test]
    async fn call_pcall_and_errors() {
        let (ctx, mut st) = setup();
        eval(&ctx, &mut st, "return redis.call('SET', KEYS[1], 'v')", &["1", "k"]).await;
        let r = eval(&ctx, &mut st, "return redis.call('GET', KEYS[1])", &["1", "k"]).await;
        assert_eq!(r, RespValue::bulk("v"));
        let r = eval(&ctx, &mut st, "return redis.pcall('LPUSH', KEYS[1], 'x')", &["1", "k"]).await;
        assert!(matches!(r, RespValue::Error(e) if e.starts_with(b"WRONGTYPE")));
        let r = eval(&ctx, &mut st, "return redis.call('LPUSH', KEYS[1], 'x')", &["1", "k"]).await;
        assert!(matches!(r, RespValue::Error(e) if e.starts_with(b"WRONGTYPE")));
        let r = eval(&ctx, &mut st, "return redis.call('GET', 'absent') == false", &["0"]).await;
        assert_eq!(r, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn cjson_and_bit() {
        let (ctx, mut st) = setup();
        let r = eval(&ctx, &mut st, "return cjson.encode({a=1})", &["0"]).await;
        assert!(matches!(r, RespValue::BulkString(Some(b)) if b.as_ref().contains(&b'a')));
        let r = eval(&ctx, &mut st, "return bit.band(7, 3)", &["0"]).await;
        assert_eq!(r, RespValue::Integer(3));
    }

    #[tokio::test]
    async fn eval_ro_rejects_writes() {
        let (ctx, mut st) = setup();
        let args = [
            Bytes::from("EVAL_RO"),
            Bytes::from("return redis.call('SET', KEYS[1], 'x')"),
            Bytes::from("1"),
            Bytes::from("k"),
        ];
        let r = dispatch(&ctx, &mut st, &args).await.remove(0);
        let msg = match &r {
            RespValue::Error(e) => String::from_utf8_lossy(e).into_owned(),
            other => panic!("{other:?}"),
        };
        assert!(
            msg.contains("Write commands") || msg.contains("read-only"),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn channels_redis_scripts_run() {
        let (ctx, mut st) = setup();
        let cleanup = "
            local backed_up = redis.call('ZRANGE', ARGV[2], 0, -1, 'WITHSCORES')
            local backed_up_len = #backed_up
            for i = backed_up_len, 1, -2 do
                redis.call('ZADD', ARGV[1], backed_up[i], backed_up[i - 1])
            end
            redis.call('DEL', ARGV[2])";
        dispatch(&ctx, &mut st, &[Bytes::from("ZADD"), Bytes::from("bak"), Bytes::from("1.5"), Bytes::from("m")]).await;
        eval(&ctx, &mut st, cleanup, &["0", "chan", "bak"]).await;
        let r = dispatch(&ctx, &mut st, &[Bytes::from("ZRANGE"), Bytes::from("chan"), Bytes::from("0"), Bytes::from("-1")]).await;
        assert_eq!(r[0], RespValue::array(vec![RespValue::bulk("m")]));
        let del = "
            local keys = redis.call('keys', ARGV[1])
            for i=1,#keys,5000 do
                redis.call('del', unpack(keys, i, math.min(i+4999, #keys)))
            end
            return #keys";
        let r = eval(&ctx, &mut st, del, &["0", "ch*"]).await;
        assert_eq!(r, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn sandbox_and_denied_commands() {
        let (ctx, mut st) = setup();
        let r = eval(&ctx, &mut st, "return type(os)", &["0"]).await;
        assert_eq!(r, RespValue::bulk("nil"));
        let r = eval(&ctx, &mut st, "return redis.pcall('EVAL', 'return 1', 0)", &["0"]).await;
        assert!(matches!(r, RespValue::Error(e) if e.starts_with(b"ERR This Redis command")));
        let r = eval(&ctx, &mut st, "return +", &["0"]).await;
        assert!(matches!(r, RespValue::Error(e) if e.starts_with(b"ERR Error compiling")));
    }
}
