//! DUMP / RESTORE / RESTORE-ASKING / MIGRATE / WAITAOF.

use crate::command::{arg_str, wrong_arity, ClientState, CommandContext};
use crate::rdb::{create_dump_payload, verify_and_decode_dump};
use crate::resp::RespValue;
use crate::value::Entry;
use bytes::Bytes;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub fn cmd_dump(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return wrong_arity("dump");
    }
    match ctx.db.get_entry(state.db_index, &args[1]) {
        None => RespValue::null_bulk(),
        Some(e) => {
            let payload = create_dump_payload(&e.value);
            RespValue::bulk(Bytes::from(payload))
        }
    }
}

pub fn cmd_restore(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    restore_inner(ctx, state, args, false)
}

pub fn cmd_restore_asking(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    // ASKING is a cluster concern (P8); semantics match RESTORE here.
    restore_inner(ctx, state, args, true)
}

fn restore_inner(
    ctx: &CommandContext,
    state: &mut ClientState,
    args: &[Bytes],
    _asking: bool,
) -> RespValue {
    // RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
    if args.len() < 4 {
        return wrong_arity("restore");
    }
    let key = args[1].clone();
    let ttl = match parse_i64(&args[2]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e),
    };
    let payload = &args[3];
    let mut replace = false;
    let mut absttl = false;
    let mut i = 4;
    while i < args.len() {
        let opt = arg_str(args, i).unwrap_or("").to_ascii_uppercase();
        match opt.as_str() {
            "REPLACE" => {
                replace = true;
                i += 1;
            }
            "ABSTTL" => {
                absttl = true;
                i += 1;
            }
            "IDLETIME" => {
                if i + 1 >= args.len() {
                    return RespValue::err("ERR syntax error");
                }
                if parse_i64(&args[i + 1]).is_err() {
                    return RespValue::err("ERR IDLETIME must be >= 0");
                }
                // Idle time ignored until P12 (LFU/LRU clocks).
                i += 2;
            }
            "FREQ" => {
                if i + 1 >= args.len() {
                    return RespValue::err("ERR syntax error");
                }
                if parse_i64(&args[i + 1]).is_err() {
                    return RespValue::err("ERR FREQ must be >= 0");
                }
                i += 2;
            }
            _ => return RespValue::err("ERR syntax error"),
        }
    }

    let value = match verify_and_decode_dump(payload) {
        Ok(v) => v,
        Err(_) => return RespValue::err("ERR DUMP payload version or checksum are wrong"),
    };

    let mut entry = Entry::new(value);
    if ttl > 0 {
        if absttl {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            if ttl > now {
                entry.expires_at = Some(Instant::now() + Duration::from_millis((ttl - now) as u64));
            } else {
                entry.expires_at = Some(Instant::now());
            }
        } else {
            entry.expires_at = Some(Instant::now() + Duration::from_millis(ttl as u64));
        }
    }

    match ctx.db.put_entry(state.db_index, key, entry, replace) {
        Ok(()) => {
            ctx.propagate(args);
            RespValue::ok()
        }
        Err("BUSYKEY") => RespValue::err("BUSYKEY Target key name already exists."),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

pub fn cmd_migrate(ctx: &CommandContext, state: &mut ClientState, args: &[Bytes]) -> RespValue {
    // MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE] [AUTH password|AUTH2 user pass] [KEYS k...]
    if args.len() < 6 {
        return wrong_arity("migrate");
    }
    let host = match arg_str(args, 1) {
        Some(h) => h.to_string(),
        None => return RespValue::err("ERR invalid host"),
    };
    let port: u16 = match arg_str(args, 2).and_then(|p| p.parse().ok()) {
        Some(p) => p,
        None => return RespValue::err("ERR invalid port"),
    };
    let key_arg = args[3].clone();
    let dest_db: usize = match arg_str(args, 4).and_then(|d| d.parse().ok()) {
        Some(d) => d,
        None => return RespValue::err("ERR invalid DB index"),
    };
    let timeout_ms: u64 = match arg_str(args, 5).and_then(|t| t.parse().ok()) {
        Some(t) => t,
        None => return RespValue::err("ERR invalid timeout"),
    };

    let mut copy = false;
    let mut replace = false;
    let mut auth: Option<(Option<String>, String)> = None;
    let mut keys: Vec<Bytes> = Vec::new();
    let mut i = 6;
    while i < args.len() {
        let opt = arg_str(args, i).unwrap_or("").to_ascii_uppercase();
        match opt.as_str() {
            "COPY" => {
                copy = true;
                i += 1;
            }
            "REPLACE" => {
                replace = true;
                i += 1;
            }
            "AUTH" => {
                if i + 1 >= args.len() {
                    return RespValue::err("ERR syntax error");
                }
                auth = Some((None, String::from_utf8_lossy(&args[i + 1]).into_owned()));
                i += 2;
            }
            "AUTH2" => {
                if i + 2 >= args.len() {
                    return RespValue::err("ERR syntax error");
                }
                auth = Some((
                    Some(String::from_utf8_lossy(&args[i + 1]).into_owned()),
                    String::from_utf8_lossy(&args[i + 2]).into_owned(),
                ));
                i += 3;
            }
            "KEYS" => {
                i += 1;
                while i < args.len() {
                    keys.push(args[i].clone());
                    i += 1;
                }
            }
            _ => return RespValue::err("ERR syntax error"),
        }
    }

    if keys.is_empty() {
        if key_arg.is_empty() {
            return RespValue::err(
                "ERR When using the empty string as key name, KEYS option must be used",
            );
        }
        keys.push(key_arg);
    }

    let mut to_move: Vec<(Bytes, Entry)> = Vec::new();
    for k in &keys {
        if let Some(e) = ctx.db.get_entry(state.db_index, k) {
            to_move.push((k.clone(), e));
        }
    }
    if to_move.is_empty() {
        return RespValue::simple(Bytes::from_static(b"NOKEY"));
    }

    let sock_addr = {
        use std::net::ToSocketAddrs;
        match (host.as_str(), port).to_socket_addrs() {
            Ok(mut addrs) => match addrs.next() {
                Some(a) => a,
                None => return RespValue::err("ERR IOERR cannot resolve host"),
            },
            Err(e) => return RespValue::err(format!("ERR IOERR {e}")),
        }
    };
    let mut stream =
        match TcpStream::connect_timeout(&sock_addr, Duration::from_millis(timeout_ms.max(1))) {
            Ok(s) => s,
            Err(e) => return RespValue::err(format!("ERR IOERR {e}")),
        };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(timeout_ms.max(1))));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(timeout_ms.max(1))));

    if let Some((user, pass)) = &auth {
        let auth_args: Vec<Bytes> = if let Some(u) = user {
            vec![
                Bytes::from_static(b"AUTH"),
                Bytes::from(u.clone()),
                Bytes::from(pass.clone()),
            ]
        } else {
            vec![Bytes::from_static(b"AUTH"), Bytes::from(pass.clone())]
        };
        if let Err(e) = redis_call(&mut stream, &auth_args) {
            return RespValue::err(format!("ERR IOERR {e}"));
        }
    }

    let select = [
        Bytes::from_static(b"SELECT"),
        Bytes::from(dest_db.to_string()),
    ];
    if let Err(e) = redis_call(&mut stream, &select) {
        return RespValue::err(format!("ERR IOERR {e}"));
    }

    for (key, entry) in &to_move {
        let payload = create_dump_payload(&entry.value);
        let ttl_ms = entry.pttl_ms().max(0);
        let mut restore_args = vec![
            Bytes::from_static(b"RESTORE"),
            key.clone(),
            Bytes::from(ttl_ms.to_string()),
            Bytes::from(payload),
        ];
        if replace {
            restore_args.push(Bytes::from_static(b"REPLACE"));
        }
        match redis_call(&mut stream, &restore_args) {
            Ok(r) if r.starts_with('+') || r.contains("OK") => {}
            Ok(r) => return RespValue::err(format!("ERR Target instance replied with error: {r}")),
            Err(e) => return RespValue::err(format!("ERR IOERR {e}")),
        }
    }

    if !copy {
        let del_keys: Vec<Bytes> = to_move.iter().map(|(k, _)| k.clone()).collect();
        ctx.db.del(state.db_index, &del_keys);
        let mut aof = vec![Bytes::from_static(b"DEL")];
        aof.extend(del_keys);
        ctx.propagate(&aof);
    }
    RespValue::ok()
}

pub fn cmd_waitaof(ctx: &CommandContext, _state: &mut ClientState, args: &[Bytes]) -> RespValue {
    // WAITAOF numlocal numreplicas timeout
    if args.len() != 4 {
        return wrong_arity("waitaof");
    }
    let numlocal: i64 = match arg_str(args, 1).and_then(|s| s.parse().ok()) {
        Some(n) if n >= 0 => n,
        _ => return RespValue::err("ERR numlocal must be >= 0"),
    };
    let numreplicas: i64 = match arg_str(args, 2).and_then(|s| s.parse().ok()) {
        Some(n) if n >= 0 => n,
        _ => return RespValue::err("ERR numreplicas must be >= 0"),
    };
    let timeout: u64 = match arg_str(args, 3).and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return RespValue::err("ERR timeout is not an integer or out of range"),
    };

    if numlocal > 0 && !ctx.aof.enabled() {
        return RespValue::err(
            "ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.",
        );
    }

    let local_ok = if numlocal > 0 {
        let target = ctx.aof.pending_gen();
        if ctx.aof.wait_fsync(target, timeout) {
            1
        } else {
            0
        }
    } else {
        0
    };

    let repl_ok = if numreplicas > 0 {
        crate::repl::wait_replica_acks(ctx, numreplicas, timeout)
    } else {
        0
    };
    RespValue::array(vec![RespValue::Integer(local_ok), RespValue::Integer(repl_ok)])
}

fn parse_i64(b: &Bytes) -> Result<i64, String> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "ERR value is not an integer or out of range".into())
}

fn redis_call(stream: &mut TcpStream, args: &[Bytes]) -> std::io::Result<String> {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
    stream.write_all(&buf)?;
    stream.flush()?;
    let mut resp = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            break;
        }
        resp.extend_from_slice(&tmp[..n]);
        if resp.windows(2).any(|w| w == b"\r\n") {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&resp).into_owned())
}
