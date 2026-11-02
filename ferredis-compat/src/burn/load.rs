//! Charge / concurrence / pipeline — résistance opérationnelle style Redis.

use super::harness::{fmt_resp, BurnCtx};
use bytes::BytesMut;
use ferredis_core::resp::{parse, RespError, RespValue};
use ferredis_core::server::write_command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const CLIENTS: usize = 32;
const OPS_PER_CLIENT: usize = 200;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "load";
    let addr = ctx.addr;
    let errors = Arc::new(AtomicU64::new(0));
    let ok_ops = Arc::new(AtomicU64::new(0));

    // --- Concurrence SET/GET/INCR multi-clients ---
    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(CLIENTS);
    for client_id in 0..CLIENTS {
        let errors = Arc::clone(&errors);
        let ok_ops = Arc::clone(&ok_ops);
        handles.push(tokio::spawn(async move {
            let mut sock = match TcpStream::connect(addr).await {
                Ok(s) => s,
                Err(_) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            for i in 0..OPS_PER_CLIENT {
                let key = format!("burn:load:{client_id}:{i}");
                let val = format!("{i}");
                if roundtrip(&mut sock, &["SET", &key, &val]).await != Some(RespValue::ok()) {
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                match roundtrip(&mut sock, &["GET", &key]).await {
                    Some(RespValue::BulkString(Some(b)))
                        if String::from_utf8_lossy(&b) == val =>
                    {
                        ok_ops.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if roundtrip(&mut sock, &["INCR", &format!("burn:ctr:{client_id}")])
                    .await
                    .is_none()
                {
                    errors.fetch_add(1, Ordering::Relaxed);
                } else {
                    ok_ops.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let elapsed = t0.elapsed();
    let err_n = errors.load(Ordering::Relaxed);
    let ok_n = ok_ops.load(Ordering::Relaxed);
    let expected_min_ok = (CLIENTS * OPS_PER_CLIENT) as u64;
    ctx.check(
        S,
        "concurrent_set_get_incr",
        elapsed,
        err_n == 0 && ok_n >= expected_min_ok,
        format!(
            "clients={CLIENTS} ops/client={OPS_PER_CLIENT} ok_ops={ok_n} errors={err_n} elapsed={elapsed:?} — Redis doit rester cohérent sous charge"
        ),
    );

    let mut c = ctx.connect().await;
    for client_id in 0..CLIENTS {
        let key = format!("burn:ctr:{client_id}");
        ctx.expect_pred(
            S,
            &format!("incr_final_{client_id}"),
            &mut c,
            &["GET", &key],
            |v| match v {
                RespValue::BulkString(Some(b)) => {
                    let n: i64 = String::from_utf8_lossy(b).parse().map_err(|e| format!("{e}"))?;
                    if n == OPS_PER_CLIENT as i64 {
                        Ok(())
                    } else {
                        Err(format!(
                            "INCR concurrent Redis = {OPS_PER_CLIENT}, got {n}"
                        ))
                    }
                }
                _ => Err("GET counter → bulk".into()),
            },
        )
        .await;
    }

    // --- Pipeline 500 commandes ---
    let t0 = Instant::now();
    let mut sock = ctx.connect().await;
    let mut payload = Vec::new();
    const PIPE: usize = 500;
    for i in 0..PIPE {
        let n = i.to_string();
        payload.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\np{n}\r\n${}\r\n{n}\r\n",
                n.len() + 1,
                n.len()
            )
            .as_bytes(),
        );
    }
    payload.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$3\r\np42\r\n");
    if let Err(e) = sock.write_all(&payload).await {
        ctx.err_case(S, "pipeline_write", t0.elapsed(), e.to_string());
    } else {
        let mut buf = BytesMut::new();
        let mut replies = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        while replies.len() < PIPE + 1 && Instant::now() < deadline {
            match parse(&mut buf) {
                Ok(v) => replies.push(v),
                Err(RespError::Incomplete) => {
                    let mut tmp = [0u8; 8192];
                    match tokio::time::timeout(Duration::from_secs(2), sock.read(&mut tmp)).await {
                        Ok(Ok(0)) => break,
                        Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
                        Ok(Err(e)) => {
                            ctx.err_case(S, "pipeline_read", t0.elapsed(), e.to_string());
                            break;
                        }
                        Err(_) => break,
                    }
                }
                Err(e) => {
                    ctx.err_case(S, "pipeline_parse", t0.elapsed(), e.to_string());
                    break;
                }
            }
        }
        let all_ok = replies.len() == PIPE + 1
            && replies[..PIPE].iter().all(|r| *r == RespValue::ok())
            && replies[PIPE] == RespValue::bulk(bytes::Bytes::from_static(b"42"));
        ctx.check(
            S,
            "pipeline_order",
            t0.elapsed(),
            all_ok,
            format!(
                "pipeline Redis : {PIPE} OK + GET=42 ; got {} replies, last={}",
                replies.len(),
                replies.last().map(fmt_resp).unwrap_or_default()
            ),
        );
    }

    // --- Multi-DB sous charge ---
    let errors = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for db in 0..8u8 {
        let errors = Arc::clone(&errors);
        handles.push(tokio::spawn(async move {
            let mut sock = match TcpStream::connect(addr).await {
                Ok(s) => s,
                Err(_) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            let db_s = db.to_string();
            if roundtrip(&mut sock, &["SELECT", &db_s]).await != Some(RespValue::ok()) {
                errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
            for i in 0..100 {
                let k = format!("mdb:{i}");
                let v = format!("db{db}");
                if roundtrip(&mut sock, &["SET", &k, &v]).await != Some(RespValue::ok()) {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            for i in 0..100 {
                let k = format!("mdb:{i}");
                let expect = format!("db{db}");
                match roundtrip(&mut sock, &["GET", &k]).await {
                    Some(RespValue::BulkString(Some(b)))
                        if String::from_utf8_lossy(&b) == expect => {}
                    _ => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    ctx.check(
        S,
        "multidb_concurrent_isolation",
        t0.elapsed(),
        errors.load(Ordering::Relaxed) == 0,
        format!(
            "8 DB × 100 clés isolées ; errors={}",
            errors.load(Ordering::Relaxed)
        ),
    );

    // --- Pub/Sub sous charge ---
    let mut sub = ctx.connect().await;
    let _ = ctx.cmd(&mut sub, &["SUBSCRIBE", "burn:load:ch"]).await;
    let received = Arc::new(AtomicU64::new(0));
    let recv_flag = Arc::clone(&received);
    let reader = tokio::spawn(async move {
        let mut buf = BytesMut::new();
        let mut tmp = [0u8; 4096];
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline && recv_flag.load(Ordering::Relaxed) < 100 {
            match parse(&mut buf) {
                Ok(RespValue::Array(Some(items)))
                    if items.first()
                        == Some(&RespValue::bulk(bytes::Bytes::from_static(b"message"))) =>
                {
                    recv_flag.fetch_add(1, Ordering::Relaxed);
                }
                Ok(_) => {}
                Err(RespError::Incomplete) => {
                    match tokio::time::timeout(Duration::from_millis(500), sub.read(&mut tmp)).await
                    {
                        Ok(Ok(0)) | Err(_) => break,
                        Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
                        Ok(Err(_)) => break,
                    }
                }
                Err(_) => break,
            }
        }
    });
    let mut pubc = ctx.connect().await;
    for i in 0..100 {
        let msg = i.to_string();
        let _ = ctx.cmd(&mut pubc, &["PUBLISH", "burn:load:ch", &msg]).await;
    }
    let _ = tokio::time::timeout(Duration::from_secs(3), reader).await;
    let got = received.load(Ordering::Relaxed);
    ctx.check(
        S,
        "pubsub_burst_100",
        Duration::from_secs(0),
        got >= 90,
        format!("PUBLISH×100 → messages reçus Redis≈100, got {got}"),
    );
}

async fn roundtrip(sock: &mut TcpStream, args: &[&str]) -> Option<RespValue> {
    let raw: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
    write_command(sock, &raw).await.ok()?;
    let mut buf = BytesMut::new();
    let mut tmp = [0u8; 4096];
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if Instant::now() > deadline {
            return None;
        }
        match parse(&mut buf) {
            Ok(v) => return Some(v),
            Err(RespError::Incomplete) => {
                let n = tokio::time::timeout(Duration::from_secs(1), sock.read(&mut tmp))
                    .await
                    .ok()?
                    .ok()?;
                if n == 0 {
                    return None;
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            Err(_) => return None,
        }
    }
}
