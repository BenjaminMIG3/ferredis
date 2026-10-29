//! Modes spéciaux redis-cli : --scan, --latency.

use crate::args::Args;
use crate::connect::Session;
use anyhow::Context;
use ferredis_core::RespValue;
use std::io::{self, IsTerminal, Write};
use std::time::{Duration, Instant};
use tokio::time::sleep;

pub async fn run_scan(session: &mut Session, args: &Args) -> anyhow::Result<()> {
    let mut cursor = 0u64;
    loop {
        let cur = cursor.to_string();
        let count = args.count.to_string();
        session
            .stream
            .write_command(&[
                b"SCAN",
                cur.as_bytes(),
                b"MATCH",
                args.pattern.as_bytes(),
                b"COUNT",
                count.as_bytes(),
            ])
            .await?;
        let resp = session.stream.read_resp().await?;
        let (next, keys) = parse_scan(&resp).context("unexpected SCAN reply")?;
        for k in keys {
            println!("{k}");
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
        if args.interval > 0.0 {
            sleep(Duration::from_secs_f64(args.interval)).await;
        }
    }
    Ok(())
}

fn parse_scan(resp: &RespValue) -> Option<(u64, Vec<String>)> {
    let items = match resp {
        RespValue::Array(Some(a)) if a.len() == 2 => a,
        _ => return None,
    };
    let cursor = match &items[0] {
        RespValue::BulkString(Some(b)) | RespValue::SimpleString(b) => {
            std::str::from_utf8(b).ok()?.parse().ok()?
        }
        RespValue::Integer(n) => *n as u64,
        _ => return None,
    };
    let keys = match &items[1] {
        RespValue::Array(Some(arr)) => arr
            .iter()
            .filter_map(|v| match v {
                RespValue::BulkString(Some(b)) | RespValue::SimpleString(b) => {
                    Some(String::from_utf8_lossy(b).into_owned())
                }
                _ => None,
            })
            .collect(),
        _ => return None,
    };
    Some((cursor, keys))
}

pub async fn run_latency(session: &mut Session, args: &Args) -> anyhow::Result<()> {
    let interval = if args.interval > 0.0 {
        args.interval
    } else {
        1.0
    };
    let mut min = f64::MAX;
    let mut max = 0.0f64;
    let mut sum = 0.0f64;
    let mut count = 0u64;
    let tty = io::stdout().is_terminal();

    if !tty || args.raw || args.csv {
        // Sample pendant `interval` secondes puis une ligne
        let deadline = Instant::now() + Duration::from_secs_f64(interval);
        while Instant::now() < deadline {
            let sample = ping_ms(session).await?;
            min = min.min(sample);
            max = max.max(sample);
            sum += sample;
            count += 1;
        }
        let avg = if count == 0 { 0.0 } else { sum / count as f64 };
        if args.csv {
            println!("{min:.3},{max:.3},{avg:.3},{count}");
        } else {
            println!("min={min:.3}, max={max:.3}, avg={avg:.3}, samples={count}");
        }
        return Ok(());
    }

    loop {
        let sample = ping_ms(session).await?;
        min = min.min(sample);
        max = max.max(sample);
        sum += sample;
        count += 1;
        let avg = sum / count as f64;
        print!(
            "\rmin: {min:.3}, max: {max:.3}, avg: {avg:.3} ({count} samples)"
        );
        io::stdout().flush()?;
        sleep(Duration::from_secs_f64(interval.min(0.1).max(0.01))).await;
    }
}

async fn ping_ms(session: &mut Session) -> anyhow::Result<f64> {
    let t0 = Instant::now();
    session.stream.write_command(&[b"PING"]).await?;
    let _ = session.stream.read_resp().await?;
    Ok(t0.elapsed().as_secs_f64() * 1000.0)
}
