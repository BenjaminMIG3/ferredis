use anyhow::Context;
use bytes::BytesMut;
use clap::Parser;
use ferredis_core::resp::{parse, RespError, RespValue};
use ferredis_core::server::{read_resp, write_command};
use std::io::{self, BufRead, Write};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(name = "ferredis-cli", about = "Interactive Ferredis / Redis CLI")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// Optional password
    #[arg(short = 'a', long)]
    auth: Option<String>,

    /// Non-interactive command args
    #[arg(trailing_var_arg = true)]
    cmd: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);
    let mut socket = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("connect to {addr}"))?;
    socket.set_nodelay(true)?;

    if let Some(pass) = &args.auth {
        write_command(&mut socket, &[b"AUTH", pass.as_bytes()]).await?;
        let resp = read_resp(&mut socket).await?;
        if matches!(resp, RespValue::Error(_)) {
            anyhow::bail!("{resp}");
        }
    }

    if !args.cmd.is_empty() {
        let refs: Vec<&[u8]> = args.cmd.iter().map(|s| s.as_bytes()).collect();
        write_command(&mut socket, &refs).await?;
        let resp = read_resp(&mut socket).await?;
        println!("{resp}");
        return Ok(());
    }

    println!("ferredis-cli connected to {addr}");
    println!("Type commands (e.g. PING, GET key). Ctrl+C to exit.");
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    loop {
        print!("{addr}> ");
        stdout.flush()?;
        let mut line = String::new();
        if stdin.lock().read_line(&mut line)? == 0 {
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.eq_ignore_ascii_case("exit") || line.eq_ignore_ascii_case("quit") {
            write_command(&mut socket, &[b"QUIT"]).await?;
            let _ = read_resp(&mut socket).await;
            break;
        }
        let parts = split_args(line);
        if parts.is_empty() {
            continue;
        }
        let refs: Vec<&[u8]> = parts.iter().map(|s| s.as_bytes()).collect();
        write_command(&mut socket, &refs).await?;

        // SUBSCRIBE: keep reading messages
        let is_sub = parts[0].eq_ignore_ascii_case("subscribe")
            || parts[0].eq_ignore_ascii_case("psubscribe");
        if is_sub {
            loop {
                let resp = read_resp(&mut socket).await?;
                println!("{resp}");
            }
        } else {
            let resp = read_resp(&mut socket).await?;
            println!("{resp}");
        }
    }
    Ok(())
}

fn split_args(line: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    for ch in line.chars() {
        match ch {
            '"' => in_quote = !in_quote,
            ' ' if !in_quote => {
                if !cur.is_empty() {
                    args.push(std::mem::take(&mut cur));
                }
            }
            _ => cur.push(ch),
        }
    }
    if !cur.is_empty() {
        args.push(cur);
    }
    args
}

#[allow(dead_code)]
fn _parse_helpers() {
    let mut b = BytesMut::new();
    let _ = parse(&mut b);
    let _ = RespError::Incomplete;
}
