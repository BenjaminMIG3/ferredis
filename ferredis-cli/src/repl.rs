//! Boucle interactive style redis-cli (historique + Tab completion).

use crate::args::Args;
use crate::completion::RedisHelper;
use crate::connect::Session;
use crate::format::Formatter;
use crate::tokenize::split_args;
use anyhow::Context;
use ferredis_core::RespValue;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::{Config, Editor};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

pub async fn run_repl(session: &mut Session, args: &Args, fmt: &Formatter) -> anyhow::Result<()> {
    let config = Config::builder()
        .completion_type(rustyline::CompletionType::List)
        .build();
    let mut rl: Editor<RedisHelper, DefaultHistory> = Editor::with_config(config)?;
    rl.set_helper(Some(RedisHelper::new()));

    let hist_path = history_path();
    let _ = rl.load_history(&hist_path);

    println!("ferredis-cli connected to {}.", session.display_host);
    if args.verbose {
        println!("db={}, cluster={}", session.db, session.cluster);
    }

    loop {
        let prompt = session.prompt();
        let line = match rl.readline(&prompt) {
            Ok(l) => l,
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(e) => return Err(e.into()),
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let _ = rl.add_history_entry(trimmed);

        if trimmed.eq_ignore_ascii_case("clear") {
            print!("\x1B[2J\x1B[1;1H");
            continue;
        }
        if trimmed.eq_ignore_ascii_case("help") || trimmed.eq_ignore_ascii_case("help ") {
            print_help(trimmed);
            continue;
        }
        if trimmed.to_ascii_lowercase().starts_with("help ") {
            print_help(trimmed);
            continue;
        }

        let parts = split_args(trimmed, args.quoted_input);
        if parts.is_empty() {
            continue;
        }

        if parts[0].eq_ignore_ascii_case("exit") || parts[0].eq_ignore_ascii_case("quit") {
            let _ = session.stream.write_command(&[b"QUIT"]).await;
            let _ = session.stream.read_resp().await;
            break;
        }

        if let Err(e) = exec_line(session, &parts, args, fmt).await {
            eprintln!("{e}");
            if args.exit_on_error {
                let _ = rl.save_history(&hist_path);
                std::process::exit(1);
            }
        }
    }

    let _ = rl.save_history(&hist_path);
    Ok(())
}

async fn exec_line(
    session: &mut Session,
    parts: &[String],
    args: &Args,
    fmt: &Formatter,
) -> anyhow::Result<()> {
    let refs: Vec<&[u8]> = parts.iter().map(|s| s.as_bytes()).collect();
    let cmd = parts[0].to_ascii_lowercase();

    session.stream.write_command(&refs).await?;

    let is_sub = matches!(
        cmd.as_str(),
        "subscribe" | "psubscribe" | "ssubscribe" | "monitor"
    );

    if is_sub {
        loop {
            let resp = session.stream.read_resp().await?;
            fmt.print(&resp)?;
        }
    }

    let mut resp = session.stream.read_resp().await?;

    // Cluster redirect : un retry
    if let RespValue::Error(ref e) = resp {
        let msg = String::from_utf8_lossy(e);
        if session.follow_redirect_if_needed(&msg).await?.is_some() {
            session.stream.write_command(&refs).await?;
            resp = session.stream.read_resp().await?;
        }
    }

    // Suivi local de SELECT
    if cmd == "select" && !Formatter::is_error(&resp) {
        if let Some(idx) = parts.get(1).and_then(|s| s.parse().ok()) {
            session.db = idx;
        }
    }
    if cmd == "reset" && !Formatter::is_error(&resp) {
        session.db = 0;
    }

    fmt.print(&resp)?;

    if args.exit_on_error && Formatter::is_error(&resp) {
        anyhow::bail!("{}", resp);
    }
    Ok(())
}

pub async fn run_oneshot(
    session: &mut Session,
    args: &Args,
    fmt: &Formatter,
    mut cmd: Vec<String>,
) -> anyhow::Result<i32> {
    apply_stdin_args(&mut cmd, args)?;

    let mut exit = 0i32;
    let reps = args.repeat.max(1);
    for i in 0..reps {
        if i > 0 && args.interval > 0.0 {
            sleep(Duration::from_secs_f64(args.interval)).await;
        }
        let refs: Vec<&[u8]> = cmd.iter().map(|s| s.as_bytes()).collect();
        session.stream.write_command(&refs).await?;
        let mut resp = session.stream.read_resp().await?;

        if let RespValue::Error(ref e) = resp {
            let msg = String::from_utf8_lossy(e);
            if session.follow_redirect_if_needed(&msg).await?.is_some() {
                session.stream.write_command(&refs).await?;
                resp = session.stream.read_resp().await?;
            }
        }

        fmt.print(&resp)?;
        if i + 1 < reps {
            print!("{}", args.mb_delim);
        }
        if Formatter::is_error(&resp) {
            exit = 1;
            if args.exit_on_error {
                break;
            }
        }
    }
    Ok(exit)
}

fn apply_stdin_args(cmd: &mut Vec<String>, args: &Args) -> anyhow::Result<()> {
    use std::io::Read;
    if args.stdin_arg {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("read stdin")?;
        let data = buf.trim_end_matches(['\r', '\n']).to_string();
        cmd.push(data);
    } else if let Some(tag) = &args.stdin_key {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("read stdin")?;
        let data = buf.trim_end_matches(['\r', '\n']).to_string();
        for a in cmd.iter_mut() {
            if a == tag {
                *a = data.clone();
            }
        }
    }
    Ok(())
}

fn history_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".rediscli_history")
}

fn print_help(line: &str) {
    let topic = line
        .split_whitespace()
        .nth(1)
        .map(|s| s.to_ascii_uppercase());
    match topic.as_deref() {
        None => {
            println!("ferredis-cli");
            println!("Type a Redis command, or HELP <command>.");
            println!("Tab completes command names from the server command table.");
            println!("Special: CLEAR, EXIT, QUIT, HELP");
        }
        Some(name) => {
            if let Some(spec) = ferredis_core::command_registry::spec_by_name(name) {
                println!(
                    "{} arity={} flags={:?}",
                    spec.name.to_ascii_lowercase(),
                    spec.arity,
                    spec.flags
                );
            } else {
                println!("(error) no help for '{}'", name.to_ascii_lowercase());
            }
        }
    }
}
