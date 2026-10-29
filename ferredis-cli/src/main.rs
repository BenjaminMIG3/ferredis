//! ferredis-cli — client compatible redis-cli.

mod args;
mod completion;
mod connect;
mod format;
mod hints;
mod modes;
mod repl;
mod tokenize;
mod uri;

use args::Args;
use clap::Parser;
use connect::connect;
use format::Formatter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let fmt = Formatter::new(args.output_mode(), args.delimiter.clone());
    let mut session = connect(&args).await?;

    if args.latency {
        modes::run_latency(&mut session, &args).await?;
        return Ok(());
    }

    if args.scan {
        modes::run_scan(&mut session, &args).await?;
        return Ok(());
    }

    if !args.cmd.is_empty() {
        let code = repl::run_oneshot(&mut session, &args, &fmt, args.cmd.clone()).await?;
        if code != 0 {
            std::process::exit(code);
        }
        return Ok(());
    }

    repl::run_repl(&mut session, &args, &fmt).await
}
