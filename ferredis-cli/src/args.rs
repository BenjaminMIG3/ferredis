//! Options alignées sur `redis-cli` officiel (noms, shorts, env).

use clap::{ArgAction, Parser, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum OutputMode {
    #[default]
    Auto,
    Raw,
    NoRaw,
    Csv,
    Json,
    QuotedJson,
}

#[derive(Parser, Debug)]
#[command(
    name = "ferredis-cli",
    about = "redis-cli compatible client for Ferredis / Redis",
    version,
    disable_help_flag = true,
    disable_version_flag = true
)]
pub struct Args {
    /// Server hostname (default: 127.0.0.1).
    #[arg(short = 'h', long = "hostname", default_value = "127.0.0.1", visible_alias = "host")]
    pub host: String,

    /// Server port (default: 6379).
    #[arg(short = 'p', long, default_value_t = 6379)]
    pub port: u16,

    /// Server connection timeout in seconds (0 = OS default).
    #[arg(short = 't', long = "timeout", default_value_t = 0.0, allow_hyphen_values = true)]
    pub timeout: f64,

    /// Server socket (overrides hostname and port).
    #[arg(short = 's', long = "socket")]
    pub socket: Option<String>,

    /// Password (`REDISCLI_AUTH` if omitted; `-a` wins over env).
    #[arg(short = 'a', long = "pass")]
    pub auth: Option<String>,

    /// ACL username (AUTH user pass). Needs `-a` / `--pass`.
    #[arg(long = "user")]
    pub user: Option<String>,

    /// Force password prompt (ignores `-a` and `REDISCLI_AUTH`).
    #[arg(long = "askpass", action = ArgAction::SetTrue)]
    pub askpass: bool,

    /// Server URI: redis://user:password@host:port/dbnum
    #[arg(short = 'u', long = "uri")]
    pub uri: Option<String>,

    /// Execute command N times.
    #[arg(short = 'r', long = "repeat", default_value_t = 1)]
    pub repeat: u64,

    /// Interval seconds when `-r` / scan / latency (sub-second ok).
    #[arg(short = 'i', long = "interval", default_value_t = 0.0, allow_hyphen_values = true)]
    pub interval: f64,

    /// Database number.
    #[arg(short = 'n', long = "db", default_value_t = 0)]
    pub db: usize,

    /// CLIENT SETNAME after connect.
    #[arg(long = "name")]
    pub client_name: Option<String>,

    /// Start session in RESP2 (HELLO 2).
    #[arg(short = '2', action = ArgAction::SetTrue)]
    pub resp2: bool,

    /// Start session in RESP3 (HELLO 3).
    #[arg(short = '3', action = ArgAction::SetTrue)]
    pub resp3: bool,

    /// Read last argument from STDIN.
    #[arg(short = 'x', action = ArgAction::SetTrue)]
    pub stdin_arg: bool,

    /// Read named argument from STDIN (placeholder tag).
    #[arg(short = 'X', long = "stdin-key")]
    pub stdin_key: Option<String>,

    /// Delimiter between response bulks for raw formatting.
    #[arg(short = 'd', long = "delimiter", default_value = "\n")]
    pub delimiter: String,

    /// Delimiter between responses for raw formatting.
    #[arg(short = 'D', long = "mb-delim", default_value = "\n")]
    pub mb_delim: String,

    /// Enable cluster mode (follow MOVED/ASK).
    #[arg(short = 'c', long = "cluster", action = ArgAction::SetTrue)]
    pub cluster: bool,

    /// Exit with error code when command fails.
    #[arg(short = 'e', action = ArgAction::SetTrue)]
    pub exit_on_error: bool,

    /// Prefer IPv4.
    #[arg(short = '4', action = ArgAction::SetTrue)]
    pub ipv4: bool,

    /// Prefer IPv6.
    #[arg(short = '6', action = ArgAction::SetTrue)]
    pub ipv6: bool,

    /// Use raw formatting for replies.
    #[arg(long = "raw", action = ArgAction::SetTrue)]
    pub raw: bool,

    /// Force formatted output even when STDOUT is not a tty.
    #[arg(long = "no-raw", action = ArgAction::SetTrue)]
    pub no_raw: bool,

    /// Force input to be handled as quoted strings.
    #[arg(long = "quoted-input", action = ArgAction::SetTrue)]
    pub quoted_input: bool,

    /// Output in CSV format.
    #[arg(long = "csv", action = ArgAction::SetTrue)]
    pub csv: bool,

    /// Output in JSON format.
    #[arg(long = "json", action = ArgAction::SetTrue)]
    pub json: bool,

    /// Same as `--json` with ASCII-safe strings.
    #[arg(long = "quoted-json", action = ArgAction::SetTrue)]
    pub quoted_json: bool,

    /// List all keys using SCAN.
    #[arg(long = "scan", action = ArgAction::SetTrue)]
    pub scan: bool,

    /// Keys pattern for `--scan` (default: *).
    #[arg(long = "pattern", default_value = "*")]
    pub pattern: String,

    /// COUNT option for SCAN modes (default: 10).
    #[arg(long = "count", default_value_t = 10)]
    pub count: i64,

    /// Continuously sample latency (PING).
    #[arg(long = "latency", action = ArgAction::SetTrue)]
    pub latency: bool,

    /// Verbose mode.
    #[arg(long = "verbose", action = ArgAction::SetTrue)]
    pub verbose: bool,

    /// Don't show warning when password is on the CLI.
    #[arg(long = "no-auth-warning", action = ArgAction::SetTrue)]
    pub no_auth_warning: bool,

    /// Output this help and exit.
    #[arg(long = "help", action = ArgAction::HelpLong)]
    pub help: Option<bool>,

    /// Output version and exit.
    #[arg(long = "version", action = ArgAction::Version)]
    pub version: Option<bool>,

    /// Non-interactive command args.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub cmd: Vec<String>,
}

impl Args {
    pub fn output_mode(&self) -> OutputMode {
        if self.quoted_json {
            OutputMode::QuotedJson
        } else if self.json {
            OutputMode::Json
        } else if self.csv {
            OutputMode::Csv
        } else if self.raw {
            OutputMode::Raw
        } else if self.no_raw {
            OutputMode::NoRaw
        } else {
            OutputMode::Auto
        }
    }
}
