//! Rendu des réponses RESP (tty / raw / csv / json) — style redis-cli.

use crate::args::OutputMode;
use ferredis_core::RespValue;
use std::io::{self, IsTerminal, Write};

pub struct Formatter {
    mode: OutputMode,
    tty: bool,
    bulk_delim: String,
}

impl Formatter {
    pub fn new(mode: OutputMode, bulk_delim: String) -> Self {
        Self {
            mode,
            tty: io::stdout().is_terminal(),
            bulk_delim,
        }
    }

    fn use_raw(&self) -> bool {
        match self.mode {
            OutputMode::Raw => true,
            OutputMode::NoRaw | OutputMode::Csv | OutputMode::Json | OutputMode::QuotedJson => false,
            OutputMode::Auto => !self.tty,
        }
    }

    pub fn print(&self, resp: &RespValue) -> io::Result<()> {
        let mut out = io::stdout().lock();
        match self.mode {
            OutputMode::Csv => {
                write_csv(&mut out, resp)?;
                writeln!(out)?;
            }
            OutputMode::Json => {
                writeln!(out, "{}", to_json(resp, false))?;
            }
            OutputMode::QuotedJson => {
                writeln!(out, "{}", to_json(resp, true))?;
            }
            _ if self.use_raw() => {
                write_raw(&mut out, resp, &self.bulk_delim)?;
                if !self.bulk_delim.ends_with('\n') {
                    writeln!(out)?;
                }
            }
            _ => writeln!(out, "{resp}")?,
        }
        out.flush()
    }

    pub fn is_error(resp: &RespValue) -> bool {
        matches!(resp, RespValue::Error(_))
    }
}

fn write_raw(out: &mut impl Write, resp: &RespValue, delim: &str) -> io::Result<()> {
    match resp {
        RespValue::SimpleString(s) | RespValue::Error(s) | RespValue::BigNumber(s) => {
            out.write_all(s)?;
        }
        RespValue::Integer(n) => write!(out, "{n}")?,
        RespValue::Null | RespValue::BulkString(None) | RespValue::Array(None) => {}
        RespValue::BulkString(Some(s)) => out.write_all(s)?,
        RespValue::Array(Some(items)) | RespValue::Set(items) | RespValue::Push(items) => {
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.write_all(delim.as_bytes())?;
                }
                write_raw(out, item, delim)?;
            }
        }
        RespValue::Bool(v) => write!(out, "{}", if *v { "1" } else { "0" })?,
        RespValue::Double(n) => write!(out, "{n}")?,
        RespValue::Verbatim { data, .. } => out.write_all(data)?,
        RespValue::Map(entries) => {
            for (i, (k, v)) in entries.iter().enumerate() {
                if i > 0 {
                    out.write_all(delim.as_bytes())?;
                }
                write_raw(out, k, delim)?;
                out.write_all(delim.as_bytes())?;
                write_raw(out, v, delim)?;
            }
        }
        RespValue::Attribute { value, .. } => write_raw(out, value, delim)?,
    }
    Ok(())
}

fn write_csv(out: &mut impl Write, resp: &RespValue) -> io::Result<()> {
    match resp {
        RespValue::Array(Some(items)) | RespValue::Set(items) | RespValue::Push(items) => {
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    write!(out, ",")?;
                }
                write_csv_atom(out, item)?;
            }
        }
        other => write_csv_atom(out, other)?,
    }
    Ok(())
}

fn write_csv_atom(out: &mut impl Write, resp: &RespValue) -> io::Result<()> {
    let s = match resp {
        RespValue::SimpleString(b)
        | RespValue::Error(b)
        | RespValue::BulkString(Some(b))
        | RespValue::BigNumber(b)
        | RespValue::Verbatim { data: b, .. } => String::from_utf8_lossy(b).into_owned(),
        RespValue::Integer(n) => n.to_string(),
        RespValue::Bool(v) => if *v { "true" } else { "false" }.into(),
        RespValue::Double(n) => n.to_string(),
        RespValue::Null | RespValue::BulkString(None) | RespValue::Array(None) => String::new(),
        other => other.to_string(),
    };
    write!(out, "\"{}\"", s.replace('"', "\"\""))
}

fn to_json(resp: &RespValue, quoted_ascii: bool) -> String {
    fn escape(s: &str, quoted_ascii: bool) -> String {
        if quoted_ascii {
            let mut out = String::with_capacity(s.len() + 2);
            out.push('"');
            for c in s.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    c if c.is_ascii_control() || !c.is_ascii() => {
                        out.push_str(&format!("\\u{:04x}", c as u32));
                    }
                    c => out.push(c),
                }
            }
            out.push('"');
            out
        } else {
            serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\""))
        }
    }

    match resp {
        RespValue::SimpleString(b) | RespValue::BulkString(Some(b)) | RespValue::BigNumber(b) => {
            escape(&String::from_utf8_lossy(b), quoted_ascii)
        }
        RespValue::Error(b) => format!(
            "{{\"error\":{}}}",
            escape(&String::from_utf8_lossy(b), quoted_ascii)
        ),
        RespValue::Integer(n) => n.to_string(),
        RespValue::Null | RespValue::BulkString(None) | RespValue::Array(None) => "null".into(),
        RespValue::Bool(v) => if *v { "true" } else { "false" }.into(),
        RespValue::Double(n) => {
            if n.is_finite() {
                n.to_string()
            } else {
                escape(&n.to_string(), quoted_ascii)
            }
        }
        RespValue::Array(Some(items)) | RespValue::Set(items) | RespValue::Push(items) => {
            let inner: Vec<_> = items.iter().map(|i| to_json(i, quoted_ascii)).collect();
            format!("[{}]", inner.join(","))
        }
        RespValue::Map(entries) => {
            let mut parts = Vec::with_capacity(entries.len());
            for (k, v) in entries {
                let key = match k {
                    RespValue::SimpleString(b) | RespValue::BulkString(Some(b)) => {
                        escape(&String::from_utf8_lossy(b), quoted_ascii)
                    }
                    other => escape(&other.to_string(), quoted_ascii),
                };
                parts.push(format!("{key}:{}", to_json(v, quoted_ascii)));
            }
            format!("{{{}}}", parts.join(","))
        }
        RespValue::Verbatim { data, .. } => escape(&String::from_utf8_lossy(data), quoted_ascii),
        RespValue::Attribute { value, .. } => to_json(value, quoted_ascii),
    }
}
