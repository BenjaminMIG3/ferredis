//! RESP2 / RESP3 protocol encode/decode.
//!
//! L'encodage respecte la version négociée par connexion (`HELLO 2|3`).
//! Le parseur accepte les deux dialectes (clients RESP3, pushes, etc.).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;
use thiserror::Error;

/// Version de protocole RESP (2 par défaut, 3 après `HELLO 3`).
pub type RespVersion = u8;

#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RespValue>>),
    /// RESP3 null (`_`).
    Null,
    /// RESP3 bool (`#t` / `#f`).
    Bool(bool),
    /// RESP3 double (`,`).
    Double(f64),
    /// RESP3 big number (`(`).
    BigNumber(Bytes),
    /// RESP3 verbatim string (`=`), ex. `txt:` / `mkd:`.
    Verbatim {
        format: Bytes,
        data: Bytes,
    },
    /// RESP3 map (`%`).
    Map(Vec<(RespValue, RespValue)>),
    /// RESP3 set (`~`).
    Set(Vec<RespValue>),
    /// RESP3 push (`>`).
    Push(Vec<RespValue>),
    /// RESP3 attribute (`|`) wrapping a value.
    Attribute {
        attrs: Vec<(RespValue, RespValue)>,
        value: Box<RespValue>,
    },
}

impl PartialEq for RespValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::SimpleString(a), Self::SimpleString(b))
            | (Self::Error(a), Self::Error(b))
            | (Self::BigNumber(a), Self::BigNumber(b)) => a == b,
            (Self::Integer(a), Self::Integer(b)) => a == b,
            (Self::BulkString(a), Self::BulkString(b)) => a == b,
            (Self::Array(a), Self::Array(b)) => a == b,
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Double(a), Self::Double(b)) => a.to_bits() == b.to_bits(),
            (Self::Verbatim { format: fa, data: da }, Self::Verbatim { format: fb, data: db }) => {
                fa == fb && da == db
            }
            (Self::Map(a), Self::Map(b)) => a == b,
            (Self::Set(a), Self::Set(b)) | (Self::Push(a), Self::Push(b)) => a == b,
            (
                Self::Attribute {
                    attrs: aa,
                    value: va,
                },
                Self::Attribute {
                    attrs: ab,
                    value: vb,
                },
            ) => aa == ab && va == vb,
            _ => false,
        }
    }
}

impl Eq for RespValue {}

impl RespValue {
    pub fn ok() -> Self {
        Self::SimpleString(Bytes::from_static(b"OK"))
    }

    pub fn pong() -> Self {
        Self::SimpleString(Bytes::from_static(b"PONG"))
    }

    pub fn err(msg: impl AsRef<[u8]>) -> Self {
        Self::Error(Bytes::copy_from_slice(msg.as_ref()))
    }

    pub fn bulk(data: impl Into<Bytes>) -> Self {
        Self::BulkString(Some(data.into()))
    }

    pub fn null_bulk() -> Self {
        Self::BulkString(None)
    }

    pub fn null_array() -> Self {
        Self::Array(None)
    }

    /// Null RESP3 (`_`) — aussi produit en RESP2 via `$-1` à l'encodage.
    pub fn null() -> Self {
        Self::Null
    }

    pub fn array(items: Vec<RespValue>) -> Self {
        Self::Array(Some(items))
    }

    pub fn map(entries: Vec<(RespValue, RespValue)>) -> Self {
        Self::Map(entries)
    }

    pub fn set(items: Vec<RespValue>) -> Self {
        Self::Set(items)
    }

    pub fn push(items: Vec<RespValue>) -> Self {
        Self::Push(items)
    }

    pub fn bool(v: bool) -> Self {
        Self::Bool(v)
    }

    pub fn double(v: f64) -> Self {
        Self::Double(v)
    }

    pub fn simple(s: impl Into<Bytes>) -> Self {
        Self::SimpleString(s.into())
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::SimpleString(b) | Self::Error(b) | Self::BigNumber(b) => Some(b),
            Self::BulkString(Some(b)) => Some(b),
            Self::Verbatim { data, .. } => Some(data),
            _ => None,
        }
    }

    /// Encodage RESP2 (rétrocompat).
    pub fn encode(&self, buf: &mut BytesMut) {
        self.encode_proto(buf, 2);
    }

    /// Encodage selon la version négociée (2 ou 3).
    pub fn encode_proto(&self, buf: &mut BytesMut, proto: RespVersion) {
        if proto >= 3 {
            self.encode_resp3(buf);
        } else {
            self.encode_resp2(buf);
        }
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        self.encode(&mut buf);
        buf
    }

    pub fn to_bytes_proto(&self, proto: RespVersion) -> BytesMut {
        let mut buf = BytesMut::new();
        self.encode_proto(&mut buf, proto);
        buf
    }

    fn encode_resp2(&self, buf: &mut BytesMut) {
        match self {
            Self::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.extend_from_slice(s);
                buf.extend_from_slice(b"\r\n");
            }
            Self::Error(s) => {
                buf.put_u8(b'-');
                buf.extend_from_slice(s);
                buf.extend_from_slice(b"\r\n");
            }
            Self::Integer(n) => encode_integer(buf, *n),
            Self::Null | Self::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            Self::Array(None) => {
                buf.extend_from_slice(b"*-1\r\n");
            }
            Self::BulkString(Some(s)) => encode_bulk(buf, s),
            Self::Array(Some(items)) => {
                buf.put_u8(b'*');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode_resp2(buf);
                }
            }
            Self::Bool(v) => encode_integer(buf, i64::from(*v)),
            Self::Double(n) => encode_bulk(buf, format_double(*n).as_bytes()),
            Self::BigNumber(n) => encode_bulk(buf, n),
            Self::Verbatim { data, .. } => encode_bulk(buf, data),
            Self::Map(entries) => {
                buf.put_u8(b'*');
                buf.extend_from_slice((entries.len() * 2).to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for (k, v) in entries {
                    k.encode_resp2(buf);
                    v.encode_resp2(buf);
                }
            }
            Self::Set(items) | Self::Push(items) => {
                buf.put_u8(b'*');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode_resp2(buf);
                }
            }
            Self::Attribute { value, .. } => value.encode_resp2(buf),
        }
    }

    fn encode_resp3(&self, buf: &mut BytesMut) {
        match self {
            Self::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.extend_from_slice(s);
                buf.extend_from_slice(b"\r\n");
            }
            Self::Error(s) => {
                buf.put_u8(b'-');
                buf.extend_from_slice(s);
                buf.extend_from_slice(b"\r\n");
            }
            Self::Integer(n) => encode_integer(buf, *n),
            Self::Null | Self::BulkString(None) | Self::Array(None) => {
                buf.extend_from_slice(b"_\r\n");
            }
            Self::BulkString(Some(s)) => encode_bulk(buf, s),
            Self::Array(Some(items)) => {
                buf.put_u8(b'*');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode_resp3(buf);
                }
            }
            Self::Bool(true) => buf.extend_from_slice(b"#t\r\n"),
            Self::Bool(false) => buf.extend_from_slice(b"#f\r\n"),
            Self::Double(n) => {
                buf.put_u8(b',');
                buf.extend_from_slice(format_double(*n).as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Self::BigNumber(n) => {
                buf.put_u8(b'(');
                buf.extend_from_slice(n);
                buf.extend_from_slice(b"\r\n");
            }
            Self::Verbatim { format, data } => {
                let total = format.len() + 1 + data.len();
                buf.put_u8(b'=');
                buf.extend_from_slice(total.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(format);
                buf.put_u8(b':');
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            Self::Map(entries) => {
                buf.put_u8(b'%');
                buf.extend_from_slice(entries.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for (k, v) in entries {
                    k.encode_resp3(buf);
                    v.encode_resp3(buf);
                }
            }
            Self::Set(items) => {
                buf.put_u8(b'~');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode_resp3(buf);
                }
            }
            Self::Push(items) => {
                buf.put_u8(b'>');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode_resp3(buf);
                }
            }
            Self::Attribute { attrs, value } => {
                buf.put_u8(b'|');
                buf.extend_from_slice(attrs.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for (k, v) in attrs {
                    k.encode_resp3(buf);
                    v.encode_resp3(buf);
                }
                value.encode_resp3(buf);
            }
        }
    }
}

fn encode_integer(buf: &mut BytesMut, n: i64) {
    buf.put_u8(b':');
    buf.extend_from_slice(n.to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
}

fn encode_bulk(buf: &mut BytesMut, s: &[u8]) {
    buf.put_u8(b'$');
    buf.extend_from_slice(s.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(s);
    buf.extend_from_slice(b"\r\n");
}

fn format_double(n: f64) -> String {
    if n.is_nan() {
        "nan".into()
    } else if n == f64::INFINITY {
        "inf".into()
    } else if n == f64::NEG_INFINITY {
        "-inf".into()
    } else {
        // Représentation compacte stable (pas de notation scientifique inutile).
        let s = format!("{n}");
        if s.contains('.') || s.contains('e') || s.contains('E') {
            s
        } else {
            format!("{n:.1}")
        }
    }
}

impl fmt::Display for RespValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SimpleString(s) => write!(f, "{}", String::from_utf8_lossy(s)),
            Self::Error(s) => write!(f, "(error) {}", String::from_utf8_lossy(s)),
            Self::Integer(n) => write!(f, "(integer) {n}"),
            Self::Null | Self::BulkString(None) | Self::Array(None) => write!(f, "(nil)"),
            Self::BulkString(Some(s)) => {
                if let Ok(text) = std::str::from_utf8(s) {
                    write!(f, "\"{text}\"")
                } else {
                    write!(f, "{s:?}")
                }
            }
            Self::Array(Some(items)) => {
                if items.is_empty() {
                    return write!(f, "(empty array)");
                }
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        writeln!(f)?;
                    }
                    write!(f, "{}. {})", i + 1, item)?;
                }
                Ok(())
            }
            Self::Bool(v) => write!(f, "(bool) {v}"),
            Self::Double(n) => write!(f, "(double) {n}"),
            Self::BigNumber(n) => write!(f, "(big number) {}", String::from_utf8_lossy(n)),
            Self::Verbatim { format, data } => write!(
                f,
                "(verbatim) {}:{}",
                String::from_utf8_lossy(format),
                String::from_utf8_lossy(data)
            ),
            Self::Map(entries) => write!(f, "(map) {} pairs", entries.len()),
            Self::Set(items) => write!(f, "(set) {} elems", items.len()),
            Self::Push(items) => write!(f, "(push) {} elems", items.len()),
            Self::Attribute { value, .. } => write!(f, "(attr) {value}"),
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RespError {
    #[error("incomplete")]
    Incomplete,
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("invalid integer")]
    InvalidInteger,
}

/// Longueur maximale d'un bulk string (512 MiB, comme `proto-max-bulk-len`).
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;
/// Nombre maximal d'éléments d'un tableau / map / set.
const MAX_ARRAY_LEN: i64 = 1024 * 1024;
/// Profondeur maximale d'imbrication acceptée.
const MAX_DEPTH: usize = 32;

/// Try to parse one RESP value from `buf`. On success, advances `buf`.
pub fn parse(buf: &mut BytesMut) -> Result<RespValue, RespError> {
    let mut pos = 0;
    let value = parse_at(buf, &mut pos, 0)?;
    buf.advance(pos);
    Ok(value)
}

/// Parse inline / telnet-style commands: `GET key\r\n` or RESP arrays.
pub fn parse_command(buf: &mut BytesMut) -> Result<Vec<Bytes>, RespError> {
    if buf.is_empty() {
        return Err(RespError::Incomplete);
    }
    if buf[0] == b'*' {
        match parse(buf)? {
            RespValue::Array(Some(items)) => {
                let mut args = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        RespValue::BulkString(Some(b)) | RespValue::SimpleString(b) => args.push(b),
                        RespValue::Integer(n) => args.push(Bytes::from(n.to_string())),
                        RespValue::Bool(v) => {
                            args.push(Bytes::from(if v { "1" } else { "0" }.to_string()))
                        }
                        RespValue::Double(n) => args.push(Bytes::from(format_double(n))),
                        RespValue::BigNumber(b) => args.push(b),
                        _ => {
                            return Err(RespError::Protocol(
                                "command args must be bulk/simple strings".into(),
                            ))
                        }
                    }
                }
                Ok(args)
            }
            _ => Err(RespError::Protocol("expected array command".into())),
        }
    } else {
        let nl = buf
            .iter()
            .position(|&b| b == b'\n')
            .ok_or(RespError::Incomplete)?;
        let mut line = buf.split_to(nl + 1);
        line.truncate(nl);
        if line.last() == Some(&b'\r') {
            line.truncate(nl - 1);
        }
        Ok(split_inline(&line))
    }
}

fn find_crlf(data: &[u8], from: usize) -> Option<usize> {
    data.get(from..)?
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|p| p + from)
}

fn read_line<'a>(data: &'a [u8], pos: &mut usize) -> Result<&'a [u8], RespError> {
    let end = find_crlf(data, *pos + 1).ok_or(RespError::Incomplete)?;
    let line = &data[*pos + 1..end];
    *pos = end + 2;
    Ok(line)
}

fn parse_at(data: &[u8], pos: &mut usize, depth: usize) -> Result<RespValue, RespError> {
    if depth > MAX_DEPTH {
        return Err(RespError::Protocol("nesting too deep".into()));
    }
    let Some(&kind) = data.get(*pos) else {
        return Err(RespError::Incomplete);
    };
    match kind {
        b'+' => Ok(RespValue::SimpleString(Bytes::copy_from_slice(read_line(data, pos)?))),
        b'-' => Ok(RespValue::Error(Bytes::copy_from_slice(read_line(data, pos)?))),
        b':' => Ok(RespValue::Integer(parse_i64(read_line(data, pos)?)?)),
        b'$' => parse_bulk(data, pos),
        b'*' => parse_aggregate(data, pos, depth, AggregateKind::Array),
        b'_' => {
            let _ = read_line(data, pos)?;
            Ok(RespValue::Null)
        }
        b'#' => {
            let line = read_line(data, pos)?;
            match line {
                b"t" | b"T" => Ok(RespValue::Bool(true)),
                b"f" | b"F" => Ok(RespValue::Bool(false)),
                _ => Err(RespError::Protocol("invalid bool".into())),
            }
        }
        b',' => {
            let line = read_line(data, pos)?;
            Ok(RespValue::Double(parse_double(line)?))
        }
        b'(' => Ok(RespValue::BigNumber(Bytes::copy_from_slice(read_line(data, pos)?))),
        b'=' => parse_verbatim(data, pos),
        b'%' => parse_map_like(data, pos, depth, false),
        b'~' => parse_aggregate(data, pos, depth, AggregateKind::Set),
        b'>' => parse_aggregate(data, pos, depth, AggregateKind::Push),
        b'|' => parse_attribute(data, pos, depth),
        other => Err(RespError::Protocol(format!(
            "unknown type byte: {}",
            other as char
        ))),
    }
}

enum AggregateKind {
    Array,
    Set,
    Push,
}

fn parse_bulk(data: &[u8], pos: &mut usize) -> Result<RespValue, RespError> {
    let len = parse_i64(read_line(data, pos)?)?;
    if len == -1 {
        return Ok(RespValue::BulkString(None));
    }
    if !(0..=MAX_BULK_LEN).contains(&len) {
        return Err(RespError::Protocol("invalid bulk length".into()));
    }
    let len = len as usize;
    let end = *pos + len;
    if data.len() < end + 2 {
        return Err(RespError::Incomplete);
    }
    if &data[end..end + 2] != b"\r\n" {
        return Err(RespError::Protocol("bulk missing CRLF".into()));
    }
    let value = Bytes::copy_from_slice(&data[*pos..end]);
    *pos = end + 2;
    Ok(RespValue::BulkString(Some(value)))
}

fn parse_verbatim(data: &[u8], pos: &mut usize) -> Result<RespValue, RespError> {
    let len = parse_i64(read_line(data, pos)?)?;
    if !(0..=MAX_BULK_LEN).contains(&len) {
        return Err(RespError::Protocol("invalid verbatim length".into()));
    }
    let len = len as usize;
    let end = *pos + len;
    if data.len() < end + 2 {
        return Err(RespError::Incomplete);
    }
    if &data[end..end + 2] != b"\r\n" {
        return Err(RespError::Protocol("verbatim missing CRLF".into()));
    }
    let raw = &data[*pos..end];
    *pos = end + 2;
    let colon = raw
        .iter()
        .position(|&b| b == b':')
        .ok_or_else(|| RespError::Protocol("verbatim missing format".into()))?;
    Ok(RespValue::Verbatim {
        format: Bytes::copy_from_slice(&raw[..colon]),
        data: Bytes::copy_from_slice(&raw[colon + 1..]),
    })
}

fn parse_aggregate(
    data: &[u8],
    pos: &mut usize,
    depth: usize,
    kind: AggregateKind,
) -> Result<RespValue, RespError> {
    let len = parse_i64(read_line(data, pos)?)?;
    if matches!(kind, AggregateKind::Array) && len == -1 {
        return Ok(RespValue::Array(None));
    }
    if !(0..=MAX_ARRAY_LEN).contains(&len) {
        return Err(RespError::Protocol("invalid array length".into()));
    }
    let mut items = Vec::with_capacity((len as usize).min(1024));
    for _ in 0..len {
        items.push(parse_at(data, pos, depth + 1)?);
    }
    Ok(match kind {
        AggregateKind::Array => RespValue::Array(Some(items)),
        AggregateKind::Set => RespValue::Set(items),
        AggregateKind::Push => RespValue::Push(items),
    })
}

fn parse_map_like(
    data: &[u8],
    pos: &mut usize,
    depth: usize,
    _attr: bool,
) -> Result<RespValue, RespError> {
    let len = parse_i64(read_line(data, pos)?)?;
    if !(0..=MAX_ARRAY_LEN).contains(&len) {
        return Err(RespError::Protocol("invalid map length".into()));
    }
    let mut entries = Vec::with_capacity((len as usize).min(1024));
    for _ in 0..len {
        let k = parse_at(data, pos, depth + 1)?;
        let v = parse_at(data, pos, depth + 1)?;
        entries.push((k, v));
    }
    Ok(RespValue::Map(entries))
}

fn parse_attribute(data: &[u8], pos: &mut usize, depth: usize) -> Result<RespValue, RespError> {
    let len = parse_i64(read_line(data, pos)?)?;
    if !(0..=MAX_ARRAY_LEN).contains(&len) {
        return Err(RespError::Protocol("invalid attribute length".into()));
    }
    let mut attrs = Vec::with_capacity((len as usize).min(1024));
    for _ in 0..len {
        let k = parse_at(data, pos, depth + 1)?;
        let v = parse_at(data, pos, depth + 1)?;
        attrs.push((k, v));
    }
    let value = Box::new(parse_at(data, pos, depth + 1)?);
    Ok(RespValue::Attribute { attrs, value })
}

fn parse_i64(bytes: &[u8]) -> Result<i64, RespError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(RespError::InvalidInteger)
}

fn parse_double(bytes: &[u8]) -> Result<f64, RespError> {
    let s = std::str::from_utf8(bytes).map_err(|_| RespError::Protocol("invalid double".into()))?;
    match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" => Ok(f64::INFINITY),
        "-inf" => Ok(f64::NEG_INFINITY),
        "nan" => Ok(f64::NAN),
        other => other
            .parse()
            .map_err(|_| RespError::Protocol("invalid double".into())),
    }
}

fn split_inline(line: &[u8]) -> Vec<Bytes> {
    let mut args = Vec::new();
    let mut i = 0;
    while i < line.len() {
        while i < line.len() && (line[i] == b' ' || line[i] == b'\t') {
            i += 1;
        }
        if i >= line.len() {
            break;
        }
        let mut arg = Vec::new();
        if line[i] == b'"' {
            i += 1;
            while i < line.len() && line[i] != b'"' {
                if line[i] == b'\\' && i + 1 < line.len() {
                    i += 1;
                    arg.push(match line[i] {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        other => other,
                    });
                } else {
                    arg.push(line[i]);
                }
                i += 1;
            }
            i += 1;
        } else {
            while i < line.len() && line[i] != b' ' && line[i] != b'\t' {
                arg.push(line[i]);
                i += 1;
            }
        }
        args.push(Bytes::from(arg));
    }
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_simple() {
        let v = RespValue::ok();
        let mut buf = v.to_bytes();
        assert_eq!(parse(&mut buf).unwrap(), RespValue::ok());
    }

    #[test]
    fn parse_array_command() {
        let mut buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"[..]);
        let args = parse_command(&mut buf).unwrap();
        assert_eq!(args.len(), 2);
        assert_eq!(&args[0][..], b"GET");
        assert_eq!(&args[1][..], b"key");
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_bulk_null() {
        let mut buf = BytesMut::from(&b"$-1\r\n"[..]);
        assert_eq!(parse(&mut buf).unwrap(), RespValue::null_bulk());
    }

    #[test]
    fn resp3_null_bool_double_map() {
        let mut buf = BytesMut::new();
        RespValue::Null.encode_proto(&mut buf, 3);
        assert_eq!(&buf[..], b"_\r\n");
        assert_eq!(parse(&mut buf).unwrap(), RespValue::Null);

        let mut buf = BytesMut::new();
        RespValue::Bool(true).encode_proto(&mut buf, 3);
        assert_eq!(parse(&mut buf).unwrap(), RespValue::Bool(true));

        let mut buf = BytesMut::new();
        RespValue::map(vec![
            (RespValue::bulk("proto"), RespValue::Integer(3)),
            (RespValue::bulk("ok"), RespValue::Bool(true)),
        ])
        .encode_proto(&mut buf, 3);
        let v = parse(&mut buf).unwrap();
        match v {
            RespValue::Map(e) => assert_eq!(e.len(), 2),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn resp3_types_roundtrip_as_resp2() {
        let mut buf = BytesMut::new();
        RespValue::Bool(true).encode_proto(&mut buf, 2);
        assert_eq!(parse(&mut buf).unwrap(), RespValue::Integer(1));

        let mut buf = BytesMut::new();
        RespValue::Null.encode_proto(&mut buf, 2);
        assert_eq!(parse(&mut buf).unwrap(), RespValue::null_bulk());
    }

    #[test]
    fn push_and_verbatim() {
        let mut buf = BytesMut::new();
        RespValue::push(vec![
            RespValue::bulk("invalidate"),
            RespValue::array(vec![RespValue::bulk("k")]),
        ])
        .encode_proto(&mut buf, 3);
        assert_eq!(buf[0], b'>');
        let v = parse(&mut buf).unwrap();
        assert!(matches!(v, RespValue::Push(_)));

        let mut buf = BytesMut::new();
        RespValue::Verbatim {
            format: Bytes::from_static(b"txt"),
            data: Bytes::from_static(b"hi"),
        }
        .encode_proto(&mut buf, 3);
        let v = parse(&mut buf).unwrap();
        match v {
            RespValue::Verbatim { format, data } => {
                assert_eq!(&format[..], b"txt");
                assert_eq!(&data[..], b"hi");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn incomplete_does_not_consume() {
        let mut buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n"[..]);
        assert_eq!(parse_command(&mut buf), Err(RespError::Incomplete));
        assert_eq!(buf.len(), 13);
        buf.extend_from_slice(b"$3\r\nkey\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().len(), 2);
    }

    #[test]
    fn pipelined_commands() {
        let mut buf = BytesMut::from(&b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"[..]);
        assert_eq!(parse_command(&mut buf).unwrap().len(), 1);
        assert_eq!(parse_command(&mut buf).unwrap().len(), 1);
        assert_eq!(parse_command(&mut buf), Err(RespError::Incomplete));
    }

    #[test]
    fn inline_commands() {
        let mut buf = BytesMut::from(&b"SET k \"a b\"\r\nPING\n"[..]);
        let a = parse_command(&mut buf).unwrap();
        assert_eq!(a, vec![Bytes::from("SET"), Bytes::from("k"), Bytes::from("a b")]);
        assert_eq!(parse_command(&mut buf).unwrap(), vec![Bytes::from("PING")]);
    }

    #[test]
    fn rejects_oversized_headers() {
        let mut buf = BytesMut::from(&b"*99999999999\r\n"[..]);
        assert!(matches!(
            parse_command(&mut buf),
            Err(RespError::Protocol(_)) | Err(RespError::InvalidInteger)
        ));
        let mut buf = BytesMut::from(&b"*1\r\n$99999999999\r\n"[..]);
        assert!(parse_command(&mut buf).is_err());
    }
}
