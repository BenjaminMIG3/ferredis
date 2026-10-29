//! Parse d'URI `redis://` / `rediss://` (sous-ensemble redis-cli `-u`).

#[derive(Debug, Clone, Default)]
pub struct RedisUri {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub db: Option<usize>,
}

pub fn parse_redis_uri(raw: &str) -> anyhow::Result<RedisUri> {
    let lower = raw.to_ascii_lowercase();
    let rest = if let Some(r) = lower.strip_prefix("redis://") {
        &raw[raw.len() - r.len()..]
    } else if let Some(r) = lower.strip_prefix("rediss://") {
        &raw[raw.len() - r.len()..]
    } else {
        anyhow::bail!("Invalid URI scheme, expected redis:// or rediss://");
    };

    let (auth_host, path) = match rest.split_once('/') {
        Some((ah, p)) => (ah, Some(p)),
        None => (rest, None),
    };

    let mut out = RedisUri::default();

    let (userinfo, hostport) = if let Some(idx) = auth_host.rfind('@') {
        (Some(&auth_host[..idx]), &auth_host[idx + 1..])
    } else {
        (None, auth_host)
    };

    if let Some(ui) = userinfo {
        if let Some((u, p)) = ui.split_once(':') {
            if !u.is_empty() {
                out.user = Some(percent_decode(u));
            }
            out.password = Some(percent_decode(p));
        } else if !ui.is_empty() {
            out.user = Some(percent_decode(ui));
        }
    }

    if !hostport.is_empty() {
        if let Some(h) = hostport.strip_prefix('[') {
            // IPv6 [addr]:port
            if let Some((addr, rest)) = h.split_once(']') {
                out.host = Some(addr.to_string());
                if let Some(port) = rest.strip_prefix(':') {
                    out.port = Some(port.parse()?);
                }
            } else {
                anyhow::bail!("invalid IPv6 host in URI");
            }
        } else if let Some((h, p)) = hostport.rsplit_once(':') {
            if !h.is_empty() {
                out.host = Some(h.to_string());
            }
            out.port = Some(p.parse()?);
        } else {
            out.host = Some(hostport.to_string());
        }
    }

    if let Some(p) = path {
        let db_part = p.split('?').next().unwrap_or(p);
        if !db_part.is_empty() {
            out.db = Some(db_part.parse()?);
        }
    }

    Ok(out)
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (from_hex(bytes[i + 1]), from_hex(bytes[i + 2])) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_uri() {
        let u = parse_redis_uri("redis://default:s3cret@localhost:6380/2").unwrap();
        assert_eq!(u.host.as_deref(), Some("localhost"));
        assert_eq!(u.port, Some(6380));
        assert_eq!(u.user.as_deref(), Some("default"));
        assert_eq!(u.password.as_deref(), Some("s3cret"));
        assert_eq!(u.db, Some(2));
    }
}
