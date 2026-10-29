//! Connexion TCP / socket, AUTH, SELECT, HELLO, CLIENT SETNAME.

use crate::args::Args;
use crate::uri::parse_redis_uri;
use anyhow::Context;
use ferredis_core::server::{read_resp, write_command};
use ferredis_core::RespValue;
use std::io::{self, Write};
use std::time::Duration;
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::time::timeout;

pub struct Session {
    pub stream: Conn,
    pub host: String,
    pub port: u16,
    pub db: usize,
    pub cluster: bool,
    pub display_host: String,
}

pub enum Conn {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl Conn {
    pub async fn write_command(&mut self, args: &[&[u8]]) -> std::io::Result<()> {
        match self {
            Self::Tcp(s) => write_command(s, args).await,
            #[cfg(unix)]
            Self::Unix(s) => write_unix_command(s, args).await,
        }
    }

    pub async fn read_resp(&mut self) -> anyhow::Result<RespValue> {
        match self {
            Self::Tcp(s) => read_resp(s).await,
            #[cfg(unix)]
            Self::Unix(s) => read_unix_resp(s).await,
        }
    }
}

#[cfg(unix)]
async fn write_unix_command(socket: &mut UnixStream, args: &[&[u8]]) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
    socket.write_all(&buf).await
}

#[cfg(unix)]
async fn read_unix_resp(socket: &mut UnixStream) -> anyhow::Result<RespValue> {
    use bytes::BytesMut;
    use ferredis_core::resp::{parse, RespError};
    use tokio::io::AsyncReadExt;
    let mut buf = BytesMut::with_capacity(4096);
    loop {
        match parse(&mut buf) {
            Ok(v) => return Ok(v),
            Err(RespError::Incomplete) => {
                let n = socket.read_buf(&mut buf).await?;
                if n == 0 {
                    anyhow::bail!("connection closed");
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
}

pub async fn connect(args: &Args) -> anyhow::Result<Session> {
    let mut host = args.host.clone();
    let mut port = args.port;
    let mut user = args.user.clone();
    let mut password = if args.askpass {
        Some(ask_password()?)
    } else if let Some(p) = &args.auth {
        Some(p.clone())
    } else {
        std::env::var("REDISCLI_AUTH").ok()
    };
    let mut db = args.db;

    if let Some(uri) = &args.uri {
        let u = parse_redis_uri(uri)?;
        if let Some(h) = u.host {
            host = h;
        }
        if let Some(p) = u.port {
            port = p;
        }
        if let Some(uu) = u.user {
            user = Some(uu);
        }
        if let Some(pw) = u.password {
            if !args.askpass {
                password = Some(pw);
            }
        }
        if let Some(d) = u.db {
            db = d;
        }
    }

    if password.is_some()
        && std::env::args().any(|a| a == "-a" || a == "--pass" || a.starts_with("--pass="))
        && !args.no_auth_warning
    {
        eprintln!(
            "Warning: Using a password with '-a' or '--pass' option on the command line interface may not be safe."
        );
    }

    let display_host = if let Some(sock) = &args.socket {
        sock.clone()
    } else {
        format!("{host}:{port}")
    };

    let stream = if let Some(sock) = &args.socket {
        connect_socket(sock, args.timeout).await?
    } else {
        connect_tcp(&host, port, args).await?
    };

    let mut session = Session {
        stream,
        host,
        port,
        db,
        cluster: args.cluster,
        display_host,
    };

    if args.resp3 {
        send_ok(&mut session, &[b"HELLO", b"3"]).await?;
    } else if args.resp2 {
        send_ok(&mut session, &[b"HELLO", b"2"]).await?;
    }

    if let Some(pass) = &password {
        match &user {
            Some(u) => {
                send_ok(
                    &mut session,
                    &[b"AUTH", u.as_bytes(), pass.as_bytes()],
                )
                .await?;
            }
            None => {
                send_ok(&mut session, &[b"AUTH", pass.as_bytes()]).await?;
            }
        }
    }

    if db != 0 {
        select_db(&mut session, db).await?;
    }

    if let Some(name) = &args.client_name {
        send_ok(
            &mut session,
            &[b"CLIENT", b"SETNAME", name.as_bytes()],
        )
        .await?;
    }

    Ok(session)
}

async fn connect_tcp(host: &str, port: u16, args: &Args) -> anyhow::Result<Conn> {
    let addr = format!("{host}:{port}");
    let fut = async {
        let s = if args.ipv4 {
            TcpStream::connect((host, port)).await?
        } else if args.ipv6 {
            // Force IPv6 lookup by wrapping if needed
            TcpStream::connect(&addr).await?
        } else {
            TcpStream::connect(&addr).await?
        };
        s.set_nodelay(true)?;
        Ok::<_, anyhow::Error>(Conn::Tcp(s))
    };
    if args.timeout > 0.0 {
        timeout(Duration::from_secs_f64(args.timeout), fut)
            .await
            .context("connection timed out")?
    } else {
        fut.await
    }
    .with_context(|| format!("Could not connect to Redis at {addr}"))
}

async fn connect_socket(path: &str, timeout_secs: f64) -> anyhow::Result<Conn> {
    #[cfg(unix)]
    {
        let fut = async {
            let s = UnixStream::connect(path).await?;
            Ok::<_, anyhow::Error>(Conn::Unix(s))
        };
        if timeout_secs > 0.0 {
            timeout(Duration::from_secs_f64(timeout_secs), fut)
                .await
                .context("connection timed out")?
        } else {
            fut.await
        }
        .with_context(|| format!("Could not connect to Redis at {path}"))
    }
    #[cfg(not(unix))]
    {
        let _ = (path, timeout_secs);
        anyhow::bail!("Unix sockets (-s) are not supported on this platform");
    }
}

pub async fn select_db(session: &mut Session, db: usize) -> anyhow::Result<()> {
    let idx = db.to_string();
    session
        .stream
        .write_command(&[b"SELECT", idx.as_bytes()])
        .await?;
    let resp = session.stream.read_resp().await?;
    if let RespValue::Error(e) = resp {
        anyhow::bail!("{}", String::from_utf8_lossy(&e));
    }
    session.db = db;
    Ok(())
}

async fn send_ok(session: &mut Session, args: &[&[u8]]) -> anyhow::Result<()> {
    session.stream.write_command(args).await?;
    let resp = session.stream.read_resp().await?;
    if let RespValue::Error(e) = resp {
        anyhow::bail!("{}", String::from_utf8_lossy(&e));
    }
    Ok(())
}

fn ask_password() -> anyhow::Result<String> {
    eprint!("Please input password: ");
    io::stderr().flush()?;
    // Pas de masquage natif portable sans dépendance ; stdin ligne (comme fallback).
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    Ok(line.trim_end_matches(['\r', '\n']).to_string())
}

impl Session {
    pub fn prompt(&self) -> String {
        if self.db == 0 {
            format!("{}> ", self.display_host)
        } else {
            format!("{}[{}]> ", self.display_host, self.db)
        }
    }

    /// Suit une redirection MOVED/ASK si cluster mode.
    pub async fn follow_redirect_if_needed(
        &mut self,
        err: &str,
    ) -> anyhow::Result<Option<()>> {
        if !self.cluster {
            return Ok(None);
        }
        // MOVED <slot> <host>:<port>  /  ASK <slot> <host>:<port>
        let parts: Vec<&str> = err.split_whitespace().collect();
        if parts.len() < 3 {
            return Ok(None);
        }
        if parts[0] != "MOVED" && parts[0] != "ASK" {
            return Ok(None);
        }
        let addr = parts[2];
        let (host, port_s) = addr.rsplit_once(':').context("bad redirect address")?;
        let port: u16 = port_s.parse()?;
        let s = TcpStream::connect((host, port)).await?;
        s.set_nodelay(true)?;
        self.stream = Conn::Tcp(s);
        self.host = host.to_string();
        self.port = port;
        self.display_host = format!("{host}:{port}");
        if self.db != 0 {
            select_db(self, self.db).await?;
        }
        Ok(Some(()))
    }
}
