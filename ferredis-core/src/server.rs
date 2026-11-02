//! Serveur TCP / TLS / socket local acceptant les clients RESP2/3.
//!
//! Chaque connexion est enregistrée dans le [`ClientRegistry`](crate::clients::ClientRegistry)
//! (visible via `CLIENT LIST` et `FERREDIS EVENTS`). Une commande bloquante en cours
//! (BLPOP, BZPOPMIN, …) est interrompue dès que le client se déconnecte ou est tué.

use crate::blocking::is_blocking_name;
use crate::clients::{ClientInfo, EventKind};
use crate::cmd_pubsub::format_pubsub_event_proto;
use crate::command::{dispatch, ClientState, CommandContext, ServerConfig};
use crate::persist::save_rdb;
use crate::pubsub::PubSubEvent;
use crate::resp::{parse_command, RespError, RespValue};
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tracing::{debug, error, info, warn};

/// Taille maximale du tampon de requête d'un client (1 GiB, comme Redis).
const MAX_QUERY_BUF: usize = 1 << 30;
/// Taille du tampon de lecture socket.
const READ_CHUNK: usize = 16 * 1024;

pub struct FerredisServer {
    pub ctx: CommandContext,
}

impl FerredisServer {
    pub fn new(config: ServerConfig) -> Self {
        let io_n = config.io_threads.max(1) as usize;
        crate::io_threads::init_global(io_n);
        Self {
            ctx: CommandContext::new(config),
        }
    }

    pub fn context(&self) -> CommandContext {
        self.ctx.clone()
    }

    pub async fn load_persistence(&self) -> anyhow::Result<()> {
        let persist = self.ctx.config.read().persist.clone();
        let acl_path = persist.dir.join("users.acl");
        crate::acl::load_acl_file_on_boot(&self.ctx.acl, &acl_path);
        // Parité Redis : AOF prioritaire s'il est activé (preamble RDB possible).
        if self.ctx.aof.enabled() {
            let aof_path = persist.dir.join(&persist.appendfilename);
            let ctx = self.ctx.clone();
            crate::persist::load_aof_with_db(
                &aof_path,
                &self.ctx.db,
                Some(&self.ctx.functions),
                |args| {
                    let ctx = ctx.clone();
                    async move {
                        let mut state = ClientState {
                            authenticated: true,
                            ..Default::default()
                        };
                        let _ = dispatch(&ctx, &mut state, &args).await;
                    }
                },
            )
            .await?;
        } else {
            crate::persist::load_rdb_with_stack(
                &persist.dir.join(&persist.dbfilename),
                &self.ctx.db,
                Some(&self.ctx.functions),
                Some(&self.ctx.search),
            )?;
        }
        Ok(())
    }

    pub async fn serve(self, addr: SocketAddr) -> std::io::Result<()> {
        self.serve_addrs(&[addr]).await
    }

    /// Écoute TCP sur toutes les adresses (multi-bind), plus TLS / socket local.
    pub async fn serve_addrs(self, addrs: &[SocketAddr]) -> std::io::Result<()> {
        match self.load_persistence().await {
            Ok(()) => {
                let persist = self.ctx.config.read().persist.clone();
                let path = if self.ctx.aof.enabled() {
                    persist.dir.join(&persist.appendfilename)
                } else {
                    persist.dir.join(&persist.dbfilename)
                };
                info!(
                    "persistence ready (dir={}, file={}, keys≈{})",
                    persist.dir.display(),
                    path.file_name().and_then(|s| s.to_str()).unwrap_or("?"),
                    self.ctx.db.dbsize(0)
                );
            }
            Err(e) => warn!("persistence load FAILED — starting empty: {e}"),
        }

        if addrs.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "no listen addresses",
            ));
        }

        let mut listeners = Vec::with_capacity(addrs.len());
        for addr in addrs {
            let listener = TcpListener::bind(addr).await?;
            let local = listener.local_addr()?;
            info!("Ferredis listening on {local}");
            listeners.push(listener);
        }
        let primary = listeners[0].local_addr()?;
        *self.ctx.config.read().listen_addr.lock() = primary.to_string();

        let (tcp_tx, mut tcp_rx) = unbounded_channel::<(TcpStream, SocketAddr)>();
        for listener in listeners {
            spawn_tcp_accept(listener, tcp_tx.clone());
        }
        drop(tcp_tx);

        let tls_acceptor = prepare_tls(&self.ctx);
        let tls_listener = prepare_tls_listener(&self.ctx, tls_acceptor.as_ref()).await?;

        #[cfg(unix)]
        let unix_listener = prepare_local_listener(&self.ctx).await?;
        #[cfg(windows)]
        let mut pipe_listener = prepare_local_listener(&self.ctx).await?;

        spawn_expiry_task(&self.ctx, 20);
        spawn_autosave_task(&self.ctx);
        crate::sentinel::spawn_sentinel_loop(self.ctx.clone());
        if self.ctx.config.read().cluster_enabled {
            let host = self
                .ctx
                .config
                .read()
                .bind
                .first()
                .cloned()
                .unwrap_or_else(|| "127.0.0.1".into());
            let port = self.ctx.config.read().port;
            self.ctx.cluster.enable(&host, port);
        }

        let mut shutdown_rx = self.ctx.config.read().control.subscribe_shutdown();

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        if let Some(req) = self.ctx.config.read().control.take_shutdown() {
                            // Save déjà fait dans cmd_shutdown pour le chemin commande ;
                            // ici on couvre Ctrl+C / signaux (should_persist défaut).
                            if req.should_persist() {
                                let path = {
                                    let cfg = self.ctx.config.read();
                                    cfg.persist.dir.join(&cfg.persist.dbfilename)
                                };
                                match save_rdb(&path, &self.ctx.db) {
                                    Ok(()) => {
                                        crate::cmd_server::mark_last_save(&self.ctx.config.read());
                                        if self.ctx.aof.enabled() {
                                            let _ = self.ctx.aof.fsync_now();
                                        }
                                    }
                                    Err(e) => warn!("shutdown save failed: {e}"),
                                }
                            }
                        }
                        info!("Ferredis shutdown requested");
                        break;
                    }
                }
                accept = tcp_rx.recv() => {
                    let Some((socket, peer)) = accept else { break };
                    spawn_accepted_tcp(&self.ctx, socket, peer);
                }
                accept = async {
                    match &tls_listener {
                        Some(l) => Some(l.accept().await),
                        None => std::future::pending().await,
                    }
                }, if tls_listener.is_some() => {
                    let Some(Ok((socket, peer))) = accept else { continue };
                    let Some(acceptor) = tls_acceptor.clone() else { continue };
                    if reject_protected(&self.ctx, peer) {
                        self.ctx.clients.record_rejected(peer.to_string(), "protected mode");
                        continue;
                    }
                    let ctx = self.ctx.clone();
                    tokio::spawn(async move {
                        match acceptor.accept(socket).await {
                            Ok(tls) => {
                                if let Err(e) = handle_stream(tls, ctx, peer.to_string(), peer.to_string()).await {
                                    debug!("tls connection {peer} closed: {e}");
                                }
                            }
                            Err(e) => debug!("tls handshake {peer}: {e}"),
                        }
                    });
                }
                accept = async {
                    #[cfg(unix)]
                    {
                        match &unix_listener {
                            Some(l) => Some(crate::unix_socket::listener::accept(l).await),
                            None => std::future::pending().await,
                        }
                    }
                    #[cfg(windows)]
                    {
                        match &mut pipe_listener {
                            Some(l) => Some(crate::unix_socket::listener::accept(l).await),
                            None => std::future::pending().await,
                        }
                    }
                    #[cfg(not(any(unix, windows)))]
                    {
                        let _: () = std::future::pending().await;
                        None
                    }
                }, if {
                    #[cfg(unix)]
                    { unix_listener.is_some() }
                    #[cfg(windows)]
                    { pipe_listener.is_some() }
                    #[cfg(not(any(unix, windows)))]
                    { false }
                } => {
                    let Some(Ok((stream, peer))) = accept else { continue };
                    let ctx = self.ctx.clone();
                    let laddr = peer.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_stream(stream, ctx, peer, laddr).await {
                            debug!("local connection closed: {e}");
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

/// Construit les adresses d'écoute depuis `bind` × `port` (fallback `127.0.0.1`).
pub fn resolve_listen_addrs(config: &ServerConfig) -> Result<Vec<SocketAddr>, String> {
    let hosts: Vec<&str> = if config.bind.is_empty() {
        vec!["127.0.0.1"]
    } else {
        config.bind.iter().map(String::as_str).collect()
    };
    let mut out = Vec::with_capacity(hosts.len());
    for host in hosts {
        let addr: SocketAddr = format!("{host}:{}", config.port)
            .parse()
            .map_err(|e| format!("invalid bind {host}:{}: {e}", config.port))?;
        out.push(addr);
    }
    Ok(out)
}

fn spawn_tcp_accept(
    listener: TcpListener,
    tx: tokio::sync::mpsc::UnboundedSender<(TcpStream, SocketAddr)>,
) {
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok(pair) => {
                    if tx.send(pair).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    debug!("tcp accept error: {e}");
                }
            }
        }
    });
}

fn spawn_accepted_tcp(ctx: &CommandContext, socket: TcpStream, peer: SocketAddr) {
    if reject_protected(ctx, peer) {
        ctx.clients
            .record_rejected(peer.to_string(), "protected mode");
        return;
    }
    debug!("accepted connection from {peer}");
    let ctx = ctx.clone();
    tokio::spawn(async move {
        if let Err(e) = handle_tcp(socket, ctx, peer).await {
            debug!("connection {peer} closed: {e}");
        }
    });
}

fn prepare_tls(ctx: &CommandContext) -> Option<tokio_rustls::TlsAcceptor> {
    let cfg = ctx.config.read();
    let (Some(cert), Some(key)) = (&cfg.tls_cert_file, &cfg.tls_key_file) else {
        return None;
    };
    let mtls = matches!(
        cfg.tls_auth_clients.to_ascii_lowercase().as_str(),
        "yes" | "optional"
    );
    let ca = cfg.tls_ca_cert_file.as_deref();
    let result = if mtls && ca.is_some() {
        crate::tls::load_acceptor_mtls(cert, key, ca)
    } else {
        crate::tls::load_acceptor(cert, key)
    };
    match result {
        Ok(a) => {
            info!("TLS acceptor ready (cert={})", cert.display());
            Some(a)
        }
        Err(e) => {
            warn!("TLS disabled: {e}");
            None
        }
    }
}

async fn prepare_tls_listener(
    ctx: &CommandContext,
    acceptor: Option<&tokio_rustls::TlsAcceptor>,
) -> std::io::Result<Option<TcpListener>> {
    if acceptor.is_none() {
        return Ok(None);
    }
    let cfg = ctx.config.read();
    let port = if cfg.tls_port > 0 {
        cfg.tls_port
    } else {
        // Même port impossible en dual ; exige tls-port dédié.
        warn!("TLS cert configured but tls-port=0 ; set tls-port for dual plain+TLS");
        return Ok(None);
    };
    let bind = cfg
        .bind
        .first()
        .cloned()
        .unwrap_or_else(|| "127.0.0.1".into());
    let addr: SocketAddr = format!("{bind}:{port}")
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let listener = TcpListener::bind(addr).await?;
    info!("Ferredis TLS listening on {}", listener.local_addr()?);
    Ok(Some(listener))
}

/// Bind socket local si `unixsocket` est configuré ; échec = erreur fatale.
#[cfg(unix)]
async fn prepare_local_listener(
    ctx: &CommandContext,
) -> std::io::Result<Option<tokio::net::UnixListener>> {
    let cfg = ctx.config.read();
    let Some(path) = cfg.unixsocket.clone() else {
        return Ok(None);
    };
    let perm = cfg.unixsocketperm.clone();
    let listener = crate::unix_socket::listener::bind(&path, &perm).await?;
    info!("Ferredis unixsocket listening on {path}");
    Ok(Some(listener))
}

/// Bind named pipe si `unixsocket` est configuré ; échec = erreur fatale.
#[cfg(windows)]
async fn prepare_local_listener(
    ctx: &CommandContext,
) -> std::io::Result<Option<crate::unix_socket::listener::PipeListener>> {
    let cfg = ctx.config.read();
    let Some(path) = cfg.unixsocket.clone() else {
        return Ok(None);
    };
    let perm = cfg.unixsocketperm.clone();
    let listener = crate::unix_socket::listener::bind(&path, &perm).await?;
    info!("Ferredis named pipe listening on {path}");
    Ok(Some(listener))
}

fn reject_protected(ctx: &CommandContext, peer: SocketAddr) -> bool {
    let cfg = ctx.config.read();
    if !cfg.protected_mode {
        return false;
    }
    if cfg.requirepass.is_some() {
        return false;
    }
    !peer.ip().is_loopback()
}

fn spawn_expiry_task(ctx: &CommandContext, sample: usize) {
    let db = Arc::clone(&ctx.db);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            tick.tick().await;
            for i in 0..db.db_count() {
                db.random_expire_sample(i, sample);
            }
        }
    });
}

fn spawn_autosave_task(ctx: &CommandContext) {
    let db = Arc::clone(&ctx.db);
    let cfg = Arc::clone(&ctx.config);
    tokio::spawn(async move {
        let mut last_dirty = 0u64;
        // Intervalle court : un redémarrage avant le premier dump ne doit pas
        // tout perdre (Redis a des save points ; ici dirty→disk périodique).
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let dirty = db.dirty.load(std::sync::atomic::Ordering::Relaxed);
            if dirty != last_dirty {
                let path = cfg.read().persist.dir.join(&cfg.read().persist.dbfilename);
                match save_rdb(&path, &db) {
                    Ok(()) => {
                        crate::cmd_server::mark_last_save(&cfg.read());
                        last_dirty = dirty;
                    }
                    Err(e) => warn!("autosave failed: {e}"),
                }
            }
        }
    });
}

enum Close {
    Peer(&'static str),
    Killed,
    Protocol(String),
}

impl Close {
    fn describe(&self) -> String {
        match self {
            Close::Peer(why) => (*why).to_string(),
            Close::Killed => "CLIENT KILL".to_string(),
            Close::Protocol(e) => format!("protocol error: {e}"),
        }
    }
}

async fn handle_tcp(
    socket: TcpStream,
    ctx: CommandContext,
    peer: SocketAddr,
) -> anyhow::Result<()> {
    socket.set_nodelay(true)?;
    let laddr = socket
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    handle_stream(socket, ctx, peer.to_string(), laddr).await
}

async fn handle_stream<S>(
    mut socket: S,
    ctx: CommandContext,
    peer: String,
    laddr: String,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let info = ctx.clients.register(peer.clone(), laddr);
    let (pubsub_id, mut pubsub_rx) = ctx.pubsub.register();
    info.set_pubsub_id(pubsub_id);

    let (push_tx, mut push_rx) = unbounded_channel::<Bytes>();
    let tracking = ctx.tracking.session(info.id);
    tracking.attach_push(push_tx);

    let authenticated =
        ctx.config.read().requirepass.is_none() && ctx.acl.authenticate("default", "").is_ok();
    let mut state = ClientState {
        authenticated,
        pubsub_id: Some(pubsub_id),
        info: Some(Arc::clone(&info)),
        ..Default::default()
    };

    let result = connection_loop(
        &mut socket,
        &ctx,
        &mut state,
        &info,
        &mut pubsub_rx,
        &mut push_rx,
    )
    .await;

    if state.is_replica_conn {
        ctx.repl.remove_replica(info.id);
    }
    ctx.pubsub.unregister(pubsub_id);
    ctx.tracking.remove(info.id);
    match &result {
        Ok(close) => {
            if let Close::Protocol(detail) = close {
                ctx.clients.push_event(
                    info.id,
                    info.addr.clone(),
                    EventKind::ProtocolError,
                    detail.clone(),
                );
            }
            ctx.clients.unregister(&info, &close.describe());
        }
        Err(e) => ctx.clients.unregister(&info, &format!("io error: {e}")),
    }
    result.map(|_| ())
}

async fn connection_loop<S>(
    socket: &mut S,
    ctx: &CommandContext,
    state: &mut ClientState,
    info: &Arc<ClientInfo>,
    pubsub_rx: &mut UnboundedReceiver<PubSubEvent>,
    push_rx: &mut UnboundedReceiver<Bytes>,
) -> anyhow::Result<Close>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = BytesMut::with_capacity(READ_CHUNK);
    let mut read_buf = vec![0u8; READ_CHUNK];
    let mut out = BytesMut::with_capacity(READ_CHUNK);

    loop {
        if let Some(rx) = &mut state.monitor_rx {
            tokio::select! {
                biased;
                _ = info.killed() => return Ok(Close::Killed),
                line = rx.recv() => {
                    if let Some(line) = line {
                        socket.write_all(&line).await?;
                    }
                }
                n = socket.read(&mut read_buf) => {
                    let n = n?;
                    if n == 0 {
                        return Ok(Close::Peer("connection closed by peer"));
                    }
                    buf.extend_from_slice(&read_buf[..n]);
                    loop {
                        match parse_command(&mut buf) {
                            Ok(args) if args.is_empty() => continue,
                            Ok(args) => {
                                let cmd = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
                                if cmd == "QUIT" {
                                    return Ok(Close::Peer("QUIT"));
                                }
                                if cmd != "PING" {
                                    let err = RespValue::err(
                                        "ERR MONITOR mode: only PING and QUIT are allowed",
                                    );
                                    write_resp(socket, &err, state.resp_version, &mut out).await?;
                                } else {
                                    let r = dispatch(ctx, state, &args).await;
                                    for resp in r {
                                        write_resp(socket, &resp, state.resp_version, &mut out).await?;
                                    }
                                }
                            }
                            Err(RespError::Incomplete) => break,
                            Err(e) => {
                                return Ok(Close::Protocol(e.to_string()));
                            }
                        }
                    }
                }
            }
            continue;
        }

        tokio::select! {
            biased;
            _ = info.killed() => return Ok(Close::Killed),
            ev = pubsub_rx.recv() => {
                let Some(event) = ev else { return Ok(Close::Peer("pubsub closed")) };
                let msg = format_pubsub_event_proto(&event, state.resp_version);
                write_resp(socket, &msg, state.resp_version, &mut out).await?;
            }
            push = push_rx.recv() => {
                let Some(bytes) = push else { continue };
                socket.write_all(&bytes).await?;
            }
            n = socket.read(&mut read_buf) => {
                let n = n?;
                if n == 0 {
                    return Ok(Close::Peer("connection closed by peer"));
                }
                buf.extend_from_slice(&read_buf[..n]);
                if buf.len() > MAX_QUERY_BUF {
                    return Ok(Close::Protocol("query buffer limit exceeded".into()));
                }
            }
        }

        loop {
            let args = match parse_command(&mut buf) {
                Ok(args) => args,
                Err(RespError::Incomplete) => break,
                Err(e) => {
                    let err = RespValue::err(format!("ERR Protocol error: {e}"));
                    err.encode_proto(&mut out, state.resp_version);
                    flush(socket, &mut out).await?;
                    return Ok(Close::Protocol(e.to_string()));
                }
            };
            if args.is_empty() {
                continue;
            }

            let blocking = is_blocking_name(&args[0]);
            if blocking {
                flush(socket, &mut out).await?;
            }
            let is_quit = args[0].eq_ignore_ascii_case(b"QUIT");

            let responses = {
                let mut fut = Box::pin(dispatch(ctx, state, &args));
                loop {
                    tokio::select! {
                        biased;
                        r = &mut fut => break r,
                        _ = info.killed() => return Ok(Close::Killed),
                        push = push_rx.recv() => {
                            if let Some(bytes) = push {
                                socket.write_all(&bytes).await?;
                            }
                        }
                        n = socket.read(&mut read_buf), if blocking => {
                            let n = n?;
                            if n == 0 {
                                return Ok(Close::Peer("connection closed by peer"));
                            }
                            buf.extend_from_slice(&read_buf[..n]);
                        }
                    }
                }
            };

            for resp in &responses {
                resp.encode_proto(&mut out, state.resp_version);
            }
            if is_quit {
                flush(socket, &mut out).await?;
                return Ok(Close::Peer("QUIT"));
            }
            // PSYNC/SYNC : bascule en mode feed réplica après FULLRESYNC + RDB.
            if state.replica_feed.is_some() {
                flush(socket, &mut out).await?;
                return replica_feed_loop(socket, ctx, state, info, &mut buf, &mut read_buf).await;
            }
        }
        flush(socket, &mut out).await?;
    }
}

/// Écrit le flux de commandes propagées et traite les REPLCONF ACK du réplica.
async fn replica_feed_loop<S>(
    socket: &mut S,
    ctx: &CommandContext,
    state: &mut ClientState,
    info: &Arc<ClientInfo>,
    buf: &mut BytesMut,
    read_buf: &mut [u8],
) -> anyhow::Result<Close>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut rx = state
        .replica_feed
        .take()
        .expect("replica_feed set before feed loop");
    let mut out = BytesMut::with_capacity(READ_CHUNK);
    loop {
        tokio::select! {
            biased;
            _ = info.killed() => return Ok(Close::Killed),
            msg = rx.recv() => {
                match msg {
                    Some(bytes) => {
                        socket.write_all(&bytes).await?;
                    }
                    None => return Ok(Close::Peer("replica feed closed")),
                }
            }
            n = socket.read(read_buf) => {
                let n = n?;
                if n == 0 {
                    return Ok(Close::Peer("replica closed connection"));
                }
                buf.extend_from_slice(&read_buf[..n]);
                loop {
                    match parse_command(buf) {
                        Ok(args) if args.is_empty() => continue,
                        Ok(args) => {
                            let responses = dispatch(ctx, state, &args).await;
                            for resp in &responses {
                                resp.encode_proto(&mut out, state.resp_version);
                            }
                            if !out.is_empty() {
                                socket.write_all(&out).await?;
                                out.clear();
                            }
                        }
                        Err(RespError::Incomplete) => break,
                        Err(e) => return Ok(Close::Protocol(e.to_string())),
                    }
                }
            }
        }
    }
}

async fn write_resp<S>(
    socket: &mut S,
    resp: &RespValue,
    proto: u8,
    scratch: &mut BytesMut,
) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    scratch.clear();
    resp.encode_proto(scratch, proto);
    socket.write_all(scratch).await?;
    scratch.clear();
    Ok(())
}

async fn flush<S>(socket: &mut S, out: &mut BytesMut) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    if out.is_empty() {
        return Ok(());
    }
    socket.write_all(out).await?;
    out.clear();
    Ok(())
}

pub async fn write_command(socket: &mut TcpStream, args: &[&[u8]]) -> std::io::Result<()> {
    let mut out = BytesMut::new();
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    socket.write_all(&out).await
}

pub async fn read_resp(socket: &mut TcpStream) -> anyhow::Result<RespValue> {
    use crate::resp::parse;
    let mut buf = BytesMut::new();
    let mut tmp = [0u8; 4096];
    loop {
        match parse(&mut buf) {
            Ok(v) => return Ok(v),
            Err(RespError::Incomplete) => {
                let n = socket.read(&mut tmp).await?;
                if n == 0 {
                    anyhow::bail!("connection closed");
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            Err(e) => anyhow::bail!("{e}"),
        }
    }
}

/// Lance un serveur éphémère sur un port libre (tests).
pub async fn spawn_test_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().unwrap();
    let server = FerredisServer::new(ServerConfig::default());
    {
        let mut cfg = server.ctx.config.write();
        cfg.port = addr.port();
        *cfg.listen_addr.lock() = addr.to_string();
    }
    tokio::spawn(async move {
        spawn_expiry_task(&server.ctx, 10);
        loop {
            match listener.accept().await {
                Ok((socket, peer)) => {
                    let ctx = server.ctx.clone();
                    tokio::spawn(async move {
                        let _ = handle_tcp(socket, ctx, peer).await;
                    });
                }
                Err(e) => {
                    error!("accept error: {e}");
                    break;
                }
            }
        }
    });
    addr
}

/// Variante configurable (TLS paths, unixsocket) pour tests d'intégration.
pub async fn spawn_test_server_with(config: ServerConfig) -> SocketAddr {
    let bind = config
        .bind
        .first()
        .cloned()
        .unwrap_or_else(|| "127.0.0.1".into());
    let port = if config.port == 0 { 0 } else { config.port };
    let listener = TcpListener::bind(format!("{bind}:{port}"))
        .await
        .expect("bind");
    let addr = listener.local_addr().unwrap();
    let mut cfg = config;
    cfg.port = addr.port();
    *cfg.listen_addr.lock() = addr.to_string();
    let server = FerredisServer::new(cfg);
    if server.ctx.config.read().cluster_enabled {
        let host = server
            .ctx
            .config
            .read()
            .bind
            .first()
            .cloned()
            .unwrap_or_else(|| "127.0.0.1".into());
        let port = addr.port();
        server.ctx.cluster.enable(&host, port);
    }

    #[cfg(any(unix, windows))]
    let local_listener = prepare_local_listener(&server.ctx)
        .await
        .expect("local listener");

    tokio::spawn(async move {
        spawn_expiry_task(&server.ctx, 10);
        crate::sentinel::spawn_sentinel_loop(server.ctx.clone());

        let (tcp_tx, mut tcp_rx) = unbounded_channel();
        spawn_tcp_accept(listener, tcp_tx);

        #[cfg(unix)]
        let unix_listener = local_listener;
        #[cfg(windows)]
        let mut pipe_listener = local_listener;

        loop {
            tokio::select! {
                accept = tcp_rx.recv() => {
                    let Some((socket, peer)) = accept else { break };
                    let ctx = server.ctx.clone();
                    tokio::spawn(async move {
                        let _ = handle_tcp(socket, ctx, peer).await;
                    });
                }
                accept = async {
                    #[cfg(unix)]
                    {
                        match &unix_listener {
                            Some(l) => Some(crate::unix_socket::listener::accept(l).await),
                            None => std::future::pending::<Option<std::io::Result<_>>>().await,
                        }
                    }
                    #[cfg(windows)]
                    {
                        match &mut pipe_listener {
                            Some(l) => Some(crate::unix_socket::listener::accept(l).await),
                            None => std::future::pending::<Option<std::io::Result<_>>>().await,
                        }
                    }
                    #[cfg(not(any(unix, windows)))]
                    {
                        let _: () = std::future::pending().await;
                        None
                    }
                }, if {
                    #[cfg(unix)]
                    { unix_listener.is_some() }
                    #[cfg(windows)]
                    { pipe_listener.is_some() }
                    #[cfg(not(any(unix, windows)))]
                    { false }
                } => {
                    let Some(Ok((stream, peer))) = accept else { continue };
                    let ctx = server.ctx.clone();
                    let laddr = peer.clone();
                    tokio::spawn(async move {
                        let _ = handle_stream(stream, ctx, peer, laddr).await;
                    });
                }
            }
        }
    });
    addr
}
