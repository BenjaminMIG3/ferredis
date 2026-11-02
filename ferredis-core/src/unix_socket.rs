//! Socket local natif : Unix domain (Linux/Unix) / named pipe (Windows).
//!
//! - **Unix :** `unixsocket` + `unixsocketperm` (chmod après bind).
//! - **Windows :** chemin style `\\.\pipe\ferredis` (ou valeur `unixsocket` conf).

use std::path::Path;

/// Chemin pipe Windows par défaut.
#[cfg(windows)]
pub const WINDOWS_PIPE_HINT: &str = r"\\.\pipe\ferredis";

/// Normalise le chemin local configuré (Unix : tel quel ; Windows : `\\.\pipe\…`).
pub fn normalize_local_path(configured: &str) -> String {
    #[cfg(windows)]
    {
        windows_pipe_path(configured)
    }
    #[cfg(not(windows))]
    {
        configured.to_string()
    }
}

/// Valide le chemin avant bind.
pub fn validate_unix_path(path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err("empty unixsocket path".into());
    }
    #[cfg(unix)]
    {
        let p = Path::new(path);
        if let Some(parent) = p.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                return Err(format!("unixsocket directory missing: {}", parent.display()));
            }
        }
    }
    #[cfg(windows)]
    {
        let _ = Path::new(path);
    }
    Ok(())
}

/// Normalise le chemin Windows vers `\\.\pipe\…`.
#[cfg(windows)]
pub fn windows_pipe_path(configured: &str) -> String {
    let n = configured.replace('/', "\\");
    if n.starts_with(r"\\.\pipe\") {
        n
    } else {
        let name = n.trim_matches(|c| c == '\\');
        format!(r"\\.\pipe\{name}")
    }
}

#[cfg(unix)]
pub mod listener {
    use super::validate_unix_path;
    use std::io;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use tokio::net::{UnixListener, UnixStream};

    pub type LocalStream = UnixStream;

    pub async fn bind(path: &str, perm_octal: &str) -> io::Result<UnixListener> {
        validate_unix_path(path).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let p = Path::new(path);
        if p.exists() {
            let _ = std::fs::remove_file(p);
        }
        let listener = UnixListener::bind(p)?;
        let mode = u32::from_str_radix(perm_octal, 8).unwrap_or(0o770);
        let mut perms = std::fs::metadata(p)?.permissions();
        perms.set_mode(mode);
        std::fs::set_permissions(p, perms)?;
        Ok(listener)
    }

    pub async fn accept(listener: &UnixListener) -> io::Result<(UnixStream, String)> {
        let (stream, _) = listener.accept().await?;
        let peer = listener
            .local_addr()
            .ok()
            .and_then(|a| a.as_pathname().map(|p| p.display().to_string()))
            .unwrap_or_else(|| "unix".into());
        Ok((stream, peer))
    }
}

#[cfg(windows)]
pub mod listener {
    use super::{normalize_local_path, validate_unix_path};
    use std::io;
    use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};

    pub type LocalStream = NamedPipeServer;

    pub struct PipeListener {
        path: String,
        server: NamedPipeServer,
    }

    pub async fn bind(path: &str, _perm_octal: &str) -> io::Result<PipeListener> {
        validate_unix_path(path).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let path = normalize_local_path(path);
        let server = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&path)?;
        Ok(PipeListener { path, server })
    }

    pub async fn accept(listener: &mut PipeListener) -> io::Result<(NamedPipeServer, String)> {
        listener.server.connect().await?;
        let connected = std::mem::replace(
            &mut listener.server,
            ServerOptions::new().create(&listener.path)?,
        );
        let peer = listener.path.clone();
        Ok((connected, peer))
    }
}
