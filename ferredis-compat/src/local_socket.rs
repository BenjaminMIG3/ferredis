//! Smoke socket local : Unix domain (Linux) / named pipe (Windows).
//! Réutilise `spawn_test_server_with` (même harness que p7/p8).

use bytes::Bytes;
use ferredis_core::command::ServerConfig;
use ferredis_core::resp::RespValue;
use ferredis_core::server::spawn_test_server_with;
use ferredis_core::unix_socket::normalize_local_path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn ping_local(path: &str) -> RespValue {
    let normalized = normalize_local_path(path);
    // Laisse le listener local démarrer (race TCP vs accept local).
    tokio::time::sleep(Duration::from_millis(50)).await;

    #[cfg(unix)]
    {
        use tokio::net::UnixStream;
        let mut s = UnixStream::connect(&normalized)
            .await
            .unwrap_or_else(|e| panic!("connect {normalized}: {e}"));
        write_command_any(&mut s, &[b"PING"]).await;
        read_resp_any(&mut s).await
    }
    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ClientOptions;
        let mut s = None;
        for _ in 0..40 {
            match ClientOptions::new().open(&normalized) {
                Ok(c) => {
                    s = Some(c);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        let mut s = s.unwrap_or_else(|| panic!("connect pipe {normalized}"));
        write_command_any(&mut s, &[b"PING"]).await;
        read_resp_any(&mut s).await
    }
}

async fn write_command_any<S: AsyncWriteExt + Unpin>(socket: &mut S, args: &[&[u8]]) {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
    socket.write_all(&buf).await.unwrap();
}

async fn read_resp_any<S: AsyncReadExt + Unpin>(socket: &mut S) -> RespValue {
    use bytes::BytesMut;
    use ferredis_core::resp::{parse, RespError};
    let mut buf = BytesMut::with_capacity(256);
    let mut tmp = [0u8; 256];
    loop {
        match parse(&mut buf) {
            Ok(v) => return v,
            Err(RespError::Incomplete) => {
                let n = socket.read(&mut tmp).await.unwrap();
                assert!(n > 0, "connection closed");
                buf.extend_from_slice(&tmp[..n]);
            }
            Err(e) => panic!("{e}"),
        }
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unix_socket_ping() {
    let path = std::env::temp_dir().join(format!(
        "ferredis-local-{}-{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let path_s = path.to_string_lossy().into_owned();
    let mut cfg = ServerConfig::default();
    cfg.port = 0;
    cfg.unixsocket = Some(path_s.clone());
    let _tcp = spawn_test_server_with(cfg).await;

    let resp = ping_local(&path_s).await;
    assert_eq!(resp, RespValue::SimpleString(Bytes::from_static(b"PONG")));

    let _ = std::fs::remove_file(&path);
}

#[cfg(windows)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn named_pipe_ping() {
    let name = format!(
        r"\\.\pipe\ferredis-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut cfg = ServerConfig::default();
    cfg.port = 0;
    cfg.unixsocket = Some(name.clone());
    let _tcp = spawn_test_server_with(cfg).await;

    let resp = ping_local(&name).await;
    assert_eq!(resp, RespValue::SimpleString(Bytes::from_static(b"PONG")));
}
