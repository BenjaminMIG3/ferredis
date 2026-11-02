//! Compatibility helpers and integration smoke tests for Ferredis.

#[cfg(test)]
mod burn;

#[cfg(test)]
mod p1;

#[cfg(test)]
mod p2;

#[cfg(test)]
mod p3;

#[cfg(test)]
mod p4;

#[cfg(test)]
mod p5;

#[cfg(test)]
mod p9;

#[cfg(test)]
mod p10;

#[cfg(test)]
mod p11;

#[cfg(test)]
mod p12;

#[cfg(test)]
mod beyond_redis;

#[cfg(test)]
mod p6;

#[cfg(test)]
mod p7;

#[cfg(test)]
mod p8;

#[cfg(test)]
mod local_socket;

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use ferredis_core::command::{dispatch, ClientState, CommandContext, ServerConfig};
    use ferredis_core::resp::RespValue;
    use ferredis_core::server::{read_resp, spawn_test_server, write_command};
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// Envoie une commande et lit sa réponse.
    pub(crate) async fn cmd(sock: &mut TcpStream, args: &[&str]) -> RespValue {
        let raw: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        cmd_raw(sock, &raw).await
    }

    /// Variante binaire de [`cmd`].
    pub(crate) async fn cmd_raw(sock: &mut TcpStream, args: &[&[u8]]) -> RespValue {
        write_command(sock, args).await.unwrap();
        read_resp(sock).await.unwrap()
    }

    pub(crate) async fn connect(addr: SocketAddr) -> TcpStream {
        TcpStream::connect(addr).await.unwrap()
    }

    pub(crate) fn dbg(v: &RespValue) -> String {
        format!("{v:?}")
    }

    /// Rejoue `args` jusqu'à 2 s tant que la réponse (en Debug) ne satisfait pas `pred`
    /// (l'état des connexions est mis à jour de façon asynchrone côté serveur).
    async fn poll_until(sock: &mut TcpStream, args: &[&str], pred: impl Fn(&str) -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if pred(&dbg(&cmd(sock, args).await)) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        false
    }

    #[tokio::test]
    async fn tcp_ping_set_get() {
        let addr = spawn_test_server().await;
        let mut sock = connect(addr).await;
        assert_eq!(cmd(&mut sock, &["PING"]).await, RespValue::pong());
        assert_eq!(
            cmd(&mut sock, &["SET", "foo", "bar"]).await,
            RespValue::ok()
        );
        assert_eq!(
            cmd(&mut sock, &["GET", "foo"]).await,
            RespValue::bulk(Bytes::from_static(b"bar"))
        );
    }

    #[tokio::test]
    async fn large_value_roundtrip() {
        let addr = spawn_test_server().await;
        let mut sock = connect(addr).await;
        let big = "x".repeat(1_500_000);
        assert_eq!(cmd(&mut sock, &["SET", "big", &big]).await, RespValue::ok());
        assert_eq!(
            cmd(&mut sock, &["STRLEN", "big"]).await,
            RespValue::Integer(big.len() as i64)
        );
    }

    #[tokio::test]
    async fn pipelined_commands_answer_in_order() {
        let addr = spawn_test_server().await;
        let mut sock = connect(addr).await;
        let mut payload = Vec::new();
        for i in 0..50 {
            let n = i.to_string();
            payload.extend_from_slice(
                format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\nk{n}\r\n${}\r\n{n}\r\n",
                    n.len() + 1,
                    n.len()
                )
                .as_bytes(),
            );
        }
        payload.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$2\r\nk7\r\n");
        sock.write_all(&payload).await.unwrap();
        // `read_resp` jette les octets excédentaires : on garde un tampon persistant.
        let mut buf = bytes::BytesMut::new();
        let mut replies = Vec::new();
        while replies.len() < 51 {
            match ferredis_core::resp::parse(&mut buf) {
                Ok(v) => replies.push(v),
                Err(_) => {
                    let mut tmp = [0u8; 4096];
                    let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut tmp))
                        .await
                        .expect("réponse pipeline manquante")
                        .unwrap();
                    assert!(n > 0, "connexion fermée");
                    buf.extend_from_slice(&tmp[..n]);
                }
            }
        }
        assert!(replies[..50].iter().all(|r| *r == RespValue::ok()));
        assert_eq!(replies[50], RespValue::bulk(Bytes::from_static(b"7")));
    }

    #[tokio::test]
    async fn pubsub_roundtrip() {
        let addr = spawn_test_server().await;
        let mut sub = connect(addr).await;
        let conf = cmd(&mut sub, &["SUBSCRIBE", "ch"]).await;
        assert!(matches!(conf, RespValue::Array(Some(_))));

        let mut pub_sock = connect(addr).await;
        assert_eq!(
            cmd(&mut pub_sock, &["PUBLISH", "ch", "hello"]).await,
            RespValue::Integer(1)
        );

        match read_resp(&mut sub).await.unwrap() {
            RespValue::Array(Some(items)) => {
                assert_eq!(items[0], RespValue::bulk(Bytes::from_static(b"message")));
                assert_eq!(items[2], RespValue::bulk(Bytes::from_static(b"hello")));
            }
            _ => panic!("expected message array"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hash_zset_multi_eval() {
        let addr = spawn_test_server().await;
        let mut sock = connect(addr).await;
        assert_eq!(
            cmd(&mut sock, &["HSET", "h", "a", "1", "b", "2"]).await,
            RespValue::Integer(2)
        );
        assert_eq!(
            cmd(&mut sock, &["HGET", "h", "a"]).await,
            RespValue::bulk(Bytes::from_static(b"1"))
        );
        assert_eq!(
            cmd(&mut sock, &["ZADD", "z", "1", "m1", "2", "m2"]).await,
            RespValue::Integer(2)
        );
        let _ = cmd(&mut sock, &["MULTI"]).await;
        assert!(matches!(
            cmd(&mut sock, &["SET", "t", "1"]).await,
            RespValue::SimpleString(_)
        ));
        let _ = cmd(&mut sock, &["INCR", "t"]).await;
        match cmd(&mut sock, &["EXEC"]).await {
            RespValue::Array(Some(items)) => assert_eq!(items.len(), 2),
            other => panic!("expected EXEC array, got {other:?}"),
        }

        let ev = cmd(
            &mut sock,
            &[
                "EVAL",
                "return redis.call('SET', KEYS[1], 'lua')",
                "1",
                "lk",
            ],
        )
        .await;
        assert!(matches!(
            ev,
            RespValue::BulkString(Some(_)) | RespValue::SimpleString(_)
        ));
    }

    /// Exécute une commande directement via `dispatch` (sans TCP).
    async fn local(ctx: &CommandContext, state: &mut ClientState, args: &[&str]) -> RespValue {
        let args: Vec<Bytes> = args.iter().map(|a| Bytes::from(a.to_string())).collect();
        dispatch(ctx, state, &args).await.remove(0)
    }

    #[tokio::test]
    async fn django_like_cache_ops() {
        let ctx = CommandContext::new(ServerConfig::default());
        let mut st = ClientState {
            authenticated: true,
            ..Default::default()
        };
        // cache.set avec TTL
        let r = local(
            &ctx,
            &mut st,
            &["SET", "galaxy:1:session", "payload", "EX", "900"],
        )
        .await;
        assert_eq!(r, RespValue::ok());
        // cache.add => SET NX
        let r = local(&ctx, &mut st, &["SET", "lock", "1", "NX", "EX", "30"]).await;
        assert_eq!(r, RespValue::ok());
        // Celery-like LPUSH / BRPOP
        let r = local(&ctx, &mut st, &["LPUSH", "celery", "task-1"]).await;
        assert_eq!(r, RespValue::Integer(1));
        let r = local(&ctx, &mut st, &["BRPOP", "celery", "1"]).await;
        assert!(matches!(r, RespValue::Array(Some(_))));
    }

    // ---- Sorted sets bloquants (channels_redis) ------------------------------------------

    #[tokio::test]
    async fn bzpopmin_blocks_until_zadd() {
        let addr = spawn_test_server().await;
        let waiter = tokio::spawn(async move {
            let mut c = connect(addr).await;
            cmd(&mut c, &["BZPOPMIN", "q", "5"]).await
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut p = connect(addr).await;
        assert_eq!(
            cmd(&mut p, &["ZADD", "q", "1.5", "msg"]).await,
            RespValue::Integer(1)
        );
        let got = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("BZPOPMIN doit se réveiller")
            .unwrap();
        match got {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], RespValue::bulk(Bytes::from_static(b"q")));
                assert_eq!(items[1], RespValue::bulk(Bytes::from_static(b"msg")));
                assert_eq!(items[2], RespValue::bulk(Bytes::from_static(b"1.5")));
            }
            other => panic!("réponse inattendue: {other:?}"),
        }
    }

    #[tokio::test]
    async fn bzpopmin_timeout_returns_null() {
        let addr = spawn_test_server().await;
        let mut c = connect(addr).await;
        let start = Instant::now();
        let r = cmd(&mut c, &["BZPOPMIN", "empty", "0.2"]).await;
        assert_eq!(r, RespValue::Array(None));
        assert!(start.elapsed() >= Duration::from_millis(150));
    }

    #[tokio::test]
    async fn zset_commands_are_consistent() {
        let addr = spawn_test_server().await;
        let mut c = connect(addr).await;
        cmd(&mut c, &["ZADD", "z", "-1", "a", "0", "b", "5", "c"]).await;
        assert_eq!(
            cmd(&mut c, &["ZCOUNT", "z", "-inf", "(5"]).await,
            RespValue::Integer(2)
        );
        let r = cmd(
            &mut c,
            &["ZRANGEBYSCORE", "z", "-inf", "+inf", "WITHSCORES"],
        )
        .await;
        assert!(dbg(&r).contains("-1"));
        assert_eq!(
            cmd(&mut c, &["ZREMRANGEBYSCORE", "z", "-inf", "0"]).await,
            RespValue::Integer(2)
        );
        assert_eq!(cmd(&mut c, &["ZCARD", "z"]).await, RespValue::Integer(1));
        let r = cmd(&mut c, &["ZPOPMAX", "z"]).await;
        assert!(dbg(&r).contains('c'));
        assert_eq!(cmd(&mut c, &["EXISTS", "z"]).await, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn blocked_client_disconnect_does_not_eat_items() {
        let addr = spawn_test_server().await;
        let mut ghost = connect(addr).await;
        write_command(&mut ghost, &[b"BLPOP", b"jobs", b"0"])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(ghost);

        let mut c = connect(addr).await;
        let ok = {
            let mut probe = connect(addr).await;
            // Seuls `c` et `probe` doivent rester connectés.
            poll_until(&mut probe, &["CLIENT", "LIST"], |l| {
                l.matches("id=").count() <= 2
            })
            .await
        };
        assert!(ok, "le client bloqué déconnecté doit être purgé");

        assert_eq!(
            cmd(&mut c, &["RPUSH", "jobs", "x"]).await,
            RespValue::Integer(1)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cmd(&mut c, &["LLEN", "jobs"]).await, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn watch_detects_other_client_write() {
        let addr = spawn_test_server().await;
        let mut a = connect(addr).await;
        let mut b = connect(addr).await;
        cmd(&mut a, &["SET", "k", "1"]).await;
        assert_eq!(cmd(&mut a, &["WATCH", "k"]).await, RespValue::ok());
        cmd(&mut b, &["SET", "k", "2"]).await;
        cmd(&mut a, &["MULTI"]).await;
        cmd(&mut a, &["SET", "k", "3"]).await;
        assert_eq!(cmd(&mut a, &["EXEC"]).await, RespValue::Array(None));
        assert_eq!(
            cmd(&mut a, &["GET", "k"]).await,
            RespValue::bulk(Bytes::from_static(b"2"))
        );
    }

    #[tokio::test]
    async fn eval_script_with_zset_and_cjson_free_logic() {
        let addr = spawn_test_server().await;
        let mut c = connect(addr).await;
        let script =
            "redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2]); return redis.call('ZCARD', KEYS[1])";
        assert_eq!(
            cmd(&mut c, &["EVAL", script, "1", "zz", "3", "m"]).await,
            RespValue::Integer(1)
        );
        let sha = match cmd(&mut c, &["SCRIPT", "LOAD", script]).await {
            RespValue::BulkString(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
            other => panic!("SCRIPT LOAD: {other:?}"),
        };
        assert_eq!(
            cmd(&mut c, &["EVALSHA", &sha, "1", "zz", "4", "n"]).await,
            RespValue::Integer(2)
        );
    }

    // ---- Visibilité des connexions ------------------------------------------------------

    #[tokio::test]
    async fn client_list_and_connection_events() {
        let addr = spawn_test_server().await;
        let mut ws = connect(addr).await;
        assert_eq!(
            cmd(&mut ws, &["CLIENT", "SETNAME", "daphne-ws"]).await,
            RespValue::ok()
        );
        let list = dbg(&cmd(&mut ws, &["CLIENT", "LIST"]).await);
        assert!(list.contains("name=daphne-ws"), "CLIENT LIST: {list}");
        let ws_port = ws.local_addr().unwrap().port().to_string();
        drop(ws);

        let mut admin = connect(addr).await;
        let seen = poll_until(&mut admin, &["FERREDIS", "EVENTS"], |ev| {
            ev.contains(&ws_port) && ev.contains("disconnect")
        })
        .await;
        assert!(seen, "l'événement de déconnexion doit être journalisé");
    }

    #[tokio::test]
    async fn client_kill_closes_connection() {
        let addr = spawn_test_server().await;
        let mut victim = connect(addr).await;
        let id = match cmd(&mut victim, &["CLIENT", "ID"]).await {
            RespValue::Integer(n) => n.to_string(),
            other => panic!("CLIENT ID: {other:?}"),
        };
        let mut admin = connect(addr).await;
        assert_eq!(
            cmd(&mut admin, &["CLIENT", "KILL", "ID", &id]).await,
            RespValue::Integer(1)
        );
        write_command(&mut victim, &[b"PING"]).await.ok();
        let closed = tokio::time::timeout(Duration::from_secs(2), read_resp(&mut victim))
            .await
            .expect("le client tué doit être déconnecté");
        assert!(closed.is_err());
    }

    #[tokio::test]
    async fn unknown_command_message_matches_redis() {
        let addr = spawn_test_server().await;
        let mut c = connect(addr).await;
        let r = dbg(&cmd(&mut c, &["NOPE", "a"]).await);
        assert!(r.contains("unknown command"), "{r}");
    }
}
