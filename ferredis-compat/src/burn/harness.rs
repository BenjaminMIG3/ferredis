//! Harness burn : attentes Redis officielles, collecte de gaps sans adaptation Ferredis.

use bytes::Bytes;
use ferredis_core::resp::RespValue;
use ferredis_core::server::{read_resp, spawn_test_server, write_command};
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

static CASE_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub enum Status {
    Pass,
    Fail { detail: String },
    Error { detail: String },
}

#[derive(Debug, Clone)]
pub struct Case {
    pub suite: &'static str,
    pub name: String,
    pub status: Status,
    #[allow(dead_code)]
    pub elapsed_ms: u128,
}

#[derive(Default)]
pub struct Report {
    pub cases: Vec<Case>,
}

impl Report {
    pub fn record(&mut self, suite: &'static str, name: impl Into<String>, status: Status, elapsed: Duration) {
        self.cases.push(Case {
            suite,
            name: name.into(),
            status,
            elapsed_ms: elapsed.as_millis(),
        });
    }

    pub fn pass_count(&self) -> usize {
        self.cases
            .iter()
            .filter(|c| matches!(c.status, Status::Pass))
            .count()
    }

    pub fn fail_count(&self) -> usize {
        self.cases
            .iter()
            .filter(|c| !matches!(c.status, Status::Pass))
            .count()
    }

    pub fn summary(&self) -> String {
        let mut out = String::new();
        let total = self.cases.len();
        let pass = self.pass_count();
        let fail = self.fail_count();
        let _ = writeln!(
            out,
            "=== BURN REDIS-OFFICIEL ===\ncas={total}  ok={pass}  gaps={fail}\n"
        );
        if fail == 0 {
            let _ = writeln!(out, "Parité sémantique Redis 8 : aucun écart détecté.");
            return out;
        }
        let _ = writeln!(out, "--- GAPS (comportement ≠ Redis officiel) ---");
        for c in &self.cases {
            match &c.status {
                Status::Pass => {}
                Status::Fail { detail } => {
                    let _ = writeln!(out, "[FAIL] {}.{} — {detail}", c.suite, c.name);
                }
                Status::Error { detail } => {
                    let _ = writeln!(out, "[ERR ] {}.{} — {detail}", c.suite, c.name);
                }
            }
        }
        // Regroupement par suite
        let mut suites: Vec<&'static str> = self
            .cases
            .iter()
            .filter(|c| !matches!(c.status, Status::Pass))
            .map(|c| c.suite)
            .collect();
        suites.sort_unstable();
        suites.dedup();
        let _ = writeln!(out, "\n--- Suites impactées ({}) ---", suites.len());
        for s in suites {
            let n = self
                .cases
                .iter()
                .filter(|c| c.suite == s && !matches!(c.status, Status::Pass))
                .count();
            let _ = writeln!(out, "  {s}: {n} gap(s)");
        }
        out
    }
}

pub struct BurnCtx {
    pub addr: SocketAddr,
    pub report: Report,
}

impl BurnCtx {
    pub async fn new() -> Self {
        Self {
            addr: spawn_test_server().await,
            report: Report::default(),
        }
    }

    pub async fn connect(&self) -> TcpStream {
        TcpStream::connect(self.addr).await.expect("connect")
    }

    pub async fn cmd(&self, sock: &mut TcpStream, args: &[&str]) -> Result<RespValue, String> {
        let raw: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.cmd_raw(sock, &raw).await
    }

    pub async fn cmd_raw(
        &self,
        sock: &mut TcpStream,
        args: &[&[u8]],
    ) -> Result<RespValue, String> {
        write_command(sock, args)
            .await
            .map_err(|e| format!("write: {e}"))?;
        read_resp(sock).await.map_err(|e| format!("read: {e}"))
    }

    pub fn check(
        &mut self,
        suite: &'static str,
        name: impl Into<String>,
        elapsed: Duration,
        ok: bool,
        detail: impl Into<String>,
    ) {
        let status = if ok {
            Status::Pass
        } else {
            Status::Fail {
                detail: detail.into(),
            }
        };
        self.report.record(suite, name, status, elapsed);
    }

    pub fn err_case(
        &mut self,
        suite: &'static str,
        name: impl Into<String>,
        elapsed: Duration,
        detail: impl Into<String>,
    ) {
        self.report.record(
            suite,
            name,
            Status::Error {
                detail: detail.into(),
            },
            elapsed,
        );
    }

    /// Assertion d'égalité stricte (valeur Redis officielle attendue).
    pub async fn expect_eq(
        &mut self,
        suite: &'static str,
        name: &str,
        sock: &mut TcpStream,
        args: &[&str],
        expected: RespValue,
    ) {
        let t0 = Instant::now();
        match self.cmd(sock, args).await {
            Ok(got) => {
                let ok = got == expected;
                let detail = if ok {
                    String::new()
                } else {
                    format!(
                        "cmd={args:?}\n  attendu Redis: {}\n  obtenu:        {}",
                        fmt_resp(&expected),
                        fmt_resp(&got)
                    )
                };
                self.check(suite, name, t0.elapsed(), ok, detail);
            }
            Err(e) => self.err_case(suite, name, t0.elapsed(), e),
        }
    }

    pub async fn expect_ok(
        &mut self,
        suite: &'static str,
        name: &str,
        sock: &mut TcpStream,
        args: &[&str],
    ) {
        self.expect_eq(suite, name, sock, args, RespValue::ok())
            .await;
    }

    pub async fn expect_int(
        &mut self,
        suite: &'static str,
        name: &str,
        sock: &mut TcpStream,
        args: &[&str],
        n: i64,
    ) {
        self.expect_eq(suite, name, sock, args, RespValue::Integer(n))
            .await;
    }

    pub async fn expect_bulk(
        &mut self,
        suite: &'static str,
        name: &str,
        sock: &mut TcpStream,
        args: &[&str],
        s: &str,
    ) {
        self.expect_eq(suite, name, sock, args, bulk(s)).await;
    }

    pub async fn expect_null_bulk(
        &mut self,
        suite: &'static str,
        name: &str,
        sock: &mut TcpStream,
        args: &[&str],
    ) {
        self.expect_eq(suite, name, sock, args, RespValue::null_bulk())
            .await;
    }

    pub async fn expect_null_array(
        &mut self,
        suite: &'static str,
        name: &str,
        sock: &mut TcpStream,
        args: &[&str],
    ) {
        self.expect_eq(suite, name, sock, args, RespValue::null_array())
            .await;
    }

    pub async fn expect_error_contains(
        &mut self,
        suite: &'static str,
        name: &str,
        sock: &mut TcpStream,
        args: &[&str],
        needle: &str,
    ) {
        let t0 = Instant::now();
        match self.cmd(sock, args).await {
            Ok(RespValue::Error(e)) => {
                let msg = String::from_utf8_lossy(&e);
                let ok = msg.contains(needle);
                let detail = if ok {
                    String::new()
                } else {
                    format!("cmd={args:?}\n  erreur Redis doit contenir {needle:?}\n  obtenu: {msg}")
                };
                self.check(suite, name, t0.elapsed(), ok, detail);
            }
            Ok(other) => self.check(
                suite,
                name,
                t0.elapsed(),
                false,
                format!(
                    "cmd={args:?}\n  attendu erreur contenant {needle:?}\n  obtenu: {}",
                    fmt_resp(&other)
                ),
            ),
            Err(e) => self.err_case(suite, name, t0.elapsed(), e),
        }
    }

    /// Predicat libre (formes non déterministes : SCAN cursor, RANDOMKEY, TIME…).
    pub async fn expect_pred(
        &mut self,
        suite: &'static str,
        name: &str,
        sock: &mut TcpStream,
        args: &[&str],
        pred: impl FnOnce(&RespValue) -> Result<(), String>,
    ) {
        let t0 = Instant::now();
        match self.cmd(sock, args).await {
            Ok(got) => match pred(&got) {
                Ok(()) => self.check(suite, name, t0.elapsed(), true, ""),
                Err(detail) => self.check(
                    suite,
                    name,
                    t0.elapsed(),
                    false,
                    format!("cmd={args:?}\n  obtenu: {}\n  {detail}", fmt_resp(&got)),
                ),
            },
            Err(e) => self.err_case(suite, name, t0.elapsed(), e),
        }
    }

    pub fn unique(&self, prefix: &str) -> String {
        let n = CASE_SEQ.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}:{n}")
    }
}

pub fn bulk(s: &str) -> RespValue {
    RespValue::bulk(Bytes::from(s.to_string()))
}

pub fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

pub fn arr(items: Vec<RespValue>) -> RespValue {
    RespValue::array(items)
}

#[allow(dead_code)]
pub fn bulks(items: &[&str]) -> RespValue {
    arr(items.iter().map(|s| bulk(s)).collect())
}

pub fn fmt_resp(v: &RespValue) -> String {
    match v {
        RespValue::SimpleString(b) => format!("+{}", String::from_utf8_lossy(b)),
        RespValue::Error(b) => format!("-{}", String::from_utf8_lossy(b)),
        RespValue::Integer(n) => format!(":{n}"),
        RespValue::BulkString(None) => "$-1".into(),
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(b);
            if s.chars().all(|c| !c.is_control() || c == '\n' || c == '\t') {
                format!("${} \"{s}\"", b.len())
            } else {
                format!("${} <{} bytes>", b.len(), b.len())
            }
        }
        RespValue::Array(None) => "*-1".into(),
        RespValue::Array(Some(items)) => {
            let inner: Vec<_> = items.iter().map(fmt_resp).collect();
            format!("*[{}]", inner.join(", "))
        }
        RespValue::Null => "_".into(),
        RespValue::Bool(b) => format!("#{b}"),
        RespValue::Double(d) => format!(",{d}"),
        other => format!("{other:?}"),
    }
}

