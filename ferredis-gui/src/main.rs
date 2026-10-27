//! Ferredis GUI : tableau de bord egui pour inspecter un serveur Redis/Ferredis.
//!
//! - `net`    : connexion persistante + thread de travail (aucune E/S dans l'UI)
//! - `theme`  : palette, styles et composants visuels
//! - `views`  : rendu de chaque onglet

mod net;
mod theme;
mod views;

use clap::Parser;
use eframe::egui;
use net::{spawn_live, spawn_worker, Backend, KeyDetail, LiveFeed, Reply, Request, Snapshot};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Intervalle d'actualisation automatique.
const REFRESH_EVERY: Duration = Duration::from_secs(2);
/// Nombre maximal d'entrées conservées dans la console.
const CONSOLE_CAPACITY: usize = 500;

#[derive(Parser, Debug)]
#[command(name = "ferredis-gui", about = "Interface graphique Ferredis")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(short, long, default_value_t = 6379)]
    port: u16,
}

fn main() -> eframe::Result<()> {
    let args = Args::parse();
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1180.0, 760.0])
            .with_min_inner_size([860.0, 520.0])
            .with_title("Ferredis"),
        ..Default::default()
    };
    eframe::run_native(
        "Ferredis",
        options,
        Box::new(move |cc| {
            let dark = cc
                .storage
                .and_then(|s| s.get_string("theme"))
                .map(|t| t != "light")
                .unwrap_or(true);
            theme::apply(&cc.egui_ctx, dark);
            Ok(Box::new(App::new(
                args.host,
                args.port,
                cc.egui_ctx.clone(),
                dark,
            )))
        }),
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Overview,
    Keys,
    Clients,
    Connections,
    Console,
    Live,
    Cluster,
    Replica,
    Acl,
}

impl Tab {
    pub const ALL: [(Tab, &'static str); 9] = [
        (Tab::Overview, "Aperçu"),
        (Tab::Keys, "Clés"),
        (Tab::Clients, "Clients"),
        (Tab::Connections, "Connexions"),
        (Tab::Console, "Console"),
        (Tab::Live, "Live"),
        (Tab::Cluster, "Cluster"),
        (Tab::Replica, "Réplica"),
        (Tab::Acl, "ACL"),
    ];
}

pub struct ConsoleEntry {
    pub input: String,
    pub output: String,
    pub is_err: bool,
    pub elapsed: Duration,
}

pub struct Toast {
    pub text: String,
    pub is_err: bool,
    pub at: Instant,
}

pub struct App {
    pub ctx: egui::Context,
    backend: Backend,

    // Connexion
    pub host: String,
    pub port: u16,
    pub connected: bool,
    pub error: Option<String>,

    // Données
    pub snapshot: Option<Snapshot>,
    pub detail: Option<KeyDetail>,
    in_flight: bool,
    refresh_now: bool,
    last_refresh: Instant,
    pub auto_refresh: bool,

    // Navigation / filtres
    pub tab: Tab,
    pub key_filter: String,
    pub selected_key: Option<String>,
    pub confirm_delete: Option<String>,
    pub event_filter: String,

    // Console
    pub console: VecDeque<ConsoleEntry>,
    pub console_input: String,
    pub history: Vec<String>,
    pub history_pos: Option<usize>,

    // Temps réel
    pub live_on: bool,
    pub feed: Arc<LiveFeed>,
    live_stop: Arc<AtomicBool>,
    live_handle: Option<JoinHandle<()>>,

    pub toast: Option<Toast>,
    pub dark: bool,
}

impl App {
    fn new(host: String, port: u16, ctx: egui::Context, dark: bool) -> Self {
        let backend = spawn_worker(host.clone(), port, ctx.clone());
        let mut app = Self {
            ctx,
            backend,
            host,
            port,
            connected: false,
            error: None,
            snapshot: None,
            detail: None,
            in_flight: false,
            refresh_now: true,
            last_refresh: Instant::now(),
            auto_refresh: true,
            tab: Tab::Overview,
            key_filter: String::new(),
            selected_key: None,
            confirm_delete: None,
            event_filter: "tous".into(),
            console: VecDeque::new(),
            console_input: String::new(),
            history: Vec::new(),
            history_pos: None,
            live_on: true,
            feed: Arc::new(LiveFeed::default()),
            live_stop: Arc::new(AtomicBool::new(false)),
            live_handle: None,
            toast: None,
            dark,
        };
        app.restart_live();
        app
    }

    // ───────────── Requêtes ─────────────

    /// Motif SCAN effectif : un texte simple devient `*texte*`.
    fn scan_pattern(&self) -> String {
        let f = self.key_filter.trim();
        if f.is_empty() {
            "*".into()
        } else if f.contains(['*', '?', '[']) {
            f.to_string()
        } else {
            format!("*{f}*")
        }
    }

    fn request_snapshot(&mut self) {
        if self.in_flight {
            return;
        }
        self.in_flight = true;
        self.refresh_now = false;
        self.last_refresh = Instant::now();
        self.backend.send(Request::Snapshot {
            pattern: self.scan_pattern(),
        });
    }

    pub fn refresh(&mut self) {
        self.refresh_now = true;
    }

    pub fn reconnect(&mut self) {
        self.backend.send(Request::Connect {
            host: self.host.trim().to_string(),
            port: self.port,
        });
        self.connected = false;
        self.error = None;
        self.snapshot = None;
        self.detail = None;
        self.in_flight = false;
        self.refresh_now = true;
        self.restart_live();
    }

    pub fn inspect(&mut self, key: &str) {
        self.selected_key = Some(key.to_string());
        self.backend.send(Request::Inspect(key.to_string()));
    }

    pub fn delete_key(&mut self, key: &str) {
        self.backend.send(Request::Delete(key.to_string()));
        if self.selected_key.as_deref() == Some(key) {
            self.selected_key = None;
            self.detail = None;
        }
    }

    pub fn kill_client(&mut self, id: &str) {
        self.backend.send(Request::KillClient(id.to_string()));
    }

    pub fn run_console(&mut self) {
        let line = self.console_input.trim().to_string();
        if line.is_empty() {
            return;
        }
        if self.history.last() != Some(&line) {
            self.history.push(line.clone());
        }
        self.history_pos = None;
        self.console_input.clear();
        self.backend.send(Request::Exec(line));
    }

    pub fn notify(&mut self, text: impl Into<String>, is_err: bool) {
        self.toast = Some(Toast {
            text: text.into(),
            is_err,
            at: Instant::now(),
        });
    }

    // ───────────── Flux temps réel ─────────────

    fn stop_live(&mut self) {
        self.live_stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.live_handle.take() {
            let _ = h.join();
        }
        self.live_stop = Arc::new(AtomicBool::new(false));
    }

    pub fn restart_live(&mut self) {
        self.stop_live();
        if self.live_on {
            self.live_handle = Some(spawn_live(
                self.host.trim().to_string(),
                self.port,
                Arc::clone(&self.feed),
                Arc::clone(&self.live_stop),
                self.ctx.clone(),
            ));
        }
    }

    // ───────────── Réponses du thread de travail ─────────────

    fn poll_replies(&mut self) {
        while let Ok(reply) = self.backend.rx.try_recv() {
            match reply {
                Reply::Snapshot(s) => {
                    self.in_flight = false;
                    self.connected = true;
                    self.error = None;
                    self.snapshot = Some(s);
                    if let Some(k) = self.selected_key.clone() {
                        self.backend.send(Request::Inspect(k));
                    }
                }
                Reply::Detail(d) => self.detail = Some(d),
                Reply::Console {
                    input,
                    output,
                    is_err,
                    elapsed,
                } => {
                    if self.console.len() == CONSOLE_CAPACITY {
                        self.console.pop_front();
                    }
                    self.console.push_back(ConsoleEntry {
                        input,
                        output,
                        is_err,
                        elapsed,
                    });
                    self.refresh();
                }
                Reply::Done(msg) => {
                    if !msg.is_empty() {
                        self.notify(msg, false);
                    }
                    self.refresh();
                }
                Reply::Down(e) => {
                    self.in_flight = false;
                    self.connected = false;
                    self.error = Some(e);
                }
            }
        }
    }
}

impl Drop for App {
    fn drop(&mut self) {
        self.stop_live();
    }
}

impl eframe::App for App {
    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        storage.set_string(
            "theme",
            if self.dark {
                "dark".to_string()
            } else {
                "light".to_string()
            },
        );
    }

    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_replies();

        // Un événement temps réel déclenche une actualisation (limitée par `in_flight`).
        if self.live_on && self.feed.dirty.swap(false, Ordering::SeqCst) {
            self.refresh_now = true;
        }
        if self.auto_refresh && self.last_refresh.elapsed() >= REFRESH_EVERY {
            self.refresh_now = true;
        }
        if self.refresh_now {
            self.request_snapshot();
        }
        if let Some(t) = &self.toast {
            if t.at.elapsed() > Duration::from_secs(4) {
                self.toast = None;
            }
        }

        views::header(self, ctx);
        views::status_bar(self, ctx);
        egui::CentralPanel::default()
            .frame(theme::panel(theme::palette::bg(), 16))
            .show(ctx, |ui| {
                views::tab_bar(self, ui);
                match self.tab {
                    Tab::Overview => views::overview(self, ui),
                    Tab::Keys => views::keys(self, ui),
                    Tab::Clients => views::clients(self, ui),
                    Tab::Connections => views::connections(self, ui),
                    Tab::Console => views::console(self, ui),
                    Tab::Live => views::live(self, ui),
                    Tab::Cluster => views::cluster(self, ui),
                    Tab::Replica => views::replica(self, ui),
                    Tab::Acl => views::acl(self, ui),
                }
            });

        ctx.request_repaint_after(Duration::from_millis(500));
    }
}
