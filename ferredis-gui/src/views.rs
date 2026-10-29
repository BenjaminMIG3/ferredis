//! Rendu des panneaux et onglets. Aucune E/S ici : on lit l'état de [`App`] et on émet des actions.

use crate::net::{clock, human_duration, KEY_LIMIT};
use crate::theme::{self, palette};
use crate::{App, Tab};
use eframe::egui::{self, Align, Color32, Layout, RichText, Vec2};

const EVENT_KINDS: [&str; 6] = [
    "tous",
    "connect",
    "disconnect",
    "auth_failed",
    "protocol_error",
    "killed",
];

fn muted(text: impl Into<String>) -> RichText {
    RichText::new(text.into()).color(palette::muted())
}

fn body(text: impl Into<String>) -> RichText {
    RichText::new(text.into()).color(palette::text())
}

fn empty_state(ui: &mut egui::Ui, title: &str, hint: &str) {
    ui.add_space(48.0);
    ui.vertical_centered(|ui| {
        ui.label(
            RichText::new(title)
                .size(17.0)
                .strong()
                .color(palette::text()),
        );
        ui.add_space(4.0);
        ui.label(muted(hint));
    });
}

fn section_title(ui: &mut egui::Ui, title: &str) {
    ui.label(
        RichText::new(title)
            .strong()
            .size(14.5)
            .color(palette::text()),
    );
}

fn format_ttl(ttl: i64) -> String {
    match ttl {
        -1 => "inf".into(),
        t if t < 0 => "-".into(),
        t => human_duration(t as u64),
    }
}

fn format_count(n: &str) -> String {
    crate::textutil::format_thousands(n)
}

// ───────────────────────────── Cadre global ─────────────────────────────

pub fn header(app: &mut App, ctx: &egui::Context) {
    egui::TopBottomPanel::top("header")
        .exact_height(52.0)
        .frame(
            theme::panel(palette::panel_bg(), 0)
                .inner_margin(egui::Margin::symmetric(16, 10))
                .stroke(theme::stroke(1.0, palette::border())),
        )
        .show(ctx, |ui| {
            ui.horizontal_centered(|ui| {
                // Marque
                let (rect, _) = ui.allocate_exact_size(Vec2::splat(16.0), egui::Sense::hover());
                ui.painter()
                    .circle_filled(rect.center(), 8.0, palette::accent());
                ui.painter()
                    .circle_filled(rect.center(), 3.5, palette::panel_bg());
                ui.label(
                    RichText::new("Ferredis")
                        .size(18.0)
                        .strong()
                        .color(palette::text()),
                );

                ui.add_space(10.0);
                if app.connected {
                    theme::badge(ui, "connecté", palette::ok());
                } else {
                    theme::badge(ui, "déconnecté", palette::err());
                }
                let local_running = app.server.is_running();
                if local_running {
                    theme::badge(ui, "serveur local", palette::info());
                }

                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    if ui.add(theme::primary_button("Connecter")).clicked() {
                        app.reconnect();
                    }

                    server_controls(app, ui);

                    let theme_label = if app.dark { "Clair" } else { "Sombre" };
                    if ui
                        .add(theme::ghost_button(theme_label))
                        .on_hover_text(if app.dark {
                            "Passer en mode clair"
                        } else {
                            "Passer en mode sombre"
                        })
                        .clicked()
                    {
                        app.dark = !app.dark;
                        theme::apply(&app.ctx, app.dark);
                    }

                    ui.add_space(6.0);
                    ui.add(
                        egui::DragValue::new(&mut app.port)
                            .range(1..=65535)
                            .speed(1),
                    );
                    ui.label(muted("Port"));
                    let host = egui::TextEdit::singleline(&mut app.host)
                        .desired_width(140.0)
                        .hint_text("hôte")
                        .text_color(palette::text());
                    if ui.add(host).lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                        app.reconnect();
                    }
                    ui.label(muted("Hôte"));
                });
            });
        });
}

/// Contrôles start/stop/restart du process local (redémarrage simple si le port change).
fn server_controls(app: &mut App, ui: &mut egui::Ui) {
    let running = app.server.is_running();
    let needs_restart = app.server_needs_restart();

    if needs_restart {
        if ui
            .add(theme::ghost_button("Redémarrer"))
            .on_hover_text("Applique hôte/port en redémarrant le serveur local")
            .clicked()
        {
            app.restart_server();
        }
    }

    if running {
        if ui
            .add(theme::ghost_button("Arrêter"))
            .on_hover_text("Arrête le ferredis-server lancé par le GUI")
            .clicked()
        {
            app.stop_server();
        }
    } else if ui
        .add(theme::ghost_button("Démarrer"))
        .on_hover_text("Lance ferredis-server sur l'hôte/port courants")
        .clicked()
    {
        app.start_server();
    }
}

pub fn status_bar(app: &mut App, ctx: &egui::Context) {
    egui::TopBottomPanel::bottom("status")
        .exact_height(36.0)
        .frame(
            theme::panel(palette::panel_bg(), 0)
                .inner_margin(egui::Margin::symmetric(16, 6))
                .stroke(theme::stroke(1.0, palette::border())),
        )
        .show(ctx, |ui| {
            ui.horizontal_centered(|ui| {
                match (&app.error, &app.snapshot) {
                    (Some(e), _) => {
                        ui.label(RichText::new(format!("! {e}")).color(palette::err()));
                    }
                    (None, Some(s)) => {
                        let version = s.info("redis_version").unwrap_or("?");
                        ui.label(muted(format!(
                            "{}:{}  ·  Redis {}  ·  {} ms  ·  {} clé(s)",
                            app.host,
                            app.port,
                            version,
                            s.rtt.as_millis(),
                            s.dbsize
                        )));
                    }
                    _ => {
                        ui.label(muted("Connexion en cours..."));
                    }
                }
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    if let Some(t) = &app.toast {
                        let color = if t.is_err {
                            palette::err()
                        } else {
                            palette::ok()
                        };
                        ui.label(RichText::new(&t.text).color(color));
                    }
                    ui.checkbox(&mut app.auto_refresh, "Auto");
                    if ui
                        .checkbox(&mut app.live_on, "Live")
                        .on_hover_text("Abonnement aux notifications keyspace")
                        .changed()
                    {
                        app.restart_live();
                    }
                    if ui.add(theme::ghost_button("Actualiser")).clicked() {
                        app.refresh();
                    }
                });
            });
        });
}

pub fn tab_bar(app: &mut App, ui: &mut egui::Ui) {
    ui.horizontal(|ui| {
        ui.spacing_mut().item_spacing.x = 4.0;
        for (tab, label) in Tab::ALL {
            let label = match (tab, &app.snapshot) {
                (Tab::Clients, Some(s)) => format!("{label} ({})", s.clients.len()),
                (Tab::Keys, Some(s)) => format!("{label} ({})", s.dbsize),
                _ => label.to_string(),
            };
            if theme::tab(ui, app.tab == tab, &label).clicked() {
                app.tab = tab;
            }
        }
    });
    ui.add_space(2.0);
    let y = ui.cursor().top();
    ui.painter().hline(
        ui.max_rect().x_range(),
        y,
        theme::stroke(1.0, palette::border()),
    );
    ui.add_space(10.0);
}

// ───────────────────────────── Aperçu ─────────────────────────────

pub fn overview(app: &mut App, ui: &mut egui::Ui) {
    let Some(s) = &app.snapshot else {
        return empty_state(
            ui,
            "En attente du serveur...",
            "Vérifie l'hôte et le port, puis clique sur Connecter.",
        );
    };

    let get = |k: &str| {
        s.info(k)
            .unwrap_or(crate::textutil::MISSING)
            .to_string()
    };
    let uptime = s
        .info("uptime_in_seconds")
        .and_then(|v| v.parse().ok())
        .map(human_duration)
        .unwrap_or_else(|| crate::textutil::MISSING.into());

    egui::ScrollArea::vertical()
        .auto_shrink([false, false])
        .show(ui, |ui| {
            ui.horizontal_wrapped(|ui| {
                ui.spacing_mut().item_spacing = Vec2::new(10.0, 10.0);
                theme::stat_card(
                    ui,
                    "Version",
                    &get("redis_version"),
                    &format!("Ferredis {}", get("ferredis_version")),
                    palette::accent(),
                );
                theme::stat_card(
                    ui,
                    "Uptime",
                    &uptime,
                    &format!("pid {}", get("process_id")),
                    palette::info(),
                );
                theme::stat_card(
                    ui,
                    "Clients",
                    &get("connected_clients"),
                    &format!("{} bloqué(s)", get("blocked_clients")),
                    palette::ok(),
                );
                theme::stat_card(
                    ui,
                    "Mémoire",
                    &get("used_memory_human"),
                    "estimation",
                    palette::info(),
                );
                theme::stat_card(
                    ui,
                    "Clés",
                    &format_count(&s.dbsize.to_string()),
                    &format!("{} canaux pub/sub", get("pubsub_channels")),
                    palette::warn(),
                );
                theme::stat_card(
                    ui,
                    "Commandes",
                    &format_count(&get("total_commands_processed")),
                    "traitées",
                    palette::text(),
                );
                theme::stat_card(
                    ui,
                    "Connexions",
                    &format_count(&get("total_connections_received")),
                    &format!("{} rejetée(s)", get("rejected_connections")),
                    palette::text(),
                );
                theme::stat_card(
                    ui,
                    "Échecs AUTH",
                    &get("auth_failures"),
                    "depuis le démarrage",
                    if get("auth_failures") == "0" {
                        palette::ok()
                    } else {
                        palette::err()
                    },
                );
            });

            ui.add_space(16.0);
            ui.columns(2, |cols| {
                theme::card().show(&mut cols[0], |ui| {
                    section_title(ui, "Keyspace");
                    ui.add_space(8.0);
                    let dbs: Vec<&(String, String)> = s
                        .info
                        .iter()
                        .filter(|(k, _)| {
                            k.starts_with("db") && k[2..].chars().all(|c| c.is_ascii_digit())
                        })
                        .collect();
                    if dbs.is_empty() {
                        ui.label(muted("Aucune base ne contient de clés."));
                    }
                    egui::Grid::new("keyspace")
                        .num_columns(2)
                        .spacing([28.0, 8.0])
                        .show(ui, |ui| {
                            for (db, stats) in dbs {
                                ui.label(
                                    RichText::new(db)
                                        .monospace()
                                        .color(palette::accent())
                                        .strong(),
                                );
                                ui.label(RichText::new(stats).monospace().color(palette::text()));
                                ui.end_row();
                            }
                        });
                });

                theme::card().show(&mut cols[1], |ui| {
                    section_title(ui, "Dernières connexions");
                    ui.add_space(8.0);
                    if s.events.is_empty() {
                        ui.label(muted("Aucun événement."));
                    }
                    for e in s.events.iter().rev().take(6) {
                        ui.horizontal(|ui| {
                            ui.label(
                                RichText::new(clock(e.at_ms))
                                    .monospace()
                                    .color(palette::muted()),
                            );
                            theme::badge(ui, &e.kind, theme::event_color(&e.kind));
                            ui.label(RichText::new(&e.addr).monospace().color(palette::text()));
                        });
                    }
                });
            });
        });
}

// ───────────────────────────── Clés ─────────────────────────────

pub fn keys(app: &mut App, ui: &mut egui::Ui) {
    let mut refresh = false;
    let mut to_inspect: Option<String> = None;

    egui::SidePanel::left("keys_list")
        .resizable(true)
        .default_width(380.0)
        .width_range(280.0..=620.0)
        .frame(
            egui::Frame::new()
                .fill(palette::card())
                .stroke(theme::stroke(1.0, palette::border()))
                .corner_radius(egui::CornerRadius::same(10))
                .inner_margin(egui::Margin::same(12)),
        )
        .show_inside(ui, |ui| {
            let resp = ui.add(
                egui::TextEdit::singleline(&mut app.key_filter)
                    .hint_text("Filtrer : texte ou motif glob (user:*)")
                    .desired_width(f32::INFINITY)
                    .text_color(palette::text()),
            );
            if resp.changed() {
                refresh = true;
            }
            let (count, truncated) = app
                .snapshot
                .as_ref()
                .map_or((0, false), |s| (s.keys.len(), s.truncated));
            ui.add_space(4.0);
            ui.label(muted(if truncated {
                format!("{count} clés affichées (limite {KEY_LIMIT}) - affine le filtre")
            } else {
                format!("{count} clé(s)")
            }));
            ui.add_space(4.0);

            let Some(snapshot) = &app.snapshot else {
                return;
            };
            if snapshot.keys.is_empty() {
                return empty_state(ui, "Aucune clé", "Rien ne correspond au filtre.");
            }
            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .show_rows(ui, 32.0, snapshot.keys.len(), |ui, range| {
                    for row in &snapshot.keys[range] {
                        let selected = app.selected_key.as_deref() == Some(row.name.as_str());
                        let bg = if selected {
                            palette::accent_soft()
                        } else {
                            Color32::TRANSPARENT
                        };
                        egui::Frame::new()
                            .fill(bg)
                            .corner_radius(egui::CornerRadius::same(6))
                            .inner_margin(egui::Margin::symmetric(6, 4))
                            .show(ui, |ui| {
                                ui.horizontal(|ui| {
                                    ui.set_min_height(24.0);
                                    theme::badge(ui, &row.ty, theme::type_color(&row.ty));
                                    let name =
                                        RichText::new(&row.name).monospace().color(palette::text());
                                    if ui.selectable_label(selected, name).clicked() {
                                        to_inspect = Some(row.name.clone());
                                    }
                                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                        ui.label(
                                            RichText::new(format_ttl(row.ttl))
                                                .small()
                                                .color(palette::muted()),
                                        );
                                    });
                                });
                            });
                    }
                });
        });

    egui::CentralPanel::default()
        .frame(egui::Frame::new().inner_margin(egui::Margin {
            left: 14,
            ..Default::default()
        }))
        .show_inside(ui, |ui| detail_panel(app, ui));

    if let Some(k) = to_inspect {
        app.inspect(&k);
    }
    if refresh {
        app.refresh();
    }
}

fn detail_panel(app: &mut App, ui: &mut egui::Ui) {
    let Some(key) = app.selected_key.clone() else {
        return empty_state(ui, "Sélectionne une clé", "Son contenu s'affichera ici.");
    };
    let Some(d) = app.detail.clone().filter(|d| d.key == key) else {
        ui.label(muted("Chargement..."));
        return;
    };

    theme::card().show(ui, |ui| {
        ui.set_width(ui.available_width());
        ui.horizontal(|ui| {
            theme::badge(ui, &d.ty, theme::type_color(&d.ty));
            ui.label(
                RichText::new(&d.key)
                    .monospace()
                    .size(16.0)
                    .strong()
                    .color(palette::text()),
            );
        });
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            ui.label(muted(format!("TTL : {}", format_ttl(d.ttl))));
            ui.label(muted(format!("Taille : {}", d.len)));
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if app.confirm_delete.as_deref() == Some(key.as_str()) {
                    if ui
                        .add(
                            egui::Button::new(RichText::new("Confirmer").color(Color32::WHITE))
                                .fill(palette::accent()),
                        )
                        .clicked()
                    {
                        app.delete_key(&key);
                        app.confirm_delete = None;
                    }
                    if ui.add(theme::ghost_button("Annuler")).clicked() {
                        app.confirm_delete = None;
                    }
                } else {
                    if ui.add(theme::ghost_button("Supprimer")).clicked() {
                        app.confirm_delete = Some(key.clone());
                    }
                    if ui.add(theme::ghost_button("Copier")).clicked() {
                        ui.ctx().copy_text(key.clone());
                    }
                }
            });
        });
    });

    ui.add_space(10.0);
    if d.truncated {
        ui.label(
            RichText::new(format!(
                "Aperçu partiel : {} sur {} éléments",
                d.rows.len(),
                d.len
            ))
            .color(palette::warn()),
        );
        ui.add_space(4.0);
    }
    egui::ScrollArea::vertical()
        .auto_shrink([false, false])
        .show(ui, |ui| {
            if d.ty == "string" {
                let value = d.rows.first().map(|r| r.1.as_str()).unwrap_or("");
                theme::card().show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    ui.add(
                        egui::Label::new(RichText::new(value).monospace().color(palette::text()))
                            .wrap(),
                    );
                });
                return;
            }
            let (col_a, col_b) = match d.ty.as_str() {
                "list" => ("Index", "Valeur"),
                "hash" => ("Champ", "Valeur"),
                "set" => ("", "Membre"),
                "zset" => ("Membre", "Score"),
                "stream" => ("ID", "Champs"),
                _ => ("", ""),
            };
            theme::card().show(ui, |ui| {
                ui.set_width(ui.available_width());
                egui::Grid::new("detail_rows")
                    .striped(true)
                    .num_columns(2)
                    .spacing([28.0, 8.0])
                    .min_col_width(80.0)
                    .show(ui, |ui| {
                        ui.label(muted(col_a));
                        ui.label(muted(col_b));
                        ui.end_row();
                        for (a, b) in &d.rows {
                            if d.ty == "set" {
                                ui.label("");
                                ui.label(RichText::new(b).monospace().color(palette::text()));
                            } else {
                                ui.label(
                                    RichText::new(a)
                                        .monospace()
                                        .color(palette::accent())
                                        .strong(),
                                );
                                ui.label(RichText::new(b).monospace().color(palette::text()));
                            }
                            ui.end_row();
                        }
                    });
            });
        });
}

// ───────────────────────────── Clients ─────────────────────────────

pub fn clients(app: &mut App, ui: &mut egui::Ui) {
    let Some(s) = &app.snapshot else {
        return empty_state(
            ui,
            "Aucune donnée",
            "En attente de la connexion au serveur.",
        );
    };
    ui.label(muted(
        "Connexions TCP actives (une application Django/Daphne apparaît ici dès qu'elle ouvre une connexion).",
    ));
    ui.add_space(8.0);
    let mut kill: Option<String> = None;
    egui::ScrollArea::both()
        .auto_shrink([false, false])
        .show(ui, |ui| {
            theme::card().show(ui, |ui| {
                egui::Grid::new("clients")
                    .striped(true)
                    .num_columns(8)
                    .spacing([20.0, 10.0])
                    .show(ui, |ui| {
                        for h in [
                            "ID",
                            "Nom",
                            "Adresse",
                            "Âge",
                            "Inactif",
                            "DB",
                            "Dernière cmd",
                            "",
                        ] {
                            ui.label(RichText::new(h).small().strong().color(palette::muted()));
                        }
                        ui.end_row();
                        for c in &s.clients {
                            let is_gui = c.name.starts_with("ferredis-gui");
                            ui.label(RichText::new(&c.id).monospace().color(palette::text()));
                            if c.name.is_empty() {
                                ui.label(muted(crate::textutil::MISSING));
                            } else {
                                ui.label(RichText::new(&c.name).color(if is_gui {
                                    palette::accent()
                                } else {
                                    palette::text()
                                }));
                            }
                            ui.label(RichText::new(&c.addr).monospace().color(palette::text()));
                            ui.label(body(human_duration(c.age)));
                            ui.label(body(human_duration(c.idle)));
                            ui.label(body(&c.db));
                            ui.label(RichText::new(&c.cmd).monospace().color(palette::info()));
                            if !is_gui && ui.small_button("Déconnecter").clicked() {
                                kill = Some(c.id.clone());
                            } else if is_gui {
                                ui.label("");
                            }
                            ui.end_row();
                        }
                    });
            });
        });
    if let Some(id) = kill {
        app.kill_client(&id);
    }
}

// ───────────────────────────── Connexions (journal) ─────────────────────────────

pub fn connections(app: &mut App, ui: &mut egui::Ui) {
    let Some(s) = &app.snapshot else {
        return empty_state(
            ui,
            "Aucune donnée",
            "En attente de la connexion au serveur.",
        );
    };
    ui.horizontal(|ui| {
        ui.label(muted(
            "Tentatives de connexion, déconnexions, échecs AUTH, erreurs de protocole.",
        ));
        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            egui::ComboBox::from_id_salt("event_filter")
                .selected_text(app.event_filter.clone())
                .show_ui(ui, |ui| {
                    for k in EVENT_KINDS {
                        ui.selectable_value(&mut app.event_filter, k.to_string(), k);
                    }
                });
            ui.label(muted("Type"));
        });
    });
    ui.add_space(8.0);
    let filter = app.event_filter.as_str();
    let rows: Vec<_> = s
        .events
        .iter()
        .rev()
        .filter(|e| filter == "tous" || e.kind == filter)
        .collect();
    if rows.is_empty() {
        return empty_state(
            ui,
            "Aucun événement",
            "Les connexions entrantes apparaîtront ici.",
        );
    }
    egui::ScrollArea::vertical()
        .auto_shrink([false, false])
        .show(ui, |ui| {
            theme::card().show(ui, |ui| {
                egui::Grid::new("events")
                    .striped(true)
                    .num_columns(5)
                    .spacing([20.0, 10.0])
                    .show(ui, |ui| {
                        for h in ["Heure (UTC)", "Type", "Client", "Adresse", "Détail"] {
                            ui.label(RichText::new(h).small().strong().color(palette::muted()));
                        }
                        ui.end_row();
                        for e in rows {
                            ui.label(
                                RichText::new(clock(e.at_ms))
                                    .monospace()
                                    .color(palette::text()),
                            );
                            theme::badge(ui, &e.kind, theme::event_color(&e.kind));
                            ui.label(
                                RichText::new(if e.client_id > 0 {
                                    e.client_id.to_string()
                                } else {
                                    crate::textutil::MISSING.into()
                                })
                                .monospace()
                                .color(palette::text()),
                            );
                            ui.label(RichText::new(&e.addr).monospace().color(palette::text()));
                            ui.label(muted(&e.detail));
                            ui.end_row();
                        }
                    });
            });
        });
}

// ───────────────────────────── Console ─────────────────────────────

pub fn console(app: &mut App, ui: &mut egui::Ui) {
    let input_height = 48.0;
    egui::ScrollArea::vertical()
        .auto_shrink([false, false])
        .stick_to_bottom(true)
        .max_height(ui.available_height() - input_height)
        .show(ui, |ui| {
            if app.console.is_empty() {
                ui.label(muted(
                    "Tape une commande Redis (ex. SET foo bar, ZRANGE z 0 -1 WITHSCORES).",
                ));
            }
            for e in &app.console {
                theme::card().show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("›").color(palette::accent()).strong());
                        ui.label(
                            RichText::new(&e.input)
                                .monospace()
                                .strong()
                                .color(palette::text()),
                        );
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            ui.label(muted(format!("{} ms", e.elapsed.as_millis())));
                        });
                    });
                    let color = if e.is_err {
                        palette::err()
                    } else {
                        palette::text()
                    };
                    ui.add_space(4.0);
                    ui.label(RichText::new(&e.output).monospace().color(color));
                });
                ui.add_space(6.0);
            }
        });

    ui.add_space(6.0);
    theme::card().show(ui, |ui| {
        ui.horizontal(|ui| {
            ui.label(
                RichText::new("›")
                    .color(palette::accent())
                    .strong()
                    .size(18.0),
            );
            let resp = ui.add(
                egui::TextEdit::singleline(&mut app.console_input)
                    .font(egui::TextStyle::Monospace)
                    .hint_text("Commande...")
                    .desired_width(ui.available_width() - 100.0)
                    .text_color(palette::text())
                    .frame(false),
            );
            if resp.has_focus() && !app.history.is_empty() {
                let (up, down) = ui.input(|i| {
                    (
                        i.key_pressed(egui::Key::ArrowUp),
                        i.key_pressed(egui::Key::ArrowDown),
                    )
                });
                if up {
                    let pos = app
                        .history_pos
                        .map_or(app.history.len() - 1, |p| p.saturating_sub(1));
                    app.history_pos = Some(pos);
                    app.console_input = app.history[pos].clone();
                } else if down {
                    match app.history_pos {
                        Some(p) if p + 1 < app.history.len() => {
                            app.history_pos = Some(p + 1);
                            app.console_input = app.history[p + 1].clone();
                        }
                        Some(_) => {
                            app.history_pos = None;
                            app.console_input.clear();
                        }
                        None => {}
                    }
                }
            }
            let submit = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
            if ui.add(theme::primary_button("Exécuter")).clicked() || submit {
                app.run_console();
                resp.request_focus();
            }
        });
    });
}

// ───────────────────────────── Live ─────────────────────────────

pub fn live(app: &mut App, ui: &mut egui::Ui) {
    let status = app
        .feed
        .status
        .lock()
        .map(|s| s.clone())
        .unwrap_or_default();
    ui.horizontal(|ui| {
        if app.live_on {
            theme::badge(ui, "actif", palette::ok());
            ui.label(muted(status));
        } else {
            theme::badge(ui, "désactivé", palette::muted());
            ui.label(muted("Active « Live » dans la barre du bas."));
        }
    });
    ui.add_space(8.0);
    let events: Vec<_> = app
        .feed
        .events
        .lock()
        .map(|q| q.iter().rev().cloned().collect())
        .unwrap_or_default();
    if events.is_empty() {
        return empty_state(
            ui,
            "Aucun événement pour l'instant",
            "Exécute une commande d'écriture pour la voir passer ici.",
        );
    }
    egui::ScrollArea::vertical()
        .auto_shrink([false, false])
        .show(ui, |ui| {
            theme::card().show(ui, |ui| {
                egui::Grid::new("live")
                    .striped(true)
                    .num_columns(4)
                    .spacing([20.0, 8.0])
                    .show(ui, |ui| {
                        for h in ["Heure", "DB", "Événement", "Clé"] {
                            ui.label(RichText::new(h).small().strong().color(palette::muted()));
                        }
                        ui.end_row();
                        for e in events {
                            ui.label(
                                RichText::new(clock(e.at_ms))
                                    .monospace()
                                    .color(palette::muted()),
                            );
                            ui.label(
                                RichText::new(format!("db{}", e.db))
                                    .monospace()
                                    .color(palette::muted()),
                            );
                            theme::badge(ui, &e.event, palette::info());
                            ui.label(RichText::new(&e.key).monospace().color(palette::text()));
                            ui.end_row();
                        }
                    });
            });
        });
}

fn info_kv(ui: &mut egui::Ui, s: &crate::net::Snapshot, keys: &[&str]) {
    egui::Grid::new(ui.id().with("info_kv"))
        .num_columns(2)
        .spacing([24.0, 6.0])
        .show(ui, |ui| {
            for k in keys {
                ui.label(RichText::new(*k).monospace().color(palette::muted()));
                ui.label(
                    RichText::new(s.info(k).unwrap_or(crate::textutil::MISSING))
                        .monospace()
                        .color(palette::text()),
                );
                ui.end_row();
            }
        });
}

/// Cluster : métriques INFO + rappel des commandes CLUSTER.
pub fn cluster(app: &mut App, ui: &mut egui::Ui) {
    let Some(s) = &app.snapshot else {
        return empty_state(ui, "Pas de snapshot", "Connecte-toi au serveur.");
    };
    theme::card().show(ui, |ui| {
        ui.label(RichText::new("Cluster").strong().size(16.0));
        ui.add_space(8.0);
        info_kv(
            ui,
            s,
            &[
                "cluster_enabled",
                "cluster_state",
                "cluster_slots_assigned",
                "cluster_known_nodes",
                "cluster_size",
            ],
        );
        ui.add_space(8.0);
        ui.label(
            RichText::new("Console : CLUSTER NODES / CLUSTER INFO / CLUSTER SLOTS")
                .small()
                .color(palette::muted()),
        );
    });
}

/// Réplication master/replica.
pub fn replica(app: &mut App, ui: &mut egui::Ui) {
    let Some(s) = &app.snapshot else {
        return empty_state(ui, "Pas de snapshot", "Connecte-toi au serveur.");
    };
    theme::card().show(ui, |ui| {
        ui.label(RichText::new("Réplication").strong().size(16.0));
        ui.add_space(8.0);
        info_kv(
            ui,
            s,
            &[
                "role",
                "connected_slaves",
                "master_replid",
                "master_repl_offset",
                "master_host",
                "master_port",
                "master_link_status",
            ],
        );
    });
}

/// ACL : pointeurs INFO + usage console.
pub fn acl(app: &mut App, ui: &mut egui::Ui) {
    let Some(s) = &app.snapshot else {
        return empty_state(ui, "Pas de snapshot", "Connecte-toi au serveur.");
    };
    theme::card().show(ui, |ui| {
        ui.label(RichText::new("ACL").strong().size(16.0));
        ui.add_space(8.0);
        info_kv(
            ui,
            s,
            &[
                "acl_access_denied_auth",
                "acl_access_denied_cmd",
                "acl_access_denied_key",
            ],
        );
        ui.add_space(8.0);
        ui.label(
            RichText::new("Console : ACL LIST / ACL USERS / ACL WHOAMI / ACL GETUSER")
                .small()
                .color(palette::muted()),
        );
        let _ = s;
    });
}
