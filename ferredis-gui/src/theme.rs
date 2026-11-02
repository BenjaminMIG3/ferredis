//! Thème clair / sombre. Les couleurs sont lues à l'appel pour suivre le mode courant.

use eframe::egui::{
    self, epaint::Shadow, Color32, CornerRadius, FontData, FontDefinitions, FontFamily, FontId,
    Margin, RichText, Stroke, TextStyle, Vec2,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

static DARK: AtomicBool = AtomicBool::new(true);
static FONTS_READY: AtomicBool = AtomicBool::new(false);

pub fn is_dark() -> bool {
    DARK.load(Ordering::Relaxed)
}

pub fn set_dark(dark: bool) {
    DARK.store(dark, Ordering::Relaxed);
}

/// Accesseurs de couleur (changent avec le mode). Contraste pensé pour la lisibilité.
pub mod palette {
    use super::{is_dark, Color32};

    pub fn bg() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0x0d, 0x11, 0x17)
        } else {
            Color32::from_rgb(0xf0, 0xf2, 0xf5)
        }
    }
    pub fn panel_bg() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0x16, 0x1b, 0x22)
        } else {
            Color32::from_rgb(0xff, 0xff, 0xff)
        }
    }
    pub fn card() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0x1c, 0x21, 0x28)
        } else {
            Color32::from_rgb(0xff, 0xff, 0xff)
        }
    }
    pub fn card_hover() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0x26, 0x2c, 0x36)
        } else {
            Color32::from_rgb(0xeb, 0xee, 0xf2)
        }
    }
    pub fn border() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0x3d, 0x44, 0x4d)
        } else {
            Color32::from_rgb(0xd0, 0xd7, 0xde)
        }
    }
    pub fn input() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0x0d, 0x11, 0x17)
        } else {
            Color32::from_rgb(0xf6, 0xf8, 0xfa)
        }
    }
    pub fn faint() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0x21, 0x27, 0x2f)
        } else {
            Color32::from_rgb(0xf6, 0xf8, 0xfa)
        }
    }
    /// Texte principal — contraste élevé (fond sombre ou clair).
    pub fn text() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0xf0, 0xf3, 0xf6)
        } else {
            Color32::from_rgb(0x1f, 0x23, 0x28)
        }
    }
    /// Texte secondaire encore lisible (≥ ~4.5:1 sur carte).
    pub fn muted() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0xb0, 0xb8, 0xc4)
        } else {
            Color32::from_rgb(0x57, 0x60, 0x6a)
        }
    }
    pub fn accent() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0xff, 0x6b, 0x6b)
        } else {
            Color32::from_rgb(0xd9, 0x38, 0x38)
        }
    }
    pub fn accent_soft() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0x3a, 0x1f, 0x24)
        } else {
            Color32::from_rgb(0xfd, 0xec, 0xec)
        }
    }
    pub fn ok() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0x3f, 0xb9, 0x50)
        } else {
            Color32::from_rgb(0x1a, 0x7f, 0x37)
        }
    }
    pub fn warn() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0xe3, 0xb3, 0x41)
        } else {
            Color32::from_rgb(0x9a, 0x67, 0x00)
        }
    }
    pub fn err() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0xff, 0x7b, 0x72)
        } else {
            Color32::from_rgb(0xcf, 0x22, 0x2e)
        }
    }
    pub fn info() -> Color32 {
        if is_dark() {
            Color32::from_rgb(0x79, 0xb8, 0xff)
        } else {
            Color32::from_rgb(0x09, 0x69, 0xcb)
        }
    }
}

pub fn type_color(ty: &str) -> Color32 {
    match ty {
        "string" => palette::ok(),
        "list" => palette::info(),
        "hash" => {
            if is_dark() {
                Color32::from_rgb(0xd2, 0xa8, 0xff)
            } else {
                Color32::from_rgb(0x82, 0x50, 0xdf)
            }
        }
        "set" => palette::warn(),
        "zset" => {
            if is_dark() {
                Color32::from_rgb(0xff, 0xa1, 0xc9)
            } else {
                Color32::from_rgb(0xbf, 0x39, 0x8a)
            }
        }
        "stream" => {
            if is_dark() {
                Color32::from_rgb(0x76, 0xe3, 0xea)
            } else {
                Color32::from_rgb(0x11, 0x7a, 0x8b)
            }
        }
        _ => palette::muted(),
    }
}

pub fn event_color(kind: &str) -> Color32 {
    match kind {
        "connect" => palette::ok(),
        "disconnect" => palette::muted(),
        "auth_failed" | "protocol_error" => palette::err(),
        "killed" => palette::warn(),
        _ => palette::info(),
    }
}

pub fn stroke(width: f32, color: Color32) -> Stroke {
    Stroke::new(width, color)
}

fn corner(r: u8) -> CornerRadius {
    CornerRadius::same(r)
}

/// Charge une police système couvrant l'accentué FR et les symboles UI (une seule fois).
pub fn install_fonts(ctx: &egui::Context) {
    if FONTS_READY.swap(true, Ordering::SeqCst) {
        return;
    }

    let mut fonts = FontDefinitions::default();
    if let Some(data) = load_first_font(ui_font_candidates()) {
        fonts
            .font_data
            .insert("system_ui".into(), Arc::new(FontData::from_owned(data)));
        if let Some(family) = fonts.families.get_mut(&FontFamily::Proportional) {
            family.insert(0, "system_ui".into());
        }
        // Secours pour les glyphes absents de la mono embarquée.
        if let Some(family) = fonts.families.get_mut(&FontFamily::Monospace) {
            family.push("system_ui".into());
        }
    }
    if let Some(data) = load_first_font(mono_font_candidates()) {
        fonts
            .font_data
            .insert("system_mono".into(), Arc::new(FontData::from_owned(data)));
        if let Some(family) = fonts.families.get_mut(&FontFamily::Monospace) {
            family.insert(0, "system_mono".into());
        }
    }
    ctx.set_fonts(fonts);
}

fn load_first_font(paths: &[&str]) -> Option<Vec<u8>> {
    paths.iter().find_map(|p| std::fs::read(p).ok())
}

fn ui_font_candidates() -> &'static [&'static str] {
    &[
        #[cfg(windows)]
        r"C:\Windows\Fonts\segoeui.ttf",
        #[cfg(windows)]
        r"C:\Windows\Fonts\arial.ttf",
        #[cfg(windows)]
        r"C:\Windows\Fonts\tahoma.ttf",
        #[cfg(target_os = "macos")]
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        #[cfg(target_os = "macos")]
        "/Library/Fonts/Arial.ttf",
        #[cfg(target_os = "linux")]
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        #[cfg(target_os = "linux")]
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        #[cfg(target_os = "linux")]
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    ]
}

fn mono_font_candidates() -> &'static [&'static str] {
    &[
        #[cfg(windows)]
        r"C:\Windows\Fonts\consola.ttf",
        #[cfg(windows)]
        r"C:\Windows\Fonts\cour.ttf",
        #[cfg(target_os = "macos")]
        "/System/Library/Fonts/Supplemental/Courier New.ttf",
        #[cfg(target_os = "linux")]
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        #[cfg(target_os = "linux")]
        "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf",
    ]
}

/// Applique le thème courant (clair ou sombre) au contexte egui.
pub fn apply(ctx: &egui::Context, dark: bool) {
    install_fonts(ctx);
    set_dark(dark);
    use palette::*;

    // egui 0.31+ conserve deux styles (dark/light) et une préférence (souvent System).
    // Sans set_theme, le style OS actif peut rester clair alors que notre palette
    // (AtomicBool) est déjà en sombre → TextEdit fond blanc + texte blanc, titres .strong() noirs.
    let theme = if dark {
        egui::Theme::Dark
    } else {
        egui::Theme::Light
    };
    ctx.set_theme(theme);

    let mut style = (*ctx.style_of(theme)).clone();
    style.text_styles = [
        (
            TextStyle::Heading,
            FontId::new(20.0, FontFamily::Proportional),
        ),
        (TextStyle::Body, FontId::new(13.5, FontFamily::Proportional)),
        (
            TextStyle::Button,
            FontId::new(13.0, FontFamily::Proportional),
        ),
        (
            TextStyle::Small,
            FontId::new(12.0, FontFamily::Proportional),
        ),
        (
            TextStyle::Monospace,
            FontId::new(12.5, FontFamily::Monospace),
        ),
    ]
    .into();

    style.spacing.item_spacing = Vec2::new(10.0, 8.0);
    style.spacing.button_padding = Vec2::new(12.0, 6.0);
    style.spacing.window_margin = Margin::same(12);
    style.spacing.interact_size.y = 28.0;
    style.spacing.scroll.bar_width = 6.0;
    style.spacing.scroll.floating = true;

    let v = &mut style.visuals;
    v.dark_mode = dark;
    // Ne pas forcer override_text_color : ça écrase les couleurs RichText
    // (badges, muted, erreurs) et nuit à la lisibilité.
    v.override_text_color = None;
    v.panel_fill = panel_bg();
    v.window_fill = card();
    v.window_stroke = stroke(1.0, border());
    v.window_corner_radius = corner(10);
    v.menu_corner_radius = corner(8);
    v.extreme_bg_color = input();
    v.faint_bg_color = faint();
    v.code_bg_color = input();
    v.hyperlink_color = info();
    v.window_shadow = Shadow {
        offset: [0, 2],
        blur: 8,
        spread: 0,
        color: Color32::from_black_alpha(if dark { 90 } else { 20 }),
    };
    v.popup_shadow = v.window_shadow;
    v.selection.bg_fill = if dark {
        Color32::from_rgb(0x2d, 0x3a, 0x4f)
    } else {
        Color32::from_rgb(0xdd, 0xe8, 0xf7)
    };
    v.selection.stroke = stroke(1.0, accent());
    v.striped = true;

    let text_c = text();
    let muted_c = muted();
    let text_stroke = stroke(1.0, text_c);
    let muted_stroke = stroke(1.0, muted_c);
    let border_stroke = stroke(1.0, border());

    v.widgets.noninteractive.bg_fill = panel_bg();
    v.widgets.noninteractive.weak_bg_fill = panel_bg();
    v.widgets.noninteractive.bg_stroke = border_stroke;
    v.widgets.noninteractive.fg_stroke = muted_stroke;
    v.widgets.noninteractive.corner_radius = corner(6);

    v.widgets.inactive.bg_fill = card();
    v.widgets.inactive.weak_bg_fill = card();
    v.widgets.inactive.bg_stroke = border_stroke;
    v.widgets.inactive.fg_stroke = text_stroke;
    v.widgets.inactive.corner_radius = corner(6);

    v.widgets.hovered.bg_fill = card_hover();
    v.widgets.hovered.weak_bg_fill = card_hover();
    v.widgets.hovered.bg_stroke = stroke(1.0, accent());
    v.widgets.hovered.fg_stroke = text_stroke;
    v.widgets.hovered.corner_radius = corner(6);

    v.widgets.active.bg_fill = accent_soft();
    v.widgets.active.weak_bg_fill = accent_soft();
    v.widgets.active.bg_stroke = stroke(1.0, accent());
    v.widgets.active.fg_stroke = text_stroke;
    v.widgets.active.corner_radius = corner(6);
    v.widgets.open = v.widgets.active;

    ctx.set_style_of(theme, style);
}

/// Champ texte aligné sur la palette (évite fond clair / texte clair désynchronisés).
pub fn text_edit_single<'t>(text: &'t mut dyn egui::TextBuffer) -> egui::TextEdit<'t> {
    egui::TextEdit::singleline(text)
        .text_color(palette::text())
        .background_color(palette::input())
}

pub fn card_frame() -> egui::Frame {
    egui::Frame::new()
        .fill(palette::card())
        .stroke(stroke(1.0, palette::border()))
        .corner_radius(corner(10))
        .inner_margin(Margin::same(14))
}

/// Alias historique.
pub fn card() -> egui::Frame {
    card_frame()
}

pub fn panel(fill: Color32, margin: i8) -> egui::Frame {
    egui::Frame::new()
        .fill(fill)
        .inner_margin(Margin::same(margin))
}

pub fn badge(ui: &mut egui::Ui, text: &str, color: Color32) {
    let (r, g, b, _) = color.to_tuple();
    let bg_a = if is_dark() { 55 } else { 40 };
    egui::Frame::new()
        .fill(Color32::from_rgba_unmultiplied(r, g, b, bg_a))
        .stroke(stroke(1.0, Color32::from_rgba_unmultiplied(r, g, b, 160)))
        .corner_radius(corner(4))
        .inner_margin(Margin::symmetric(7, 2))
        .show(ui, |ui| {
            ui.label(RichText::new(text).color(color).small().strong());
        });
}

/// Carte de statistique : valeur toujours en couleur de texte (lisible).
pub fn stat_card(ui: &mut egui::Ui, label: &str, value: &str, hint: &str, accent: Color32) {
    card().show(ui, |ui| {
        ui.set_min_width(152.0);
        ui.vertical(|ui| {
            ui.horizontal(|ui| {
                let (rect, _) = ui.allocate_exact_size(Vec2::new(3.0, 12.0), egui::Sense::hover());
                ui.painter().rect_filled(rect, corner(2), accent);
                ui.label(
                    RichText::new(label)
                        .small()
                        .color(palette::muted())
                        .strong(),
                );
            });
            ui.add_space(2.0);
            ui.label(
                RichText::new(value)
                    .size(22.0)
                    .strong()
                    .color(palette::text()),
            );
            if !hint.is_empty() {
                ui.label(RichText::new(hint).small().color(palette::muted()));
            }
        });
    });
}

pub fn primary_button(text: &str) -> egui::Button<'static> {
    egui::Button::new(RichText::new(text).strong().color(Color32::WHITE))
        .fill(palette::accent())
        .stroke(Stroke::NONE)
        .corner_radius(corner(6))
}

pub fn ghost_button(text: &str) -> egui::Button<'static> {
    egui::Button::new(RichText::new(text).color(palette::text()))
        .fill(Color32::TRANSPARENT)
        .stroke(stroke(1.0, palette::border()))
        .corner_radius(corner(6))
}

pub fn tab(ui: &mut egui::Ui, selected: bool, text: &str) -> egui::Response {
    let color = if selected {
        palette::text()
    } else {
        palette::muted()
    };
    let label = RichText::new(text).size(13.5).color(color).strong();
    let resp = ui.add(
        egui::Button::new(label)
            .fill(if selected {
                palette::accent_soft()
            } else {
                Color32::TRANSPARENT
            })
            .stroke(Stroke::NONE)
            .corner_radius(corner(6)),
    );
    if selected {
        let r = resp.rect;
        ui.painter().line_segment(
            [
                r.left_bottom() + Vec2::new(6.0, 0.0),
                r.right_bottom() + Vec2::new(-6.0, 0.0),
            ],
            stroke(2.0, palette::accent()),
        );
    }
    resp
}
