//! Décodage et formatage d'affichage (responsabilité unique : texte UI/Redis).
//!
//! - UTF-8 valide → texte (+ échappement des contrôles)
//! - Latin-1 textuel (accentué FR) → décodage Latin-1
//! - Pickle Python/Django → aperçu clé = valeur (extraction sûre, sans unpickle)
//! - Autre binaire → ASCII imprimable + `\xHH`

use std::fmt::Write;

/// Placeholder UI ASCII-safe (l'em dash U+2014 devient souvent un cube sans police système).
pub const MISSING: &str = "-";

/// Convertit des octets Redis en texte affichable sans glyphes « tofu ».
pub fn from_redis_bytes(bytes: &[u8]) -> String {
    if is_pickle(bytes) {
        if let Some(preview) = pickle_preview(bytes) {
            return preview;
        }
        return escape_bytes(bytes);
    }
    if looks_compressed(bytes) {
        return format!("[binaire compressé, {} octets]", bytes.len());
    }
    if let Ok(s) = std::str::from_utf8(bytes) {
        return sanitize_controls(s);
    }
    if is_latin1_text(bytes) {
        let decoded: String = bytes.iter().map(|&b| b as char).collect();
        return sanitize_controls(&decoded);
    }
    escape_bytes(bytes)
}

/// Séparateur de milliers ASCII (évite U+202F souvent absent des polices egui).
pub fn format_thousands(digits: &str) -> String {
    if digits.chars().any(|c| !c.is_ascii_digit()) {
        return digits.to_owned();
    }
    let chars: Vec<char> = digits.chars().collect();
    let mut out = String::with_capacity(chars.len() + chars.len() / 3);
    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 {
            out.push(' ');
        }
        out.push(*c);
    }
    out
}

fn is_pickle(bytes: &[u8]) -> bool {
    matches!(bytes, [0x80, ver, ..] if (2..=5).contains(ver))
}

fn looks_compressed(bytes: &[u8]) -> bool {
    matches!(bytes, [0x1f, 0x8b, ..] | [0x78, 0x01 | 0x9c | 0xda, ..])
}

/// Aperçu lisible : extrait les chaînes pickle (opcodes unicode) sans exécuter le pickle.
fn pickle_preview(bytes: &[u8]) -> Option<String> {
    let strings = extract_pickle_strings(bytes);
    if strings.is_empty() {
        return None;
    }

    let mut out = String::from("[pickle]\n");
    let mut i = 0;
    while i < strings.len() {
        let cur = &strings[i];
        if looks_like_key(cur) {
            if let Some(val) = strings.get(i + 1) {
                let _ = writeln!(out, "{cur} = {val}");
                i += 2;
                continue;
            }
        }
        let _ = writeln!(out, "- {cur}");
        i += 1;
    }
    Some(out)
}

fn looks_like_key(s: &str) -> bool {
    if s.is_empty() || s.len() > 128 {
        return false;
    }
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

/// Parcours minimal des opcodes string ; ignore le reste (pas d'exécution).
fn extract_pickle_strings(bytes: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            // PROTO + version
            0x80 if i + 1 < bytes.len() => i += 2,
            // FRAME + u64 len (proto 4+)
            0x95 if i + 9 <= bytes.len() => i += 9,
            // SHORT_BINUNICODE: u8 len + utf8
            0x8c => {
                if let Some((s, next)) = read_str(bytes, i + 1, 1) {
                    out.push(s);
                    i = next;
                } else {
                    i += 1;
                }
            }
            // BINUNICODE: u32 le len + utf8
            b'X' => {
                if let Some((s, next)) = read_str(bytes, i + 1, 4) {
                    out.push(s);
                    i = next;
                } else {
                    i += 1;
                }
            }
            // BINUNICODE8: u64 le len + utf8
            0x8d => {
                if let Some((s, next)) = read_str(bytes, i + 1, 8) {
                    out.push(s);
                    i = next;
                } else {
                    i += 1;
                }
            }
            // SHORT_BINSTRING / BINSTRING (bytes, souvent ASCII)
            b'U' => {
                if let Some((s, next)) = read_bytes_as_text(bytes, i + 1, 1) {
                    out.push(s);
                    i = next;
                } else {
                    i += 1;
                }
            }
            b'T' => {
                if let Some((s, next)) = read_bytes_as_text(bytes, i + 1, 4) {
                    out.push(s);
                    i = next;
                } else {
                    i += 1;
                }
            }
            // STOP
            b'.' => break,
            _ => i += 1,
        }
    }
    out
}

fn read_len(bytes: &[u8], at: usize, width: usize) -> Option<usize> {
    let end = at.checked_add(width)?;
    if end > bytes.len() {
        return None;
    }
    let n = match width {
        1 => bytes[at] as usize,
        4 => u32::from_le_bytes(bytes[at..end].try_into().ok()?) as usize,
        8 => u64::from_le_bytes(bytes[at..end].try_into().ok()?) as usize,
        _ => return None,
    };
    Some(n)
}

fn read_str(bytes: &[u8], at: usize, len_width: usize) -> Option<(String, usize)> {
    let n = read_len(bytes, at, len_width)?;
    let start = at + len_width;
    let end = start.checked_add(n)?;
    if end > bytes.len() {
        return None;
    }
    let s = std::str::from_utf8(&bytes[start..end]).ok()?.to_owned();
    Some((s, end))
}

fn read_bytes_as_text(bytes: &[u8], at: usize, len_width: usize) -> Option<(String, usize)> {
    let n = read_len(bytes, at, len_width)?;
    let start = at + len_width;
    let end = start.checked_add(n)?;
    if end > bytes.len() {
        return None;
    }
    let slice = &bytes[start..end];
    if slice.iter().all(|&b| (32..=126).contains(&b) || b == b'\t') {
        Some((String::from_utf8_lossy(slice).into_owned(), end))
    } else {
        // Avance quand même le curseur pour ne pas rescanner le payload.
        Some((escape_bytes(slice), end))
    }
}

/// Latin-1 « texte » : ASCII + lettres 0xA0–0xFF, sans opcodes 0x80–0x9F.
fn is_latin1_text(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return true;
    }
    let textish = bytes
        .iter()
        .filter(|&&b| matches!(b, b'\t' | b'\n' | b'\r' | 32..=126 | 0xa0..=0xff))
        .count();
    textish * 10 >= bytes.len() * 9
}

fn sanitize_controls(s: &str) -> String {
    if !s.chars().any(is_unsafe_char) {
        return s.to_owned();
    }
    let mut out = String::with_capacity(s.len() + 16);
    for c in s.chars() {
        match c {
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            c if is_unsafe_char(c) => push_escaped_char(&mut out, c),
            c => out.push(c),
        }
    }
    out
}

fn is_unsafe_char(c: char) -> bool {
    (c.is_control() && c != '\t' && c != '\n' && c != '\r') || c == '\u{FEFF}' || c == '\u{FFFD}'
}

fn push_escaped_char(out: &mut String, c: char) {
    let n = c as u32;
    if n <= 0xff {
        let _ = write!(out, "\\x{n:02x}");
    } else {
        let _ = write!(out, "\\u{{{n:x}}}");
    }
}

fn escape_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 4);
    for &b in bytes {
        match b {
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            32..=126 => out.push(b as char),
            _ => {
                let _ = write!(out, "\\x{b:02x}");
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_roundtrip() {
        assert_eq!(from_redis_bytes("été".as_bytes()), "été");
    }

    #[test]
    fn latin1_french() {
        assert_eq!(from_redis_bytes(&[0xe9, b't', 0xe9]), "été");
    }

    #[test]
    fn kombu_null_bytes_escaped() {
        let raw = b"worker.#\0\0\0\0celeryev.7bc2dc0e-aee5-4644-9ad8-fbde65a09df1";
        let shown = from_redis_bytes(raw);
        assert_eq!(
            shown,
            r"worker.#\x00\x00\x00\x00celeryev.7bc2dc0e-aee5-4644-9ad8-fbde65a09df1"
        );
    }

    #[test]
    fn django_session_pickle_preview() {
        // Équivalent du dump utilisateur : session auth Django (proto 5).
        let mut raw = vec![
            0x80, 0x05, 0x95, 0xaf, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, b'}', 0x94, b'(',
        ];
        for (s, op) in [
            ("_auth_user_id", true),
            ("1", true),
            ("_auth_user_backend", true),
            ("django.contrib.auth.backends.ModelBackend", true),
            ("_auth_user_hash", true),
            (
                "fae3506e8203d0b031b679c729c08d0f47280b61898f0a68b95b5096211c4aa1",
                true,
            ),
        ] {
            let _ = op;
            raw.push(0x8c);
            raw.push(s.len() as u8);
            raw.extend_from_slice(s.as_bytes());
            raw.push(0x94);
        }
        raw.extend_from_slice(&[b'u', b'.']);

        let shown = from_redis_bytes(&raw);
        assert!(shown.starts_with("[pickle]\n"));
        assert!(shown.contains("_auth_user_id = 1"));
        assert!(shown.contains("_auth_user_backend = django.contrib.auth.backends.ModelBackend"));
        assert!(shown.contains(
            "_auth_user_hash = fae3506e8203d0b031b679c729c08d0f47280b61898f0a68b95b5096211c4aa1"
        ));
        assert!(!shown.contains(r"\x80\x05"));
    }

    #[test]
    fn thousands_spacing() {
        assert_eq!(format_thousands("1234567"), "1 234 567");
    }
}
