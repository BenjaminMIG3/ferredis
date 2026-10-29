//! Découpage d'arguments style redis-cli (guillemets, escapes basiques).

/// Découpe une ligne interactive. Si `quoted_input`, parse escapes `\xNN`, `\"`, `\\`.
pub fn split_args(line: &str, quoted_input: bool) -> Vec<String> {
    if quoted_input {
        split_quoted(line)
    } else {
        split_simple(line)
    }
}

fn split_simple(line: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut in_double = false;
    let mut in_single = false;
    for ch in line.chars() {
        match ch {
            '"' if !in_single => in_double = !in_double,
            '\'' if !in_double => in_single = !in_single,
            ' ' | '\t' if !in_double && !in_single => {
                if !cur.is_empty() {
                    args.push(std::mem::take(&mut cur));
                }
            }
            _ => cur.push(ch),
        }
    }
    if !cur.is_empty() {
        args.push(cur);
    }
    args
}

fn split_quoted(line: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut chars = line.chars().peekable();
    let mut in_double = false;
    let mut in_single = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if !in_single => in_double = !in_double,
            '\'' if !in_double => in_single = !in_single,
            '\\' if in_double || in_single => {
                match chars.next() {
                    Some('n') => cur.push('\n'),
                    Some('r') => cur.push('\r'),
                    Some('t') => cur.push('\t'),
                    Some('x') => {
                        let h1 = chars.next();
                        let h2 = chars.next();
                        if let (Some(a), Some(b)) = (h1, h2) {
                            if let (Some(hi), Some(lo)) = (a.to_digit(16), b.to_digit(16)) {
                                cur.push(char::from_u32((hi << 4) | lo).unwrap_or('\0'));
                                continue;
                            }
                        }
                        cur.push('x');
                        if let Some(a) = h1 {
                            cur.push(a);
                        }
                        if let Some(b) = h2 {
                            cur.push(b);
                        }
                    }
                    Some(c) => cur.push(c),
                    None => cur.push('\\'),
                }
            }
            ' ' | '\t' if !in_double && !in_single => {
                if !cur.is_empty() {
                    args.push(std::mem::take(&mut cur));
                }
            }
            _ => cur.push(ch),
        }
    }
    if !cur.is_empty() {
        args.push(cur);
    }
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_split() {
        assert_eq!(split_args(r#"SET k "hello world""#, false), vec!["SET", "k", "hello world"]);
    }

    #[test]
    fn quoted_hex() {
        assert_eq!(
            split_args(r#""a\x00b""#, true),
            vec!["a\0b"]
        );
    }
}
