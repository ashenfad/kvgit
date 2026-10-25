//! JSON as Python's `json` module writes and reads it.
//!
//! Every hash and record in a store is JSON that Python kvgit wrote, so
//! the bytes have to match `json.dumps(value, separators=(",", ":"))`,
//! with `sort_keys=True` where Python passes it:
//!
//! - **Escaping:** everything outside printable ASCII is written as
//!   `\uXXXX` in lowercase hex (`ensure_ascii`), DEL included, and a
//!   character outside the basic plane as its surrogate pair.
//! - **Key order:** `sort_keys` orders by code point, which is the order
//!   of the keys' UTF-8 bytes.
//! - **Floats:** Python's `repr`, the shortest digits that read back to
//!   the same double, as `1.0`, `1e-05`, `1e+16`; `NaN`, `Infinity`
//!   and `-Infinity` for the values JSON has no spelling for.
//!
//! [`loads`] keeps what Python's `json.loads` keeps: an integer stays an
//! integer however large, a number with a fraction or an exponent is a
//! float, objects keep their keys in the order read, and of a repeated
//! key the first position holds the last value. Text Python wrote reads
//! back to a value that writes the same bytes. A lone surrogate, which
//! Python reads into a `str`, is an error: a Rust string can't hold one.
//! So is nesting deeper than [`MAX_DEPTH`], which no record comes near:
//! it would otherwise exhaust the stack, wasm's above all.

use std::fmt::Write as _;

/// A JSON value as Python holds one.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    /// An integer that fits in 64 bits.
    Int(i64),
    /// An integer that doesn't, as its decimal digits (with a leading
    /// `-` when negative, never with leading zeros).
    BigInt(String),
    Float(f64),
    Str(String),
    Array(Vec<Value>),
    /// Keys in the order written; no key appears twice.
    Object(Vec<(String, Value)>),
}

impl Value {
    /// The value under `key`, if this is an object holding it.
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Object(pairs) => pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }
}

/// `json.dumps(value, separators=(",", ":"))`, with `sort_keys=True`
/// when `sort_keys` is set.
pub fn dumps(value: &Value, sort_keys: bool) -> String {
    let mut out = String::new();
    write_value(&mut out, value, sort_keys);
    out
}

fn write_value(out: &mut String, value: &Value, sort_keys: bool) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Int(n) => {
            let _ = write!(out, "{n}");
        }
        Value::BigInt(digits) => out.push_str(digits),
        Value::Float(x) => out.push_str(&float_repr(*x)),
        Value::Str(s) => write_string(out, s),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(out, item, sort_keys);
            }
            out.push(']');
        }
        Value::Object(pairs) => {
            let mut ordered: Vec<&(String, Value)> = pairs.iter().collect();
            if sort_keys {
                ordered.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            }
            out.push('{');
            for (i, (key, item)) in ordered.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(out, key);
                out.push(':');
                write_value(out, item, sort_keys);
            }
            out.push('}');
        }
    }
}

/// A string as Python writes it with `ensure_ascii`.
pub fn write_string(out: &mut String, s: &str) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            ' '..='~' => out.push(ch),
            _ => {
                let mut units = [0u16; 2];
                for unit in ch.encode_utf16(&mut units) {
                    let _ = write!(out, "\\u{unit:04x}");
                }
            }
        }
    }
    out.push('"');
}

/// A float as Python's `repr` writes it: the shortest digits that read
/// back to the same double, in fixed notation with at least one digit
/// after the point, or in exponent notation when the decimal exponent
/// is below -4 or at least 16, with a sign and at least two exponent
/// digits.
pub fn float_repr(x: f64) -> String {
    if x.is_nan() {
        return "NaN".into();
    }
    if x.is_infinite() {
        return if x > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    if x == 0.0 {
        return if x.is_sign_negative() { "-0.0" } else { "0.0" }.into();
    }
    let sign = if x < 0.0 { "-" } else { "" };
    let (digits, exp) = shortest_digits(x.abs());
    if !(-4..16).contains(&exp) {
        let frac = &digits[1..];
        let point = if frac.is_empty() {
            String::new()
        } else {
            format!(".{frac}")
        };
        let exp_sign = if exp < 0 { '-' } else { '+' };
        return format!("{sign}{}{point}e{exp_sign}{:02}", &digits[..1], exp.abs());
    }
    if exp < 0 {
        return format!("{sign}0.{}{digits}", "0".repeat((-exp - 1) as usize));
    }
    let whole_len = exp as usize + 1;
    if digits.len() <= whole_len {
        format!("{sign}{digits}{}.0", "0".repeat(whole_len - digits.len()))
    } else {
        format!("{sign}{}.{}", &digits[..whole_len], &digits[whole_len..])
    }
}

/// The shortest digits that read back to `x` (positive and finite),
/// and the decimal exponent of the first: the digits of `d.ddd × 10^exp`.
/// Where two spellings of that length are equally near `x`, the one
/// ending in an even digit, as CPython chooses.
fn shortest_digits(x: f64) -> (String, i32) {
    // `{:e}` writes the shortest round-tripping digits as d.ddde<n>, the
    // nearest of them, but doesn't promise how it breaks an exact tie.
    let sci = format!("{x:e}");
    let (mantissa, exp) = sci.split_once('e').expect("{:e} writes an exponent");
    let exp: i32 = exp.parse().expect("{:e} writes an integer exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    if (digits.as_bytes()[digits.len() - 1] - b'0') % 2 == 0 {
        return (digits, exp);
    }
    let text = |d: &str| format!("{}.{}e{exp}", &d[..1], &d[1..]);
    for neighbour in [step(&digits, -1), step(&digits, 1)].into_iter().flatten() {
        if neighbour.len() != digits.len() || text(&neighbour).parse::<f64>() != Ok(x) {
            continue;
        }
        let low = if neighbour < digits {
            &neighbour
        } else {
            &digits
        };
        if exactly_midway(x, low, exp) {
            return (neighbour, exp);
        }
    }
    (digits, exp)
}

/// Whether `x` is exactly `low` followed by a 5, in the digits of
/// `d.ddd × 10^exp`: midway between `low` and the next number up.
fn exactly_midway(x: f64, low: &str, exp: i32) -> bool {
    // A double's exact decimal expansion never runs past 767 digits.
    let exact = format!("{x:.800e}");
    let (mantissa, exact_exp) = exact.split_once('e').expect("{:e} writes an exponent");
    let exact_digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let midpoint = format!("{low}5");
    exact_exp.parse::<i32>() == Ok(exp)
        && exact_digits.starts_with(&midpoint)
        && exact_digits[midpoint.len()..].bytes().all(|b| b == b'0')
}

/// `digits` as a decimal integer plus `by` (1 or -1), keeping its
/// length; `None` where that would need another digit or lose one.
fn step(digits: &str, by: i8) -> Option<String> {
    let mut out = digits.as_bytes().to_vec();
    let (edge, wrap) = if by > 0 { (b'9', b'0') } else { (b'0', b'9') };
    for byte in out.iter_mut().rev() {
        if *byte != edge {
            *byte = byte.wrapping_add_signed(by);
            let text = String::from_utf8(out).expect("ASCII digits");
            return if text.starts_with('0') {
                None
            } else {
                Some(text)
            };
        }
        *byte = wrap;
    }
    None
}

/// Why text didn't parse.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
    /// Byte offset into the text.
    pub position: usize,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at position {}", self.message, self.position)
    }
}

impl std::error::Error for ParseError {}

/// The deepest arrays and objects may nest in text [`loads`] reads.
pub const MAX_DEPTH: usize = 256;

/// Parse JSON text as Python's `json.loads` does.
pub fn loads(text: &[u8]) -> Result<Value, ParseError> {
    let mut parser = Parser {
        text,
        at: 0,
        depth: 0,
    };
    let value = parser.value()?;
    parser.space();
    if parser.at != text.len() {
        return Err(parser.fail("extra data"));
    }
    Ok(value)
}

struct Parser<'a> {
    text: &'a [u8],
    at: usize,
    /// Arrays and objects open around the current position.
    depth: usize,
}

impl Parser<'_> {
    fn fail(&self, message: &str) -> ParseError {
        ParseError {
            message: message.into(),
            position: self.at,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.text.get(self.at).copied()
    }

    fn space(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.at += 1;
        }
    }

    fn word(&mut self, word: &str) -> bool {
        if self.text[self.at..].starts_with(word.as_bytes()) {
            self.at += word.len();
            true
        } else {
            false
        }
    }

    fn value(&mut self) -> Result<Value, ParseError> {
        self.space();
        match self.peek() {
            Some(b'{' | b'[') => {
                if self.depth == MAX_DEPTH {
                    return Err(self.fail("nested too deeply"));
                }
                self.depth += 1;
                let value = if self.peek() == Some(b'{') {
                    self.object()
                } else {
                    self.array()
                };
                self.depth -= 1;
                value
            }
            Some(b'"') => Ok(Value::Str(self.string()?)),
            _ if self.word("true") => Ok(Value::Bool(true)),
            _ if self.word("false") => Ok(Value::Bool(false)),
            _ if self.word("null") => Ok(Value::Null),
            _ if self.word("NaN") => Ok(Value::Float(f64::NAN)),
            _ if self.word("Infinity") => Ok(Value::Float(f64::INFINITY)),
            _ if self.word("-Infinity") => Ok(Value::Float(f64::NEG_INFINITY)),
            _ => self.number(),
        }
    }

    fn object(&mut self) -> Result<Value, ParseError> {
        self.at += 1;
        let mut pairs: Vec<(String, Value)> = Vec::new();
        self.space();
        if self.peek() == Some(b'}') {
            self.at += 1;
            return Ok(Value::Object(pairs));
        }
        loop {
            self.space();
            if self.peek() != Some(b'"') {
                return Err(self.fail("expected a key"));
            }
            let key = self.string()?;
            self.space();
            if self.peek() != Some(b':') {
                return Err(self.fail("expected ':'"));
            }
            self.at += 1;
            let value = self.value()?;
            match pairs.iter_mut().find(|(k, _)| *k == key) {
                Some(pair) => pair.1 = value,
                None => pairs.push((key, value)),
            }
            self.space();
            match self.peek() {
                Some(b'}') => {
                    self.at += 1;
                    return Ok(Value::Object(pairs));
                }
                Some(b',') => self.at += 1,
                _ => return Err(self.fail("expected ',' or '}'")),
            }
        }
    }

    fn array(&mut self) -> Result<Value, ParseError> {
        self.at += 1;
        let mut items = Vec::new();
        self.space();
        if self.peek() == Some(b']') {
            self.at += 1;
            return Ok(Value::Array(items));
        }
        loop {
            items.push(self.value()?);
            self.space();
            match self.peek() {
                Some(b']') => {
                    self.at += 1;
                    return Ok(Value::Array(items));
                }
                Some(b',') => self.at += 1,
                _ => return Err(self.fail("expected ',' or ']'")),
            }
        }
    }

    fn string(&mut self) -> Result<String, ParseError> {
        self.at += 1;
        let mut out = String::new();
        loop {
            let start = self.at;
            while let Some(b) = self.peek() {
                if b == b'"' || b == b'\\' || b < 0x20 {
                    break;
                }
                self.at += 1;
            }
            let run = std::str::from_utf8(&self.text[start..self.at]).map_err(|_| ParseError {
                message: "invalid UTF-8".into(),
                position: start,
            })?;
            out.push_str(run);
            match self.peek() {
                Some(b'"') => {
                    self.at += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.at += 1;
                    self.escape(&mut out)?;
                }
                Some(_) => return Err(self.fail("control character in a string")),
                None => return Err(self.fail("unterminated string")),
            }
        }
    }

    fn escape(&mut self, out: &mut String) -> Result<(), ParseError> {
        let ch = self
            .peek()
            .ok_or_else(|| self.fail("unterminated string"))?;
        self.at += 1;
        match ch {
            b'"' => out.push('"'),
            b'\\' => out.push('\\'),
            b'/' => out.push('/'),
            b'b' => out.push('\u{08}'),
            b'f' => out.push('\u{0c}'),
            b'n' => out.push('\n'),
            b'r' => out.push('\r'),
            b't' => out.push('\t'),
            b'u' => {
                let unit = self.hex4()?;
                let code = if (0xd800..0xdc00).contains(&unit)
                    && self.text[self.at..].starts_with(b"\\u")
                {
                    let save = self.at;
                    self.at += 2;
                    let low = self.hex4()?;
                    if (0xdc00..0xe000).contains(&low) {
                        0x10000 + ((unit - 0xd800) << 10) + (low - 0xdc00)
                    } else {
                        self.at = save;
                        unit
                    }
                } else {
                    unit
                };
                let ch = char::from_u32(code).ok_or_else(|| self.fail("lone surrogate"))?;
                out.push(ch);
            }
            _ => return Err(self.fail("invalid escape")),
        }
        Ok(())
    }

    fn hex4(&mut self) -> Result<u32, ParseError> {
        let digits = self
            .text
            .get(self.at..self.at + 4)
            .ok_or_else(|| self.fail("short \\u escape"))?;
        let text = std::str::from_utf8(digits).map_err(|_| self.fail("invalid \\u escape"))?;
        let unit = u32::from_str_radix(text, 16).map_err(|_| self.fail("invalid \\u escape"))?;
        self.at += 4;
        Ok(unit)
    }

    fn number(&mut self) -> Result<Value, ParseError> {
        let start = self.at;
        if self.peek() == Some(b'-') {
            self.at += 1;
        }
        match self.peek() {
            Some(b'0') => self.at += 1,
            Some(b'1'..=b'9') => self.digits(),
            _ => {
                self.at = start;
                return Err(self.fail("expected a value"));
            }
        }
        let mut float = false;
        if self.peek() == Some(b'.') && matches!(self.text.get(self.at + 1), Some(b'0'..=b'9')) {
            float = true;
            self.at += 1;
            self.digits();
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            let save = self.at;
            self.at += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.at += 1;
            }
            if matches!(self.peek(), Some(b'0'..=b'9')) {
                float = true;
                self.digits();
            } else {
                self.at = save;
            }
        }
        let text = std::str::from_utf8(&self.text[start..self.at]).expect("digits are ASCII");
        if float {
            return Ok(Value::Float(
                text.parse().expect("a JSON number parses as f64"),
            ));
        }
        Ok(match text.parse::<i64>() {
            Ok(n) => Value::Int(n),
            Err(_) => Value::BigInt(text.to_string()),
        })
    }

    fn digits(&mut self) {
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.at += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(pairs: &[(&str, Value)]) -> Value {
        Value::Object(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }

    #[test]
    fn floats_as_python_repr_writes_them() {
        // Each pair is `repr(x)` in CPython.
        let cases: &[(f64, &str)] = &[
            (1.0, "1.0"),
            (-2.5, "-2.5"),
            (0.1, "0.1"),
            (0.5, "0.5"),
            (1e-4, "0.0001"),
            (1e-5, "1e-05"),
            (1.5e-7, "1.5e-07"),
            (123456.789, "123456.789"),
            (1e15, "1000000000000000.0"),
            (1e16, "1e+16"),
            (1.2345e16, "1.2345e+16"),
            (9007199254740993.0, "9007199254740992.0"),
            (4503599627370495.5, "4503599627370495.5"),
            // 105594072938994.625, exactly midway between two shortest
            // spellings: the even one.
            (f64::from_bits(0x42d8_0262_4d4e_7ca8), "105594072938994.62"),
            (5e-324, "5e-324"),
            (2.5e-300, "2.5e-300"),
            (1.7976931348623157e308, "1.7976931348623157e+308"),
            (1700000000.25, "1700000000.25"),
            (1790533742.0508668, "1790533742.0508668"),
            (100.0, "100.0"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ];
        for (x, want) in cases {
            assert_eq!(float_repr(*x), *want, "{x:e}");
        }
    }

    #[test]
    fn strings_escape_everything_outside_printable_ascii() {
        let s = "quote \" backslash \\ slash / tab \t nl \n cr \r bs \u{08} ff \u{0c} \u{00} \u{1f} \u{7f} é ✓ 😀";
        assert_eq!(
            dumps(&Value::Str(s.into()), false),
            r#""quote \" backslash \\ slash / tab \t nl \n cr \r bs \b ff \f \u0000 \u001f \u007f \u00e9 \u2713 \ud83d\ude00""#
        );
    }

    #[test]
    fn sort_keys_orders_by_code_point() {
        let value = obj(&[
            ("b", Value::Int(1)),
            ("a", Value::Int(2)),
            ("B", Value::Int(3)),
            ("\u{e9}", Value::Int(4)),
            ("\u{1f600}", Value::Int(5)),
            ("\u{ffff}", Value::Int(6)),
            ("_", Value::Int(7)),
        ]);
        assert_eq!(
            dumps(&value, true),
            r#"{"B":3,"_":7,"a":2,"b":1,"\u00e9":4,"\uffff":6,"\ud83d\ude00":5}"#
        );
        assert_eq!(
            dumps(&value, false),
            r#"{"b":1,"a":2,"B":3,"\u00e9":4,"\ud83d\ude00":5,"\uffff":6,"_":7}"#
        );
    }

    #[test]
    fn sort_keys_reaches_nested_objects() {
        let value = Value::Array(vec![obj(&[
            ("z", obj(&[("y", Value::Null), ("x", Value::Bool(false))])),
            ("a", Value::Array(vec![])),
        ])]);
        assert_eq!(
            dumps(&value, true),
            r#"[{"a":[],"z":{"x":false,"y":null}}]"#
        );
    }

    #[test]
    fn python_records_read_back_to_their_bytes() {
        for text in [
            r#"["kvgit/4",["821bf06b4dcb406ea508a4a992eadc22f29850cd"],"52b8ed78",1700000000.25,null]"#,
            r#"["c1:k",{"size":26,"created_at":1790533742.0508668}]"#,
            r#"["c1:k",{"size":2,"created_at":1790533742.0,"chunks":["c1","c2"]}]"#,
            r#"{"children":{"0":"ab","f":"cd"},"kind":"branch"}"#,
            r#"{"ints":[0,-1,9007199254740993,-123456789012345678901234567890],"t":true}"#,
            r#"{"floats":[1e-05,1.5e-07,1e+16,5e-324,-0.0,NaN,Infinity,-Infinity]}"#,
            r#""\u00fcn\u00efc\u00f6d\u00e9 \ud834\udd1e \u2028""#,
            "[]",
            "{}",
            r#""""#,
        ] {
            let value = loads(text.as_bytes()).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(dumps(&value, false), text);
        }
    }

    #[test]
    fn numbers_keep_python_types() {
        assert_eq!(loads(b"3").unwrap(), Value::Int(3));
        assert_eq!(loads(b"-0").unwrap(), Value::Int(0));
        assert_eq!(loads(b"3.0").unwrap(), Value::Float(3.0));
        assert_eq!(loads(b"3e2").unwrap(), Value::Float(300.0));
        assert_eq!(loads(b"1e400").unwrap(), Value::Float(f64::INFINITY));
        assert_eq!(
            loads(b"18446744073709551616").unwrap(),
            Value::BigInt("18446744073709551616".into())
        );
        assert_eq!(dumps(&loads(b"1E5").unwrap(), false), "100000.0");
    }

    #[test]
    fn a_repeated_key_keeps_its_first_place_and_last_value() {
        let value = loads(br#"{"a":1,"b":2,"a":3}"#).unwrap();
        assert_eq!(dumps(&value, false), r#"{"a":3,"b":2}"#);
    }

    #[test]
    fn whitespace_and_escapes_python_accepts() {
        let value = loads(b" { \"a\" : [ 1 , \"\\/\\u00e9\" ] } ").unwrap();
        assert_eq!(dumps(&value, false), r#"{"a":[1,"/\u00e9"]}"#);
    }

    #[test]
    fn nesting_past_the_limit_is_an_error() {
        let deep = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
        assert!(loads(deep(MAX_DEPTH).as_bytes()).is_ok());
        let err = loads(deep(MAX_DEPTH + 1).as_bytes()).unwrap_err();
        assert_eq!(err.message, "nested too deeply");
        assert!(loads(deep(100_000).as_bytes()).is_err());
        assert!(
            loads(
                format!(
                    "{}1{}",
                    "{\"a\":".repeat(MAX_DEPTH + 1),
                    "}".repeat(MAX_DEPTH + 1)
                )
                .as_bytes()
            )
            .is_err()
        );
    }

    #[test]
    fn malformed_text_is_an_error() {
        for text in [
            &b""[..],
            b"[1,]",
            b"{\"a\" 1}",
            b"\"open",
            b"01",
            b"[1] x",
            b"\"\\ud800\"",
            b"\"\x01\"",
            b"tru",
        ] {
            assert!(loads(text).is_err(), "{:?}", String::from_utf8_lossy(text));
        }
    }
}
