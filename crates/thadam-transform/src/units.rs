//! A small UCUM-subset unit parser and numeric unit conversion.
//!
//! A [`Unit`] is modelled as `base = x * scale + offset` over eight base
//! dimensions `[length, mass, time, electric current, temperature, amount,
//! luminous intensity, plane angle]`. Mass is expressed in grams, so `kg` has
//! scale 1000 and `N` has scale 1000.

use std::f64::consts::PI;

/// Maximum accepted length of a unit code, in bytes.
pub const MAX_CODE_LEN: usize = 128;

/// Maximum accepted absolute value of a component exponent.
pub const MAX_EXPONENT: u32 = 12;

const DIM_COUNT: usize = 8;
type Dims = [i8; DIM_COUNT];
const NO_DIMS: Dims = [0; DIM_COUNT];

/// Relative tolerance within which a scale ratio is snapped to a power of ten.
const SNAP_TOLERANCE: f64 = 1e-12;

/// Exactly representable powers of ten (`10^0` ..= `10^22`).
const POW10: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];

/// Errors produced while parsing unit codes or building conversions.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum UnitError {
    /// The unit code is the empty string.
    #[error("unit code is empty")]
    Empty,
    /// The unit code exceeds [`MAX_CODE_LEN`] bytes.
    #[error("unit code is longer than {MAX_CODE_LEN} bytes")]
    TooLong,
    /// An atom name (optionally with prefix) is not recognised.
    #[error("unknown unit `{0}`")]
    UnknownUnit(String),
    /// The code is not valid under the supported grammar.
    #[error("syntax error at byte {pos}: {message}")]
    Syntax {
        /// Byte offset of the offending input.
        pos: usize,
        /// Human-readable description of the problem.
        message: String,
    },
    /// An exponent is malformed or out of the supported range.
    #[error("bad exponent: {0}")]
    BadExponent(String),
    /// An offset unit (such as `Cel`) was combined with anything else.
    #[error("offset unit `{0}` cannot be used in a compound expression")]
    OffsetUnitInCompound(String),
    /// The two units have different dimensions.
    #[error("cannot convert `{from}` to `{to}`: incompatible dimensions")]
    Incompatible {
        /// Code of the source unit.
        from: String,
        /// Code of the target unit.
        to: String,
    },
}

/// A parsed unit: dimensions plus an affine map onto the base units.
#[derive(Clone, Debug, PartialEq)]
pub struct Unit {
    dims: Dims,
    scale: f64,
    offset: f64,
    original: String,
}

impl Unit {
    /// Parses a case-sensitive UCUM-subset unit code.
    ///
    /// Codes longer than [`MAX_CODE_LEN`] bytes and exponents whose absolute
    /// value exceeds [`MAX_EXPONENT`] are rejected. Never panics.
    pub fn parse(code: &str) -> Result<Unit, UnitError> {
        if code.is_empty() {
            return Err(UnitError::Empty);
        }
        if code.len() > MAX_CODE_LEN {
            return Err(UnitError::TooLong);
        }
        let parsed = Parser::new(code).parse_expr()?;
        Ok(Unit {
            dims: parsed.dims,
            scale: parsed.scale,
            offset: parsed.offset,
            original: code.to_owned(),
        })
    }

    /// Returns the original text this unit was parsed from.
    pub fn code(&self) -> &str {
        &self.original
    }

    /// Returns `true` if every dimension exponent is zero.
    pub fn is_dimensionless(&self) -> bool {
        self.dims == NO_DIMS
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Kind {
    Identity,
    Mul(f64),
    Div(f64),
    Affine {
        s_from: f64,
        o_from: f64,
        o_to: f64,
        s_to: f64,
    },
}

/// A numeric conversion between two compatible units.
#[derive(Clone, Debug, PartialEq)]
pub struct UnitConversion {
    kind: Kind,
}

impl UnitConversion {
    /// Builds the conversion `from` -> `to`.
    ///
    /// Returns [`UnitError::Incompatible`] if the dimensions differ.
    pub fn between(from: &Unit, to: &Unit) -> Result<UnitConversion, UnitError> {
        if from.dims != to.dims {
            return Err(UnitError::Incompatible {
                from: from.original.clone(),
                to: to.original.clone(),
            });
        }

        // Equal offsets cancel exactly, which keeps pure rescalings such as
        // `dCel` -> `Cel` on the exact multiply/divide path.
        if from.offset != to.offset {
            return Ok(UnitConversion {
                kind: Kind::Affine {
                    s_from: from.scale,
                    o_from: from.offset,
                    o_to: to.offset,
                    s_to: to.scale,
                },
            });
        }

        let ratio = from.scale / to.scale;
        if !ratio.is_finite() || ratio <= 0.0 {
            return Err(UnitError::Syntax {
                pos: 0,
                message: format!(
                    "conversion factor from `{}` to `{}` is out of range",
                    from.original, to.original
                ),
            });
        }
        let kind = snap_power_of_ten(ratio).unwrap_or(Kind::Mul(ratio));
        Ok(UnitConversion { kind })
    }

    /// Parses both codes and builds the conversion between them.
    pub fn from_codes(from: &str, to: &str) -> Result<UnitConversion, UnitError> {
        Self::between(&Unit::parse(from)?, &Unit::parse(to)?)
    }

    /// Returns `true` if the conversion is exactly the identity (factor 1, no offset).
    pub fn is_identity(&self) -> bool {
        self.kind == Kind::Identity
    }

    /// Converts a single value.
    pub fn apply_f64(&self, x: f64) -> f64 {
        match self.kind {
            Kind::Identity => x,
            Kind::Mul(f) => x * f,
            Kind::Div(d) => x / d,
            Kind::Affine {
                s_from,
                o_from,
                o_to,
                s_to,
            } => (x * s_from + o_from - o_to) / s_to,
        }
    }

    /// Converts a single `f32` value, computing in `f64` and rounding back.
    pub fn apply_f32(&self, x: f32) -> f32 {
        self.apply_f64(f64::from(x)) as f32
    }

    /// Converts every element of the slice in place.
    pub fn apply_f64_slice(&self, xs: &mut [f64]) {
        for x in xs {
            *x = self.apply_f64(*x);
        }
    }

    /// Converts every element of the slice in place, each element via `f64`.
    pub fn apply_f32_slice(&self, xs: &mut [f32]) {
        for x in xs {
            *x = self.apply_f32(*x);
        }
    }
}

/// Snaps `ratio` to an exact power of ten when it is within [`SNAP_TOLERANCE`] of one.
///
/// Division by an exact `10^n` avoids the error of multiplying by an inexact
/// reciprocal such as `0.001`.
fn snap_power_of_ten(ratio: f64) -> Option<Kind> {
    let exp = ratio.log10().round();
    if !exp.is_finite() || exp.abs() > 22.0 {
        return None;
    }
    let exp = exp as i32;
    let p = *POW10.get(usize::try_from(exp.unsigned_abs()).ok()?)?;
    let target = if exp < 0 { 1.0 / p } else { p };
    if (ratio - target).abs() > SNAP_TOLERANCE * target {
        return None;
    }
    Some(match exp {
        0 => Kind::Identity,
        1.. => Kind::Mul(p),
        _ => Kind::Div(p),
    })
}

struct Atom {
    name: &'static str,
    dims: Dims,
    scale: f64,
    offset: f64,
    metric: bool,
}

const fn atom(name: &'static str, dims: Dims, scale: f64, offset: f64, metric: bool) -> Atom {
    Atom {
        name,
        dims,
        scale,
        offset,
        metric,
    }
}

// Dimension order: [length, mass, time, current, temperature, amount, luminous, angle].
const ATOMS: &[Atom] = &[
    atom("m", [1, 0, 0, 0, 0, 0, 0, 0], 1.0, 0.0, true),
    atom("g", [0, 1, 0, 0, 0, 0, 0, 0], 1.0, 0.0, true),
    atom("s", [0, 0, 1, 0, 0, 0, 0, 0], 1.0, 0.0, true),
    atom("A", [0, 0, 0, 1, 0, 0, 0, 0], 1.0, 0.0, true),
    atom("K", [0, 0, 0, 0, 1, 0, 0, 0], 1.0, 0.0, true),
    atom("mol", [0, 0, 0, 0, 0, 1, 0, 0], 1.0, 0.0, true),
    atom("cd", [0, 0, 0, 0, 0, 0, 1, 0], 1.0, 0.0, true),
    atom("rad", [0, 0, 0, 0, 0, 0, 0, 1], 1.0, 0.0, true),
    atom("Hz", [0, 0, -1, 0, 0, 0, 0, 0], 1.0, 0.0, true),
    atom("N", [1, 1, -2, 0, 0, 0, 0, 0], 1e3, 0.0, true),
    atom("Pa", [-1, 1, -2, 0, 0, 0, 0, 0], 1e3, 0.0, true),
    atom("J", [2, 1, -2, 0, 0, 0, 0, 0], 1e3, 0.0, true),
    atom("W", [2, 1, -3, 0, 0, 0, 0, 0], 1e3, 0.0, true),
    atom("V", [2, 1, -3, -1, 0, 0, 0, 0], 1e3, 0.0, true),
    atom("C", [0, 0, 1, 1, 0, 0, 0, 0], 1.0, 0.0, true),
    atom("Ohm", [2, 1, -3, -2, 0, 0, 0, 0], 1e3, 0.0, true),
    atom("F", [-2, -1, 4, 2, 0, 0, 0, 0], 1e-3, 0.0, true),
    atom("T", [0, 1, -2, -1, 0, 0, 0, 0], 1e3, 0.0, true),
    atom("Wb", [2, 1, -2, -1, 0, 0, 0, 0], 1e3, 0.0, true),
    atom("S", [-2, -1, 3, 2, 0, 0, 0, 0], 1e-3, 0.0, true),
    atom("H", [2, 1, -2, -2, 0, 0, 0, 0], 1e3, 0.0, true),
    atom("bar", [-1, 1, -2, 0, 0, 0, 0, 0], 1e8, 0.0, true),
    atom("l", [3, 0, 0, 0, 0, 0, 0, 0], 1e-3, 0.0, true),
    atom("L", [3, 0, 0, 0, 0, 0, 0, 0], 1e-3, 0.0, true),
    atom("Cel", [0, 0, 0, 0, 1, 0, 0, 0], 1.0, 273.15, true),
    atom("min", [0, 0, 1, 0, 0, 0, 0, 0], 60.0, 0.0, false),
    atom("h", [0, 0, 1, 0, 0, 0, 0, 0], 3600.0, 0.0, false),
    atom("d", [0, 0, 1, 0, 0, 0, 0, 0], 86_400.0, 0.0, false),
    atom("deg", [0, 0, 0, 0, 0, 0, 0, 1], PI / 180.0, 0.0, false),
    atom("%", NO_DIMS, 0.01, 0.0, false),
    atom("[g]", [1, 0, -2, 0, 0, 0, 0, 0], 9.80665, 0.0, false),
    atom(
        "[degF]",
        [0, 0, 0, 0, 1, 0, 0, 0],
        5.0 / 9.0,
        273.15 - 32.0 * 5.0 / 9.0,
        false,
    ),
    atom(
        "[psi]",
        [-1, 1, -2, 0, 0, 0, 0, 0],
        6894.757293168 * 1e3,
        0.0,
        false,
    ),
    atom("[in_i]", [1, 0, 0, 0, 0, 0, 0, 0], 0.0254, 0.0, false),
    atom("[ft_i]", [1, 0, 0, 0, 0, 0, 0, 0], 0.3048, 0.0, false),
    atom("[mi_i]", [1, 0, 0, 0, 0, 0, 0, 0], 1609.344, 0.0, false),
    atom("[lb_av]", [0, 1, 0, 0, 0, 0, 0, 0], 453.592_37, 0.0, false),
    atom(
        "[kn_i]",
        [1, 0, -1, 0, 0, 0, 0, 0],
        1852.0 / 3600.0,
        0.0,
        false,
    ),
];

const PREFIXES: &[(&str, f64)] = &[
    ("Y", 1e24),
    ("Z", 1e21),
    ("E", 1e18),
    ("P", 1e15),
    ("T", 1e12),
    ("G", 1e9),
    ("M", 1e6),
    ("k", 1e3),
    ("h", 1e2),
    ("da", 1e1),
    ("d", 1e-1),
    ("c", 1e-2),
    ("m", 1e-3),
    ("u", 1e-6),
    ("\u{b5}", 1e-6),
    ("n", 1e-9),
    ("p", 1e-12),
    ("f", 1e-15),
    ("a", 1e-18),
    ("z", 1e-21),
    ("y", 1e-24),
];

fn find_atom(name: &str) -> Option<&'static Atom> {
    ATOMS.iter().find(|a| a.name == name)
}

/// Resolves a token to `(dims, scale, offset)`, preferring an exact atom match
/// so that `min`, `cd`, `Pa`, `h` and `d` are never split into prefix + atom.
fn resolve_atom(token: &str) -> Option<(Dims, f64, f64)> {
    if let Some(a) = find_atom(token) {
        return Some((a.dims, a.scale, a.offset));
    }
    PREFIXES.iter().find_map(|&(prefix, factor)| {
        let a = find_atom(token.strip_prefix(prefix)?)?;
        a.metric.then_some((a.dims, factor * a.scale, a.offset))
    })
}

struct Parsed {
    dims: Dims,
    scale: f64,
    offset: f64,
}

struct Component {
    dims: Dims,
    scale: f64,
    offset: f64,
    name: String,
}

struct Term {
    dims: Dims,
    scale: f64,
    offset: f64,
    offset_name: Option<String>,
    exponent: i32,
}

struct Parser<'a> {
    src: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(src: &'a str) -> Self {
        Parser { src, pos: 0 }
    }

    fn rest(&self) -> &'a str {
        self.src.get(self.pos..).unwrap_or("")
    }

    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn bump(&mut self, c: char) {
        self.pos += c.len_utf8();
    }

    fn syntax<T>(&self, message: impl Into<String>) -> Result<T, UnitError> {
        Err(UnitError::Syntax {
            pos: self.pos,
            message: message.into(),
        })
    }

    fn take_while(&mut self, pred: impl Fn(char) -> bool) -> &'a str {
        let rest = self.rest();
        let end = rest
            .char_indices()
            .find(|&(_, c)| !pred(c))
            .map_or(rest.len(), |(i, _)| i);
        self.pos += end;
        rest.get(..end).unwrap_or("")
    }

    fn parse_expr(&mut self) -> Result<Parsed, UnitError> {
        let mut dims = NO_DIMS;
        let mut scale = 1.0_f64;
        let mut offset = 0.0_f64;
        let mut offset_name: Option<String> = None;
        let mut terms = 0_usize;
        let mut clean = true;

        let mut divide = self.peek() == Some('/');
        if divide {
            self.bump('/');
            clean = false;
        }

        loop {
            let term = self.parse_term()?;
            terms += 1;
            for (d, t) in dims.iter_mut().zip(term.dims) {
                let combined = if divide {
                    d.checked_sub(t)
                } else {
                    d.checked_add(t)
                };
                *d = combined.ok_or_else(|| {
                    UnitError::BadExponent("dimension exponent out of range".into())
                })?;
            }
            scale = if divide {
                scale / term.scale
            } else {
                scale * term.scale
            };
            if !scale.is_finite() || scale == 0.0 {
                return Err(UnitError::BadExponent("scale out of range".into()));
            }
            if let Some(name) = term.offset_name {
                clean &= term.exponent == 1;
                offset = term.offset;
                offset_name = Some(name);
            }

            match self.peek() {
                None => break,
                Some('.') => {
                    self.bump('.');
                    divide = false;
                }
                Some('/') => {
                    self.bump('/');
                    divide = true;
                }
                Some(c) => {
                    return self.syntax(format!("unexpected character {c:?}, expected '.' or '/'"));
                }
            }
        }

        if let Some(name) = offset_name
            && (terms != 1 || !clean)
        {
            return Err(UnitError::OffsetUnitInCompound(name));
        }
        Ok(Parsed {
            dims,
            scale,
            offset,
        })
    }

    fn parse_term(&mut self) -> Result<Term, UnitError> {
        let component = self.parse_component()?;
        self.skip_annotations()?;
        let exponent = self.parse_exponent()?;
        self.skip_annotations()?;

        let mut dims = NO_DIMS;
        // |exponent| <= MAX_EXPONENT, so the narrowing is lossless.
        let e = exponent as i8;
        for (out, d) in dims.iter_mut().zip(component.dims) {
            *out = d.checked_mul(e).ok_or_else(|| {
                UnitError::BadExponent(format!(
                    "dimension exponent of `{}` out of range",
                    component.name
                ))
            })?;
        }
        let scale = component.scale.powi(exponent);
        if !scale.is_finite() || scale == 0.0 {
            return Err(UnitError::BadExponent(format!(
                "scale of `{}` out of range",
                component.name
            )));
        }
        let is_offset = component.offset != 0.0;
        Ok(Term {
            dims,
            scale,
            offset: component.offset,
            offset_name: is_offset.then_some(component.name),
            exponent,
        })
    }

    fn parse_component(&mut self) -> Result<Component, UnitError> {
        let dimensionless = |name: String, scale: f64| Component {
            dims: NO_DIMS,
            scale,
            offset: 0.0,
            name,
        };
        match self.peek() {
            Some('[') => {
                let rest = self.rest();
                let Some(end) = rest.find(']') else {
                    return self.syntax("unterminated '['");
                };
                let name = rest.get(..=end).unwrap_or(rest);
                let Some(&Atom {
                    dims,
                    scale,
                    offset,
                    ..
                }) = find_atom(name)
                else {
                    return Err(UnitError::UnknownUnit(name.to_owned()));
                };
                self.pos += name.len();
                Ok(Component {
                    dims,
                    scale,
                    offset,
                    name: name.to_owned(),
                })
            }
            Some('%') => {
                self.bump('%');
                Ok(dimensionless("%".to_owned(), 0.01))
            }
            Some('{') => Ok(dimensionless("{}".to_owned(), 1.0)),
            Some(c) if c.is_ascii_digit() => {
                let start = self.pos;
                let digits = self.take_while(|c| c.is_ascii_digit());
                match digits.parse::<u64>() {
                    Ok(0) => {
                        self.pos = start;
                        self.syntax("zero is not a valid multiplier")
                    }
                    Ok(n) => Ok(dimensionless(digits.to_owned(), n as f64)),
                    Err(_) => {
                        self.pos = start;
                        self.syntax("integer literal too large")
                    }
                }
            }
            Some(c) if c.is_alphabetic() => {
                let token = self.take_while(char::is_alphabetic);
                match resolve_atom(token) {
                    Some((dims, scale, offset)) => Ok(Component {
                        dims,
                        scale,
                        offset,
                        name: token.to_owned(),
                    }),
                    None => Err(UnitError::UnknownUnit(token.to_owned())),
                }
            }
            Some(c) => self.syntax(format!("unexpected character {c:?}, expected a unit")),
            None => self.syntax("expected a unit"),
        }
    }

    fn parse_exponent(&mut self) -> Result<i32, UnitError> {
        let start = self.pos;
        let negative = match self.peek() {
            Some('-') => {
                self.bump('-');
                true
            }
            Some('+') => {
                self.bump('+');
                false
            }
            Some(c) if c.is_ascii_digit() => false,
            _ => return Ok(1),
        };
        let digits = self.take_while(|c| c.is_ascii_digit());
        let text = self.src.get(start..self.pos).unwrap_or("");
        let magnitude = digits
            .parse::<u32>()
            .ok()
            .filter(|&m| m <= MAX_EXPONENT)
            .and_then(|m| i32::try_from(m).ok())
            .ok_or_else(|| UnitError::BadExponent(text.to_owned()))?;
        Ok(if negative { -magnitude } else { magnitude })
    }

    fn skip_annotations(&mut self) -> Result<(), UnitError> {
        while self.peek() == Some('{') {
            let start = self.pos;
            let mut depth = 0_usize;
            loop {
                match self.peek() {
                    None => {
                        self.pos = start;
                        return self.syntax("unterminated annotation");
                    }
                    Some(c) => {
                        match c {
                            '{' => depth += 1,
                            '}' => depth = depth.saturating_sub(1),
                            _ => {}
                        }
                        self.bump(c);
                        if depth == 0 {
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn conv(from: &str, to: &str) -> UnitConversion {
        UnitConversion::from_codes(from, to).unwrap()
    }

    fn close(a: f64, b: f64, tol: f64) -> bool {
        (a - b).abs() <= tol * b.abs().max(1.0)
    }

    #[test]
    fn millivolt_to_volt_is_exact() {
        let c = conv("mV", "V");
        assert_eq!(c.apply_f64(47910.0), 47.91);
        assert_eq!(c.apply_f64(47910.0), 47910.0 / 1000.0);
    }

    #[test]
    fn milliampere_to_ampere_is_exact() {
        assert_eq!(conv("mA", "A").apply_f64(-3120.0), -3.12);
    }

    #[test]
    fn decicelsius_to_celsius_is_exact() {
        let c = conv("dCel", "Cel");
        assert_eq!(c.apply_f64(612.0), 61.2);
        assert_eq!(c.apply_f64(612.0), 612.0 / 10.0);
    }

    #[test]
    fn celsius_to_kelvin() {
        let y = conv("Cel", "K").apply_f64(25.0);
        assert_eq!(y, 25.0 * 1.0 + 273.15 - 0.0);
        assert!(close(y, 298.15, 1e-12));
    }

    #[test]
    fn decicelsius_to_kelvin_applies_offset() {
        assert!(close(conv("dCel", "K").apply_f64(250.0), 298.15, 1e-12));
    }

    #[test]
    fn kelvin_and_fahrenheit_to_celsius() {
        assert!(close(conv("K", "Cel").apply_f64(373.15), 100.0, 1e-9));
        assert!(close(conv("[degF]", "Cel").apply_f64(212.0), 100.0, 1e-9));
        assert!(close(conv("[degF]", "Cel").apply_f64(32.0), 0.0, 1e-9));
        assert!(close(conv("Cel", "[degF]").apply_f64(100.0), 212.0, 1e-9));
    }

    #[test]
    fn every_atom_parses() {
        for a in ATOMS {
            let u = Unit::parse(a.name).unwrap();
            assert_eq!(u.dims, a.dims, "{}", a.name);
            assert_eq!(u.scale, a.scale, "{}", a.name);
            assert_eq!(u.offset, a.offset, "{}", a.name);
            assert_eq!(u.code(), a.name);
        }
    }

    #[test]
    fn every_prefix_and_metric_atom_parses() {
        for &(prefix, factor) in PREFIXES {
            for a in ATOMS.iter().filter(|a| a.metric) {
                let code = format!("{prefix}{}", a.name);
                if find_atom(&code).is_some() {
                    continue;
                }
                let u = Unit::parse(&code).unwrap();
                assert_eq!(u.dims, a.dims, "{code}");
                assert_eq!(u.scale, factor * a.scale, "{code}");
            }
        }
    }

    #[test]
    fn prefix_on_non_metric_atom_is_unknown() {
        for code in ["kmin", "dh", "kdeg", "k%", "mfoo"] {
            assert!(
                matches!(Unit::parse(code), Err(UnitError::UnknownUnit(_))),
                "{code}"
            );
        }
    }

    #[test]
    fn ambiguity_exact_atoms_win() {
        let min = Unit::parse("min").unwrap();
        assert_eq!(min.scale, 60.0);
        let cd = Unit::parse("cd").unwrap();
        assert_eq!(cd.dims, [0, 0, 0, 0, 0, 0, 1, 0]);
        assert_eq!(cd.scale, 1.0);
        let pa = Unit::parse("Pa").unwrap();
        assert_eq!(pa.dims, [-1, 1, -2, 0, 0, 0, 0, 0]);
        assert_eq!(Unit::parse("h").unwrap().scale, 3600.0);
        assert_eq!(Unit::parse("d").unwrap().scale, 86_400.0);
        assert_eq!(Unit::parse("T").unwrap().dims, [0, 1, -2, -1, 0, 0, 0, 0]);
    }

    #[test]
    fn ambiguity_prefixed_forms() {
        let dcel = Unit::parse("dCel").unwrap();
        assert_eq!(dcel.scale, 0.1);
        assert_eq!(dcel.offset, 273.15);
        assert_eq!(Unit::parse("mm").unwrap().scale, 1e-3);
        assert_eq!(Unit::parse("kg").unwrap().scale, 1e3);
        assert_eq!(Unit::parse("kPa").unwrap().scale, 1e6);
        assert_eq!(Unit::parse("mmol").unwrap().dims, [0, 0, 0, 0, 0, 1, 0, 0]);
        assert_eq!(Unit::parse("dam").unwrap().scale, 10.0);
        assert_eq!(Unit::parse("hPa").unwrap().scale, 1e5);
        assert_eq!(Unit::parse("\u{b5}m").unwrap().scale, 1e-6);
        assert_eq!(Unit::parse("um").unwrap().scale, 1e-6);
        assert!(close(conv("mV", "V").apply_f64(1000.0), 1.0, 1e-15));
        assert_eq!(
            Unit::parse("mV").unwrap().dims,
            Unit::parse("V").unwrap().dims
        );
        assert_eq!(Unit::parse("[g]").unwrap().scale, 9.80665);
    }

    #[test]
    fn compound_expressions() {
        let u = Unit::parse("m/s2").unwrap();
        assert_eq!(u.dims, [1, 0, -2, 0, 0, 0, 0, 0]);
        assert!(Unit::parse("kg.m/s2").unwrap().dims == Unit::parse("N").unwrap().dims);
        assert_eq!(Unit::parse("kg.m/s2").unwrap().scale, 1000.0);
        assert_eq!(
            Unit::parse("/s").unwrap().dims,
            Unit::parse("Hz").unwrap().dims
        );
        assert_eq!(Unit::parse("m-1").unwrap().dims, [-1, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(Unit::parse("m+2").unwrap().dims, [2, 0, 0, 0, 0, 0, 0, 0]);
        let ab_c = Unit::parse("m/s/s").unwrap();
        assert_eq!(ab_c.dims, Unit::parse("m/s2").unwrap().dims);
        let left_to_right = Unit::parse("m/s.s").unwrap();
        assert_eq!(left_to_right.dims, [1, 0, 0, 0, 0, 0, 0, 0]);
        assert!(Unit::parse("1").unwrap().is_dimensionless());
        assert!(Unit::parse("%").unwrap().is_dimensionless());
        assert!(!Unit::parse("rad").unwrap().is_dimensionless());
        assert_eq!(Unit::parse("10").unwrap().scale, 10.0);
        assert_eq!(Unit::parse("10-3").unwrap().scale, 10.0_f64.powi(-3));
        assert_eq!(Unit::parse("m12").unwrap().dims[0], 12);
        assert_eq!(Unit::parse("m-12").unwrap().dims[0], -12);
    }

    #[test]
    fn derived_unit_conversions() {
        assert!(close(conv("km/h", "m/s").apply_f64(36.0), 10.0, 1e-12));
        assert!(close(
            conv("deg/s", "rad/s").apply_f64(1.0),
            PI / 180.0,
            1e-12
        ));
        assert!(close(conv("%", "1").apply_f64(50.0), 0.5, 1e-12));
        assert!(close(conv("[g]", "m/s2").apply_f64(1.0), 9.80665, 1e-12));
        assert!(close(conv("[in_i]", "mm").apply_f64(1.0), 25.4, 1e-12));
        assert!(close(conv("[ft_i]", "[in_i]").apply_f64(1.0), 12.0, 1e-12));
        assert!(close(conv("[mi_i]", "km").apply_f64(1.0), 1.609344, 1e-12));
        assert!(close(
            conv("[lb_av]", "kg").apply_f64(1.0),
            0.45359237,
            1e-12
        ));
        assert!(close(conv("[kn_i]", "km/h").apply_f64(1.0), 1.852, 1e-12));
        assert!(close(
            conv("[psi]", "kPa").apply_f64(1.0),
            6.894757293168,
            1e-12
        ));
        assert!(close(conv("bar", "kPa").apply_f64(1.0), 100.0, 1e-12));
        assert!(close(conv("L", "m3").apply_f64(1.0), 1e-3, 1e-12));
        assert!(close(conv("h", "min").apply_f64(1.0), 60.0, 1e-12));
    }

    #[test]
    fn annotations_are_ignored() {
        let plain = Unit::parse("1").unwrap();
        let annotated = Unit::parse("{count}").unwrap();
        assert_eq!(plain.dims, annotated.dims);
        assert_eq!(plain.scale, annotated.scale);
        assert_eq!(annotated.code(), "{count}");
        assert!(conv("{count}", "1").is_identity());
        let u = Unit::parse("mL{total}/min").unwrap();
        assert_eq!(u.dims, Unit::parse("mL/min").unwrap().dims);
        assert_eq!(u.scale, Unit::parse("mL/min").unwrap().scale);
        assert!(Unit::parse("m2{area}").is_ok());
        assert!(Unit::parse("{a{b}c}/s").is_ok());
        assert!(Unit::parse("Cel{room}").is_ok());
    }

    #[test]
    fn error_cases() {
        assert_eq!(Unit::parse(""), Err(UnitError::Empty));
        assert_eq!(Unit::parse(&"m".repeat(129)), Err(UnitError::TooLong));
        assert!(Unit::parse(&"m.".repeat(64)).is_err());
        assert!(Unit::parse(&format!("m{}", ".m".repeat(63))).is_ok());
        assert!(matches!(
            Unit::parse("Cel/s"),
            Err(UnitError::OffsetUnitInCompound(_))
        ));
        assert!(matches!(
            Unit::parse("2.Cel"),
            Err(UnitError::OffsetUnitInCompound(_))
        ));
        assert!(matches!(
            Unit::parse("Cel2"),
            Err(UnitError::OffsetUnitInCompound(_))
        ));
        assert!(matches!(
            Unit::parse("/Cel"),
            Err(UnitError::OffsetUnitInCompound(_))
        ));
        assert!(matches!(
            Unit::parse("[degF].m"),
            Err(UnitError::OffsetUnitInCompound(_))
        ));
        assert!(Unit::parse("mCel").is_ok());
        assert!(Unit::parse("Cel1").is_ok());
        assert_eq!(
            Unit::parse("foo"),
            Err(UnitError::UnknownUnit("foo".to_owned()))
        );
        assert_eq!(
            Unit::parse("[xyz]"),
            Err(UnitError::UnknownUnit("[xyz]".to_owned()))
        );
        assert!(matches!(
            Unit::parse("m s"),
            Err(UnitError::Syntax { pos: 1, .. })
        ));
        assert!(matches!(
            Unit::parse(" m"),
            Err(UnitError::Syntax { pos: 0, .. })
        ));
        assert!(matches!(
            Unit::parse("m "),
            Err(UnitError::Syntax { pos: 1, .. })
        ));
        for code in [
            "m.", ".m", "m..s", "/", "m//s", "[g", "m{abc", "m}", "2m", "m*s", "0", "m)",
        ] {
            assert!(
                matches!(Unit::parse(code), Err(UnitError::Syntax { .. })),
                "{code}"
            );
        }
        for code in [
            "m99",
            "m13",
            "m-13",
            "m+13",
            "m-",
            "m+",
            "m99999999999999999999",
            "s-99",
        ] {
            assert!(
                matches!(Unit::parse(code), Err(UnitError::BadExponent(_))),
                "{code}"
            );
        }
        assert!(matches!(
            Unit::parse("99999999999999999999999"),
            Err(UnitError::Syntax { .. })
        ));
        assert!(matches!(
            Unit::parse("Ym12.Ym12"),
            Err(UnitError::BadExponent(_))
        ));
    }

    #[test]
    fn incompatible_units() {
        assert_eq!(
            UnitConversion::from_codes("m", "s"),
            Err(UnitError::Incompatible {
                from: "m".to_owned(),
                to: "s".to_owned()
            })
        );
        assert!(matches!(
            UnitConversion::from_codes("rad", "1"),
            Err(UnitError::Incompatible { .. })
        ));
        assert!(matches!(
            UnitConversion::from_codes("foo", "m"),
            Err(UnitError::UnknownUnit(_))
        ));
    }

    #[test]
    fn identity_detection() {
        assert!(conv("mV", "mV").is_identity());
        assert!(conv("Cel", "Cel").is_identity());
        assert!(conv("[degF]", "[degF]").is_identity());
        assert!(conv("m/s", "m/s").is_identity());
        assert!(!conv("mV", "V").is_identity());
        assert!(!conv("Cel", "K").is_identity());
        assert!(!conv("dCel", "Cel").is_identity());
        let id = conv("mV", "mV");
        assert_eq!(id.apply_f64(-0.0).to_bits(), (-0.0_f64).to_bits());
        let nan = f64::from_bits(0x7ff8_0000_0000_1234);
        assert_eq!(id.apply_f64(nan).to_bits(), nan.to_bits());
    }

    #[test]
    fn non_finite_values_pass_through() {
        for c in [conv("mV", "V"), conv("Cel", "K"), conv("km/h", "m/s")] {
            assert!(c.apply_f64(f64::NAN).is_nan());
            assert!(c.apply_f32(f32::NAN).is_nan());
            assert_eq!(c.apply_f64(f64::INFINITY), f64::INFINITY);
            assert_eq!(c.apply_f32(f32::NEG_INFINITY), f32::NEG_INFINITY);
        }
    }

    #[test]
    fn f32_computes_in_f64_then_rounds() {
        let c = conv("mV", "V");
        let x = 47910.0_f32;
        assert_eq!(c.apply_f32(x), (f64::from(x) / 1000.0) as f32);
        assert_eq!(conv("Cel", "K").apply_f32(25.0), 298.15_f32);
    }

    #[test]
    fn slices_match_scalar_application() {
        for (from, to) in [("mV", "V"), ("Cel", "K"), ("mV", "mV"), ("km/h", "m/s")] {
            let c = conv(from, to);
            let src = [0.0, -0.0, 1.5, -47910.0, 1e300, f64::NAN, f64::INFINITY];
            let mut xs = src;
            c.apply_f64_slice(&mut xs);
            for (got, x) in xs.iter().zip(src) {
                assert_eq!(got.to_bits(), c.apply_f64(x).to_bits());
            }
            let src32 = [
                0.0_f32,
                -0.0,
                1.5,
                -47910.0,
                3.0e38,
                f32::NAN,
                f32::INFINITY,
            ];
            let mut ys = src32;
            c.apply_f32_slice(&mut ys);
            for (got, x) in ys.iter().zip(src32) {
                assert_eq!(got.to_bits(), c.apply_f32(x).to_bits());
            }
        }
        let c = conv("mV", "V");
        c.apply_f64_slice(&mut []);
        c.apply_f32_slice(&mut []);
    }

    #[test]
    fn code_returns_original_text() {
        assert_eq!(Unit::parse("km/h").unwrap().code(), "km/h");
    }

    fn finite_f64() -> impl Strategy<Value = f64> {
        any::<f64>().prop_filter("finite", |x| x.is_finite())
    }

    proptest! {
        #[test]
        fn roundtrip_within_tolerance(x in finite_f64()) {
            let pairs = [
                ("mV", "V"),
                ("dCel", "Cel"),
                ("Cel", "K"),
                ("deg", "rad"),
                ("km/h", "m/s"),
            ];
            for (a, b) in pairs {
                let y = conv(a, b).apply_f64(x);
                let back = conv(b, a).apply_f64(y);
                if y.is_finite() && back.is_finite() {
                    // Absolute floor of 1.0: (x + 273.15) - 273.15 cannot be
                    // relatively accurate for x near zero.
                    prop_assert!(
                        (back - x).abs() <= 1e-9 * x.abs().max(1.0),
                        "{a}->{b}->{a}: {x} -> {y} -> {back}"
                    );
                }
            }
        }

        #[test]
        fn parse_never_panics_printable_ascii(s in "[ -~]{0,300}") {
            let _ = Unit::parse(&s);
        }

        #[test]
        fn parse_never_panics_very_long(s in "[ -~]{500,4000}") {
            prop_assert_eq!(Unit::parse(&s), Err(UnitError::TooLong));
        }

        #[test]
        fn parse_never_panics_grammar_alphabet(s in "[a-zA-Z0-9./{}%+\\[\\]_-]{0,128}") {
            let _ = Unit::parse(&s);
        }

        #[test]
        fn parse_never_panics_arbitrary_unicode(s in "\\PC{0,128}") {
            let _ = Unit::parse(&s);
        }
    }
}
