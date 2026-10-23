//! Attribute argument parsing shared by `#[workflow]` and `#[step]`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Expr, ExprLit, Lit, Meta, Token, parse::Parser, punctuated::Punctuated};

type Metas = Punctuated<Meta, Token![,]>;

pub fn parse_metas(tokens: TokenStream) -> syn::Result<Vec<Meta>> {
    Ok(Metas::parse_terminated
        .parse2(tokens)?
        .into_iter()
        .collect())
}

/// `"500ms"`, `"30s"`, `"5m"`, `"2h"`, `"1d"` to milliseconds.
pub fn parse_duration_ms(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("duration `{s}` needs a unit: ms, s, m, h or d (e.g. \"30s\")"))?;
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().map_err(|_| format!("invalid duration `{s}`"))?;
    let mult = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        other => return Err(format!("unknown duration unit `{other}` in `{s}`")),
    };
    n.checked_mul(mult)
        .ok_or_else(|| format!("duration `{s}` is too large"))
}

pub fn lit_str(expr: &Expr) -> syn::Result<String> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(s), ..
        }) => Ok(s.value()),
        other => Err(syn::Error::new_spanned(other, "expected a string literal")),
    }
}

pub fn lit_u32(expr: &Expr) -> syn::Result<u32> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Int(i), ..
        }) => i.base10_parse(),
        other => Err(syn::Error::new_spanned(
            other,
            "expected an integer literal",
        )),
    }
}

pub fn duration_expr(expr: &Expr) -> syn::Result<TokenStream> {
    let ms = parse_duration_ms(&lit_str(expr)?).map_err(|m| syn::Error::new_spanned(expr, m))?;
    Ok(quote! { ::std::time::Duration::from_millis(#ms) })
}

/// Key/value pairs inside `name(a = 1, b = "x")`.
pub fn pairs(meta: &Meta) -> syn::Result<Vec<(String, Expr)>> {
    let Meta::List(list) = meta else {
        return Err(syn::Error::new_spanned(
            meta,
            "expected `name(key = value, ...)`",
        ));
    };
    let mut out = vec![];
    for m in parse_metas(list.tokens.clone())? {
        match m {
            Meta::NameValue(nv) => {
                let key = nv
                    .path
                    .get_ident()
                    .map(|i| i.to_string())
                    .unwrap_or_default();
                out.push((key, nv.value));
            }
            other => return Err(syn::Error::new_spanned(other, "expected `key = value`")),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_unit() {
        assert_eq!(parse_duration_ms("500ms"), Ok(500));
        assert_eq!(parse_duration_ms("30s"), Ok(30_000));
        assert_eq!(parse_duration_ms("5m"), Ok(300_000));
        assert_eq!(parse_duration_ms("2h"), Ok(7_200_000));
        assert_eq!(parse_duration_ms("1d"), Ok(86_400_000));
    }

    #[test]
    fn rejects_bad_durations_with_a_helpful_message() {
        assert!(
            parse_duration_ms("30")
                .unwrap_err()
                .contains("needs a unit")
        );
        assert!(
            parse_duration_ms("5x")
                .unwrap_err()
                .contains("unknown duration unit")
        );
        assert!(parse_duration_ms("s").unwrap_err().contains("invalid"));
        assert!(parse_duration_ms("99999999999999999d").is_err());
    }
}
