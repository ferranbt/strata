//! Reading the serde attributes that decide a field's external name, so a
//! generated schema's column names match the JSON keys the struct actually
//! (de)serializes to.

use syn::Token;

/// The container `#[serde(rename_all = "...")]` rule, if any.
pub fn rename_all(attrs: &[syn::Attribute]) -> Option<String> {
    string_value(attrs, "rename_all")
}

/// The field `#[serde(rename = "...")]` value. Supports both `rename = "x"` and
/// `rename(serialize = "x", deserialize = "y")` (preferring the serialize name).
pub fn rename(attrs: &[syn::Attribute]) -> Option<String> {
    if let Some(simple) = string_value(attrs, "rename") {
        return Some(simple);
    }
    // `rename(serialize = "x", ...)` form.
    let mut out = None;
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") && meta.input.peek(syn::token::Paren) {
                meta.parse_nested_meta(|inner| {
                    if inner.path.is_ident("serialize") {
                        out = Some(inner.value()?.parse::<syn::LitStr>()?.value());
                    } else if inner.input.peek(Token![=]) {
                        let _ = inner.value()?.parse::<syn::LitStr>()?;
                    }
                    Ok(())
                })?;
            } else {
                consume_value(&meta)?;
            }
            Ok(())
        });
    }
    out
}

/// The string value of a top-level `#[serde(<key> = "...")]`, scanning all serde
/// attributes. Other keys are consumed and ignored.
fn string_value(attrs: &[syn::Attribute], key: &str) -> Option<String> {
    let mut out = None;
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident(key) && meta.input.peek(Token![=]) {
                out = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else {
                consume_value(&meta)?;
            }
            Ok(())
        });
    }
    out
}

/// Consume an unrelated serde meta's payload (`= value` or `( ... )`) so
/// `parse_nested_meta` can continue past keys we don't care about.
fn consume_value(meta: &syn::meta::ParseNestedMeta) -> syn::Result<()> {
    if meta.input.peek(Token![=]) {
        // String-valued keys (with, skip_serializing_if, default, …) parse as a
        // string literal; that's all serde uses here.
        let _ = meta.value()?.parse::<syn::LitStr>()?;
    } else if meta.input.peek(syn::token::Paren) {
        let content;
        syn::parenthesized!(content in meta.input);
        let _ = content.parse::<proc_macro2::TokenStream>();
    }
    Ok(())
}

/// Apply a serde `rename_all` rule to a snake_case field name.
pub fn apply_rename_all(rule: &str, name: &str) -> String {
    let words: Vec<&str> = name.split('_').filter(|w| !w.is_empty()).collect();
    let cap = |w: &str| {
        let mut c = w.chars();
        match c.next() {
            Some(first) => first.to_uppercase().collect::<String>() + c.as_str(),
            None => String::new(),
        }
    };
    match rule {
        "lowercase" => words.concat().to_lowercase(),
        "UPPERCASE" => words.concat().to_uppercase(),
        "PascalCase" => words.iter().map(|w| cap(w)).collect(),
        "camelCase" => words
            .iter()
            .enumerate()
            .map(|(i, w)| if i == 0 { w.to_string() } else { cap(w) })
            .collect(),
        "snake_case" => words.join("_"),
        "SCREAMING_SNAKE_CASE" => words.join("_").to_uppercase(),
        "kebab-case" => words.join("-"),
        "SCREAMING-KEBAB-CASE" => words.join("-").to_uppercase(),
        // Unknown rule: leave the name unchanged.
        _ => name.to_string(),
    }
}

/// If `ty` is `Option<Inner>`, return `Inner`.
pub fn option_inner(ty: &syn::Type) -> Option<&syn::Type> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        syn::GenericArgument::Type(inner) => Some(inner),
        _ => None,
    })
}
