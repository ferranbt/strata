//! `#[derive(HasSchema)]` — generate [`schema::HasSchema`] (the struct's `Schema`)
//! and [`schema::HasDataType`] (so it nests as a field's `DataType::Struct`) for a
//! struct with named fields. Each field's `DataType` comes from the field type's own
//! `HasDataType` impl; an `Option<_>` field is marked nullable (and unwrapped).
//!
//! Field names follow serde: a field-level `#[serde(rename = "...")]` wins,
//! otherwise a container `#[serde(rename_all = "...")]` rule is applied, else the
//! Rust field name is used. This keeps the schema's column names aligned with the
//! JSON keys the struct actually (de)serializes to — essential when a schema and
//! its rows are piped together (e.g. into a SQL table).
//!
//! A field can also carry the `#[schema(...)]` helper attribute: `key` marks it as
//! part of the row's key (one field, or several for a composite key), declared on
//! the type and rides on its `DataType` to a sink as the merge/upsert key; and
//! `description = "..."` sets a description surfaced in the exported JSON Schema.
//! Both may be combined, e.g. `#[schema(key, description = "...")]`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Fields, GenericArgument, PathArguments, Token, Type, parse_macro_input,
};

#[proc_macro_derive(HasSchema, attributes(schema))]
pub fn derive_has_schema(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;

    let named = match &ast.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "HasSchema can only be derived for structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(name, "HasSchema can only be derived for structs")
                .to_compile_error()
                .into();
        }
    };

    let rename_all = serde_rename_all(&ast.attrs);

    let fields = named.iter().map(|field| {
        let ident = field.ident.as_ref().unwrap();
        // serde precedence: field `rename` > container `rename_all` > Rust name.
        let field_name = serde_rename(&field.attrs)
            .or_else(|| {
                rename_all
                    .as_deref()
                    .map(|rule| apply_rename_all(rule, &ident.to_string()))
            })
            .unwrap_or_else(|| ident.to_string());
        let (inner_ty, nullable) = match option_inner(&field.ty) {
            Some(inner) => (inner, true),
            None => (&field.ty, false),
        };
        // TODO: Make this a bit more generic, if we were to have more keys here
        // it would be ugly to extend this return argument.
        let (is_key, is_cursor, description) = schema_attrs(&field.attrs);
        let base = quote! {
            ::schema::Field::new(
                #field_name,
                <#inner_ty as ::schema::HasDataType>::data_type(),
                #nullable,
            )
        };
        // No attributes → the bare constructor; otherwise apply them to a local:
        // `annotate` mutates in place (returns `()`), `with_description` is owned.
        if !is_key && !is_cursor && description.is_none() {
            base
        } else {
            let key_call = is_key.then(|| quote! { field.annotate(::schema::Field::KEY, "true"); });
            let cursor_call =
                is_cursor.then(|| quote! { field.annotate(::schema::Field::CURSOR, "true"); });
            let desc_call = description.map(|text| quote! { field = field.with_description(#text); });
            quote! {
                {
                    let mut field = #base;
                    #key_call
                    #cursor_call
                    #desc_call
                    field
                }
            }
        }
    });

    quote! {
        impl ::schema::HasSchema for #name {
            fn schema() -> ::schema::Schema {
                ::schema::Schema::new(::std::vec![ #(#fields),* ])
            }
        }

        // So a struct nests as a field's cell type: its `DataType` is the `Struct`
        // built from its own schema.
        impl ::schema::HasDataType for #name {
            fn data_type() -> ::schema::DataType {
                <#name as ::schema::HasSchema>::schema().to_datatype()
            }
        }
    }
    .into()
}

/// Read a field's `#[schema(...)]` helper attribute: `(is_key, is_cursor,
/// description)` from `key`, `cursor`, and `description = "..."` (any combination
/// may be present).
fn schema_attrs(attrs: &[syn::Attribute]) -> (bool, bool, Option<String>) {
    let mut is_key = false;
    let mut is_cursor = false;
    let mut description = None;
    for attr in attrs {
        if !attr.path().is_ident("schema") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("key") {
                is_key = true;
            } else if meta.path.is_ident("cursor") {
                is_cursor = true;
            } else if meta.path.is_ident("description") {
                description = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            }
            Ok(())
        });
    }
    (is_key, is_cursor, description)
}

/// If `ty` is `Option<Inner>`, return `Inner`.
fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else { return None };
    let segment = path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    })
}

/// The container `#[serde(rename_all = "...")]` rule, if any.
fn serde_rename_all(attrs: &[syn::Attribute]) -> Option<String> {
    serde_string_value(attrs, "rename_all")
}

/// The field `#[serde(rename = "...")]` value. Supports both `rename = "x"` and
/// `rename(serialize = "x", deserialize = "y")` (preferring the serialize name).
fn serde_rename(attrs: &[syn::Attribute]) -> Option<String> {
    if let Some(simple) = serde_string_value(attrs, "rename") {
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
fn serde_string_value(attrs: &[syn::Attribute], key: &str) -> Option<String> {
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
fn apply_rename_all(rule: &str, name: &str) -> String {
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
