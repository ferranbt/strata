//! `#[derive(HasSchema)]` — generate [`strata_schema::HasSchema`] (the struct's `Schema`)
//! and [`strata_schema::HasDataType`] (so it nests as a field's `DataType::Struct`) for a
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
use syn::{Data, DeriveInput, Fields, parse_macro_input};

use crate::serde_attrs;

pub fn derive(input: TokenStream) -> TokenStream {
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

    let rename_all = serde_attrs::rename_all(&ast.attrs);

    let fields = named.iter().map(|field| {
        let ident = field.ident.as_ref().unwrap();
        // serde precedence: field `rename` > container `rename_all` > Rust name.
        let field_name = serde_attrs::rename(&field.attrs)
            .or_else(|| {
                rename_all
                    .as_deref()
                    .map(|rule| serde_attrs::apply_rename_all(rule, &ident.to_string()))
            })
            .unwrap_or_else(|| ident.to_string());
        let (inner_ty, nullable) = match serde_attrs::option_inner(&field.ty) {
            Some(inner) => (inner, true),
            None => (&field.ty, false),
        };
        // TODO: Make this a bit more generic, if we were to have more keys here
        // it would be ugly to extend this return argument.
        let (is_key, is_cursor, description, reference) = schema_attrs(&field.attrs);
        let base = quote! {
            ::strata_schema::Field::new(
                #field_name,
                <#inner_ty as ::strata_schema::HasDataType>::data_type(),
                #nullable,
            )
        };
        // No attributes → the bare constructor; otherwise apply them to a local:
        // `annotate` mutates in place (returns `()`), `with_description` is owned.
        if !is_key && !is_cursor && description.is_none() && reference.is_none() {
            base
        } else {
            let key_call = is_key.then(|| quote! { field.annotate(::strata_schema::Field::KEY, "true"); });
            let cursor_call =
                is_cursor.then(|| quote! { field.annotate(::strata_schema::Field::CURSOR, "true"); });
            let ref_call =
                reference.map(|target| quote! { field.annotate(::strata_schema::Field::REF, #target); });
            let desc_call = description.map(|text| quote! { field = field.with_description(#text); });
            quote! {
                {
                    let mut field = #base;
                    #key_call
                    #cursor_call
                    #ref_call
                    #desc_call
                    field
                }
            }
        }
    });

    quote! {
        impl ::strata_schema::HasSchema for #name {
            fn schema() -> ::strata_schema::Schema {
                ::strata_schema::Schema::new(::std::vec![ #(#fields),* ])
            }
        }

        // So a struct nests as a field's cell type: its `DataType` is the `Struct`
        // built from its own schema.
        impl ::strata_schema::HasDataType for #name {
            fn data_type() -> ::strata_schema::DataType {
                <#name as ::strata_schema::HasSchema>::schema().to_datatype()
            }
        }
    }
    .into()
}

/// Read a field's `#[schema(...)]` helper attribute: `(is_key, is_cursor,
/// description)` from `key`, `cursor`, and `description = "..."` (any combination
/// may be present).
fn schema_attrs(attrs: &[syn::Attribute]) -> (bool, bool, Option<String>, Option<String>) {
    let mut is_key = false;
    let mut is_cursor = false;
    let mut description = None;
    let mut reference = None;
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
            } else if meta.path.is_ident("references") {
                reference = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            }
            Ok(())
        });
    }
    (is_key, is_cursor, description, reference)
}
