//! `#[config]` — a small provider-config helper on top of serde.
//!
//! Put it above a config struct and it rewrites the struct so that, for every
//! field carrying `#[config(default_env = "ENV")]`, a missing key is filled from
//! the environment variable `ENV` during deserialization. You write nothing else:
//! no `#[derive(Deserialize)]`, no `#[serde(default = "…")]`.
//!
//! ```ignore
//! #[config]
//! struct Config {
//!     #[config(default_env = "CLICKHOUSE_URL")]
//!     url: String,
//!     #[config(default_env = "CLICKHOUSE_USER")]
//!     user: String,
//! }
//! ```
//!
//! expands (roughly) to:
//!
//! ```ignore
//! #[derive(serde::Deserialize)]
//! struct Config {
//!     #[serde(default = "Config::default_url")]
//!     url: String,
//!     #[serde(default = "Config::default_user")]
//!     user: String,
//! }
//! impl Config {
//!     fn default_url() -> String { std::env::var("CLICKHOUSE_URL").…unwrap_or_default() }
//!     fn default_user() -> String { std::env::var("CLICKHOUSE_USER").…unwrap_or_default() }
//! }
//! // when *every* field is env-defaulted, also:
//! impl Default for Config { fn default() -> Self { serde_json::from_str("{}")… } }
//! ```
//!
//! The env-backed default reads the variable, parses it into the field type, and
//! falls back to the type's `Default` when the variable is unset or unparsable.
//!
//! Requirements on the using crate: `serde` (with `derive`) and `serde_json` in
//! scope as `::serde` / `::serde_json`. The generated `default_*` functions are
//! associated to the struct, so several config structs may live in one module
//! without their defaults colliding.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Attribute, Fields, ItemStruct, LitStr, parse_macro_input};

pub fn attribute(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(item as ItemStruct);
    let name = item.ident.clone();

    let fields = match &mut item.fields {
        Fields::Named(named) => &mut named.named,
        _ => {
            return syn::Error::new_spanned(&name, "#[config] requires a struct with named fields")
                .to_compile_error()
                .into();
        }
    };

    // Rewrite each field: pull its `#[config(default_env = "ENV")]` (if any) and
    // replace it with a `#[serde(default = "Name::default_<field>")]` pointing at
    // a generated associated fn. Fields without it stay required.
    let mut default_fns = Vec::new();
    let mut all_env_defaulted = !fields.is_empty();
    for field in fields.iter_mut() {
        let ident = field.ident.clone().unwrap();
        let ty = field.ty.clone();

        let env = take_default_env(&mut field.attrs);
        let Some(env) = env else {
            all_env_defaulted = false;
            continue;
        };

        let fn_name = format_ident!("default_{}", ident);
        let default_path = format!("{name}::{fn_name}");
        let serde_default: Attribute = syn::parse_quote!(#[serde(default = #default_path)]);
        field.attrs.push(serde_default);

        default_fns.push(quote! {
            #[doc(hidden)]
            fn #fn_name() -> #ty {
                ::std::env::var(#env)
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or_default()
            }
        });
    }

    // A `Default` via empty-object decode only makes sense if every field has an
    // env-backed default; otherwise the decode would fail on a required field.
    let default_impl = if all_env_defaulted {
        quote! {
            impl ::std::default::Default for #name {
                fn default() -> Self {
                    ::serde_json::from_str("{}")
                        .expect("#[config] default requires every field to be env-defaulted")
                }
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #[derive(::serde::Deserialize)]
        #item

        impl #name {
            #(#default_fns)*
        }

        #default_impl
    }
    .into()
}

/// Remove every `#[config(...)]` attribute from `attrs`, returning the
/// `default_env = "ENV"` value if one was present.
fn take_default_env(attrs: &mut Vec<Attribute>) -> Option<String> {
    let mut env = None;
    attrs.retain(|attr| {
        if !attr.path().is_ident("config") {
            return true;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("default_env") {
                env = Some(meta.value()?.parse::<LitStr>()?.value());
            }
            Ok(())
        });
        false
    });
    env
}
