//! `#[derive(Provider)]` — generate the whole `Provider` impl from a struct.
//!
//! The config type is named by a container attribute, so the provider struct
//! keeps exactly the fields it wants:
//!
//! ```ignore
//! #[derive(strata_sdk::Provider)]
//! #[config(Config)]
//! struct Github {
//!     http: HttpClient,
//! }
//!
//! impl Github {
//!     fn new(config: Config) -> Result<Self> { ... }
//!     fn routes(router: &mut Router<Self>) { ... }
//! }
//! ```
//!
//! Everything the author writes lives in one ordinary `impl` block; the trait
//! impl is generated in full. `name()` is the struct name lowercased. A provider
//! with no settings omits the attribute and writes `fn new() -> Result<Self>`.
//!
//! The hook for routes is `routes`, not `register`: it shares the trait method's
//! signature, so a generated `Self::register(router)` would resolve back to the
//! trait method and recurse forever if an author forgot to write it.

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Meta, parse_macro_input};

pub fn derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = &ast.ident;
    let name = ident.to_string().to_lowercase();

    let config = match config_type(&ast.attrs) {
        Ok(config) => config,
        Err(e) => return e.to_compile_error().into(),
    };

    let (schema, build) = match config {
        Some(ty) => (
            quote! { <#ty as ::strata_sdk::config::ConfigSchema>::config_schema() },
            quote! {
                let config = <#ty as ::strata_sdk::config::ConfigSchema>::from_config(config)?;
                Self::new(config)
            },
        ),
        None => (quote! { ::strata_schema::Schema::empty() }, quote! { Self::new() }),
    };

    quote! {
        impl ::strata_sdk::provider::Provider for #ident {
            fn name() -> &'static str {
                #name
            }

            fn config_schema() -> ::strata_schema::Schema {
                #schema
            }

            fn new(config: &::strata_sdk::config::ProviderConfig) -> ::anyhow::Result<Self> {
                #build
            }

            fn register(router: &mut ::strata_sdk::router::Router<Self>) {
                Self::routes(router)
            }
        }
    }
    .into()
}

fn config_type(attrs: &[syn::Attribute]) -> syn::Result<Option<syn::Path>> {
    let mut found = None;
    for attr in attrs {
        if !attr.path().is_ident("config") {
            continue;
        }
        // `#[config = Config]` is not available: rustc requires a literal on the
        // right of a name-value attribute, before the derive ever sees it.
        let Meta::List(_) = &attr.meta else {
            return Err(syn::Error::new_spanned(
                attr,
                "expected `#[config(Type)]`, e.g. `#[config(Config)]`",
            ));
        };
        found = Some(attr.parse_args::<syn::Path>()?);
    }
    Ok(found)
}
