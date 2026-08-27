//! `#[config]` — describe and resolve a provider's settings.
//!
//! Each field is read from the provider's config key, else from the environment
//! variable named by `env`, else from a declared `default`. A field satisfied by
//! none of those is an error naming the sources, unless it is `Option<_>`.
//!
//! ```ignore
//! #[config]
//! struct Config {
//!     #[config(env = "GITHUB_TOKEN", description = "API token", secret)]
//!     api_key: String,
//!     #[config(default = "./warehouse")]
//!     warehouse: String,
//!     region: Option<String>,
//! }
//! ```
//!
//! generates `ConfigSchema` for the struct: `config_schema()` describing the
//! fields (each annotated with its `env` name and whether it is `secret`), and
//! `from_config()` resolving them.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Attribute, Fields, ItemStruct, LitStr, parse_macro_input};

use crate::serde_attrs;

struct Setting {
    env: Option<String>,
    default: Option<String>,
    description: Option<String>,
    secret: bool,
}

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

    let mut described = Vec::new();
    let mut resolved = Vec::new();
    for field in fields.iter_mut() {
        let ident = field.ident.clone().unwrap();
        let key = ident.to_string();
        let setting = match take_setting(&mut field.attrs) {
            Ok(setting) => setting,
            Err(e) => return e.to_compile_error().into(),
        };

        let (ty, optional) = match serde_attrs::option_inner(&field.ty) {
            Some(inner) => (inner.clone(), true),
            None => (field.ty.clone(), false),
        };
        // A field with a default is always satisfiable, so it is not required —
        // but it still resolves to a value, so only an `Option<_>` field keeps
        // the `Option`.
        let required = !optional && setting.default.is_none();
        let nullable = optional || setting.default.is_some();

        let env = option_str(&setting.env);
        let default = option_str(&setting.default);
        let annotations = [
            setting.env.map(|env| quote! { field.annotate("env", #env); }),
            setting
                .default
                .map(|value| quote! { field.annotate("default", #value); }),
            setting
                .secret
                .then(|| quote! { field.annotate("secret", "true"); }),
            setting
                .description
                .map(|text| quote! { field = field.with_description(#text); }),
        ];
        described.push(quote! {
            {
                let mut field = ::strata_schema::Field::new(
                    #key,
                    <#ty as ::strata_schema::HasDataType>::data_type(),
                    #nullable,
                );
                #(#annotations)*
                field
            }
        });

        let call = quote! {
            ::strata_sdk::config::resolve::<#ty>(
                config, #key, #env, #default, #required,
            )?
        };
        let value = match optional {
            true => call,
            false => quote! { #call.expect("a required or defaulted setting resolves") },
        };
        resolved.push(quote! { #ident: #value });
    }

    quote! {
        #item

        impl ::strata_sdk::config::ConfigSchema for #name {
            fn config_schema() -> ::strata_schema::Schema {
                ::strata_schema::Schema::new(::std::vec![ #(#described),* ])
            }

            fn from_config(
                config: &::strata_sdk::config::ProviderConfig,
            ) -> ::anyhow::Result<Self> {
                Ok(Self { #(#resolved),* })
            }
        }
    }
    .into()
}

fn option_str(value: &Option<String>) -> proc_macro2::TokenStream {
    match value {
        Some(value) => quote! { ::std::option::Option::Some(#value) },
        None => quote! { ::std::option::Option::None },
    }
}

/// Remove every `#[config(...)]` attribute from `attrs`, returning what it
/// declared. An unknown key is rejected rather than ignored, so a typo does not
/// silently drop a setting.
fn take_setting(attrs: &mut Vec<Attribute>) -> syn::Result<Setting> {
    let mut setting = Setting {
        env: None,
        default: None,
        description: None,
        secret: false,
    };
    let mut error = None;
    attrs.retain(|attr| {
        if !attr.path().is_ident("config") {
            return true;
        }
        if let Err(e) = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("env") {
                setting.env = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("default") {
                setting.default = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("description") {
                setting.description = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("secret") {
                setting.secret = true;
            } else {
                return Err(meta.error("unknown #[config] key (env, default, description, secret)"));
            }
            Ok(())
        }) {
            error = Some(e);
        }
        false
    });
    match error {
        Some(e) => Err(e),
        None => Ok(setting),
    }
}
