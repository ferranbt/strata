
use proc_macro::TokenStream;

mod config;
mod provider;
mod schema;
mod serde_attrs;

#[proc_macro_derive(HasSchema, attributes(schema))]
pub fn derive_has_schema(input: TokenStream) -> TokenStream {
    schema::derive(input)
}

#[proc_macro_derive(Provider, attributes(config))]
pub fn derive_provider(input: TokenStream) -> TokenStream {
    provider::derive(input)
}

#[proc_macro_attribute]
pub fn config(attr: TokenStream, item: TokenStream) -> TokenStream {
    config::attribute(attr, item)
}