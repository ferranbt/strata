
use proc_macro::TokenStream;

mod config;
mod schema;
mod serde_attrs;

#[proc_macro_derive(HasSchema, attributes(schema))]
pub fn derive_has_schema(input: TokenStream) -> TokenStream {
    schema::derive(input)
}

#[proc_macro_attribute]
pub fn config(attr: TokenStream, item: TokenStream) -> TokenStream {
    config::attribute(attr, item)
}