//! The row predicate for a `/tables/:table` read, and its compilation to a SQL
//! `WHERE` clause. Arrives as the opaque `?filter=<json>` param and is handed to a
//! provider as a [`Filter`] struct; each provider calls [`where_clause`] with its
//! own dialect quote to splice the clause into its query.

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{quote_ident, quote_str};

/// A row predicate: a boolean tree of column comparisons. `And`/`Or` nest; a `Cmp`
/// leaf compares one column to a JSON value. Only SQL providers interpret it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Filter {
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Cmp {
        field: String,
        op: Op,
        #[serde(default)]
        value: Value,
    },
}

/// A comparison operator in a [`Filter`] leaf. `In`/`NotIn` take an array value;
/// `IsNull`/`NotNull` ignore the value.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
    NotIn,
    IsNull,
    NotNull,
    Like,
}

impl Filter {
    /// Convert to a SQL boolean expression (no `WHERE` keyword), quoting column
    /// identifiers with `quote` and escaping string literals, so a provider splices
    /// it into its dialect. Values render by JSON type; identifiers and strings are
    /// escaped, so the result is injection-safe.
    pub fn to_sql(&self, quote: char) -> Result<String> {
        match self {
            Filter::And(parts) => join(parts, "AND", quote),
            Filter::Or(parts) => join(parts, "OR", quote),
            Filter::Cmp { field, op, value } => cmp_sql(field, *op, value, quote),
        }
    }
}

pub fn where_clause(filter: Option<&Filter>, quote: char) -> Result<String> {
    match filter {
        Some(filter) => Ok(format!("WHERE {} ", filter.to_sql(quote)?)),
        None => Ok(String::new()),
    }
}

/// Decode the `?filter=` param's JSON into a [`Filter`].
pub(super) fn parse_filter(raw: &str) -> Result<Filter> {
    serde_json::from_str(raw).map_err(|e| anyhow!("decoding filter: {e}"))
}

fn join(parts: &[Filter], sep: &str, quote: char) -> Result<String> {
    if parts.is_empty() {
        bail!("empty `{}` filter group", sep.to_lowercase());
    }
    let parts = parts
        .iter()
        .map(|p| p.to_sql(quote))
        .collect::<Result<Vec<_>>>()?;
    Ok(format!("({})", parts.join(&format!(" {sep} "))))
}

fn cmp_sql(field: &str, op: Op, value: &Value, quote: char) -> Result<String> {
    let col = quote_ident(field, quote);
    Ok(match op {
        Op::Eq => format!("{col} = {}", literal(value)?),
        Op::Ne => format!("{col} <> {}", literal(value)?),
        Op::Lt => format!("{col} < {}", literal(value)?),
        Op::Lte => format!("{col} <= {}", literal(value)?),
        Op::Gt => format!("{col} > {}", literal(value)?),
        Op::Gte => format!("{col} >= {}", literal(value)?),
        Op::Like => format!("{col} LIKE {}", str_literal(value)?),
        Op::In => format!("{col} IN ({})", list_literal(value)?),
        Op::NotIn => format!("{col} NOT IN ({})", list_literal(value)?),
        Op::IsNull => format!("{col} IS NULL"),
        Op::NotNull => format!("{col} IS NOT NULL"),
    })
}

/// Render a scalar JSON value as a SQL literal. Strings are single-quote escaped;
/// numbers/bools/null render structurally, so none can inject.
fn literal(value: &Value) -> Result<String> {
    Ok(match value {
        Value::String(s) => quote_str(s),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::Null => "NULL".to_string(),
        other => bail!("unsupported filter value `{other}`"),
    })
}

fn str_literal(value: &Value) -> Result<String> {
    match value {
        Value::String(s) => Ok(quote_str(s)),
        other => bail!("`like` needs a string value, got `{other}`"),
    }
}

fn list_literal(value: &Value) -> Result<String> {
    let Value::Array(items) = value else {
        bail!("`in`/`not_in` need an array value");
    };
    if items.is_empty() {
        bail!("`in`/`not_in` need a non-empty array");
    }
    Ok(items
        .iter()
        .map(literal)
        .collect::<Result<Vec<_>>>()?
        .join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn filter(value: serde_json::Value) -> Filter {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn compiles_and_or_tree_with_escaping() {
        // (state IN ('CA','NY')) AND (congress >= 118 OR title LIKE '%act%')
        let f = filter(json!({
            "and": [
                { "cmp": { "field": "state", "op": "in", "value": ["CA", "NY"] } },
                { "or": [
                    { "cmp": { "field": "congress", "op": "gte", "value": 118 } },
                    { "cmp": { "field": "title", "op": "like", "value": "%act%" } }
                ] }
            ]
        }));
        assert_eq!(
            f.to_sql('"').unwrap(),
            r#"("state" IN ('CA', 'NY') AND ("congress" >= 118 OR "title" LIKE '%act%'))"#
        );
    }

    #[test]
    fn escapes_quotes_in_identifiers_and_strings() {
        let f = filter(json!({ "cmp": { "field": "wei\"rd", "op": "eq", "value": "o'brien" } }));
        assert_eq!(f.to_sql('"').unwrap(), r#""wei""rd" = 'o''brien'"#);
    }

    #[test]
    fn null_ops_ignore_value() {
        let f = filter(json!({ "cmp": { "field": "deleted_at", "op": "is_null" } }));
        assert_eq!(f.to_sql('`').unwrap(), "`deleted_at` IS NULL");
    }
}
