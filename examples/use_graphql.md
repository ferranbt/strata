# Query a materialized source over GraphQL

Pipe a Hacker News RSS feed into a local SQLite table, then query it over GraphQL
with a filter and a field projection.

## 1. Config

```toml
# strata.toml
[provider.hn]
backend = "rss"
url = "https://hnrss.org/frontpage"

[provider.local]
backend = "sqlite"
path = "./local.sqlite"
```

## 2. Materialize the feed into SQLite

```bash
strata pipe hn /items local /tables/headlines
```

## 3. Serve

`serve` starts the GraphQL server at `http://127.0.0.1:8080/graphql`, with the SDL at `/schema`.

```bash
strata serve
```

Each SQLite table becomes a query field named `<mount>_<table>`, here
`local_headlines`. `where` compiles to a SQL filter; the selected fields become the
projection (only those columns are returned).

## 4. Query it

Projection (fetch only `title`):

```bash
curl -s 127.0.0.1:8080/graphql -H 'content-type: application/json' \
  -d '{"query":"{ local_headlines(limit: 2) { title } }"}'
```

Filter + projection (`title` contains "AI", return `id` and `title`):

```bash
curl -s 127.0.0.1:8080/graphql -H 'content-type: application/json' \
  -d '{"query":"{ local_headlines(where: {cmp: {field: \"title\", op: \"like\", value: \"%AI%\"}}, limit: 3) { id title } }"}'
```

## 5. Inspect the schema

```bash
curl -s 127.0.0.1:8080/schema
```

Any backend you can `pipe` into (Postgres, MySQL, ClickHouse) is queryable the same way.
