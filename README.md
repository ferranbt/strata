# Strata

A unified, typed data layer over heterogeneous data sources. Every backend is
exposed through the **same four verbs** and described by the **same schema
model**, addressed by an API path. A backend can be a public API (RSS, Congress,
GitHub), a database (Postgres, MySQL, SQLite, ClickHouse), or a lakehouse table
(Iceberg).

Today that layer covers typed access and data movement. The direction is a
catalog-first data platform, with datasets, schemas, cursors, and lineage built on
the same uniform surface.

The four verbs:

- **`get`**: read one entity (e.g. `get /repos/rust-lang/rust` on the GitHub
  provider).
- **`list`**: read a collection as a stream of typed rows, following the source's
  cursor to page through it for you (e.g. `list /items` on an RSS provider).
- **`create`**: write one entity.
- **`put`**: write a stream of typed rows into a sink. The provider reads the
  stream's schema to create the sink if it does not exist yet.

Because `list` produces a stream and `put` consumes one, any source can flow into any sink.

## Why

- **Unified interface.** You address a source by path, never by backend type, so
  swapping SQLite for Iceberg is a config change, not a rewrite.
- **The schema travels with the data.** A read carries its own schema, so a sink
  can create and validate itself from it. There is no need for hand-written table
  definitions.
- **Move data without glue code.** `pipe` connects a `list` to a `put`, so getting
  an API into any database is one command. There is no need for bespoke scripts.

## Example

Three providers, each a mount name bound to a backend provider: `hn` (a Hacker News RSS
feed), `lake` (a local Iceberg warehouse), and `local` (a SQLite file).

```toml
# strata.toml
[provider.hn]
backend = "rss"
url = "https://hnrss.org/frontpage"

[provider.lake]
backend = "iceberg"
warehouse = "./warehouse"

[provider.local]
backend = "sqlite"
path = "./local.sqlite"
```

Commands address a provider by its mount name, then the path within it, written
`<mount> <path>`.

Read the feed directly from the `hn` provider. It returns a stream of typed rows:

```bash
strata call list hn /items
```

Pipe it into `lake` (Iceberg), then pipe that into `local` (SQLite):

```bash
strata pipe hn /items lake /tables/headlines
strata pipe lake /tables/headlines local /tables/headlines
```

Each sink was created from the schema flowing into it. Now `list` reads back the
same rows from either one:

```bash
strata call list lake /tables/headlines
strata call list local /tables/headlines
```

The source, the lakehouse table, and the database all answer `list` with the same
data. The second `pipe` didn't care that its sink was SQLite. Any backend that
implements `put` writes the data in the same way, and the behaviour is identical.

## More

- `strata list`: mounted providers, or `strata list <provider>` for its endpoints.
- `strata schema <provider>`: JSON description of every endpoint (inputs, body, response).
- `strata call <get|list> <provider> <path>`: read one endpoint (`--follow` drains a list).
- `strata serve`: expose everything over an Arrow Flight (gRPC) server.
