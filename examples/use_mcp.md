# Read a source from an MCP client

Pipe a Hacker News RSS feed into a local SQLite table, then let a model discover
and read it over MCP.

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

`serve` starts the MCP server at `http://127.0.0.1:8081/mcp`.

```bash
strata serve
```

## 4. The tools

The examples below use the MCP Inspector CLI as the client.

```bash
mcp-inspector --cli http://127.0.0.1:8081/mcp --transport http --method tools/list
```

| tool | what it answers |
| :- | :- |
| `list_providers` | the mounted providers |
| `describe_provider` | every endpoint of one provider |
| `resolve_endpoint` | the schema of one concrete path |
| `get` | one entity from a `get` endpoint |
| `list` | one page of rows from a `list` endpoint |

Reads only. `put` takes an Arrow stream, so use `strata pipe` to write.

## 5. Call them

What is mounted:

```bash
mcp-inspector --cli http://127.0.0.1:8081/mcp --transport http \
  --method tools/call --tool-name "list_providers";
# {"providers":["dummy","hn","local"]}
```

The table's real columns, without reading a row. This is how a model learns what
it can filter and project on:

```bash
mcp-inspector --cli http://127.0.0.1:8081/mcp --transport http \
  --method tools/call --tool-name "resolve_endpoint" \
  --tool-args-json '{"provider":"local","path":"/tables/headlines"}'
# {"method":"list","path":"/tables/:table","params":["table"],
#  "response":{"type":"object","properties":{"id":{"type":"string"},
#    "title":{"anyOf":[{"type":"string"},{"type":"null"}]}, ...}},
#  "metadata":{"strategy":"Offset","disposition":"Append","queryable":true}}
```

Read rows, projecting to the columns it needs:

```bash
mcp-inspector --cli http://127.0.0.1:8081/mcp --transport http \
  --method tools/call --tool-name "list" \
  --tool-args-json '{"provider":"local","path":"/tables/headlines","limit":2,"fields":["title"]}'
# {"items":[{"title":"DeepSeek V4 Flash 0731 Intelligence, Performance and Price Analysis"},
#           {"title":"Danube's record low levels force shutdown of Hungary's only nuclear plant"}],
#  "cursor":{"next":"{\"offset\":2}"}}
```
