# Golem Base: Querying Entities — Quick Tutorial

This tutorial shows how to run a local node, create a few entities with annotations, and query them via `golembase_queryEntities`.

## 1) Prerequisites
- Docker + Docker Compose (for a local node) or `make geth` if building locally.
- Go 1.23+ (for the CLI examples).
- curl (for raw JSON-RPC examples).

## 2) Start a Local Node
```bash
# From repo root
docker-compose up -d
# RPC available at http://localhost:8545
```
See `RUN_LOCALLY.md` for service details.

## 3) Create and Fund an Account
```bash
# Create a local wallet (stored under ~/.config/golembase)
go run ./cmd/golembase account create
# Fund it with ETH on the dev node
go run ./cmd/golembase account fund
```

## 4) Create Example Entities
```bash
# Create a document entity
go run ./cmd/golembase entity create \
  --string type:doc --string name:report --num version:2 --data "Q1 summary"

# Create an image entity
go run ./cmd/golembase entity create \
  --string type:img --string name:banner --num width:1024 --data "PNG bytes..."
```

## 5) Query with the CLI
```bash
# All docs with version >= 2
go run ./cmd/golembase query 'type = "doc" && version >= 2'

# Images or docs named starting with "ban" (glob)
go run ./cmd/golembase query '(type = "img" || type = "doc") && name ~ "ban*"'
```

## 6) Query via JSON-RPC
```bash
curl -s -X POST http://localhost:8545 \
  -H 'Content-Type: application/json' \
  --data '{
    "jsonrpc":"2.0","id":1,
    "method":"golembase_queryEntities",
    "params":["type = \"doc\" && version >= 2"]
  }'
```
The result is an array of objects: `{ "key": "0x…", "value": "…" }` where `key` is the entity hash and `value` is the payload bytes.

## Query Language Cheatsheet
- Fields: annotation keys like `type`, `name`, `version` (letters/digits/underscore; no `$`).
- Values: strings in double quotes, or unsigned integers.
- Operators: `=`, `<`, `<=`, `>`, `>=`, `~` (glob on strings), `&&`, `||`, parentheses.
- Meta-field: `$owner = "0x..."` filters by owner address.
Examples:
- `name = "report"`
- `version >= 2 && version < 5`
- `name ~ "ban*"`
- `(type = "doc" || type = "img") && $owner = "0xABCDEF..."`

### About the `~` (glob) operator
- Backend: SQLite GLOB (Unix shell-style), case-sensitive, matches the entire value.
- Wildcards:
  - `*` any sequence of characters (including empty)
  - `?` any single character
  - `[abc]` any one of the listed chars; `[a-z]` ranges
- Contains match: wrap with `*` on both sides, e.g. `name ~ "*part*"`.
- Prefix/suffix examples:
  - Prefix: `name ~ "ban*"` (starts with "ban")
  - Suffix: `filename ~ "*.json"` (ends with `.json`)
- Character-class examples:
  - `code ~ "v[0-9][0-9]"` (exactly two digits)
  - `hex ~ "[0-9a-f]*"` (lowercase hex)
- Escaping special glob chars (`*`, `?`, `[`): place them in brackets to match literally:
  - Literal asterisk: `filename ~ "file_[*].json"`
  - Literal question mark: `name ~ "what_[?]"`
  - Literal left bracket: `text ~ "prefix_[[]value"`
- Notes:
  - `~` applies to string annotations only; use `=`/`<`/`>` for numbers.
  - To include quotes inside the pattern, escape them in the query string: `title ~ "He said \"hi\"*"`.
  - Over JSON-RPC, remember to escape JSON string quotes again.

## Troubleshooting
- No results: verify annotations were set (use `golembase entity create --string ... --num ...`).
- RPC errors: ensure the node is running and `http://localhost:8545` is reachable.
- Identifier errors: keys must match `[\p{L}_][\p{L}\p{N}_]*`.
