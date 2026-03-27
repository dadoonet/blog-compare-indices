# escli-rs Command Reference

All commands read `ESCLI_URL` and `ESCLI_API_KEY` from the environment (sourced via `.env.sh`).
JSON bodies are passed via stdin using a here-string (`<<<`).

---

## Refresh an index ✅

```bash
./escli indices refresh --index <index>
```

Forces a refresh so all recently indexed documents are visible to searches.
Useful before a reindex or comparison to avoid missing documents that are
not yet in a searchable segment.

---

## Delete an index ✅

```bash
./escli delete <index>
```

---

## Create an index ✅

```bash
./escli create <index>
```

---

## Connection test ✅

```bash
./escli info
```

---

## Document count ✅

```bash
./escli count --index <index>
# When using the Docker image, there's a bug and you must pass a body, even if empty.
./escli count --index <index> <<< '{}'
```

Example:

```bash
COUNT=$(./escli count --index index-a | jq -r '.count')
```

---

## Load documents from a file ✅

```bash
# Ingest an NDJSON bulk file (action + document pairs); batching handled internally.
./escli utils load --size <batch_size> <file.ndjson>

# Ingest an NDJSON bulk file (action + document pairs); batching handled internally. but index is set globally not per action
./escli utils load --index <index> --size <batch_size> <file.jsonl>
```

The format is inferred from the file extension (`.ndjson` vs `.json`/`.jsonl`).
`--size` controls the number of documents per bulk request (default: 500).

---

## Bulk indexing (low-level) ✅

```bash
# Pass via stdin — required when escli runs in Docker (no access to host paths)
# --index sets the target index for all operations in the file,
# so the action headers only need to contain _id.
./escli bulk --index <index> --input - < file.ndjson
```

The file must follow the Elasticsearch NDJSON bulk format:

```json
{"index":{"_id":"id-000001"}}
{"title":"Document id-000001","value":42}
```

---

## Open Point-in-Time ✅

```bash
./escli open_point_in_time <INDEX> <KEEP_ALIVE>
```

Example:

```bash
PIT_ID=$(./escli open_point_in_time index-a 5m | jq -r '.id')
# When using the Docker image, there's a bug and you must pass a body, even if empty.
PIT_ID=$(./escli open_point_in_time index-a 5m <<< '{}' | jq -r '.id')
```

---

## Search (with PIT + `search_after`) ✅

```bash
./escli search <<< '<json_body>'
```

Example:

```bash
./escli search <<< '{
  "size": 1000,
  "_source": false,
  "query": {"match_all": {}},
  "pit": {"id": "<pit_id>", "keep_alive": "5m"},
  "sort": [{"_shard_doc": "asc"}],
  "search_after": [<last_sort_value>]
}'
```

Note: when using a PIT, do not pass `--index` — the index is embedded in the PIT id.

---

## Multi-get (`_mget`) ✅

```bash
./escli mget --index <index> <<< '{"ids": ["id-1", "id-2"]}'
```

---

## Dump documents from an index ✅

```bash
./escli utils dump <index> [options]
```

Dumps all documents from one or more indices as bulk-compatible NDJSON (action line + source line per document).
Uses PIT internally for a consistent read. Output goes to stdout by default.

Options:

| Flag               | Default | Description                                                                  |
|--------------------|---------|------------------------------------------------------------------------------|
| `--size <n>`       | `500`   | Documents per batch                                                          |
| `--keep-alive <t>` | `1m`    | PIT keep-alive duration                                                      |
| `--output <file>`  | stdout  | Write output to a file instead of stdout                                     |
| `--skip-index-name`|         | Omit `_index` from action lines (produces `{"index":{}}`)                   |
| `--add-id`         |         | Include `_id` in action lines                                                |
| `--query <file>`   |         | Path to a file containing an Elasticsearch query clause to filter documents  |

The `--query` file contains a query clause (not a full search body):

```json
{ "term": { "status": "active" } }
```

Use `-` to read the query from stdin:

```bash
cat query.json | ./escli utils dump my-index --query -
```

Examples:

```bash
# Dump all documents, pipe into another index
./escli utils dump index-a --skip-index-name | ./escli utils load --index index-b

# Dump with _id preserved (for re-indexing to the same index)
./escli utils dump my-index --add-id | ./escli utils load --index my-index

# Dump only documents matching an ids query
echo '{"ids":{"values":["id-1","id-2"]}}' > /tmp/query.json
./escli utils dump index-a --query /tmp/query.json --skip-index-name --add-id \
  | ./escli utils load --index index-b
```

---

## Close Point-in-Time ✅

```bash
./escli close_point_in_time <<< '{"id": "<pit_id>"}'
```
