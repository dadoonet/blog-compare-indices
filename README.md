# Compare Elasticsearch Indices

Blog post and demo showing how to compare two Elasticsearch indices and find missing documents.

## Structure

```
.
├── blog/
│   └── compare-elasticsearch-indices.md   # The blog article
└── demo/
    ├── .env.es.example                    # Template for ES credentials
    ├── .env.demo                          # Demo parameters (committed)
    ├── .env.sh                            # Sources both env files
    ├── escli                              # escli-rs Docker wrapper
    ├── setup.sh                           # Start ES + pull escli image
    ├── init-dataset.sh                    # Generate index-a and index-b
    ├── compare-indices.sh                 # Find missing documents
    ├── reindex-missing.sh                 # Re-index missing docs (TODO)
    └── ESCLI_COMMANDS.md                  # escli-rs command reference
```

## Blog Post

The article explains how to efficiently detect documents present in one index but missing in another, using:

- **PIT + `search_after`** to paginate through the source index (IDs only)
- **`_mget`** to batch-check document existence in the target index

## Demo

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) — runs Elasticsearch and escli-rs
- `curl` — used by the setup script
- `jq` — used by the comparison script (`brew install jq` / `apt install jq`)

### 1. Setup

Start a local Elasticsearch instance using [elastic/start-local](https://github.com/elastic/start-local)
and pull the [escli-rs](https://github.com/Anaethelion/escli-rs) Docker image:

```bash
cd demo
./setup.sh
```

This will:

- Start Elasticsearch + Kibana via Docker on `http://localhost:9200`
- Create `demo/.env.es` with the connection URL and API key
- Pull the `ghcr.io/anaethelion/escli` Docker image for your architecture

If Elasticsearch is already running, skip that step:

```bash
./setup.sh --skip-start-local
```

### 2. Configure

`demo/.env.es` is populated automatically by `setup.sh`. If you need to connect to a different
Elasticsearch instance, edit it manually:

```bash
# demo/.env.es
ESCLI_URL=http://localhost:9200
ESCLI_API_KEY=your-api-key
```

Dataset parameters (number of documents, miss rate, batch sizes) are in `demo/.env.demo`
and can be edited freely — that file is committed with sensible defaults.

### 3. Generate the datasets

Create `index-a` (full dataset) and `index-b` (dataset with randomly missing documents):

```bash
./init-dataset.sh
```

Default parameters (override in `demo/.env.demo` or via CLI flags):

| Parameter | Default | Description |
|---|---|---|
| `NUM_DOCS` | `1000` | Documents generated in `index-a` |
| `MISS_RATE` | `1` | % of docs randomly omitted from `index-b` |
| `BULK_BATCH_SIZE` | `500` | Documents per bulk API call |
| `INDEX_A` | `index-a` | Source index name |
| `INDEX_B` | `index-b` | Target index name |

Example with custom values:

```bash
./init-dataset.sh --num-docs 5000 --miss-rate 5
```

### 4. Compare the indices

Find all documents present in `index-a` but missing from `index-b`:

```bash
./compare-indices.sh
```

The script will:

1. Run `_count` on both indices — exits early if counts are equal
2. Open a Point-in-Time on `index-a` for a consistent snapshot
3. Paginate through all IDs using `search_after`
4. Batch-check existence in `index-b` via `_mget`
5. Write missing IDs to `missing-ids.txt` and print a summary

Example output:

```txt
[compare] Counting documents...
  → index-a: 1000 documents
  → index-b: 991 documents
[compare] index-b has 9 fewer document(s) than index-a. Starting full ID comparison...

[compare] Opening Point-in-Time on index-a (keep-alive: 5m)...
[compare] Scanning index-a in batches of 1000...
  → Page 1: 1000 IDs fetched (1000 total checked so far)...

[compare] Comparison complete.
  Total documents in index-a:            1000
  Total documents in index-b:            991
  Documents checked:                     1000
  Documents missing from index-b:        9
  Missing rate:                          ~0%
  → Missing IDs written to: /path/to/demo/missing-ids.txt
```

Override defaults via CLI:

```bash
./compare-indices.sh --batch-size 500 --output my-missing.txt
```

### 5. Re-index missing documents

> **Coming soon** — `reindex-missing.sh` will read `missing-ids.txt`, fetch the documents
> from `index-a`, and bulk-index them into `index-b`.
