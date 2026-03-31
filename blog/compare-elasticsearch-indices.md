# How to Compare Two Elasticsearch Indices and Find Missing Documents

When managing Elasticsearch indices, you may need to verify that all documents present in one index also exist in another — after a reindex operation, a migration, or a data pipeline. Elasticsearch doesn't provide a built-in "diff" command for this, but the right approach depends on one key question: **are your document IDs stable between the two indices?**

## The Problem

Imagine you have two indices, `index-a` (source) and `index-b` (target), and you want to find all documents that exist in `index-a` but are missing from `index-b`.

A naive approach — querying both indices and comparing results in memory — won't scale. Elasticsearch is designed to handle millions of documents, and loading them all at once is not practical.

There are two scenarios:

1. **IDs are stable**: both indices use the same `_id` for the same document (e.g. `emp_no` as the document ID). This is the easy case.
2. **IDs are generated**: documents were ingested through different pipelines that assigned random or sequential IDs. You can't compare by `_id` — you need to match on content.

Let's walk through both.

## Step 0 — A Lighter CLI for Elasticsearch

All the examples in this post use [escli](https://github.com/Anaethelion/escli-rs), a small Rust CLI that wraps the Elasticsearch REST API. It reads your cluster URL and credentials from environment variables, so you never have to repeat authentication headers on every command.

To see why that matters, here is a typical `_search` call with raw `curl`:

```bash
curl -X GET \
  -H "Authorization: ApiKey $ELASTIC_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":{"term":{"user.id":"kimchy"}}}' \
  "$ELASTICSEARCH_URL/my-index-000001/_search"
```

With `escli`, the same request becomes:

```bash
./escli search --index my-index-000001 <<< '{"query":{"term":{"user.id":"kimchy"}}}'
```

The credentials live in a `.env` file that escli sources automatically — no `-H "Authorization: ..."` on every call, no risk of leaking secrets in shell history. The request body is passed via stdin (`<<<`), which makes it easy to pipe in multi-line JSON built dynamically with `jq`.

## Step 1 — Count Documents in Both Indices

Before doing a full scan, get a quick count of each index. If the counts match, the indices are likely in sync and there is no need to scan at all.

```bash
./escli count --index index-a
./escli count --index index-b
```

The `_count` API returns:

```json
{ "count": 1000000 }
```

If the counts differ, proceed to the full comparison.

## Step 2 — When IDs Are Stable: Use `op_type=create`

If both indices use the same `_id` for the same document — for example, because you indexed documents using a functional business key like `emp_no` rather than a generated UUID — you can find and fix missing documents in a single `_reindex` call.

### Why functional IDs matter

Using a meaningful field as `_id` (instead of a random UUID) is a best practice when the data has a natural key. It means:

- The same document always gets the same `_id`, regardless of which pipeline ingested it.
- You can use `op_type=create` to skip documents that already exist in the target.
- No client-side scanning or comparison is needed.

### The `op_type=create` trick

`_reindex` with `op_type=create` tries to create each document from the source in the target. If a document with the same `_id` already exists, Elasticsearch reports it as a `version_conflict` and moves on — it does **not** overwrite the existing document. Setting `conflicts=proceed` tells the API to continue instead of aborting on the first conflict.

```bash
./escli reindex <<< '{
  "source": { "index": "index-a" },
  "dest":   { "index": "index-b", "op_type": "create" },
  "conflicts": "proceed"
}'
```

The response tells you exactly what happened:

```json
{
  "total": 1000000,
  "created": 49594,
  "version_conflicts": 950406,
  "failures": []
}
```

- `created`: documents that were missing from `index-b` and have now been added.
- `version_conflicts`: documents that already existed in `index-b` and were left untouched.

**No scanning, no client-side comparison, no intermediate file.** Everything happens server-side in about 6 seconds on a 1M-document dataset.

## Step 3 — When IDs Are Not Stable: Business-Key Comparison

Sometimes you can't rely on `_id`. A document pipeline that generates IDs at ingestion time will assign a different `_id` each time the same record is processed. If `index-a` and `index-b` were populated by two such pipelines, the same employee record might have `_id: "abc123"` in one index and `_id: "xyz789"` in the other — even though the underlying data is identical.

In this case you need to match documents by content rather than by ID. The key is to identify a set of fields that together form a unique business key.

For an employee dataset, a reasonable business key is `(first_name, last_name, birth_date)`. A document in `index-a` is "missing" from `index-b` if no document in `index-b` has the same combination of those three fields.

### 3a — Scan the source with PIT + `search_after`

Open a [Point-in-Time (PIT)](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-open-point-in-time) on the source index to get a consistent snapshot, then paginate through it fetching only the business-key fields:

```bash
./escli open_point_in_time index-a 5m
# → { "id": "46ToAwMDaWR..." }
```

```bash
./escli search <<< '{
  "size": 10000,
  "_source": ["first_name", "last_name", "birth_date"],
  "pit": { "id": "46ToAwMDaWR...", "keep_alive": "5m" },
  "sort": [{ "_shard_doc": "asc" }]
}'
```

The sort key `_shard_doc` is the most efficient sort for full-index pagination: it uses the internal Lucene document order with no overhead. Repeat with `search_after` until the response contains zero hits. Always close the PIT when done:

```bash
./escli close_point_in_time <<< '{"id": "46ToAwMDaWR..."}'
```

### 3b — Check each page against the target via `_msearch`

For each page of source documents, build one `_msearch` request with one sub-query per document. Each sub-query uses a `bool/must` on the three business-key fields and requests `size: 0` — we only need to know whether a match exists, not retrieve the document itself.

```bash
./escli msearch << 'EOF'
{"index": "index-b"}
{"size":0,"query":{"bool":{"must":[{"term":{"first_name.keyword":"Alice1"}},{"term":{"last_name.keyword":"Smith"}},{"term":{"birth_date":"1985-03-12"}}]}}}
{"index": "index-b"}
{"size":0,"query":{"bool":{"must":[{"term":{"first_name.keyword":"Bob2"}},{"term":{"last_name.keyword":"Jones"}},{"term":{"birth_date":"1990-07-24"}}]}}}
EOF
```

The response contains one entry per sub-query, in the same order:

```json
{
  "responses": [
    { "hits": { "total": { "value": 1 } } },
    { "hits": { "total": { "value": 0 } } }
  ]
}
```

`total.value == 0` means no document in `index-b` matches that business key — the document is missing. Collect the corresponding `_id` from the source page.

> **Why `_msearch` instead of a single `bool/should`?**
> A `bool/should` query combining all source documents into one request would return at most `size` results, silently truncating matches when there are more documents than the page size. `_msearch` sends one independent sub-query per document — each gets its own `total.value` — so there is no truncation.

> **Note on `.keyword` sub-fields**: `term` queries require exact (keyword) matching. The `first_name` and `last_name` fields must have a `.keyword` sub-field in the index mapping. The demo's `mapping.json` includes this.

### 3c — Speed it up with `split-by-date`

If the business key includes a date field, you can partition the source into date slices and run each slice as an independent job. Each slice opens its own PIT with a `range` filter on `birth_date`, runs its own msearch loop, and writes its results to a separate file. The parent script launches all slices in parallel and aggregates the results when they are all done.

```
[compare] Launching 5 slices in parallel...

  → Slice 1: 1960-01-01 → 1969-12-31 ✅ — 244408 checked, 12207 missing
  → Slice 2: 1970-01-01 → 1979-12-31 ✅ — 243624 checked, 12212 missing
  → Slice 3: 1980-01-01 → 1989-12-31 ✅ — 243551 checked, 11921 missing
  → Slice 4: 1990-01-01 → 1999-12-31 ✅ — 243895 checked, 11991 missing
  → Slice 5: 2000-01-01 → 2009-12-31 ✅ — 24522 checked, 1263 missing
```

## Performance on a 1M Dataset

To validate the approaches, the demo generates 1,000,000 documents in `index-a` and deliberately skips ~5% in `index-b` (49,594 missing documents), then runs the full compare → reindex cycle.

Results on a MacBook M3 Pro:

**Dataset generation** (`init-dataset.sh`):

```txt
Documents generated:                 1000000
Indexed into index-a:                1000000
Indexed into index-b:                950406
Effective miss rate:                 ~5%
Duration:                            2m 30s
```

**Comparison** (`compare-indices.sh`):

| Strategy           | Duration | How it works                               |
|--------------------|----------|--------------------------------------------|
| `op_type=create`   | **6s**   | Full `_reindex` server-side, skips existing|
| `querydsl`         | 37s      | PIT scan + `_mget` existence check by ID   |
| `business-key`     | 1m 38s   | PIT scan + `_msearch` by business key      |
| `split-by-date`    | **32s**  | Same as `business-key`, 5 slices in parallel|

**Reindex of missing documents** (`reindex-missing.sh`):

```txt
IDs read from input file:            49594
Documents re-indexed into index-b:   49594
Duration:                            4s
```

The `op_type=create` approach is fastest because everything is server-side and requires no client-side scanning. The `split-by-date` strategy brings the business-key approach within range of the `querydsl` strategy through parallelism.

## Decision Tree

```
Are _id values stable between both indices?
├── Yes → _reindex with op_type=create          (6s, server-side)
└── No  → Do you have a reliable business key?
          ├── Yes, simple scan is fast enough → business-key   (1m 38s)
          └── Yes, and you need more speed    → split-by-date  (32s, parallel)
```

## Conclusion

Elasticsearch doesn't offer a native index diff command, but the right strategy depends on your data model:

- **Use functional `_id`s** (a natural business key like `emp_no`) whenever possible. It unlocks the simplest and fastest approach: `_reindex` with `op_type=create` finds and fills gaps in one server-side call.
- **When IDs are unstable**, match by business key using PIT + `_msearch`. Partition by a date field and run slices in parallel to recover most of the performance.

The complete demo — dataset generation, comparison scripts, and reindex scripts — is available at [[URL_PLACE_HOLDER](https://github.com/dadoonet/blog-compare-indices/)].
