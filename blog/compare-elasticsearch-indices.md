# How to Compare Two Elasticsearch Indices and Find Missing Documents

When managing Elasticsearch indices, you may need to verify that all documents present in one index also exist in another — after a reindex operation, a migration, or a data pipeline. Elasticsearch doesn't provide a built-in "diff" command for this, but you can do it efficiently by combining four APIs: `_count`, `_search` with Point-in-Time, `_mget`, and `_reindex`.

## The Problem

Imagine you have two indices, `index-a` (source) and `index-b` (target), and you want to find all documents that exist in `index-a` but are missing from `index-b`.

A naive approach — querying both indices and comparing results in memory — won't scale. Elasticsearch is designed to handle millions of documents, and loading them all at once is not practical.

## The Strategy

The approach has four steps:

1. **Count documents** in both indices. A count mismatch is a fast indicator that something is off.
2. **Open a Point-in-Time (PIT)** on the source index to get a consistent, stable snapshot.
3. **Paginate through all document IDs** in the source using `search_after`, fetching only `_id` (no `_source`).
4. **Batch-check existence** in the target index using `_mget`.
5. **Reindex missing documents** using `_reindex` with an `ids` query — entirely server-side, no document data crosses the network.

Let's walk through each step.

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

The credentials live in a `.env` file that escli sources automatically — no `-H "Authorization: ..."` on every call, no risk of leaking secrets in shell history. The request body is passed via stdin (`<<<`), which also makes it easy to pipe in multi-line JSON built dynamically with `jq`.

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

## Step 2 — Open a Point-in-Time on the Source Index

A [Point-in-Time (PIT)](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-open-point-in-time) freezes a consistent view of the index for the duration of the scan. Without it, concurrent writes could cause documents to appear or disappear across pages, making the comparison unreliable.

```bash
./escli open_point_in_time index-a 5m
```

The response contains a `id` field — save it, you'll use it on every subsequent search request:

```json
{ "id": "46ToAwMDaWR..." }
```

When the scan is complete (or if an error occurs), always close the PIT to release server resources:

```bash
./escli close_point_in_time <<< '{"id": "46ToAwMDaWR..."}'
```

## Step 3 — Paginate Through Document IDs Using `search_after`

With the PIT open, paginate through `index-a` in batches using `search_after`. Setting `"_source": false` means Elasticsearch only reads the `_id` field — no document content is transferred, which keeps each page fast and lightweight.

For the first page:

```bash
./escli search <<< '{
  "size": 10000,
  "_source": false,
  "pit": { "id": "46ToAwMDaWR...", "keep_alive": "5m" },
  "sort": [{ "_shard_doc": "asc" }]
}'
```

The sort key `_shard_doc` is the most efficient sort for full-index pagination: it uses the internal Lucene document order with no overhead.

For subsequent pages, take the `sort` value of the last hit and pass it as `search_after`:

```bash
./escli search <<< '{
  "size": 10000,
  "_source": false,
  "pit": { "id": "46ToAwMDaWR...", "keep_alive": "5m" },
  "sort": [{ "_shard_doc": "asc" }],
  "search_after": [1234567]
}'
```

Elasticsearch may return an updated PIT ID in each response — always use the latest one for the next request.

Repeat until the response contains zero hits: that signals the end of the index.

## Step 4 — Batch-Check Existence in the Target Index via `_mget`

For each page of IDs collected in the previous step, issue a single `_mget` request against `index-b`. This checks hundreds or thousands of IDs in one round-trip without fetching any document content.

```bash
./escli mget --index index-b --_source false <<< '{
  "ids": ["id-000001", "id-000002", "id-000003", "..."]
}'
```

The response contains one entry per requested ID, each with a `found` flag:

```json
{
  "docs": [
    { "_id": "id-000001", "found": true  },
    { "_id": "id-000002", "found": false },
    { "_id": "id-000003", "found": true  }
  ]
}
```

Any entry where `"found": false` is a document missing from `index-b`. Collect those IDs — those are what you need to fix.

## Step 5 — Reindex Missing Documents via `_reindex`

Once you have the list of missing IDs, use the `_reindex` API with an `ids` query to copy them from `index-a` into `index-b`. The key advantage over fetching and re-posting documents yourself is that **everything happens server-side**: Elasticsearch reads documents directly from the source shard and writes them to the destination, without any document content ever crossing the network to your client.

Process the missing IDs in batches (10,000 at a time is a good default):

```bash
./escli reindex <<< '{
  "source": {
    "index": "index-a",
    "query": { "ids": { "values": ["id-000002", "id-000007", "..."] } }
  },
  "dest": { "index": "index-b" }
}'
```

The response tells you exactly how many documents were created or updated:

```json
{
  "total": 10000,
  "created": 10000,
  "updated": 0,
  "failures": []
}
```

Repeat for each batch until all missing IDs have been processed.

## Performance on a 1M Dataset

To validate the approach, the demo generates 1,000,000 documents into `index-a` and deliberately skips ~5% of them in `index-b`, then runs the full compare → reindex cycle.

Results on a MacBook M3 Pro:

**Dataset generation** (`init-dataset.sh`):

```txt
Documents generated:                 1000000
Indexed into index-a:                1000000
Indexed into index-b:                949443
Skipped from index-b:                50557
Effective miss rate:                 ~5%
index-a batch stats (50 batches):    avg 2s build, 0s index
index-b batch stats (48 batches):    avg 2s build, 0s index
Duration:                            2m 30s
```

The dataset generation is CPU-bound on the bash side (building NDJSON payloads), not I/O-bound — bulk indexing itself is near-instant.

**Comparison** (`compare-indices.sh`):

```txt
Total documents in index-a:          1000000
Total documents in index-b:          949443
Documents checked:                   1000000
Documents missing from index-b:      50557
Missing rate:                        ~5%
Duration:                            37s
```

**Reindex of missing documents** (`reindex-missing.sh`):

```txt
IDs read from input file:            50557
Documents re-indexed into index-b:   50557
Duration:                            4s
```

**37 seconds to scan 1,000,000 documents and identify 50,557 missing ones. 4 seconds to reindex them all.** The approach scales well because each network round-trip carries thousands of IDs, and `_source: false` keeps the search pages small.

There is also a trick here. As we are just checking for the document `_id` field, the `_mget` requests are extremely fast. If we were fetching `_source` to compare content, the network and deserialization overhead would be much higher, and the performance would degrade significantly.

## Conclusion

Elasticsearch doesn't offer a native index diff command, but combining `_count`, PIT-based pagination, `_mget` existence checks, and targeted `_reindex` gives you an efficient and scalable way to identify — and fix — missing documents between two indices, without ever touching documents that are already in sync.

The same pattern can be extended to:

- **Detect content differences**: fetch `_source` during the scan and compare field values.
- **Multi-index comparison**: run the same script against a list of target indices in parallel.
- **Continuous sync monitoring**: schedule the comparison to run periodically and alert on drift.

The complete demo, including dataset generation and reindex scripts, is available at [[URL_PLACE_HOLDER](https://github.com/dadoonet/blog-compare-indices/)].
