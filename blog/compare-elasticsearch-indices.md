# How to Compare Two Elasticsearch Indices and Find Missing Documents

When managing Elasticsearch indices, you may need to verify that all documents present in one index also exist in another — after a reindex operation, a migration, or a data pipeline. Elasticsearch doesn't provide a built-in "diff" command for this, but you can do it efficiently by combining the Search API and the Multi Get API.

## The Problem

Imagine you have two indices, `index-a` (source) and `index-b` (target), and you want to find all documents that exist in `index-a` but are missing from `index-b`.

A naive approach — querying both indices and comparing results in memory — won't scale. Elasticsearch is designed to handle millions of documents, and loading them all at once is not practical.

## The Approach

The solution has two steps:

1. **Scroll through `index-a`** to retrieve all document IDs (no need to fetch the full `_source`).
2. **Use the Multi Get API (`_mget`)** to check, in batches, whether those IDs exist in `index-b`.

This approach is efficient because:

- Scrolling with `_source: false` is lightweight — Elasticsearch only reads the `_id` field.
- `_mget` lets you check hundreds of IDs in a single HTTP request instead of issuing one query per document.

## Step 1 — Scroll Through the Source Index

Use the [Point in Time (PIT)](https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html) + `search_after` pattern (preferred over the legacy Scroll API for recent Elasticsearch versions):

```http
POST /index-a/_pit?keep_alive=5m
```

Then paginate using `search_after`:

```json
POST /_search
{
  "size": 1000,
  "query": { "match_all": {} },
  "_source": false,
  "pit": {
    "id": "<pit_id>",
    "keep_alive": "5m"
  },
  "sort": [{ "_shard_doc": "asc" }]
}
```

Collect the `_id` of every hit and repeat until no more results are returned.

## Step 2 — Check for Missing Documents via `_mget`

For each batch of IDs collected in step 1, issue an `_mget` request against `index-b`:

```json
POST /index-b/_mget
{
  "ids": ["id-1", "id-2", "id-3", "..."]
}
```

In the response, each item in `docs` has a `found` field. Any item where `"found": false` is a document missing from `index-b`.

```json
{
  "docs": [
    { "_id": "id-1", "found": true },
    { "_id": "id-2", "found": false },
    { "_id": "id-3", "found": true }
  ]
}
```

## Python Implementation

See the `demo/` directory for a complete, runnable Python script. Here is the core logic:

```python
from elasticsearch import Elasticsearch

def find_missing_documents(es: Elasticsearch, source_index: str, target_index: str, batch_size: int = 1000):
    # Open a Point in Time on the source index
    pit = es.open_point_in_time(index=source_index, keep_alive="5m")
    pit_id = pit["id"]

    search_after = None
    missing_ids = []

    try:
        while True:
            body = {
                "size": batch_size,
                "query": {"match_all": {}},
                "_source": False,
                "pit": {"id": pit_id, "keep_alive": "5m"},
                "sort": [{"_shard_doc": "asc"}],
            }
            if search_after:
                body["search_after"] = search_after

            response = es.search(body=body)
            hits = response["hits"]["hits"]

            if not hits:
                break

            # Extract IDs and check against target index
            ids = [hit["_id"] for hit in hits]
            mget_response = es.mget(index=target_index, body={"ids": ids})

            for doc in mget_response["docs"]:
                if not doc["found"]:
                    missing_ids.append(doc["_id"])

            search_after = hits[-1]["sort"]
    finally:
        es.close_point_in_time(body={"id": pit_id})

    return missing_ids
```

## Performance Considerations

- **Batch size**: A batch of 500–2000 IDs per `_mget` call is a good balance. Too large and you risk HTTP payload limits; too small and you increase round-trip overhead.
- **Parallelism**: If the source index is large, you can split it by routing or by using [slice scroll](https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#slice-scroll) to parallelize the scrolling across multiple workers.
- **Only IDs, not source**: Always set `"_source": false` when scrolling — you only need IDs, not the document content.

## Conclusion

Elasticsearch doesn't offer a native index diff command, but combining PIT-based pagination with batch `_mget` lookups gives you an efficient and scalable way to identify missing documents between two indices. The same pattern can be extended to compare field values or detect documents that exist in both indices but differ in content.
