# Compare Elasticsearch Indices

Blog post and demo showing how to compare two Elasticsearch indices and find missing documents.

## Structure

```
.
├── blog/
│   └── compare-elasticsearch-indices.md   # The blog article
└── demo/                                  # Runnable demo (work in progress)
```

## Blog Post

The article explains how to efficiently detect documents present in one index but missing in another, using:

- **PIT + `search_after`** to paginate through the source index (IDs only)
- **`_mget`** to batch-check document existence in the target index

## Demo

See the `demo/` directory for a runnable example.
