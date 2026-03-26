#!/usr/bin/env bash
# reindex-missing.sh — Re-index documents from index-a into index-b.
#
# Reads the list of missing document IDs produced by compare-indices.sh,
# fetches each document from index-a in batches using _mget, and reindexes
# them into index-b using the bulk API.
#
# Usage: ./reindex-missing.sh [options]
#   --url <url>         Elasticsearch URL             (default: from .env)
#   --api-key <key>     API key                       (default: from .env)
#   --index-a <name>    Source index                  (default: index-a)
#   --index-b <name>    Target index                  (default: index-b)
#   --input <file>      File with missing IDs         (default: missing-ids.txt)
#   --batch-size <n>    IDs per _mget / bulk call     (default: 500)
#
# TODO: implement this script once compare-indices.sh is validated.
# Outline of the implementation:
#
#   1. Read IDs line by line from the input file
#   2. Group them into batches of --batch-size
#   3. For each batch:
#        a. Fetch documents from index-a using _mget (with _source: true this time)
#        b. Build a bulk NDJSON payload from the returned _source objects
#        c. Index them into index-b using the bulk API
#   4. Print a summary (total reindexed, errors)

echo "reindex-missing.sh is not yet implemented."
echo "Run compare-indices.sh first to generate the list of missing IDs."
exit 1
