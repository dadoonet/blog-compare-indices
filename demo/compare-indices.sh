#!/usr/bin/env bash
# compare-indices.sh — Find documents present in index-a but missing from index-b.
#
# Strategies:
#   querydsl (default)
#     1. Run _count on both indices to detect a mismatch early.
#     2. Open a Point-in-Time (PIT) on index-a for a consistent snapshot.
#     3. Paginate through all document IDs using search_after (_source: false).
#     4. For each page, batch-check existence in index-b via _mget.
#     5. Collect missing IDs, write them to a file, and print a summary.
#
#   esql
#     Not yet implemented.
#
# Requirements: escli (./escli wrapper), jq
#
# Usage: ./compare-indices.sh [options]
#   --source <name>        Source index                   (default: index-a)
#   --target <name>        Target index                   (default: index-b)
#   --batch-size <n>       IDs per search page / _mget call (default: 10000 - can't be more than ES's max_result_window)
#   --pit-keep-alive <dur> PIT keep-alive duration        (default: 5m)
#   --output <file>        Output file for missing IDs    (default: missing-ids.txt)
#   --strategy <name>      Comparison strategy            (default: querydsl)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ESCLI="${SCRIPT_DIR}/escli"

source "${SCRIPT_DIR}/lib/log.sh"   "compare"
source "${SCRIPT_DIR}/lib/utils.sh"
_PROG_INIT=0

# In TTY mode: overwrite the current line in place.
# In non-TTY mode (pipe/redirect): print normally with a newline.
_progress() {
    if (( IS_TTY )); then
        if (( _PROG_INIT == 0 )); then
            printf "  → %s" "$1"
            _PROG_INIT=1
        else
            printf "\r\033[K  → %s" "$1"
        fi
    else
        info "$1"
    fi
}

SECONDS=0   # bash built-in: counts elapsed seconds automatically

# ── Load defaults from .env.sh, then allow CLI overrides ──────────────────────
set -a && set +u && source "${SCRIPT_DIR}/.env.sh" && set -u && set +a

: "${INDEX_A:=index-a}"
: "${INDEX_B:=index-b}"
: "${BATCH_SIZE:=10000}"
: "${PIT_KEEP_ALIVE:=5m}"
: "${OUTPUT_FILE:=missing-ids.txt}"
: "${STRATEGY:=querydsl}"

while [[ $# -gt 0 ]]; do
    case $1 in
        --source)          INDEX_A="$2";          shift 2 ;;
        --target)          INDEX_B="$2";          shift 2 ;;
        --batch-size)      BATCH_SIZE="$2";       shift 2 ;;
        --pit-keep-alive)  PIT_KEEP_ALIVE="$2";   shift 2 ;;
        --output)          OUTPUT_FILE="$2";      shift 2 ;;
        --strategy)        STRATEGY="$2";         shift 2 ;;
        -h|--help)
            grep '^# ' "$0" | head -25 | sed 's/^# \?//'
            exit 0 ;;
        *) die "Unknown option: $1" ;;
    esac
done

case "$STRATEGY" in
    querydsl) ;;
    esql) die "Strategy 'esql' is not yet implemented." ;;
    *) die "Unknown strategy: ${STRATEGY}. Valid values: querydsl, esql" ;;
esac

command -v jq >/dev/null || die "jq is required (brew install jq / apt install jq)"
[ -f "$ESCLI" ]          || die "escli not found at ${ESCLI}. Run ./setup.sh first."

# ── 1. Count documents in both indices ────────────────────────────────────────
# A count mismatch is a fast indicator that documents are missing.
# Equal counts do not rule out differences (e.g. same count but different IDs),
# but for the purpose of this demo we treat equal counts as "in sync".
log "Counting documents..."

COUNT_A=$("$ESCLI" count --index "$INDEX_A" | jq -r '.count')
COUNT_B=$("$ESCLI" count --index "$INDEX_B" | jq -r '.count')

info "${INDEX_A}: ${COUNT_A} documents"
info "${INDEX_B}: ${COUNT_B} documents"

if (( COUNT_A == COUNT_B )); then
    log "Both indices have the same document count (${COUNT_A}). Indices appear to be in sync."
    exit 0
fi

DIFF=$(( COUNT_A - COUNT_B ))
warn "${INDEX_B} has ${DIFF} fewer document(s) than ${INDEX_A}. Starting full ID comparison..."
echo ""

# ── 2. Open a Point-in-Time on index-a ───────────────────────────────────────
# A PIT freezes a consistent view of the index for the duration of the scan.
# Without it, concurrent writes could cause documents to appear or disappear
# across pages, making the comparison unreliable.
log "Opening Point-in-Time on ${INDEX_A} (keep-alive: ${PIT_KEEP_ALIVE})..."

PIT_ID=$("$ESCLI" open_point_in_time "$INDEX_A" "$PIT_KEEP_ALIVE" | jq -r '.id // empty')
[ -z "$PIT_ID" ] && die "Failed to open PIT."

# Always close the PIT on exit, even on error, to release server resources
close_pit() {
    local exit_code=$?
    log "Closing Point-in-Time..."
    "$ESCLI" close_point_in_time <<< "{\"id\":\"${PIT_ID}\"}" > /dev/null 2>&1 || true
    exit $exit_code
}
trap close_pit EXIT

# ── 3. Paginate through index-a using search_after ────────────────────────────
log "Scanning ${INDEX_A} in batches of ${BATCH_SIZE}..."
(( IS_TTY )) || echo ""

OUTPUT_PATH="${SCRIPT_DIR}/${OUTPUT_FILE}"
> "$OUTPUT_PATH"     # create or truncate the output file

SEARCH_AFTER="null"  # null = first page; updated from each response
TOTAL_CHECKED=0
MISSING_COUNT=0
PAGE=0
TOTAL_SEARCH_MS=0
TOTAL_MGET_MS=0

while true; do
    PAGE=$(( PAGE + 1 ))

    # Build the search request body.
    # - sort by _shard_doc: the most efficient sort for full-index pagination
    #   because it uses the internal Lucene document order with no overhead.
    # - _source: false — we only need the _id, not the document content.
    # - search_after is omitted on the first page (null) and set to the sort
    #   value of the last hit on every subsequent page.
    # - The PIT ID can be refreshed by Elasticsearch on each response; we
    #   always use the latest value returned.
    SEARCH_BODY=$(jq -n \
        --arg    pit_id        "$PIT_ID"         \
        --argjson search_after "$SEARCH_AFTER"   \
        --argjson size         "$BATCH_SIZE"     \
        --arg    keep_alive    "$PIT_KEEP_ALIVE" \
        '{
            "size": $size,
            "_source": false,
            "pit": {"id": $pit_id, "keep_alive": $keep_alive},
            "sort": [{"_shard_doc": "asc"}]
        } + (if $search_after != null then {"search_after": $search_after} else {} end)')

    SEARCH_START=$(now_ms)
    SEARCH_RESPONSE=$("$ESCLI" search <<< "$SEARCH_BODY")
    SEARCH_MS=$(( $(now_ms) - SEARCH_START ))
    TOTAL_SEARCH_MS=$(( TOTAL_SEARCH_MS + SEARCH_MS ))

    # Refresh the PIT ID — Elasticsearch may return an updated one
    NEW_PIT_ID=$(echo "$SEARCH_RESPONSE" | jq -r '.pit_id // empty')
    [ -n "$NEW_PIT_ID" ] && PIT_ID="$NEW_PIT_ID"

    HITS=$(echo "$SEARCH_RESPONSE" | jq -c '.hits.hits')
    HIT_COUNT=$(echo "$HITS" | jq 'length')

    # An empty page means we have reached the end of the index
    (( HIT_COUNT == 0 )) && break

    TOTAL_CHECKED=$(( TOTAL_CHECKED + HIT_COUNT ))

    # Advance the cursor: search_after uses the sort value of the last hit
    SEARCH_AFTER=$(echo "$HITS" | jq -c '.[-1].sort')

    # ── 4. Check which IDs are missing from index-b via _mget ─────────────────
    # _mget returns one entry per requested ID. The "found" flag tells us
    # whether the document exists in the target index, without fetching its content.
    IDS_JSON=$(echo "$HITS" | jq -c '[.[]._id]')
    MGET_BODY=$(jq -n --argjson ids "$IDS_JSON" '{"ids": $ids}')
    MGET_START=$(now_ms)
    MGET_RESPONSE=$("$ESCLI" mget --index "$INDEX_B" --_source false <<< "$MGET_BODY")
    MGET_MS=$(( $(now_ms) - MGET_START ))
    TOTAL_MGET_MS=$(( TOTAL_MGET_MS + MGET_MS ))

    _progress "Page ${PAGE} $(progress_bar $TOTAL_CHECKED $COUNT_A) - ⏳ search: $(format_ms $SEARCH_MS), mget: $(format_ms $MGET_MS)"

    # Extract IDs where found == false
    MISSING_IDS=$(echo "$MGET_RESPONSE" | jq -r '.docs[] | select(.found == false) | ._id')

    if [ -n "$MISSING_IDS" ]; then
        echo "$MISSING_IDS" >> "$OUTPUT_PATH"
        MISSING_COUNT=$(( MISSING_COUNT + $(echo "$MISSING_IDS" | wc -l | tr -d ' ') ))
    fi
done

# ── 5. Summary ────────────────────────────────────────────────────────────────
(( IS_TTY && _PROG_INIT )) && printf "\r\033[K" || echo ""
log "Comparison complete."
printf "  %-38s %d\n" "Total documents in ${INDEX_A}:"       "$COUNT_A"
printf "  %-38s %d\n" "Total documents in ${INDEX_B}:"       "$COUNT_B"
printf "  %-38s %d\n" "Documents checked:"                    "$TOTAL_CHECKED"
printf "  %-38s %d\n" "Documents missing from ${INDEX_B}:"   "$MISSING_COUNT"

if (( MISSING_COUNT > 0 && TOTAL_CHECKED > 0 )); then
    PCT=$(( MISSING_COUNT * 100 / TOTAL_CHECKED ))
    printf "  %-38s ~%d%%\n" "Missing rate:" "$PCT"
fi
if (( PAGE > 0 )); then
    printf "  %-38s search: total %s, avg %s/page\n" \
        "Scan stats (${PAGE} pages):" \
        "$(format_ms $TOTAL_SEARCH_MS)" \
        "$(format_ms $(( TOTAL_SEARCH_MS / PAGE )))"
    printf "  %-38s mget:   total %s, avg %s/page\n" \
        "" \
        "$(format_ms $TOTAL_MGET_MS)" \
        "$(format_ms $(( TOTAL_MGET_MS / PAGE )))"
fi
printf "  %-38s %s\n" "Duration:" "$(format_duration $SECONDS)"

if (( MISSING_COUNT > 0 )); then
    echo ""
    info "Missing IDs written to: ${OUTPUT_PATH}"
    warn "Run ./reindex-missing.sh to reindex them from ${INDEX_A} into ${INDEX_B}."
else
    echo ""
    log "No missing documents found."
fi
