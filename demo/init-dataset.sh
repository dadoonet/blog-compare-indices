#!/usr/bin/env bash
# init-dataset.sh — Delete and recreate index-a and index-b with sample data.
#
# index-a receives every generated document.
# index-b receives the same documents except for a randomly distributed subset,
# controlled by MISS_RATE. Documents are skipped individually (not in blocks)
# so that the distribution is uniform across the index.
#
# Document IDs use a zero-padded counter: id-000001, id-000002, …
# This makes them easy to sort, inspect, and validate manually.
#
# Usage: ./init-dataset.sh [options]
#   --num-docs <n>         Documents to generate         (default: 1000)
#   --miss-rate <pct>      % of docs omitted from index-b (default: 1)
#   --index-a <name>       Source index name             (default: index-a)
#   --index-b <name>       Target index name             (default: index-b)
#   --bulk-batch-size <n>  Docs per bulk request         (default: 500)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log()  { echo -e "${GREEN}[init]${NC} $*"; }
info() { echo -e "${BLUE}  →${NC} $*"; }
warn() { echo -e "${YELLOW}[init]${NC} $*"; }
die()  { echo -e "${RED}[init] ERROR:${NC} $*" >&2; exit 1; }

ESCLI_BIN="${SCRIPT_DIR}/escli"

# ── Load defaults from .env, then allow CLI overrides ─────────────────────────
set -a && set +u && source "${SCRIPT_DIR}/.env.sh" && set -u && set +a

: "${INDEX_A:=index-a}"
: "${INDEX_B:=index-b}"
: "${NUM_DOCS:=1000}"
: "${MISS_RATE:=1}"
: "${BULK_BATCH_SIZE:=500}"

while [[ $# -gt 0 ]]; do
    case $1 in
        --num-docs)        NUM_DOCS="$2";          shift 2 ;;
        --miss-rate)       MISS_RATE="$2";         shift 2 ;;
        --index-a)         INDEX_A="$2";           shift 2 ;;
        --index-b)         INDEX_B="$2";           shift 2 ;;
        --bulk-batch-size) BULK_BATCH_SIZE="$2";   shift 2 ;;
        -h|--help)
            grep '^# ' "$0" | head -15 | sed 's/^# \?//'
            exit 0 ;;
        *) die "Unknown option: $1" ;;
    esac
done

[ -f "$ESCLI_BIN" ] || die "escli not found at ${ESCLI_BIN}. Run ./setup.sh first."

# ── Helpers ───────────────────────────────────────────────────────────────────

# Delete an index; silently ignore the error if the index does not exist yet
delete_index() {
    local index="$1"
    if "$ESCLI_BIN" delete "$index" &>/dev/null; then
        info "Deleted existing index: ${index}"
    else
        info "Index ${index} did not exist — nothing to delete"
    fi
}

# Send a bulk NDJSON file to Elasticsearch and truncate the file afterwards.
# Aborts if the bulk response contains errors.
flush_bulk() {
    local file="$1"
    [ -s "$file" ] || return 0     # nothing to send

    local response
    # Pass the file via stdin — the escli Docker container cannot access
    # host file paths directly, but stdin is forwarded via the -i flag.
    response=$("${ESCLI_BIN}" bulk --input - < "$file")

    # Fail fast on bulk errors (partial failures are still reported as HTTP 200)
    if echo "$response" | grep -q '"errors":true'; then
        die "Bulk indexing errors detected: $(echo "$response" | grep -o '"reason":"[^"]*"' | head -3)"
    fi

    > "$file"    # truncate for the next batch
}

# ── Vocabulary for random document content ─────────────────────────────────────
CATEGORIES=("alpha" "beta" "gamma" "delta" "epsilon")
STATUSES=("active" "inactive" "pending" "archived")
TAGS=("important" "urgent" "review" "draft" "final" "legacy" "new" "experimental")

# ── 1. Delete existing indices ────────────────────────────────────────────────
log "Deleting existing indices..."
delete_index "$INDEX_A"
delete_index "$INDEX_B"

# ── 2. Generate and index documents ───────────────────────────────────────────
log "Generating ${NUM_DOCS} documents (miss rate: ${MISS_RATE}%)..."

TMPDIR_BULK=$(mktemp -d)
BULK_A="${TMPDIR_BULK}/bulk-a.ndjson"
BULK_B="${TMPDIR_BULK}/bulk-b.ndjson"
touch "$BULK_A" "$BULK_B"

# Counters
COUNT_A=0         # docs indexed into index-a
COUNT_B=0         # docs indexed into index-b
COUNT_SKIP=0      # docs omitted from index-b
BATCH_A=0         # docs buffered in current index-a batch
BATCH_B=0         # docs buffered in current index-b batch

for i in $(seq 1 "$NUM_DOCS"); do
    DOC_ID=$(printf "id-%06d" "$i")

    # Random field values
    VALUE=$(( RANDOM % 1000 ))
    CATEGORY=${CATEGORIES[$(( RANDOM % ${#CATEGORIES[@]} ))]}
    STATUS=${STATUSES[$(( RANDOM % ${#STATUSES[@]} ))]}
    TAG1=${TAGS[$(( RANDOM % ${#TAGS[@]} ))]}
    TAG2=${TAGS[$(( RANDOM % ${#TAGS[@]} ))]}
    # Simple decimal score without platform-specific bc
    SCORE="${RANDOM: -2}.${RANDOM: -2}"
    # Date derived from doc number — avoids platform-specific date arithmetic
    MONTH=$(( ((i - 1) / 28 % 12) + 1 ))
    DAY=$(( ((i - 1) % 28) + 1 ))
    CREATED_AT=$(printf "2024-%02d-%02dT00:00:00Z" "$MONTH" "$DAY")

    DOC=$(printf '{"title":"Document %s","category":"%s","status":"%s","value":%d,"score":"%s","tags":["%s","%s"],"created_at":"%s"}' \
        "$DOC_ID" "$CATEGORY" "$STATUS" "$VALUE" "$SCORE" "$TAG1" "$TAG2" "$CREATED_AT")

    # index-a always receives every document
    printf '{"index":{"_index":"%s","_id":"%s"}}\n%s\n' "$INDEX_A" "$DOC_ID" "$DOC" >> "$BULK_A"
    COUNT_A=$(( COUNT_A + 1 ))
    BATCH_A=$(( BATCH_A + 1 ))

    # index-b: each document has a MISS_RATE % probability of being skipped.
    # (RANDOM % 100) yields values 0–99 uniformly; values below MISS_RATE trigger a skip.
    # This ensures misses are scattered uniformly, not grouped in a block.
    if (( RANDOM % 100 >= MISS_RATE )); then
        printf '{"index":{"_index":"%s","_id":"%s"}}\n%s\n' "$INDEX_B" "$DOC_ID" "$DOC" >> "$BULK_B"
        COUNT_B=$(( COUNT_B + 1 ))
        BATCH_B=$(( BATCH_B + 1 ))
    else
        COUNT_SKIP=$(( COUNT_SKIP + 1 ))
    fi

    # Flush when either batch buffer is full
    if (( BATCH_A >= BULK_BATCH_SIZE )); then
        info "Indexing batch into ${INDEX_A} (${COUNT_A} docs so far)..."
        flush_bulk "$BULK_A"
        BATCH_A=0
    fi
    if (( BATCH_B >= BULK_BATCH_SIZE )); then
        info "Indexing batch into ${INDEX_B} (${COUNT_B} docs so far)..."
        flush_bulk "$BULK_B"
        BATCH_B=0
    fi
done

# Flush any remaining documents in the buffers
if (( BATCH_A > 0 )); then
    info "Indexing final batch into ${INDEX_A}..."
    flush_bulk "$BULK_A"
fi
if (( BATCH_B > 0 )); then
    info "Indexing final batch into ${INDEX_B}..."
    flush_bulk "$BULK_B"
fi

rm -rf "$TMPDIR_BULK"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
log "Indexing complete."
printf "  %-36s %d\n"   "Documents generated:"             "$NUM_DOCS"
printf "  %-36s %d\n"   "Indexed into ${INDEX_A}:"         "$COUNT_A"
printf "  %-36s %d\n"   "Indexed into ${INDEX_B}:"         "$COUNT_B"
printf "  %-36s %d\n"   "Skipped from ${INDEX_B}:"         "$COUNT_SKIP"

if (( NUM_DOCS > 0 )); then
    # Integer percentage (no bc dependency)
    PCT=$(( COUNT_SKIP * 100 / NUM_DOCS ))
    printf "  %-36s ~%d%%\n" "Effective miss rate:" "$PCT"
fi

echo ""
log "Run ./compare-indices.sh to find the missing document IDs."
