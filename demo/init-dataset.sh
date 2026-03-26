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
#   --num-docs <n>         Documents to generate         (default: 1000000)
#   --miss-rate <pct>      % of docs omitted from index-b (default: 5)
#   --index-a <name>       Source index name             (default: index-a)
#   --index-b <name>       Target index name             (default: index-b)
#   --bulk-batch-size <n>  Docs per bulk request         (default: 10000)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log()  { echo -e "${GREEN}[init]${NC} $*"; }
info() { echo -e "${BLUE}  →${NC} $*"; }
warn() { echo -e "${YELLOW}[init]${NC} $*"; }
die()  { echo -e "${RED}[init] ERROR:${NC} $*" >&2; exit 1; }

format_duration() {
    local s=$1
    local h=$(( s / 3600 )) m=$(( (s % 3600) / 60 )) sec=$(( s % 60 ))
    (( h > 0 ))  && printf "%dh %02dm %02ds" $h $m $sec && return
    (( m > 0 ))  && printf "%dm %02ds" $m $sec          && return
    printf "%ds" $sec
}

SECONDS=0   # bash built-in: counts elapsed seconds automatically

ESCLI_BIN="${SCRIPT_DIR}/escli"

# ── Load defaults from .env, then allow CLI overrides ─────────────────────────
set -a && set +u && source "${SCRIPT_DIR}/.env.sh" && set -u && set +a

: "${INDEX_A:=index-a}"
: "${INDEX_B:=index-b}"
: "${NUM_DOCS:=1000000}"
: "${MISS_RATE:=5}"
: "${BULK_BATCH_SIZE:=10000}"

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
    if "$ESCLI_BIN" indices delete "$index" &>/dev/null; then
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

# Per-batch timing (seconds)
BATCH_START_A=$SECONDS
BATCH_START_B=$SECONDS
TOTAL_BUILD_A=0; TOTAL_INDEX_A=0; BATCH_COUNT_A=0
TOTAL_BUILD_B=0; TOTAL_INDEX_B=0; BATCH_COUNT_B=0

# Open file descriptors once for the entire loop.
# Writing via >&3 / >&4 avoids reopening the file on every append (>>),
# which was causing most of the I/O overhead.
exec 3>"$BULK_A"
exec 4>"$BULK_B"

# for (( )) avoids the $(seq ...) subshell
for (( i=1; i<=NUM_DOCS; i++ )); do

    # printf -v writes directly into the variable — no fork, unlike $(printf ...)
    printf -v DOC_ID "id-%06d" "$i"

    VALUE=$(( RANDOM % 1000 ))
    CATEGORY=${CATEGORIES[$(( RANDOM % ${#CATEGORIES[@]} ))]}
    STATUS=${STATUSES[$(( RANDOM % ${#STATUSES[@]} ))]}
    TAG1=${TAGS[$(( RANDOM % ${#TAGS[@]} ))]}
    TAG2=${TAGS[$(( RANDOM % ${#TAGS[@]} ))]}
    SCORE="${RANDOM: -2}.${RANDOM: -2}"
    MONTH=$(( ((i - 1) / 28 % 12) + 1 ))
    DAY=$(( ((i - 1) % 28) + 1 ))
    printf -v CREATED_AT "2024-%02d-%02dT00:00:00Z" "$MONTH" "$DAY"

    printf -v DOC \
        '{"title":"Document %s","category":"%s","status":"%s","value":%d,"score":"%s","tags":["%s","%s"],"created_at":"%s"}' \
        "$DOC_ID" "$CATEGORY" "$STATUS" "$VALUE" "$SCORE" "$TAG1" "$TAG2" "$CREATED_AT"

    # index-a always receives every document
    printf '{"index":{"_index":"%s","_id":"%s"}}\n%s\n' "$INDEX_A" "$DOC_ID" "$DOC" >&3
    COUNT_A=$(( COUNT_A + 1 ))
    BATCH_A=$(( BATCH_A + 1 ))

    # index-b: each document has a MISS_RATE % probability of being skipped.
    # (RANDOM % 100) yields values 0–99 uniformly; values below MISS_RATE trigger a skip.
    # This ensures misses are scattered uniformly, not grouped in a block.
    if (( RANDOM % 100 >= MISS_RATE )); then
        printf '{"index":{"_index":"%s","_id":"%s"}}\n%s\n' "$INDEX_B" "$DOC_ID" "$DOC" >&4
        COUNT_B=$(( COUNT_B + 1 ))
        BATCH_B=$(( BATCH_B + 1 ))
    else
        COUNT_SKIP=$(( COUNT_SKIP + 1 ))
    fi

    # Flush when either batch buffer is full.
    # The FD must be closed before flushing (so the OS flushes its buffer to disk)
    # and reopened afterwards for the next batch.
    if (( BATCH_A >= BULK_BATCH_SIZE )); then
        exec 3>&-
        BUILD_TIME=$(( SECONDS - BATCH_START_A ))
        INDEX_START=$SECONDS
        flush_bulk "$BULK_A"
        INDEX_TIME=$(( SECONDS - INDEX_START ))
        info "Indexing batch into ${INDEX_A} (${COUNT_A} docs so far) - ⏳ ${BUILD_TIME}s to build, ${INDEX_TIME}s to index"
        TOTAL_BUILD_A=$(( TOTAL_BUILD_A + BUILD_TIME ))
        TOTAL_INDEX_A=$(( TOTAL_INDEX_A + INDEX_TIME ))
        BATCH_COUNT_A=$(( BATCH_COUNT_A + 1 ))
        exec 3>"$BULK_A"
        BATCH_A=0
        BATCH_START_A=$SECONDS
    fi
    if (( BATCH_B >= BULK_BATCH_SIZE )); then
        exec 4>&-
        BUILD_TIME=$(( SECONDS - BATCH_START_B ))
        INDEX_START=$SECONDS
        flush_bulk "$BULK_B"
        INDEX_TIME=$(( SECONDS - INDEX_START ))
        info "Indexing batch into ${INDEX_B} (${COUNT_B} docs so far) - ⏳ ${BUILD_TIME}s to build, ${INDEX_TIME}s to index"
        TOTAL_BUILD_B=$(( TOTAL_BUILD_B + BUILD_TIME ))
        TOTAL_INDEX_B=$(( TOTAL_INDEX_B + INDEX_TIME ))
        BATCH_COUNT_B=$(( BATCH_COUNT_B + 1 ))
        exec 4>"$BULK_B"
        BATCH_B=0
        BATCH_START_B=$SECONDS
    fi
done

# Close file descriptors before flushing the final batches
exec 3>&-
exec 4>&-

# Flush any remaining documents in the buffers
if (( BATCH_A > 0 )); then
    BUILD_TIME=$(( SECONDS - BATCH_START_A ))
    INDEX_START=$SECONDS
    flush_bulk "$BULK_A"
    INDEX_TIME=$(( SECONDS - INDEX_START ))
    info "Indexing final batch into ${INDEX_A} (${COUNT_A} docs so far) - ⏳ ${BUILD_TIME}s to build, ${INDEX_TIME}s to index"
    TOTAL_BUILD_A=$(( TOTAL_BUILD_A + BUILD_TIME ))
    TOTAL_INDEX_A=$(( TOTAL_INDEX_A + INDEX_TIME ))
    BATCH_COUNT_A=$(( BATCH_COUNT_A + 1 ))
fi
if (( BATCH_B > 0 )); then
    BUILD_TIME=$(( SECONDS - BATCH_START_B ))
    INDEX_START=$SECONDS
    flush_bulk "$BULK_B"
    INDEX_TIME=$(( SECONDS - INDEX_START ))
    info "Indexing final batch into ${INDEX_B} (${COUNT_B} docs so far) - ⏳ ${BUILD_TIME}s to build, ${INDEX_TIME}s to index"
    TOTAL_BUILD_B=$(( TOTAL_BUILD_B + BUILD_TIME ))
    TOTAL_INDEX_B=$(( TOTAL_INDEX_B + INDEX_TIME ))
    BATCH_COUNT_B=$(( BATCH_COUNT_B + 1 ))
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
    PCT=$(( COUNT_SKIP * 100 / NUM_DOCS ))
    printf "  %-36s ~%d%%\n" "Effective miss rate:" "$PCT"
fi

if (( BATCH_COUNT_A > 0 )); then
    printf "  %-36s avg %ds build, %ds index\n" \
        "${INDEX_A} batch stats (${BATCH_COUNT_A} batches):" \
        "$(( TOTAL_BUILD_A / BATCH_COUNT_A ))" \
        "$(( TOTAL_INDEX_A / BATCH_COUNT_A ))"
fi
if (( BATCH_COUNT_B > 0 )); then
    printf "  %-36s avg %ds build, %ds index\n" \
        "${INDEX_B} batch stats (${BATCH_COUNT_B} batches):" \
        "$(( TOTAL_BUILD_B / BATCH_COUNT_B ))" \
        "$(( TOTAL_INDEX_B / BATCH_COUNT_B ))"
fi
printf "  %-36s %s\n" "Duration:" "$(format_duration $SECONDS)"

echo ""
log "Run ./compare-indices.sh to find the missing document IDs."
