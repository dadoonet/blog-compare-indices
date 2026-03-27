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
#   --miss-rate <pct>      % of docs omitted from target (default: 5)
#   --source <name>        Source index name             (default: index-a)
#   --target <name>        Target index name             (default: index-b)
#   --bulk-batch-size <n>  Docs per bulk request         (default: 10000)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/lib/log.sh"   "init"
source "${SCRIPT_DIR}/lib/utils.sh"

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
        --source)          INDEX_A="$2";           shift 2 ;;
        --target)          INDEX_B="$2";           shift 2 ;;
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

# Per-batch timing (milliseconds)
BATCH_START_A=$(now_ms)
BATCH_START_B=$(now_ms)
TOTAL_BUILD=0;   BATCH_COUNT_BUILD=0
TOTAL_WRITE_A=0; TOTAL_INDEX_A=0; BATCH_COUNT_A=0
TOTAL_WRITE_B=0; TOTAL_INDEX_B=0; BATCH_COUNT_B=0

# In-place three-line progress (TTY only).
# _PROG_D / _PROG_A / _PROG_B hold the current status string for each line.
# _prog_draw prints all three lines; on subsequent calls it moves the cursor up and rewrites.
_PROG_INIT=0
_PROG_D="building first batch..."
_PROG_A="waiting..."
_PROG_B="waiting..."

_prog_draw() {
    if (( _PROG_INIT == 0 )); then
        printf "  → %-12s %s\n" "dataset:" "$_PROG_D"
        printf "  → %-12s %s\n" "${INDEX_A}:" "$_PROG_A"
        printf "  → %-12s %s\n" "${INDEX_B}:" "$_PROG_B"
        _PROG_INIT=1
    else
        printf "\033[3A\r"
        printf "\033[2K  → %-12s %s\n" "dataset:" "$_PROG_D"
        printf "\033[2K  → %-12s %s\n" "${INDEX_A}:" "$_PROG_A"
        printf "\033[2K  → %-12s %s\n" "${INDEX_B}:" "$_PROG_B"
    fi
}

_show_d() {
    if (( IS_TTY )); then _PROG_D="$1"; _prog_draw
    else info "dataset: $1"; fi
}

_show_a() {
    if (( IS_TTY )); then _PROG_A="$1"; _prog_draw
    else info "${INDEX_A}: $1"; fi
}

_show_b() {
    if (( IS_TTY )); then _PROG_B="$1"; _prog_draw
    else info "${INDEX_B}: $1"; fi
}

# Open file descriptors once for the entire loop.
# Writing via >&3 / >&4 avoids reopening the file on every append (>>),
# which was causing most of the I/O overhead.
exec 3>"$BULK_A"
exec 4>"$BULK_B"

(( IS_TTY )) && _prog_draw   # print initial progress lines before the loop starts

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
        BUILD_TIME=$(( $(now_ms) - BATCH_START_A ))
        WRITE_START=$(now_ms); exec 3>&-
        WRITE_TIME=$(( $(now_ms) - WRITE_START ))
        INDEX_START=$(now_ms)
        flush_bulk "$BULK_A"
        INDEX_TIME=$(( $(now_ms) - INDEX_START ))
        _show_d "$(progress_bar $COUNT_A $NUM_DOCS) - ⏳ $(format_ms $BUILD_TIME)"
        _show_a "$(progress_bar $COUNT_A $NUM_DOCS) - ⏳ $(format_ms $INDEX_TIME)"
        TOTAL_BUILD=$(( TOTAL_BUILD + BUILD_TIME ))
        BATCH_COUNT_BUILD=$(( BATCH_COUNT_BUILD + 1 ))
        TOTAL_WRITE_A=$(( TOTAL_WRITE_A + WRITE_TIME ))
        TOTAL_INDEX_A=$(( TOTAL_INDEX_A + INDEX_TIME ))
        BATCH_COUNT_A=$(( BATCH_COUNT_A + 1 ))
        exec 3>"$BULK_A"
        BATCH_A=0
        BATCH_START_A=$(now_ms)
    fi
    if (( BATCH_B >= BULK_BATCH_SIZE )); then
        WRITE_START=$(now_ms); exec 4>&-
        WRITE_TIME=$(( $(now_ms) - WRITE_START ))
        INDEX_START=$(now_ms)
        flush_bulk "$BULK_B"
        INDEX_TIME=$(( $(now_ms) - INDEX_START ))
        _show_b "$(progress_bar $(( COUNT_B + COUNT_SKIP )) $NUM_DOCS) - ⏳ $(format_ms $INDEX_TIME)"
        TOTAL_WRITE_B=$(( TOTAL_WRITE_B + WRITE_TIME ))
        TOTAL_INDEX_B=$(( TOTAL_INDEX_B + INDEX_TIME ))
        BATCH_COUNT_B=$(( BATCH_COUNT_B + 1 ))
        exec 4>"$BULK_B"
        BATCH_B=0
        BATCH_START_B=$(now_ms)
    fi
done

# Flush any remaining documents in the buffers.
# The exec close is done here (not before) so we can measure the write time properly.
if (( BATCH_A > 0 )); then
    BUILD_TIME=$(( $(now_ms) - BATCH_START_A ))
    WRITE_START=$(now_ms); exec 3>&-
    WRITE_TIME=$(( $(now_ms) - WRITE_START ))
    INDEX_START=$(now_ms)
    flush_bulk "$BULK_A"
    INDEX_TIME=$(( $(now_ms) - INDEX_START ))
    _show_d "$(progress_bar $COUNT_A $NUM_DOCS) - ⏳ $(format_ms $BUILD_TIME)"
    _show_a "$(progress_bar $COUNT_A $NUM_DOCS) - ⏳ $(format_ms $INDEX_TIME)"
    TOTAL_BUILD=$(( TOTAL_BUILD + BUILD_TIME ))
    BATCH_COUNT_BUILD=$(( BATCH_COUNT_BUILD + 1 ))
    TOTAL_WRITE_A=$(( TOTAL_WRITE_A + WRITE_TIME ))
    TOTAL_INDEX_A=$(( TOTAL_INDEX_A + INDEX_TIME ))
    BATCH_COUNT_A=$(( BATCH_COUNT_A + 1 ))
else
    exec 3>&-
fi
if (( BATCH_B > 0 )); then
    WRITE_START=$(now_ms); exec 4>&-
    WRITE_TIME=$(( $(now_ms) - WRITE_START ))
    INDEX_START=$(now_ms)
    flush_bulk "$BULK_B"
    INDEX_TIME=$(( $(now_ms) - INDEX_START ))
    _show_b "$(progress_bar $(( COUNT_B + COUNT_SKIP )) $NUM_DOCS) - ⏳ $(format_ms $INDEX_TIME)"
    TOTAL_WRITE_B=$(( TOTAL_WRITE_B + WRITE_TIME ))
    TOTAL_INDEX_B=$(( TOTAL_INDEX_B + INDEX_TIME ))
    BATCH_COUNT_B=$(( BATCH_COUNT_B + 1 ))
else
    exec 4>&-
fi

rm -rf "$TMPDIR_BULK"

# ── Summary ───────────────────────────────────────────────────────────────────
(( IS_TTY && _PROG_INIT )) && printf "\033[3A\r\033[J" || echo ""
log "Indexing complete."
printf "  %-36s %d\n"   "Documents generated:"             "$NUM_DOCS"
printf "  %-36s %d\n"   "Indexed into ${INDEX_A}:"         "$COUNT_A"
printf "  %-36s %d\n"   "Indexed into ${INDEX_B}:"         "$COUNT_B"
printf "  %-36s %d\n"   "Skipped from ${INDEX_B}:"         "$COUNT_SKIP"

if (( NUM_DOCS > 0 )); then
    PCT=$(( COUNT_SKIP * 100 / NUM_DOCS ))
    printf "  %-36s ~%d%%\n" "Effective miss rate:" "$PCT"
fi

if (( BATCH_COUNT_BUILD > 0 )); then
    printf "  %-36s build: total %s, avg %s/batch\n" \
        "generate docs (${BATCH_COUNT_BUILD} batches):" \
        "$(format_ms $TOTAL_BUILD)" "$(format_ms $(( TOTAL_BUILD / BATCH_COUNT_BUILD )))"
fi
if (( BATCH_COUNT_A > 0 )); then
    printf "  %-36s write: total %s, avg %s/batch\n" \
        "${INDEX_A} (${BATCH_COUNT_A} batches):" \
        "$(format_ms $TOTAL_WRITE_A)" "$(format_ms $(( TOTAL_WRITE_A / BATCH_COUNT_A )))"
    printf "  %-36s index: total %s, avg %s/batch\n" "" \
        "$(format_ms $TOTAL_INDEX_A)" "$(format_ms $(( TOTAL_INDEX_A / BATCH_COUNT_A )))"
fi
if (( BATCH_COUNT_B > 0 )); then
    printf "  %-36s write: total %s, avg %s/batch\n" \
        "${INDEX_B} (${BATCH_COUNT_B} batches):" \
        "$(format_ms $TOTAL_WRITE_B)" "$(format_ms $(( TOTAL_WRITE_B / BATCH_COUNT_B )))"
    printf "  %-36s index: total %s, avg %s/batch\n" "" \
        "$(format_ms $TOTAL_INDEX_B)" "$(format_ms $(( TOTAL_INDEX_B / BATCH_COUNT_B )))"
fi
printf "  %-36s %s\n" "Duration:" "$(format_duration $SECONDS)"

# ── Snapshot: copy target index so it can be restored cheaply ─────────────────
# Saves a clean copy of INDEX_B as "index-target". Use ./copy-index.sh to
# restore it before re-running reindex-missing.sh without regenerating the dataset.
echo ""
log "Saving snapshot of ${INDEX_B} → index-target..."
"${SCRIPT_DIR}/copy-index.sh" --source "$INDEX_B" --target "index-target"

echo ""
log "Run ./compare-indices.sh to find the missing document IDs."
log "Run ./copy-index.sh to restore ${INDEX_B} from the snapshot before re-testing."
