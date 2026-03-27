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
# Bulk files are cached in dataset/ and reused if the document count matches.
# Delete dataset/bulk-*.ndjson to force regeneration.
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
            grep '^# ' "$0" | head -18 | sed 's/^# \?//'
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

# ── Load employee dataset into memory ─────────────────────────────────────────
DATASET_FILE="${SCRIPT_DIR}/dataset/employees.ndjson"
[ -f "$DATASET_FILE" ] || die "Dataset not found: ${DATASET_FILE}. Run ./setup.sh first."
EMPLOYEES=()
if (( BASH_VERSINFO[0] >= 4 )); then
    mapfile -t EMPLOYEES < "$DATASET_FILE"
else
    # mapfile is bash 4+; fall back to a read loop for bash 3.x (macOS system bash)
    while IFS= read -r line; do EMPLOYEES+=("$line"); done < "$DATASET_FILE"
fi
EMPLOYEE_COUNT=${#EMPLOYEES[@]}
info "Loaded ${EMPLOYEE_COUNT} employee records from $(basename "$DATASET_FILE")"

# ── Cache file paths ───────────────────────────────────────────────────────────
DATASET_DIR="${SCRIPT_DIR}/dataset"
BULK_A_CACHE="${DATASET_DIR}/bulk-${INDEX_A}.ndjson"
BULK_B_CACHE="${DATASET_DIR}/bulk-${INDEX_B}.ndjson"
EXPECTED_LINES_A=$(( NUM_DOCS * 2 ))

# ── Counters ───────────────────────────────────────────────────────────────────
COUNT_A=0
COUNT_B=0
COUNT_SKIP=0

# ── In-place three-line progress (TTY only) ────────────────────────────────────
# _PROG_D / _PROG_A / _PROG_B hold the current status string for each line.
# _prog_draw prints all three lines; on subsequent calls it moves the cursor up and rewrites.
_PROG_INIT=0
_PROG_D="..."
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

(( IS_TTY )) && _prog_draw   # print initial progress lines

# ── 1. Check cache validity ────────────────────────────────────────────────────
NEED_GENERATE=true
if [[ -f "$BULK_A_CACHE" && -f "$BULK_B_CACHE" ]]; then
    ACTUAL_LINES=$(wc -l < "$BULK_A_CACHE" | tr -d ' ')
    if (( ACTUAL_LINES == EXPECTED_LINES_A )); then
        COUNT_A=$NUM_DOCS
        COUNT_B=$(( $(wc -l < "$BULK_B_CACHE" | tr -d ' ') / 2 ))
        COUNT_SKIP=$(( NUM_DOCS - COUNT_B ))
        _show_d "cache valid — ${NUM_DOCS} docs, skipping generation"
        NEED_GENERATE=false
    else
        warn "Cache mismatch (expected ${EXPECTED_LINES_A} lines, got ${ACTUAL_LINES}). Regenerating..."
    fi
fi

# ── 2. Generate bulk files (if needed) ────────────────────────────────────────
# Writes dataset/bulk-{index-a,index-b}.ndjson without making any ES calls.
# The loop is intentionally lean: one array lookup + two printf writes per doc.
if $NEED_GENERATE; then
    log "Generating ${NUM_DOCS} documents (miss rate: ${MISS_RATE}%)..."

    GEN_START=$(now_ms)

    # Open FDs once — avoids reopening the file on every iteration
    exec 3>"$BULK_A_CACHE"
    exec 4>"$BULK_B_CACHE"

    # for (( )) avoids the $(seq ...) subshell
    for (( i=1; i<=NUM_DOCS; i++ )); do

        # Pick a random employee document from the in-memory array.
        # The bulk header is built once — it's identical for both indices.
        DOC="${EMPLOYEES[$(( RANDOM % EMPLOYEE_COUNT ))]}"
        printf -v HEADER '{"index":{"_id":"id-%06d"}}' "$i"

        printf '%s\n%s\n' "$HEADER" "$DOC" >&3
        COUNT_A=$(( COUNT_A + 1 ))

        # index-b: each document has a MISS_RATE % probability of being skipped.
        # (RANDOM % 100) yields values 0–99 uniformly; values below MISS_RATE trigger a skip.
        if (( RANDOM % 100 >= MISS_RATE )); then
            printf '%s\n%s\n' "$HEADER" "$DOC" >&4
            COUNT_B=$(( COUNT_B + 1 ))
        else
            COUNT_SKIP=$(( COUNT_SKIP + 1 ))
        fi

        # Refresh progress display every BULK_BATCH_SIZE docs
        if (( i % BULK_BATCH_SIZE == 0 )); then
            _GEN_MS=$(( $(now_ms) - GEN_START ))
            _show_d "$(progress_bar $i $NUM_DOCS) - ⏳ $(format_ms $_GEN_MS)"
        fi
    done

    exec 3>&-
    exec 4>&-

    GEN_MS=$(( $(now_ms) - GEN_START ))
    _show_d "$(progress_bar $NUM_DOCS $NUM_DOCS) - ✓ generated in $(format_ms $GEN_MS)"
fi

# ── 3. Delete existing indices ─────────────────────────────────────────────────
log "Deleting existing indices..."
delete_index "$INDEX_A"
delete_index "$INDEX_B"

# ── 4. Index from cache files ──────────────────────────────────────────────────
# escli utils load handles batching internally (--size docs per bulk request).
# Note: this requires the native binary since it takes a file path argument.
# When the Docker image gains stdin support, this could become:
#   ./escli utils load --index ${INDEX_A} --size "$BULK_BATCH_SIZE" < "$BULK_A_CACHE"

# Parse "Batch N: X indexed, Y errors" lines from escli utils load and update
# the progress display after each batch. Process substitution (< <(...)) keeps
# the while loop in the current shell so _show_* variables remain accessible.
# stderr is merged into stdout (2>&1) because escli writes batch lines to stderr.
_load_with_progress() {
    local total="$1" show_fn="$2"; shift 2
    local batch_num=0 docs_done=0 elapsed=0 errors=0 last_t now
    last_t=$(now_ms)

    while IFS= read -r line; do
        if [[ "$line" =~ ^Batch\ ([0-9]+):\ ([0-9]+)\ indexed,\ ([0-9]+)\ errors ]]; then
            batch_num="${BASH_REMATCH[1]}"
            errors="${BASH_REMATCH[3]}"
            (( errors > 0 )) && die "Batch ${batch_num}: ${errors} bulk error(s)"
            now=$(now_ms)
            elapsed=$(( now - last_t )); last_t=$now
            docs_done=$(( batch_num * BULK_BATCH_SIZE ))
            (( docs_done > total )) && docs_done=$total
            "$show_fn" "$(progress_bar $docs_done $total) - ⏳ $(format_ms $elapsed)"
        fi
    done < <("$@" 2>&1)
}

# ── index-a ────────────────────────────────────────────────────────────────────
log "Indexing ${COUNT_A} documents into ${INDEX_A}..."
T_A=$(now_ms)
_load_with_progress "$COUNT_A" _show_a \
    "${ESCLI_BIN}" utils load --index "$INDEX_A" --size "$BULK_BATCH_SIZE" "$BULK_A_CACHE"
TOTAL_INDEX_A=$(( $(now_ms) - T_A ))
_show_a "✅ ${COUNT_A} docs in $(format_ms $TOTAL_INDEX_A)"

# ── index-b ────────────────────────────────────────────────────────────────────
log "Indexing ${COUNT_B} documents into ${INDEX_B}..."
T_B=$(now_ms)
_load_with_progress "$COUNT_B" _show_b \
    "${ESCLI_BIN}" utils load --index "$INDEX_B" --size "$BULK_BATCH_SIZE" "$BULK_B_CACHE"
TOTAL_INDEX_B=$(( $(now_ms) - T_B ))
_show_b "✅ ${COUNT_B} docs in $(format_ms $TOTAL_INDEX_B)"

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

printf "  %-36s %s\n" "Indexed ${INDEX_A} in:"             "$(format_ms $TOTAL_INDEX_A)"
printf "  %-36s %s\n" "Indexed ${INDEX_B} in:"             "$(format_ms $TOTAL_INDEX_B)"
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
