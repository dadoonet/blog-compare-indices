#!/usr/bin/env bash
# init-dataset.sh — Delete and recreate index-a and index-b with sample data.
#
# index-a receives every generated document.
# index-b receives the same documents except for a randomly distributed subset,
# controlled by MISS_RATE. Documents are skipped individually (not in blocks)
# so that the distribution is uniform across the index.
#
# Document IDs are built as {emp_no}_{counter} (e.g. "10010_1", "10010_365").
# This makes _id functional (tied to the employee) while remaining globally unique.
# Each document's first_name is suffixed with the counter (e.g. "Alice156") and
# birth_date is replaced with a random date in [1960, 2000] so that
# (first_name, last_name, birth_date) forms a globally unique business key.
#
# Bulk files are cached in dataset/ and reused if the document count and _id
# format match. Delete dataset/bulk-*.ndjson to force full regeneration.
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
MAPPING_FILE="${SCRIPT_DIR}/mapping.json"

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

[ -f "$ESCLI_BIN" ]    || die "escli not found at ${ESCLI_BIN}. Run ./setup.sh first."
[ -f "$MAPPING_FILE" ] || die "Mapping file not found: ${MAPPING_FILE}."

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

# Create an index with the shared mapping (mapping.json)
create_index() {
    local index="$1"
    "$ESCLI_BIN" indices create "$index" --input "$MAPPING_FILE" &>/dev/null
    info "Created index with mapping: ${index}"
}

# ── Load employee dataset into memory ─────────────────────────────────────────
# Four parallel arrays are built from a single pass over the dataset file:
#   EMPLOYEES[]        — full JSON document per employee
#   EMP_NOS[]          — emp_no value (used as _id prefix)
#   ORIG_BIRTH_DATES[] — original birth_date string (replaced at generation time)
#   ORIG_FIRST_NAMES[] — original first_name string (counter suffix appended at generation time)
# All four share the same index so IDX safely addresses all arrays.
DATASET_FILE="${SCRIPT_DIR}/dataset/employees.ndjson"
[ -f "$DATASET_FILE" ] || die "Dataset not found: ${DATASET_FILE}. Run ./setup.sh first."
EMPLOYEES=()
EMP_NOS=()
ORIG_BIRTH_DATES=()
ORIG_FIRST_NAMES=()
if (( BASH_VERSINFO[0] >= 4 )); then
    mapfile -t EMPLOYEES        < "$DATASET_FILE"
    mapfile -t EMP_NOS          < <(jq -r '.emp_no'          "$DATASET_FILE")
    mapfile -t ORIG_BIRTH_DATES < <(jq -r '.birth_date'      "$DATASET_FILE")
    mapfile -t ORIG_FIRST_NAMES < <(jq -r '.first_name // ""' "$DATASET_FILE")
else
    # mapfile is bash 4+; fall back to a read loop for bash 3.x (macOS system bash)
    while IFS= read -r line; do EMPLOYEES+=("$line"); done < "$DATASET_FILE"
    while IFS= read -r v; do EMP_NOS+=("$v");          done < <(jq -r '.emp_no'          "$DATASET_FILE")
    while IFS= read -r v; do ORIG_BIRTH_DATES+=("$v"); done < <(jq -r '.birth_date'      "$DATASET_FILE")
    while IFS= read -r v; do ORIG_FIRST_NAMES+=("$v"); done < <(jq -r '.first_name // ""' "$DATASET_FILE")
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

# ── Single-line in-place progress (used during sequential indexing) ────────────
_INLINE_INIT=0
_show_inline() {
    if (( IS_TTY )); then
        if (( _INLINE_INIT == 0 )); then
            printf "  → %s" "$1"; _INLINE_INIT=1
        else
            printf "\r\033[K  → %s" "$1"
        fi
    else info "$1"; fi
}

# ── 1. Check cache validity ────────────────────────────────────────────────────
# Four conditions must hold:
#   - both cache files exist
#   - correct line count
#   - new _id format (emp_no_i, not id-NNNNNN)
#   - first_name has a numeric counter suffix (e.g. "Alice1")
#   - dataset hash matches (guards against employees.ndjson being replaced/cleaned)
DATASET_HASH_FILE="${DATASET_DIR}/.employees-hash"
CURRENT_HASH=$(md5 -q "$DATASET_FILE" 2>/dev/null || md5sum "$DATASET_FILE" | cut -d' ' -f1)
CACHED_HASH=""
[ -f "$DATASET_HASH_FILE" ] && CACHED_HASH=$(cat "$DATASET_HASH_FILE")

NEED_GENERATE=true
if [[ -f "$BULK_A_CACHE" && -f "$BULK_B_CACHE" ]]; then
    ACTUAL_LINES=$(wc -l < "$BULK_A_CACHE" | tr -d ' ')
    { read -r _CACHE_FIRST_LINE; read -r _CACHE_DOC_LINE; } < "$BULK_A_CACHE"
    if (( ACTUAL_LINES == EXPECTED_LINES_A )) && \
       [[ "$_CACHE_FIRST_LINE" != *'"_id":"id-'* ]] && \
       [[ "$_CACHE_DOC_LINE" =~ \"first_name\":\"[^\"]+[0-9]+\" ]] && \
       [[ "$CURRENT_HASH" == "$CACHED_HASH" ]]; then
        COUNT_A=$NUM_DOCS
        COUNT_B=$(( $(wc -l < "$BULK_B_CACHE" | tr -d ' ') / 2 ))
        COUNT_SKIP=$(( NUM_DOCS - COUNT_B ))
        log "Cache valid — ${NUM_DOCS} docs, skipping generation."
        NEED_GENERATE=false
    elif [[ "$CURRENT_HASH" != "$CACHED_HASH" ]]; then
        warn "Dataset has changed (employees.ndjson hash mismatch) — regenerating..."
    elif [[ "$_CACHE_FIRST_LINE" == *'"_id":"id-'* ]]; then
        warn "Cache uses old _id format (id-NNNNNN) — regenerating..."
    elif [[ ! "$_CACHE_DOC_LINE" =~ \"first_name\":\"[^\"]+[0-9]+\" ]]; then
        warn "Cache uses old first_name format (no counter suffix) — regenerating..."
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
    _PROG_D="generating..."
    (( IS_TTY )) && _prog_draw   # start the 3-line block only during generation

    # Open FDs once — avoids reopening the file on every iteration
    exec 3>"$BULK_A_CACHE"
    exec 4>"$BULK_B_CACHE"

    # for (( )) avoids the $(seq ...) subshell
    for (( i=1; i<=NUM_DOCS; i++ )); do

        # Pick a random employee from the in-memory arrays (all three share the same index).
        IDX=$(( RANDOM % EMPLOYEE_COUNT ))
        DOC="${EMPLOYEES[$IDX]}"

        # _id = emp_no + global counter → unique and traceable back to the source employee.
        printf -v HEADER '{"index":{"_id":"%s_%d"}}' "${EMP_NOS[$IDX]}" "$i"

        # Suffix first_name with the global counter to guarantee a unique business key.
        # bash literal substitution (no subshell) keeps the loop O(1) per iteration.
        # Employees with a null first_name get "Employee<i>" as a placeholder.
        ORIG_FN="${ORIG_FIRST_NAMES[$IDX]}"
        if [[ -n "$ORIG_FN" ]]; then
            DOC="${DOC/"\"first_name\":\"${ORIG_FN}\""/"\"first_name\":\"${ORIG_FN}${i}\""}"
        else
            DOC="${DOC/"\"first_name\":null"/"\"first_name\":\"Employee${i}\""}"
        fi

        # Replace the employee's original birth_date with a random date [1960, 2000].
        YEAR=$(( 1960 + RANDOM % 41 ))
        MONTH=$(( 1 + RANDOM % 12 ))
        DAY=$(( 1 + RANDOM % 28 ))
        printf -v NEW_BD '%04d-%02d-%02d' "$YEAR" "$MONTH" "$DAY"
        DOC="${DOC/"${ORIG_BIRTH_DATES[$IDX]}"/"$NEW_BD"}"

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

    # Persist the dataset hash so future runs can detect employees.ndjson changes
    echo "$CURRENT_HASH" > "$DATASET_HASH_FILE"

    GEN_MS=$(( $(now_ms) - GEN_START ))
    _show_d "$(progress_bar $NUM_DOCS $NUM_DOCS) - ✓ generated in $(format_ms $GEN_MS)"

    # Erase the progress block so subsequent log lines appear cleanly
    (( IS_TTY && _PROG_INIT )) && printf "\033[3A\r\033[J"
    _PROG_INIT=0
    _PROG_A="waiting..."
    _PROG_B="waiting..."
    log "Generated ${NUM_DOCS} documents in $(format_ms $GEN_MS)."
fi

# ── 3. Delete and recreate indices with explicit mapping ───────────────────────
log "Deleting existing indices..."
delete_index "$INDEX_A"
delete_index "$INDEX_B"
log "Creating indices with mapping..."
create_index "$INDEX_A"
create_index "$INDEX_B"

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
log "Indexing ${COUNT_A} docs into ${INDEX_A}..."
_INLINE_INIT=0
T_A=$(now_ms)
_load_with_progress "$COUNT_A" _show_inline \
    "${ESCLI_BIN}" utils load --index "$INDEX_A" --size "$BULK_BATCH_SIZE" "$BULK_A_CACHE"
TOTAL_INDEX_A=$(( $(now_ms) - T_A ))
(( IS_TTY && _INLINE_INIT )) && printf "\r\033[K"
log "Indexing complete. (${COUNT_A} docs in $(format_ms $TOTAL_INDEX_A))"

# ── index-b ────────────────────────────────────────────────────────────────────
log "Indexing ${COUNT_B} docs into ${INDEX_B}..."
_INLINE_INIT=0
T_B=$(now_ms)
_load_with_progress "$COUNT_B" _show_inline \
    "${ESCLI_BIN}" utils load --index "$INDEX_B" --size "$BULK_BATCH_SIZE" "$BULK_B_CACHE"
TOTAL_INDEX_B=$(( $(now_ms) - T_B ))
(( IS_TTY && _INLINE_INIT )) && printf "\r\033[K"
log "Indexing complete. (${COUNT_B} docs in $(format_ms $TOTAL_INDEX_B))"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
log "Summary."
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
