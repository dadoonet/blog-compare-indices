#!/usr/bin/env bash
# reindex-missing.sh — Re-index documents from index-a into index-b.
#
# Reads the list of missing document IDs produced by compare-indices.sh,
# fetches each document from index-a in batches using _mget, then reindexes
# them into index-b using the bulk API.
#
# Requirements: escli (./escli wrapper), jq
#
# Usage: ./reindex-missing.sh [options]
#   --source <name>     Source index                  (default: index-a)
#   --target <name>     Target index                  (default: index-b)
#   --input <file>      File with missing IDs         (default: missing-ids.txt)
#   --batch-size <n>    IDs per _mget / bulk call     (default: 10000)
#   --strategy <name>   Reindex strategy              (default: reindex)
#                         mgetbulk    _mget from index-a + bulk into index-b
#                         reindex     _reindex API with ids query, batched from input file (default)
#                         reindex-all _reindex API with no filter (full copy)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ESCLI="${SCRIPT_DIR}/escli"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log()  { echo -e "${GREEN}[reindex]${NC} $*"; }
info() { echo -e "${BLUE}  →${NC} $*"; }
warn() { echo -e "${YELLOW}[reindex]${NC} $*"; }
die()  { echo -e "${RED}[reindex] ERROR:${NC} $*" >&2; exit 1; }

format_duration() {
    local s=$1
    local h=$(( s / 3600 )) m=$(( (s % 3600) / 60 )) sec=$(( s % 60 ))
    (( h > 0 ))  && printf "%dh %02dm %02ds" $h $m $sec && return
    (( m > 0 ))  && printf "%dm %02ds" $m $sec          && return
    printf "%ds" $sec
}

now_ms() {
    if (( BASH_VERSINFO[0] >= 5 )); then
        local t="${EPOCHREALTIME/[.,]/}"
        echo "$(( t / 1000 ))"
    else
        perl -MTime::HiRes=time -e 'printf "%d\n", time()*1000'
    fi
}

format_ms() {
    local ms=$1
    if (( ms < 1000 )); then
        printf "%dms" "$ms"
    elif (( ms < 60000 )); then
        printf "%d.%03ds" "$(( ms / 1000 ))" "$(( ms % 1000 ))"
    else
        local s=$(( ms / 1000 ))
        printf "%dm %02d.%03ds" "$(( s / 60 ))" "$(( s % 60 ))" "$(( ms % 1000 ))"
    fi
}

IS_TTY=0; [ -t 1 ] && IS_TTY=1
_PROG_INIT=0

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
: "${OUTPUT_FILE:=missing-ids.txt}"
: "${BATCH_SIZE:=10000}"
: "${STRATEGY:=reindex}"

while [[ $# -gt 0 ]]; do
    case $1 in
        --source)     INDEX_A="$2";     shift 2 ;;
        --target)     INDEX_B="$2";     shift 2 ;;
        --input)      OUTPUT_FILE="$2"; shift 2 ;;
        --batch-size) BATCH_SIZE="$2";  shift 2 ;;
        --strategy)   STRATEGY="$2";    shift 2 ;;
        -h|--help)
            grep '^# ' "$0" | head -20 | sed 's/^# \?//'
            exit 0 ;;
        *) die "Unknown option: $1" ;;
    esac
done

case "$STRATEGY" in
    mgetbulk|reindex|reindex-all) ;;
    *) die "Unknown strategy: ${STRATEGY}. Valid values: mgetbulk, reindex, reindex-all" ;;
esac

command -v jq >/dev/null || die "jq is required (brew install jq / apt install jq)"
[ -f "$ESCLI" ]          || die "escli not found at ${ESCLI}. Run ./setup.sh first."

# ── strategy: reindex-all ─────────────────────────────────────────────────────
# Full copy of index-a into index-b via the _reindex API. No input file needed.
if [[ "$STRATEGY" == "reindex-all" ]]; then
    log "Strategy: reindex-all — copying all documents from ${INDEX_A} into ${INDEX_B}..."
    echo ""

    REINDEX_BODY=$(jq -n \
        --arg src "$INDEX_A" \
        --arg dst "$INDEX_B" \
        '{"source":{"index":$src},"dest":{"index":$dst}}')

    REINDEX_RESPONSE=$("$ESCLI" reindex <<< "$REINDEX_BODY")

    TOTAL=$(echo "$REINDEX_RESPONSE"  | jq '.total')
    CREATED=$(echo "$REINDEX_RESPONSE" | jq '.created')
    UPDATED=$(echo "$REINDEX_RESPONSE" | jq '.updated')
    FAILURES=$(echo "$REINDEX_RESPONSE" | jq '.failures | length')

    echo ""
    log "Re-indexing complete."
    printf "  %-38s %d\n" "Documents processed:"                "$TOTAL"
    printf "  %-38s %d\n" "Created in ${INDEX_B}:"             "$CREATED"
    printf "  %-38s %d\n" "Updated in ${INDEX_B}:"             "$UPDATED"
    if (( FAILURES > 0 )); then
        printf "  %-38s %d\n" "Failures:" "$FAILURES"
        warn "Some documents failed to reindex."
    fi
    printf "  %-38s %s\n" "Duration:" "$(format_duration $SECONDS)"
    exit 0
fi

# ── strategies that require an input file ─────────────────────────────────────
INPUT_FILE="${SCRIPT_DIR}/${OUTPUT_FILE}"
[ -f "$INPUT_FILE" ]     || die "Input file not found: ${INPUT_FILE}. Run ./compare-indices.sh first."

TOTAL_IDS=$(wc -l < "$INPUT_FILE" | tr -d ' ')
[ "$TOTAL_IDS" -eq 0 ]   && { log "Input file is empty — nothing to reindex."; exit 0; }

log "Re-indexing ${TOTAL_IDS} missing document(s) from ${INDEX_A} into ${INDEX_B}..."
log "Strategy: ${STRATEGY} | Batch size: ${BATCH_SIZE}"
echo ""

# ── Process a batch of IDs — mgetbulk strategy ────────────────────────────────
# Fetches documents from index-a via _mget, then bulk-indexes them into index-b.
# Documents not found in index-a (e.g. deleted since the comparison ran) are
# counted and skipped rather than treated as errors.
process_batch_mgetbulk() {
    local -a ids=("$@")

    # Build the _mget request body
    local ids_json mget_body mget_response
    ids_json=$(printf '%s\n' "${ids[@]}" | jq -R . | jq -s .)
    mget_body=$(jq -n --argjson ids "$ids_json" '{"ids": $ids}')
    local mget_start mget_ms
    mget_start=$(now_ms)
    mget_response=$("$ESCLI" mget --index "$INDEX_A" <<< "$mget_body")
    mget_ms=$(( $(now_ms) - mget_start ))
    TOTAL_MGET_MS=$(( TOTAL_MGET_MS + mget_ms ))

    # Build bulk NDJSON: one action line + one source line per found document.
    # Documents where found == false are silently skipped (counted separately).
    local bulk_ndjson
    bulk_ndjson=$(echo "$mget_response" | jq -r --arg index "$INDEX_B" '
        .docs[] |
        select(.found == true) |
        ({"index": {"_index": $index, "_id": ._id}} | tojson),
        (._source | tojson)
    ')

    # Count how many were not found (deleted from index-a since comparison)
    local not_found
    not_found=$(echo "$mget_response" | jq '[.docs[] | select(.found == false)] | length')
    NOT_FOUND_TOTAL=$(( NOT_FOUND_TOTAL + not_found ))

    local bulk_ms=0
    if [ -n "$bulk_ndjson" ]; then
        local bulk_response bulk_start
        bulk_start=$(now_ms)
        bulk_response=$("$ESCLI" bulk --input - <<< "$bulk_ndjson")
        bulk_ms=$(( $(now_ms) - bulk_start ))
        TOTAL_BULK_MS=$(( TOTAL_BULK_MS + bulk_ms ))

        if echo "$bulk_response" | grep -q '"errors":true'; then
            die "Bulk errors detected: $(echo "$bulk_response" | grep -o '"reason":"[^"]*"' | head -3)"
        fi

        local indexed
        indexed=$(echo "$bulk_response" | jq '[.items[].index | select(.status == 200 or .status == 201)] | length')
        REINDEXED_TOTAL=$(( REINDEXED_TOTAL + indexed ))
    fi
    BATCH_DETAIL="⏳ mget: $(format_ms $mget_ms), bulk: $(format_ms $bulk_ms)"
}

# ── Process a batch of IDs — reindex strategy ─────────────────────────────────
# Uses the _reindex API with an ids query to copy only the specified documents.
process_batch_reindex() {
    local -a ids=("$@")

    local ids_json reindex_body reindex_response
    ids_json=$(printf '%s\n' "${ids[@]}" | jq -R . | jq -s .)
    reindex_body=$(jq -n \
        --arg     src  "$INDEX_A" \
        --arg     dst  "$INDEX_B" \
        --argjson ids  "$ids_json" \
        '{"source":{"index":$src,"query":{"ids":{"values":$ids}}},"dest":{"index":$dst}}')

    local reindex_start reindex_ms
    reindex_start=$(now_ms)
    reindex_response=$("$ESCLI" reindex <<< "$reindex_body")
    reindex_ms=$(( $(now_ms) - reindex_start ))
    TOTAL_REINDEX_MS=$(( TOTAL_REINDEX_MS + reindex_ms ))

    if echo "$reindex_response" | jq -e '.failures | length > 0' > /dev/null; then
        die "Reindex failures: $(echo "$reindex_response" | jq '.failures[:3]')"
    fi

    local created updated
    created=$(echo "$reindex_response" | jq '.created')
    updated=$(echo "$reindex_response" | jq '.updated')
    REINDEXED_TOTAL=$(( REINDEXED_TOTAL + created + updated ))
    BATCH_DETAIL="⏳ reindex: $(format_ms $reindex_ms)"
}

# ── Paginate through the input file in batches ────────────────────────────────
REINDEXED_TOTAL=0
NOT_FOUND_TOTAL=0
BATCH=()
BATCH_NUM=0
IDS_PROCESSED=0
TOTAL_MGET_MS=0
TOTAL_BULK_MS=0
TOTAL_REINDEX_MS=0
BATCH_DETAIL=""

while IFS= read -r doc_id; do
    [ -z "$doc_id" ] && continue
    BATCH+=("$doc_id")

    if (( ${#BATCH[@]} >= BATCH_SIZE )); then
        BATCH_NUM=$(( BATCH_NUM + 1 ))
        case "$STRATEGY" in
            mgetbulk) process_batch_mgetbulk "${BATCH[@]}" ;;
            reindex)  process_batch_reindex  "${BATCH[@]}" ;;
        esac
        IDS_PROCESSED=$(( IDS_PROCESSED + ${#BATCH[@]} ))
        PCT=$(( IDS_PROCESSED * 100 / TOTAL_IDS ))
        _progress "Batch ${BATCH_NUM} (${IDS_PROCESSED}/${TOTAL_IDS}, ${PCT}%) - ${BATCH_DETAIL}"
        BATCH=()
    fi
done < "$INPUT_FILE"

# Flush the last partial batch
if (( ${#BATCH[@]} > 0 )); then
    BATCH_NUM=$(( BATCH_NUM + 1 ))
    case "$STRATEGY" in
        mgetbulk) process_batch_mgetbulk "${BATCH[@]}" ;;
        reindex)  process_batch_reindex  "${BATCH[@]}" ;;
    esac
    IDS_PROCESSED=$(( IDS_PROCESSED + ${#BATCH[@]} ))
    _progress "Batch ${BATCH_NUM} (${IDS_PROCESSED}/${TOTAL_IDS}, 100%) - ${BATCH_DETAIL}"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
(( IS_TTY && _PROG_INIT )) && printf "\n"
echo ""
log "Re-indexing complete."
printf "  %-38s %d\n" "IDs read from input file:"              "$TOTAL_IDS"
printf "  %-38s %d\n" "Documents re-indexed into ${INDEX_B}:"  "$REINDEXED_TOTAL"

if (( NOT_FOUND_TOTAL > 0 )); then
    printf "  %-38s %d\n" "Not found in ${INDEX_A} (skipped):" "$NOT_FOUND_TOTAL"
    warn "Some documents were no longer present in ${INDEX_A}."
fi

if [[ "$STRATEGY" == "mgetbulk" ]] && (( BATCH_NUM > 0 )); then
    printf "  %-38s mget:   total %s, avg %s/batch\n" \
        "Batch stats (${BATCH_NUM} batches):" \
        "$(format_ms $TOTAL_MGET_MS)" \
        "$(format_ms $(( TOTAL_MGET_MS / BATCH_NUM )))"
    printf "  %-38s bulk:   total %s, avg %s/batch\n" \
        "" \
        "$(format_ms $TOTAL_BULK_MS)" \
        "$(format_ms $(( TOTAL_BULK_MS / BATCH_NUM )))"
elif [[ "$STRATEGY" == "reindex" ]] && (( BATCH_NUM > 0 )); then
    printf "  %-38s reindex: total %s, avg %s/batch\n" \
        "Batch stats (${BATCH_NUM} batches):" \
        "$(format_ms $TOTAL_REINDEX_MS)" \
        "$(format_ms $(( TOTAL_REINDEX_MS / BATCH_NUM )))"
fi
printf "  %-38s %s\n" "Duration:" "$(format_duration $SECONDS)"
