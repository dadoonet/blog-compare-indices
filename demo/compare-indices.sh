#!/usr/bin/env bash
# compare-indices.sh — Find documents present in index-a but missing from index-b.
#
# Strategies:
#   querydsl
#     1. Run _count on both indices to detect a mismatch early.
#     2. Open a Point-in-Time (PIT) on index-a for a consistent snapshot.
#     3. Paginate through all document IDs using search_after (_source: false).
#     4. For each page, batch-check existence in index-b via _mget.
#     5. Collect missing IDs, write them to a file, and print a summary.
#
#   business-key
#     Compares by (first_name, last_name, birth_date) instead of _id.
#     Useful when documents were ingested via different pipelines and _id values
#     do not correspond between indices.
#     1. Open a PIT on index-a for a consistent snapshot.
#     2. Paginate with search_after, fetching only the three business-key fields.
#     3. For each page, build one msearch request with one sub-query per source
#        doc (size:0, bool/must on the three fields). Each sub-response is
#        independent: total.value == 0 means no match in target → missing.
#     4. Zip source docs with msearch responses by index to collect missing _ids.
#
#   split-by-date
#     Same as business-key but partitions the source index by birth_date time
#     slices before scanning. Each slice opens its own PIT, reducing per-pass
#     volume and enabling future parallelism.
#     1. Aggregate min/max birth_date on the source index.
#     2. Divide the date range into slices of --slice-years years.
#     3. For each slice: open a PIT, paginate with a birth_date range filter,
#        run the same msearch sub-query logic as business-key.
#     4. Append missing _ids from all slices into the output file.
#
#   esql
#     Not yet implemented.
#
# Requirements: escli (./escli wrapper), jq
#
# Usage: ./compare-indices.sh [options]
#   --source <name>        Source index                      (default: index-a)
#   --target <name>        Target index                      (default: index-b)
#   --batch-size <n>       Docs per search page              (default: 10000)
#   --pit-keep-alive <dur> PIT keep-alive duration           (default: 5m)
#   --output <file>        Output file for missing IDs       (default: missing-ids.txt)
#   --strategy <name>      Comparison strategy               (default: business-key)
#   --slice-years <n>      Years per date slice (split-by-date only) (default: 10)

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
: "${STRATEGY:=business-key}"
: "${SLICE_YEARS:=10}"

while [[ $# -gt 0 ]]; do
    case $1 in
        --source)          INDEX_A="$2";          shift 2 ;;
        --target)          INDEX_B="$2";          shift 2 ;;
        --batch-size)      BATCH_SIZE="$2";       shift 2 ;;
        --pit-keep-alive)  PIT_KEEP_ALIVE="$2";   shift 2 ;;
        --output)          OUTPUT_FILE="$2";      shift 2 ;;
        --strategy)        STRATEGY="$2";         shift 2 ;;
        --slice-years)     SLICE_YEARS="$2";      shift 2 ;;
        -h|--help)
            grep '^# ' "$0" | head -44 | sed 's/^# \?//'
            exit 0 ;;
        *) die "Unknown option: $1" ;;
    esac
done

case "$STRATEGY" in
    querydsl) ;;
    business-key) ;;
    split-by-date) ;;
    esql) die "Strategy 'esql' is not yet implemented." ;;
    *) die "Unknown strategy: ${STRATEGY}. Valid values: querydsl, business-key, split-by-date, esql" ;;
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
else
    DIFF=$(( COUNT_A - COUNT_B ))
    warn "${INDEX_B} has ${DIFF} fewer document(s) than ${INDEX_A}."
fi
echo ""

# ── strategy: business-key ────────────────────────────────────────────────────
# Compares by (first_name, last_name, birth_date) — no _id matching.
# Each source page is used to build a bool/should query against the target.
# Source docs whose tuple has no counterpart in the target response are missing.
if [[ "$STRATEGY" == "business-key" ]]; then
    log "Opening Point-in-Time on ${INDEX_A} (keep-alive: ${PIT_KEEP_ALIVE})..."
    BK_PIT_ID=$("$ESCLI" open_point_in_time "$INDEX_A" "$PIT_KEEP_ALIVE" | jq -r '.id // empty')
    [ -z "$BK_PIT_ID" ] && die "Failed to open PIT."

    close_pit() {
        local exit_code=$?
        log "Closing Point-in-Time..."
        "$ESCLI" close_point_in_time <<< "{\"id\":\"${BK_PIT_ID}\"}" > /dev/null 2>&1 || true
        exit $exit_code
    }
    trap close_pit EXIT

    log "Scanning ${INDEX_A} by business key in batches of ${BATCH_SIZE}..."
    (( IS_TTY )) || echo ""

    OUTPUT_PATH="${SCRIPT_DIR}/${OUTPUT_FILE}"
    > "$OUTPUT_PATH"

    SEARCH_AFTER="null"
    TOTAL_CHECKED=0
    MISSING_COUNT=0
    PAGE=0
    TOTAL_SEARCH_MS=0
    TOTAL_MSEARCH_MS=0

    while true; do
        PAGE=$(( PAGE + 1 ))

        # Fetch source docs with only the three business-key fields
        SEARCH_BODY=$(jq -n \
            --arg    pit_id        "$BK_PIT_ID"      \
            --argjson search_after "$SEARCH_AFTER"   \
            --argjson size         "$BATCH_SIZE"     \
            --arg    keep_alive    "$PIT_KEEP_ALIVE" \
            '{
                "size": $size,
                "_source": ["first_name", "last_name", "birth_date"],
                "pit": {"id": $pit_id, "keep_alive": $keep_alive},
                "sort": [{"_shard_doc": "asc"}]
            } + (if $search_after != null then {"search_after": $search_after} else {} end)')

        SEARCH_START=$(now_ms)
        SEARCH_RESPONSE=$("$ESCLI" search <<< "$SEARCH_BODY")
        SEARCH_MS=$(( $(now_ms) - SEARCH_START ))
        TOTAL_SEARCH_MS=$(( TOTAL_SEARCH_MS + SEARCH_MS ))

        NEW_PIT_ID=$(echo "$SEARCH_RESPONSE" | jq -r '.pit_id // empty')
        [ -n "$NEW_PIT_ID" ] && BK_PIT_ID="$NEW_PIT_ID"

        HITS=$(echo "$SEARCH_RESPONSE" | jq -c '.hits.hits')
        HIT_COUNT=$(echo "$HITS" | jq 'length')
        (( HIT_COUNT == 0 )) && break

        TOTAL_CHECKED=$(( TOTAL_CHECKED + HIT_COUNT ))
        SEARCH_AFTER=$(echo "$HITS" | jq -c '.[-1].sort')

        # Filter out docs with any null business-key field — term queries reject nulls.
        # FILTERED is a JSON array aligned 1-to-1 with the msearch responses.
        FILTERED=$(echo "$HITS" | jq -c '[.[] | select(
            ._source.first_name != null and
            ._source.last_name  != null and
            ._source.birth_date != null
        )]')

        FILTERED_COUNT=$(echo "$FILTERED" | jq 'length')
        MSEARCH_MS=0

        if (( FILTERED_COUNT > 0 )); then
            # Extract source _ids in the same order as the msearch sub-queries.
            IDS=$(echo "$FILTERED" | jq -c '[.[]._id]')

            # Build msearch NDJSON: one (header + body) pair per source doc.
            # size:0 — we only need hits.total.value, not the actual documents.
            # tojson safely escapes special characters in field values.
            MSEARCH_BODY=$(echo "$FILTERED" | jq -r \
                --arg idx "$INDEX_B" \
                '.[] | (
                    ({"index": $idx} | tojson),
                    ({
                        "size": 0,
                        "query": {"bool": {"must": [
                            {"term": {"first_name.keyword": ._source.first_name}},
                            {"term": {"last_name.keyword":  ._source.last_name}},
                            {"term": {"birth_date":         ._source.birth_date}}
                        ]}}
                    } | tojson)
                )')

            MSEARCH_START=$(now_ms)
            MSEARCH_RESPONSE=$("$ESCLI" msearch <<< "$MSEARCH_BODY")
            MSEARCH_MS=$(( $(now_ms) - MSEARCH_START ))
            TOTAL_MSEARCH_MS=$(( TOTAL_MSEARCH_MS + MSEARCH_MS ))

            # to_entries gives [{key:0, value:response0}, ...].
            # .key is the index into $ids — no transpose needed.
            # MSEARCH_RESPONSE is piped via stdin to avoid ARG_MAX limits.
            MISSING_IDS=$(echo "$MSEARCH_RESPONSE" | jq -r \
                --argjson ids "$IDS" \
                '.responses | to_entries[] |
                 select(.value.hits.total.value == 0) |
                 $ids[.key]')

            if [ -n "$MISSING_IDS" ]; then
                echo "$MISSING_IDS" >> "$OUTPUT_PATH"
                MISSING_COUNT=$(( MISSING_COUNT + $(echo "$MISSING_IDS" | wc -l | tr -d ' ') ))
            fi
        fi

        _progress "Page ${PAGE} $(progress_bar $TOTAL_CHECKED $COUNT_A) - ⏳ search: $(format_ms $SEARCH_MS), msearch: $(format_ms $MSEARCH_MS)"
    done

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
        printf "  %-38s search:  total %s, avg %s/page\n" \
            "Scan stats (${PAGE} pages):" \
            "$(format_ms $TOTAL_SEARCH_MS)" \
            "$(format_ms $(( TOTAL_SEARCH_MS / PAGE )))"
        printf "  %-38s msearch: total %s, avg %s/page\n" \
            "" \
            "$(format_ms $TOTAL_MSEARCH_MS)" \
            "$(format_ms $(( TOTAL_MSEARCH_MS / PAGE )))"
    fi
    printf "  %-38s %s\n" "Duration:" "$(format_duration $SECONDS)"
    if (( MISSING_COUNT > 0 )); then
        echo ""
        info "Missing IDs written to: ${OUTPUT_PATH}"
        warn "Run ./reindex-missing.sh --strategy mgetbulk to reindex them."
    else
        echo ""
        log "No missing documents found."
    fi
    exit 0
fi

# ── strategy: split-by-date ──────────────────────────────────────────────────
# Partitions the source index into birth_date slices of SLICE_YEARS years each.
# Each slice runs independently in a background job (parallel), opening its own
# PIT and running the same msearch business-key logic with a range filter.
# In TTY mode a live status block is updated in place; results are aggregated
# after all jobs complete.
if [[ "$STRATEGY" == "split-by-date" ]]; then
    # 1. Aggregate min/max birth_date to determine slice boundaries
    log "Aggregating birth_date range on ${INDEX_A}..."
    DATE_AGG=$("$ESCLI" search --index "$INDEX_A" <<< \
        '{"size":0,"aggs":{"min_d":{"min":{"field":"birth_date"}},"max_d":{"max":{"field":"birth_date"}}}}')
    MIN_YEAR=$(echo "$DATE_AGG" | jq -r '.aggregations.min_d.value_as_string' | cut -c1-4)
    MAX_YEAR=$(echo "$DATE_AGG" | jq -r '.aggregations.max_d.value_as_string' | cut -c1-4)
    [ -z "$MIN_YEAR" ] || [ -z "$MAX_YEAR" ] && die "Could not determine birth_date range from ${INDEX_A}."
    info "birth_date range: ${MIN_YEAR} → ${MAX_YEAR} (${SLICE_YEARS}-year slices)"
    echo ""

    OUTPUT_PATH="${SCRIPT_DIR}/${OUTPUT_FILE}"
    > "$OUTPUT_PATH"

    SLICE_TMPDIR=$(mktemp -d)
    _sbd_cleanup() {
        jobs -p 2>/dev/null | while IFS= read -r pid; do kill "$pid" 2>/dev/null || true; done
        rm -rf "$SLICE_TMPDIR"
    }
    trap _sbd_cleanup EXIT

    # ── Per-slice worker — runs in a subshell (called with &) ────────────────
    # Writes missing IDs to ${SLICE_TMPDIR}/missing_N.txt
    # Writes key=value stats to ${SLICE_TMPDIR}/stats_N.txt (atomic rename)
    _run_slice() {
        local slice_num=$1 gte=$2 lte=$3

        trap - EXIT
        _SLICE_PIT=""
        _slice_pit_close() {
            [ -n "$_SLICE_PIT" ] && \
                "$ESCLI" close_point_in_time <<< "{\"id\":\"${_SLICE_PIT}\"}" >/dev/null 2>&1 || true
        }
        trap _slice_pit_close EXIT

        _SLICE_PIT=$("$ESCLI" open_point_in_time "$INDEX_A" "$PIT_KEEP_ALIVE" | jq -r '.id // empty')
        [ -z "$_SLICE_PIT" ] && { warn "Slice ${slice_num}: failed to open PIT"; exit 1; }

        local out_missing="${SLICE_TMPDIR}/missing_${slice_num}.txt"
        local out_stats="${SLICE_TMPDIR}/stats_${slice_num}.txt"
        > "$out_missing"

        local search_after="null"
        local slice_checked=0 slice_missing=0 page=0
        local total_search_ms=0 total_msearch_ms=0

        while true; do
            page=$(( page + 1 ))

            local search_body
            search_body=$(jq -n \
                --arg    pit_id        "$_SLICE_PIT"     \
                --argjson search_after "$search_after"   \
                --argjson size         "$BATCH_SIZE"     \
                --arg    keep_alive    "$PIT_KEEP_ALIVE" \
                --arg    gte           "$gte"            \
                --arg    lte           "$lte"            \
                '{
                    "size": $size,
                    "_source": ["first_name", "last_name", "birth_date"],
                    "pit": {"id": $pit_id, "keep_alive": $keep_alive},
                    "sort": [{"_shard_doc": "asc"}],
                    "query": {"range": {"birth_date": {"gte": $gte, "lte": $lte}}}
                } + (if $search_after != null then {"search_after": $search_after} else {} end)')

            local search_start search_response search_ms
            search_start=$(now_ms)
            search_response=$("$ESCLI" search <<< "$search_body")
            search_ms=$(( $(now_ms) - search_start ))
            total_search_ms=$(( total_search_ms + search_ms ))

            local new_pit_id
            new_pit_id=$(echo "$search_response" | jq -r '.pit_id // empty')
            [ -n "$new_pit_id" ] && _SLICE_PIT="$new_pit_id"

            local hits hit_count
            hits=$(echo "$search_response" | jq -c '.hits.hits')
            hit_count=$(echo "$hits" | jq 'length')
            (( hit_count == 0 )) && break

            slice_checked=$(( slice_checked + hit_count ))
            search_after=$(echo "$hits" | jq -c '.[-1].sort')

            local filtered filtered_count
            filtered=$(echo "$hits" | jq -c '[.[] | select(
                ._source.first_name != null and
                ._source.last_name  != null and
                ._source.birth_date != null
            )]')
            filtered_count=$(echo "$filtered" | jq 'length')

            if (( filtered_count > 0 )); then
                local ids msearch_body msearch_start msearch_response msearch_ms missing_ids
                ids=$(echo "$filtered" | jq -c '[.[]._id]')

                msearch_body=$(echo "$filtered" | jq -r \
                    --arg idx "$INDEX_B" \
                    '.[] | (
                        ({"index": $idx} | tojson),
                        ({
                            "size": 0,
                            "query": {"bool": {"must": [
                                {"term": {"first_name.keyword": ._source.first_name}},
                                {"term": {"last_name.keyword":  ._source.last_name}},
                                {"term": {"birth_date":         ._source.birth_date}}
                            ]}}
                        } | tojson)
                    )')

                msearch_start=$(now_ms)
                msearch_response=$("$ESCLI" msearch <<< "$msearch_body")
                msearch_ms=$(( $(now_ms) - msearch_start ))
                total_msearch_ms=$(( total_msearch_ms + msearch_ms ))

                missing_ids=$(echo "$msearch_response" | jq -r \
                    --argjson ids "$ids" \
                    '.responses | to_entries[] |
                     select(.value.hits.total.value == 0) |
                     $ids[.key]')

                if [ -n "$missing_ids" ]; then
                    echo "$missing_ids" >> "$out_missing"
                    slice_missing=$(( slice_missing + $(echo "$missing_ids" | wc -l | tr -d ' ') ))
                fi
            fi
        done

        "$ESCLI" close_point_in_time <<< "{\"id\":\"${_SLICE_PIT}\"}" >/dev/null 2>&1 || true
        _SLICE_PIT=""
        trap - EXIT

        # Atomic write: rename so the polling loop never sees a partial file
        printf 'SLICE_CHECKED=%d\nSLICE_MISSING=%d\nSLICE_PAGES=%d\nSLICE_SEARCH_MS=%d\nSLICE_MSEARCH_MS=%d\n' \
            "$slice_checked" "$slice_missing" "$page" "$total_search_ms" "$total_msearch_ms" \
            > "${out_stats}.tmp"
        mv "${out_stats}.tmp" "$out_stats"

        # In non-TTY mode the parent has no live display — print completion here
        (( IS_TTY )) || info "Slice ${slice_num} done: ${slice_checked} checked, ${slice_missing} missing"
    }

    # 2. Compute all slice boundaries upfront
    SLICE_NUM=0
    SLICE_START=$MIN_YEAR
    SLICE_GTES=()
    SLICE_LTES=()
    while (( SLICE_START <= MAX_YEAR )); do
        SLICE_GTES+=("${SLICE_START}-01-01")
        SLICE_LTES+=("$(( SLICE_START + SLICE_YEARS - 1 ))-12-31")
        SLICE_NUM=$(( SLICE_NUM + 1 ))
        SLICE_START=$(( SLICE_START + SLICE_YEARS ))
    done

    # 3. Print initial status block (TTY) then launch all slices
    if (( IS_TTY )); then
        log "Launching ${SLICE_NUM} slices in parallel..."
        echo ""
        for (( s=1; s<=SLICE_NUM; s++ )); do
            printf "  → Slice %d: %s → %s ⏳\n" \
                "$s" "${SLICE_GTES[$((s-1))]}" "${SLICE_LTES[$((s-1))]}"
        done
    else
        log "Launching ${SLICE_NUM} slices in parallel..."
    fi

    SLICE_PIDS=()
    for (( s=1; s<=SLICE_NUM; s++ )); do
        _run_slice "$s" "${SLICE_GTES[$((s-1))]}" "${SLICE_LTES[$((s-1))]}" &
        SLICE_PIDS+=($!)
    done

    # 4. Monitor progress
    FAILED_SLICES=0
    SLICE_DONE=()
    for (( s=1; s<=SLICE_NUM; s++ )); do SLICE_DONE[$s]=0; done
    COMPLETED=0

    if (( IS_TTY )); then
        # Cursor is currently at the line below the last slice status line.
        # To update slice s: move up (SLICE_NUM - s + 1) lines, rewrite, move back down.
        while (( COMPLETED < SLICE_NUM )); do
            sleep 0.3
            for (( s=1; s<=SLICE_NUM; s++ )); do
                [ "${SLICE_DONE[$s]}" = "1" ] && continue

                local_ok=1
                if [ -f "${SLICE_TMPDIR}/stats_${s}.txt" ]; then
                    : # success
                elif ! kill -0 "${SLICE_PIDS[$((s-1))]}" 2>/dev/null; then
                    local_ok=0  # PID gone, no stats file → failed
                else
                    continue    # still running
                fi

                SLICE_DONE[$s]=1
                COMPLETED=$(( COMPLETED + 1 ))

                lines_up=$(( SLICE_NUM - s + 1 ))
                lines_down=$(( SLICE_NUM - s ))
                printf "\033[%dA\r\033[K" "$lines_up"

                if (( local_ok )); then
                    SLICE_CHECKED=0; SLICE_MISSING=0
                    source "${SLICE_TMPDIR}/stats_${s}.txt"
                    printf "  → Slice %d: %s → %s ✅ — %d checked, %d missing\n" \
                        "$s" "${SLICE_GTES[$((s-1))]}" "${SLICE_LTES[$((s-1))]}" \
                        "$SLICE_CHECKED" "$SLICE_MISSING"
                else
                    printf "  → Slice %d: %s → %s ❌ (failed)\n" \
                        "$s" "${SLICE_GTES[$((s-1))]}" "${SLICE_LTES[$((s-1))]}"
                    FAILED_SLICES=$(( FAILED_SLICES + 1 ))
                fi

                (( lines_down > 0 )) && printf "\033[%dB" "$lines_down"
            done
        done
        # Reap background processes (they are already done at this point)
        for pid in "${SLICE_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
    else
        for i in "${!SLICE_PIDS[@]}"; do
            wait "${SLICE_PIDS[$i]}" || FAILED_SLICES=$(( FAILED_SLICES + 1 ))
        done
    fi

    (( FAILED_SLICES > 0 )) && die "${FAILED_SLICES} slice(s) failed."

    # 5. Aggregate results from all slices (in slice order)
    TOTAL_CHECKED=0
    MISSING_COUNT=0
    TOTAL_PAGES=0
    TOTAL_SEARCH_MS=0
    TOTAL_MSEARCH_MS=0

    for (( s=1; s<=SLICE_NUM; s++ )); do
        [ -f "${SLICE_TMPDIR}/missing_${s}.txt" ] && \
            cat "${SLICE_TMPDIR}/missing_${s}.txt" >> "$OUTPUT_PATH" || true
        if [ -f "${SLICE_TMPDIR}/stats_${s}.txt" ]; then
            SLICE_CHECKED=0; SLICE_MISSING=0; SLICE_PAGES=0; SLICE_SEARCH_MS=0; SLICE_MSEARCH_MS=0
            source "${SLICE_TMPDIR}/stats_${s}.txt"
            TOTAL_CHECKED=$(( TOTAL_CHECKED + SLICE_CHECKED ))
            MISSING_COUNT=$(( MISSING_COUNT + SLICE_MISSING ))
            TOTAL_PAGES=$(( TOTAL_PAGES + SLICE_PAGES ))
            TOTAL_SEARCH_MS=$(( TOTAL_SEARCH_MS + SLICE_SEARCH_MS ))
            TOTAL_MSEARCH_MS=$(( TOTAL_MSEARCH_MS + SLICE_MSEARCH_MS ))
        fi
    done

    rm -rf "$SLICE_TMPDIR"
    trap - EXIT

    echo ""
    log "Comparison complete."
    printf "  %-38s %d\n" "Total documents in ${INDEX_A}:"      "$COUNT_A"
    printf "  %-38s %d\n" "Total documents in ${INDEX_B}:"      "$COUNT_B"
    printf "  %-38s %d\n" "Documents checked:"                   "$TOTAL_CHECKED"
    printf "  %-38s %d\n" "Documents missing from ${INDEX_B}:"  "$MISSING_COUNT"
    if (( MISSING_COUNT > 0 && TOTAL_CHECKED > 0 )); then
        PCT=$(( MISSING_COUNT * 100 / TOTAL_CHECKED ))
        printf "  %-38s ~%d%%\n" "Missing rate:" "$PCT"
    fi
    printf "  %-38s %d (${SLICE_YEARS}-year slices, parallel)\n" "Date slices:" "$SLICE_NUM"
    if (( TOTAL_PAGES > 0 )); then
        printf "  %-38s search:  total %s, avg %s/page\n" \
            "Scan stats (${TOTAL_PAGES} pages):" \
            "$(format_ms $TOTAL_SEARCH_MS)" \
            "$(format_ms $(( TOTAL_SEARCH_MS / TOTAL_PAGES )))"
        printf "  %-38s msearch: total %s, avg %s/page\n" \
            "" \
            "$(format_ms $TOTAL_MSEARCH_MS)" \
            "$(format_ms $(( TOTAL_MSEARCH_MS / TOTAL_PAGES )))"
    fi
    printf "  %-38s %s\n" "Duration:" "$(format_duration $SECONDS)"
    if (( MISSING_COUNT > 0 )); then
        echo ""
        info "Missing IDs written to: ${OUTPUT_PATH}"
        warn "Run ./reindex-missing.sh --strategy mgetbulk to reindex them."
    else
        echo ""
        log "No missing documents found."
    fi
    exit 0
fi

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
