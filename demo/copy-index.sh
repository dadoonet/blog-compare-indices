#!/usr/bin/env bash
# copy-index.sh — Delete target index and copy all documents from source into it.
#
# Uses the _reindex API with no query filter (full copy).
# The target index is deleted first so the result is always a clean copy.
#
# Default usage (restore index-target back to index-b):
#   ./copy-index.sh
#
# Usage: ./copy-index.sh [options]
#   --source <name>   Source index  (default: index-target)
#   --target <name>   Target index  (default: index-b)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ESCLI="${SCRIPT_DIR}/escli"

source "${SCRIPT_DIR}/lib/log.sh"   "copy-index"
source "${SCRIPT_DIR}/lib/utils.sh"

SECONDS=0

# ── Load defaults from .env.sh, then allow CLI overrides ──────────────────────
set -a && set +u && source "${SCRIPT_DIR}/.env.sh" && set -u && set +a

: "${COPY_SOURCE:=index-target}"
: "${COPY_TARGET:=index-b}"

while [[ $# -gt 0 ]]; do
    case $1 in
        --source) COPY_SOURCE="$2"; shift 2 ;;
        --target) COPY_TARGET="$2"; shift 2 ;;
        -h|--help)
            grep '^# ' "$0" | head -12 | sed 's/^# \?//'
            exit 0 ;;
        *) die "Unknown option: $1" ;;
    esac
done

[ -f "$ESCLI" ] || die "escli not found at ${ESCLI}. Run ./setup.sh first."

# ── 1. Delete the target index ─────────────────────────────────────────────────
log "Deleting ${COPY_TARGET}..."
if "$ESCLI" indices delete "$COPY_TARGET" &>/dev/null; then
    info "Deleted existing index: ${COPY_TARGET}"
else
    info "Index ${COPY_TARGET} did not exist — nothing to delete"
fi

# ── 2. Reindex source → target ─────────────────────────────────────────────────
log "Copying ${COPY_SOURCE} → ${COPY_TARGET}..."

REINDEX_BODY=$(jq -n \
    --arg src "$COPY_SOURCE" \
    --arg dst "$COPY_TARGET" \
    '{"source":{"index":$src},"dest":{"index":$dst}}')

REINDEX_RESPONSE=$("$ESCLI" reindex --timeout "5m" <<< "$REINDEX_BODY")

TOTAL=$(echo "$REINDEX_RESPONSE"   | jq '.total')
CREATED=$(echo "$REINDEX_RESPONSE" | jq '.created')
FAILURES=$(echo "$REINDEX_RESPONSE" | jq '.failures | length')

if (( FAILURES > 0 )); then
    die "Reindex completed with ${FAILURES} failure(s): $(echo "$REINDEX_RESPONSE" | jq '.failures[:3]')"
fi

echo ""
log "Copy complete."
printf "  %-30s %s → %s\n" "Indices:" "$COPY_SOURCE" "$COPY_TARGET"
printf "  %-30s %d\n"       "Documents copied:"        "$TOTAL"
printf "  %-30s %d\n"       "Created in target:"       "$CREATED"
printf "  %-30s %s\n"       "Duration:"                "$(format_duration $SECONDS)"
