#!/usr/bin/env bash
# setup.sh — Bootstrap the demo environment.
#
# What it does:
#   1. Starts a local Elasticsearch instance using elastic/start-local (Docker-based)
#   2. Downloads the escli-rs binary for the current OS/architecture
#   3. Populates demo/.env with the connection details extracted from start-local
#
# Prerequisites: Docker, curl
# Optional but recommended: jq (required by compare-indices.sh)
#
# Usage: ./setup.sh [--skip-start-local] [--skip-escli]
#   --skip-start-local   Skip the start-local step (useful if ES is already running)
#   --skip-escli         Skip the escli-rs download step

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log()  { echo -e "${GREEN}[setup]${NC} $*"; }
info() { echo -e "${BLUE}  →${NC} $*"; }
warn() { echo -e "${YELLOW}[setup]${NC} $*"; }
die()  { echo -e "${RED}[setup] ERROR:${NC} $*" >&2; exit 1; }

# Portable in-place sed: macOS requires an explicit (empty) backup extension
_sed_i() { [[ "$OSTYPE" == darwin* ]] && sed -i '' "$@" || sed -i "$@"; }

# ── Argument parsing ──────────────────────────────────────────────────────────
SKIP_START_LOCAL=false
SKIP_ESCLI=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-start-local) SKIP_START_LOCAL=true ;;
        --skip-escli)       SKIP_ESCLI=true ;;
        -h|--help)
            grep '^# ' "$0" | head -12 | sed 's/^# \?//'
            exit 0 ;;
        *) die "Unknown option: $1" ;;
    esac
    shift
done

# ── 1. Check prerequisites ────────────────────────────────────────────────────
log "Checking prerequisites..."
command -v docker >/dev/null || die "Docker is required. Install it from https://docs.docker.com/get-docker/"
command -v curl   >/dev/null || die "curl is required but not found."
command -v jq     >/dev/null || warn "jq not found — it is required by compare-indices.sh (brew install jq)"

# ── Ensure .env.es exists (always, regardless of --skip-start-local) ──────────
ES_ENV="${SCRIPT_DIR}/.env.es"
if [ ! -f "$ES_ENV" ]; then
    cp "${SCRIPT_DIR}/.env.es.example" "$ES_ENV"
    log "Created .env.es from .env.es.example — review and update credentials if needed."
fi

# ── 2. Start Elasticsearch via start-local ────────────────────────────────────
# elastic/start-local sets up Elasticsearch + Kibana in Docker with a single
# command. It creates an elastic-start-local/ directory with docker-compose.yml
# and a .env containing the connection credentials.
START_LOCAL_DIR="${SCRIPT_DIR}/elastic-start-local"

if ! $SKIP_START_LOCAL; then
    if [ -d "$START_LOCAL_DIR" ]; then
        log "Found existing elastic-start-local directory. Starting containers..."
        (cd "$START_LOCAL_DIR" && docker compose up -d)
    else
        log "Downloading and running elastic/start-local..."
        # start-local creates ./elastic-start-local/ relative to the working directory
        (cd "$SCRIPT_DIR" && curl -fsSL https://elastic.co/start-local | sh -s -- --esonly)
    fi

    # ── 3. Extract connection details from start-local .env ───────────────────
    SL_ENV="${START_LOCAL_DIR}/.env"
    [ -f "$SL_ENV" ] || die "start-local did not produce a .env file at: ${SL_ENV}"

    # Source the start-local .env so that variable references are resolved.
    # ES_LOCAL_URL is defined as http://localhost:${ES_LOCAL_PORT}, so we need
    # ES_LOCAL_PORT to be set first — sourcing handles this automatically.
    set +u; source "$SL_ENV"; set -u

    ES_URL="${ES_LOCAL_URL:-http://localhost:9200}"
    ES_KEY="${ES_LOCAL_API_KEY:-}"

    info "Elasticsearch URL : ${ES_URL}"
    if [ -n "$ES_KEY" ]; then
        info "API key           : (found)"
    else
        warn "No API key found in start-local .env — set ESCLI_API_KEY manually in demo/.env"
    fi

    _sed_i "s|^ESCLI_URL=.*|ESCLI_URL=${ES_URL}|"         "$ES_ENV"
    _sed_i "s|^ESCLI_API_KEY=.*|ESCLI_API_KEY=${ES_KEY}|" "$ES_ENV"
    log ".env.es updated with connection details."
fi

# ── 5. Set up escli-rs ────────────────────────────────────────────────────────
# Preferred: download the native binary (much faster than Docker).
# Fallback:  pull the Docker image if the binary download fails.
# https://github.com/Anaethelion/escli-rs

# Load ESCLI_VERSION from .env.escli
set -a && set +u && source "${SCRIPT_DIR}/.env.escli" && set -u && set +a

ESCLI_BINARY="${SCRIPT_DIR}/escli-bin"
ESCLI_VERSION_FILE="${SCRIPT_DIR}/.escli-version"

# Pick Docker image tag for the fallback
case "$(uname -m)" in
    arm64|aarch64) ESCLI_IMAGE="ghcr.io/anaethelion/escli:latest-arm64" ;;
    x86_64)        ESCLI_IMAGE="ghcr.io/anaethelion/escli:latest-amd64" ;;
    *)             die "Unsupported architecture: $(uname -m)" ;;
esac

if ! $SKIP_ESCLI; then
    # Check if the correct version is already installed
    INSTALLED_VERSION=""
    [ -f "$ESCLI_VERSION_FILE" ] && INSTALLED_VERSION=$(cat "$ESCLI_VERSION_FILE")

    if [ -f "$ESCLI_BINARY" ] && [ "$INSTALLED_VERSION" = "$ESCLI_VERSION" ]; then
        log "escli binary ${ESCLI_VERSION} already installed — skipping download."
    else
        case "$(uname -s)" in
            Darwin) BINARY_NAME="escli-macos" ;;
            Linux)  BINARY_NAME="escli-linux" ;;
            *)      die "Unsupported OS: $(uname -s)" ;;
        esac
        DOWNLOAD_URL="https://github.com/Anaethelion/escli-rs/releases/download/${ESCLI_VERSION}/${BINARY_NAME}"

        log "Downloading escli-rs ${ESCLI_VERSION} from ${DOWNLOAD_URL}..."
        if curl -fsSL "$DOWNLOAD_URL" -o "$ESCLI_BINARY"; then
            chmod +x "$ESCLI_BINARY"
            # Remove macOS quarantine flag for unsigned binaries
            [[ "$OSTYPE" == darwin* ]] && xattr -d com.apple.quarantine "$ESCLI_BINARY" 2>/dev/null || true
            echo "$ESCLI_VERSION" > "$ESCLI_VERSION_FILE"
            log "escli binary installed at ${ESCLI_BINARY}."
        else
            warn "Binary download failed. Falling back to Docker image..."
            docker pull "$ESCLI_IMAGE" && log "Docker image ${ESCLI_IMAGE} ready as fallback."
        fi
    fi
fi

# ── 6. Smoke test ─────────────────────────────────────────────────────────────
if [ -f "${SCRIPT_DIR}/escli" ] && [ -f "${SCRIPT_DIR}/.env.es" ]; then
    log "Testing connection to Elasticsearch..."
    set -a; set +u; source "${SCRIPT_DIR}/.env.sh"; set -u; set +a
    if "${SCRIPT_DIR}/escli" info &>/dev/null; then
        log "Connection OK."
    else
        warn "Cannot reach Elasticsearch yet — it may still be starting up."
        warn "Wait a few seconds then run: ./escli info"
    fi
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
log "Setup complete. Next steps:"
info "1. Review .env.demo (dataset parameters)"
info "2. ./init-dataset.sh     — generate index-a and index-b with sample data"
info "3. ./compare-indices.sh  — find documents missing from index-b"
