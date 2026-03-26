#!/usr/bin/env bash
# .env.sh — Single entry point to load all demo environment variables.
# Source this file in any script that needs ES credentials or demo parameters.
#
# This file is committed and contains no secrets.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Elasticsearch credentials — not committed, populated by setup.sh
if [ -f "${SCRIPT_DIR}/.env.es" ]; then
    source "${SCRIPT_DIR}/.env.es"
else
    echo "[env] Warning: .env.es not found. Copy .env.es.example to .env.es and fill in credentials." >&2
fi

# Demo parameters — committed, no secrets
source "${SCRIPT_DIR}/.env.demo"
