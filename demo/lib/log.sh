#!/usr/bin/env bash
# lib/log.sh — Colored logging functions.
#
# Usage: source lib/log.sh <prefix>
#   prefix  Label shown in brackets, e.g. "init" → [init], "compare" → [compare]
#
# Defines: log, info, warn, die
# Exports: RED GREEN YELLOW BLUE NC (ANSI color codes)

_LOG_PREFIX="${1:?lib/log.sh: prefix argument required (e.g.: source lib/log.sh init)}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

log()  { echo -e "${GREEN}[${_LOG_PREFIX}]${NC} $*"; }
info() { echo -e "${BLUE}  →${NC} $*"; }
warn() { echo -e "${YELLOW}[${_LOG_PREFIX}]${NC} $*"; }
die()  { echo -e "${RED}[${_LOG_PREFIX}] ERROR:${NC} $*" >&2; exit 1; }
