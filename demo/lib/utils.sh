#!/usr/bin/env bash
# lib/utils.sh — Shared utility functions: timing, formatting, progress bar, TTY detection.
#
# Usage: source lib/utils.sh
#
# Defines: format_duration, now_ms, format_ms, progress_bar
# Sets:    IS_TTY (1 if stdout is a terminal, 0 otherwise)

# Formats a whole-second duration as a human-readable string (e.g. "2m 05s")
format_duration() {
    local s=$1
    local h=$(( s / 3600 )) m=$(( (s % 3600) / 60 )) sec=$(( s % 60 ))
    (( h > 0 )) && printf "%dh %02dm %02ds" $h $m $sec && return
    (( m > 0 )) && printf "%dm %02ds" $m $sec          && return
    printf "%ds" $sec
}

# Returns current time in milliseconds.
# Uses EPOCHREALTIME (bash 5+) with locale-safe dot/comma stripping; falls back to perl.
now_ms() {
    if (( BASH_VERSINFO[0] >= 5 )); then
        local t="${EPOCHREALTIME/[.,]/}"
        echo "$(( t / 1000 ))"
    else
        perl -MTime::HiRes=time -e 'printf "%d\n", time()*1000'
    fi
}

# Formats a millisecond duration as a human-readable string (e.g. "312ms", "2.103s", "1m 05.200s")
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

# Renders a Unicode block progress bar.
# Usage: progress_bar <current> <total>
# Output example: [████████████░░░░░░░░░░░░░]  48%
progress_bar() {
    local current=$1 total=$2 width=25
    local pct=$(( current * 100 / total ))
    local filled=$(( current * width / total ))
    local empty=$(( width - filled ))
    local bar="" i
    for (( i=0; i<filled; i++ )); do bar+="█"; done
    for (( i=0; i<empty;  i++ )); do bar+="░"; done
    printf "[%s] %3d%%" "$bar" "$pct"
}

# 1 if stdout is an interactive terminal, 0 otherwise (pipe / redirect / CI)
IS_TTY=0; [ -t 1 ] && IS_TTY=1
