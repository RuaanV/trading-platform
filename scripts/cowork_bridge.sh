#!/usr/bin/env bash
# =============================================================================
# cowork_bridge.sh — Cowork ↔ Trading Platform File Bridge
#
# Polls for trigger files written by Cowork and dispatches accordingly.
#
# PORTFOLIO_REFRESH_TRIGGER  → runs make load-personal-portfolio
# QUERY_TRIGGER              → runs query_google_holdings.py, writes QUERY_RESULT.txt
#
# Log: $LOG_FILE
#
# Started automatically by launchd (com.trading.cowork-bridge).
# =============================================================================

PROJECT_DIR="$HOME/code/trading-platform"

# Legacy ditto-session outputs (portfolio refresh trigger lives here)
_DITTO_OUTPUTS="$HOME/Library/Application Support/Claude/local-agent-mode-sessions/ab64c463-b4c4-4f84-96c5-942e11413f41/ac211dbb-0c41-4748-82c2-4d217cd62659/agent/local_ditto_ac211dbb-0c41-4748-82c2-4d217cd62659/outputs"

# Current active session outputs (query trigger + query script live here)
_ACTIVE_OUTPUTS="$HOME/Library/Application Support/Claude/local-agent-mode-sessions/ab64c463-b4c4-4f84-96c5-942e11413f41/ac211dbb-0c41-4748-82c2-4d217cd62659/local_1c4634f6-f437-4577-b625-9aaf8099c106/outputs"

TRIGGER_FILE="$_DITTO_OUTPUTS/PORTFOLIO_REFRESH_TRIGGER"
RESULT_FILE="$_DITTO_OUTPUTS/PORTFOLIO_REFRESH_RESULT.txt"

QUERY_TRIGGER_FILE="$_ACTIVE_OUTPUTS/QUERY_TRIGGER"
QUERY_SCRIPT="$_ACTIVE_OUTPUTS/query_google_holdings.py"
QUERY_RESULT_FILE="$_ACTIVE_OUTPUTS/QUERY_RESULT.txt"

LOG_FILE="$PROJECT_DIR/logs/cowork_bridge.log"
POLL_INTERVAL=10  # seconds between checks

# ---------------------------------------------------------------------------
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_FILE"
}

run_refresh() {
    log "Trigger detected — starting portfolio refresh"

    # Remove the trigger first so Cowork can't double-trigger
    rm -f "$TRIGGER_FILE"

    # Run the refresh inside the project directory, capture all output
    local output
    local exit_code
    output=$(cd "$PROJECT_DIR" && make load-personal-portfolio 2>&1)
    exit_code=$?

    local timestamp
    timestamp="$(date '+%Y-%m-%d %H:%M:%S')"

    if [ "$exit_code" -eq 0 ]; then
        log "Refresh SUCCEEDED (exit 0)"
        {
            echo "STATUS=SUCCESS"
            echo "TIMESTAMP=$timestamp"
            echo "EXIT_CODE=0"
            echo "---"
            echo "$output"
        } > "$RESULT_FILE"
    else
        log "Refresh FAILED (exit $exit_code)"
        {
            echo "STATUS=FAILED"
            echo "TIMESTAMP=$timestamp"
            echo "EXIT_CODE=$exit_code"
            echo "---"
            echo "$output"
        } > "$RESULT_FILE"
    fi

    log "Result written to PORTFOLIO_REFRESH_RESULT.txt"
}

run_query() {
    log "QUERY_TRIGGER detected — running query_google_holdings.py"

    rm -f "$QUERY_TRIGGER_FILE"

    local output
    local exit_code
    output=$(cd "$PROJECT_DIR" && "$PROJECT_DIR/.venv/bin/python" "$QUERY_SCRIPT" 2>&1)
    exit_code=$?

    local timestamp
    timestamp="$(date '+%Y-%m-%d %H:%M:%S')"

    if [ "$exit_code" -eq 0 ]; then
        log "Query SUCCEEDED (exit 0)"
        # The script writes QUERY_RESULT.txt itself; also prepend status header
        {
            echo "STATUS=SUCCESS"
            echo "TIMESTAMP=$timestamp"
            echo "EXIT_CODE=0"
            echo "---"
            echo "$output"
        } > "$QUERY_RESULT_FILE"
    else
        log "Query FAILED (exit $exit_code)"
        {
            echo "STATUS=FAILED"
            echo "TIMESTAMP=$timestamp"
            echo "EXIT_CODE=$exit_code"
            echo "---"
            echo "$output"
        } > "$QUERY_RESULT_FILE"
    fi

    log "Query result written to QUERY_RESULT.txt"
}

# ---------------------------------------------------------------------------
log "cowork_bridge.sh started (PID $$, poll interval ${POLL_INTERVAL}s)"
log "Watching for refresh trigger at: $TRIGGER_FILE"
log "Watching for query trigger at:   $QUERY_TRIGGER_FILE"

while true; do
    if [ -f "$TRIGGER_FILE" ]; then
        run_refresh
    fi
    if [ -f "$QUERY_TRIGGER_FILE" ]; then
        run_query
    fi
    sleep "$POLL_INTERVAL"
done
