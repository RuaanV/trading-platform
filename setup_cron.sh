#!/bin/bash
# Adds portfolio price refresh and news sentiment cron jobs to your Mac's crontab.
#
# Price refresh runs 4x per trading day (Mon–Fri, UTC):
#   08:05  → ~09:05 BST / 08:05 GMT  (near UK open)
#   13:00  → ~14:00 BST / 13:00 GMT  (mid-day)
#   16:35  → ~17:35 BST / 16:35 GMT  (after UK close at 16:30)
#   21:05  → ~17:05 EDT / 16:05 EST  (after US close at 16:00 ET)
#
# News sentiment runs once daily at 21:30 UTC (after final US close price refresh).
#
# Run once: bash setup_cron.sh

PROJECT_DIR="/Users/ruaan.venter/code/trading-platform"
PYTHON="$PROJECT_DIR/.venv/bin/python"
PRICE_LOG="$PROJECT_DIR/logs/price_refresh.log"
NEWS_LOG="$PROJECT_DIR/logs/news_refresh.log"

PRICE_CMD="cd $PROJECT_DIR && TZ=UTC $PYTHON $PROJECT_DIR/data_pipeline/load_personal_portfolio.py >> $PRICE_LOG 2>&1 && $PYTHON $PROJECT_DIR/data_pipeline/load_isa_portfolio.py >> $PRICE_LOG 2>&1"

PRICE_CRON_1=" 5  8 * * 1-5 $PRICE_CMD"
PRICE_CRON_2=" 0 13 * * 1-5 $PRICE_CMD"
PRICE_CRON_3="35 16 * * 1-5 $PRICE_CMD"
PRICE_CRON_4=" 5 21 * * 1-5 $PRICE_CMD"

NEWS_CRON="30 21 * * 1-5 cd $PROJECT_DIR && TZ=UTC $PYTHON $PROJECT_DIR/data_pipeline/refresh_holding_news.py >> $NEWS_LOG 2>&1"

# Create logs dir if it doesn't exist
mkdir -p "$PROJECT_DIR/logs"

# Remove any old single price cron entries before adding the new 4x schedule
CURRENT_CRONTAB=$(crontab -l 2>/dev/null)
CLEANED=$(echo "$CURRENT_CRONTAB" | grep -vF "load_personal_portfolio.py" | grep -vF "load_isa_portfolio.py")

NEW_CRONTAB="$CLEANED
$PRICE_CRON_1
$PRICE_CRON_2
$PRICE_CRON_3
$PRICE_CRON_4"

echo "$NEW_CRONTAB" | crontab -
echo "✓ Price cron jobs set — 4x daily at 08:05, 13:00, 16:35, 21:05 UTC (Mon–Fri)."

# News sentiment job (idempotent)
if crontab -l 2>/dev/null | grep -qF "refresh_holding_news.py"; then
    echo "✓ News sentiment cron job already exists — no changes made."
else
    ( crontab -l 2>/dev/null; echo "$NEWS_CRON" ) | crontab -
    echo "✓ News sentiment cron job added. Runs at 21:30 UTC (Mon–Fri)."
fi

echo ""
echo "Current crontab:"
crontab -l
