#!/bin/bash
# Adds the portfolio price refresh cron job to your Mac's crontab.
# Schedule matches the Cowork scheduled task: 16:45 UTC, Mon–Fri
# (which is 17:45 BST in summer, 16:45 GMT in winter).
#
# Run once: bash setup_cron.sh

PROJECT_DIR="/Users/ruaan.venter/code/trading-platform"
PYTHON="$PROJECT_DIR/.venv/bin/python"
PRICE_LOG="$PROJECT_DIR/logs/price_refresh.log"
NEWS_LOG="$PROJECT_DIR/logs/news_refresh.log"

PRICE_CRON="45 16 * * 1-5 cd $PROJECT_DIR && TZ=UTC $PYTHON $PROJECT_DIR/data_pipeline/load_personal_portfolio.py >> $PRICE_LOG 2>&1 && $PYTHON $PROJECT_DIR/data_pipeline/load_isa_portfolio.py >> $PRICE_LOG 2>&1"
NEWS_CRON="0 17 * * 1-5 cd $PROJECT_DIR && TZ=UTC $PYTHON $PROJECT_DIR/data_pipeline/refresh_holding_news.py >> $NEWS_LOG 2>&1"

# Create logs dir if it doesn't exist
mkdir -p "$PROJECT_DIR/logs"

# Price refresh job
if crontab -l 2>/dev/null | grep -qF "load_personal_portfolio.py"; then
    echo "✓ Price cron job already exists — no changes made."
else
    ( crontab -l 2>/dev/null; echo "$PRICE_CRON" ) | crontab -
    echo "✓ Price cron job added. Runs at 16:45 UTC (Mon–Fri)."
fi

# News sentiment job
if crontab -l 2>/dev/null | grep -qF "refresh_holding_news.py"; then
    echo "✓ News sentiment cron job already exists — no changes made."
else
    ( crontab -l 2>/dev/null; echo "$NEWS_CRON" ) | crontab -
    echo "✓ News sentiment cron job added. Runs at 17:00 UTC (Mon–Fri)."
fi

echo ""
echo "Current crontab:"
crontab -l
