"""Batch refresh of Yahoo Finance news sentiment for all active portfolio holdings.

Intended to be run as a daily cron job after the price refresh, e.g. 17:00 UTC Mon–Fri.
Skips cash positions and fund codes (symbols starting with '0P').
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from data_pipeline.holding_news import refresh_symbol_news_sentiment
from data_pipeline.personal_portfolios import fetch_portfolio_holdings

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    level=logging.INFO,
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

SKIP_SYMBOLS = {"CASH"}


def _is_skippable(symbol: str) -> bool:
    upper = symbol.strip().upper()
    return upper in SKIP_SYMBOLS or upper.startswith("0P")


def run() -> None:
    started_at = datetime.now(timezone.utc)
    log.info("news_refresh started at=%s", started_at.isoformat())

    holdings = fetch_portfolio_holdings()
    if holdings.empty:
        log.info("no holdings found — nothing to refresh")
        return

    symbols = sorted(
        {str(t).strip().upper() for t in holdings["ticker"].dropna() if not _is_skippable(str(t))}
    )
    log.info("symbols_to_refresh count=%d symbols=%s", len(symbols), symbols)

    success, skipped, failed = 0, 0, 0
    for symbol in symbols:
        try:
            headlines, summary = refresh_symbol_news_sentiment(symbol)
            count = len(headlines)
            if count == 0:
                log.info("symbol=%s status=no_news_today", symbol)
                skipped += 1
            else:
                label = summary.sentiment_label if summary else "unknown"
                log.info("symbol=%s status=ok headlines=%d sentiment=%s", symbol, count, label)
                success += 1
        except Exception as exc:  # noqa: BLE001
            log.error("symbol=%s status=error error=%s", symbol, exc)
            failed += 1

    finished_at = datetime.now(timezone.utc)
    duration = (finished_at - started_at).total_seconds()
    log.info(
        "news_refresh finished at=%s duration_s=%.1f success=%d skipped=%d failed=%d",
        finished_at.isoformat(),
        duration,
        success,
        skipped,
        failed,
    )


if __name__ == "__main__":
    run()
