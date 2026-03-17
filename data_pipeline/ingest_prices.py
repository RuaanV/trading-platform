"""Fetch latest stock prices using yfinance, Massive API, or both."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from price_providers import PriceProviderError, get_latest_price

def ingest_prices() -> None:
    symbol = os.getenv("PRICE_SYMBOL", "AAPL")
    provider = os.getenv("PRICE_PROVIDER", "yfinance").strip().lower()
    run_at = datetime.now(timezone.utc).isoformat()

    providers = ["yfinance", "massive"] if provider == "both" else [provider]
    quotes: list[str] = []

    for selected in providers:
        try:
            quote = get_latest_price(symbol, selected)
            quotes.append(
                f"{quote.provider}: {quote.symbol} price={quote.price:.4f} {quote.currency} as_of={quote.as_of}"
            )
        except PriceProviderError as exc:
            quotes.append(f"{selected}: ERROR {exc}")

    print(f"[{run_at}] ingest_prices")
    for line in quotes:
        print(f"  - {line}")


if __name__ == "__main__":
    ingest_prices()
