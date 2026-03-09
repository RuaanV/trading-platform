"""Ingest daily OHLCV price data into warehouse staging tables."""

from datetime import datetime, timezone


def ingest_prices() -> None:
    # TODO: Replace mock ingestion with your real market data provider client.
    print(f"[{datetime.now(timezone.utc).isoformat()}] ingest_prices: stub run complete")


if __name__ == "__main__":
    ingest_prices()
