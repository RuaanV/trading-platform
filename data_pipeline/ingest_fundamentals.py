"""Ingest fundamentals/filings data into warehouse staging tables."""

from datetime import datetime, timezone


def ingest_fundamentals() -> None:
    # TODO: Replace mock ingestion with your fundamentals provider integration.
    print(f"[{datetime.now(timezone.utc).isoformat()}] ingest_fundamentals: stub run complete")


if __name__ == "__main__":
    ingest_fundamentals()
