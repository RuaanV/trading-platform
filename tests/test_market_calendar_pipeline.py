"""Tests for market calendar scaffolding."""

from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd

from data_pipeline.ingest_market_calendar import (
    _normalize_event_timestamp,
    build_market_calendar,
)


class MarketCalendarPipelineTest(unittest.TestCase):
    def test_normalize_event_timestamp_handles_sequence(self) -> None:
        timestamp = _normalize_event_timestamp(
            [None, "2026-07-24T20:00:00Z", "2026-08-01T20:00:00Z"]
        )

        self.assertIsNotNone(timestamp)
        self.assertEqual(timestamp.isoformat(), "2026-07-24T20:00:00+00:00")

    def test_build_market_calendar_filters_fixture_rows_by_year_and_symbol(self) -> None:
        fixture = pd.DataFrame(
            [
                {
                    "symbol": "GOOG",
                    "company": "Alphabet Inc",
                    "event_type": "earnings",
                    "event_name": "Earnings Date",
                    "event_date": "2026-04-25",
                    "event_timestamp": "2026-04-25T20:00:00+00:00",
                    "calendar_year": 2026,
                    "source_provider": "yfinance",
                    "source_payload": json.dumps({"event": "earnings"}),
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                },
                {
                    "symbol": "NVDA",
                    "company": "NVIDIA Corp",
                    "event_type": "earnings",
                    "event_name": "Earnings Date",
                    "event_date": "2025-11-20",
                    "event_timestamp": "2025-11-20T21:00:00+00:00",
                    "calendar_year": 2025,
                    "source_provider": "yfinance",
                    "source_payload": json.dumps({"event": "earnings"}),
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                },
            ]
        )

        with patch(
            "data_pipeline.ingest_market_calendar._load_fixture_events",
            return_value=fixture,
        ):
            result = build_market_calendar(
                provider="yfinance",
                symbols=["GOOG", "MSFT", "AAPL", "AMZN", "NVDA"],
                calendar_year=2026,
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["symbol"], "GOOG")
        self.assertEqual(int(result.iloc[0]["calendar_year"]), 2026)


if __name__ == "__main__":
    unittest.main()
