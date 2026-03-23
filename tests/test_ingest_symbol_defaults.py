"""Tests for default tracked symbol sets used by ingestion scripts."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from data_pipeline import ingest_company_data, ingest_market_calendar


class IngestSymbolDefaultsTest(unittest.TestCase):
    def test_company_data_defaults_include_bae_systems(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                ingest_company_data._symbols_to_ingest(),
                ["AAPL", "AMZN", "GOOG", "MSFT", "BA.L"],
            )

    def test_company_data_prefers_explicit_symbols(self) -> None:
        with patch.dict(os.environ, {"YF_SYMBOLS": "msft, ba.l"}, clear=True):
            self.assertEqual(ingest_company_data._symbols_to_ingest(), ["MSFT", "BA.L"])

    def test_market_calendar_defaults_include_bae_systems(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                ingest_market_calendar._symbols_to_ingest(),
                ["GOOG", "MSFT", "AAPL", "AMZN", "NVDA", "BA.L"],
            )

    def test_market_calendar_prefers_explicit_symbols(self) -> None:
        with patch.dict(os.environ, {"MARKET_CALENDAR_SYMBOLS": "nvda, ba.l"}, clear=True):
            self.assertEqual(ingest_market_calendar._symbols_to_ingest(), ["NVDA", "BA.L"])


if __name__ == "__main__":
    unittest.main()
