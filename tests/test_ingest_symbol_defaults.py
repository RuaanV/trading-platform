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
                [
                    "AAPL",
                    "AMZN",
                    "GOOG",
                    "MSFT",
                    "NVDA",
                    "RGTI",
                    "ASC.L",
                    "BA.L",
                    "GSK.L",
                    "HLN.L",
                    "ISF.L",
                    "IUKD.L",
                    "LLOY.L",
                    "NWG.L",
                    "VOD.L",
                    "0P0000RU81.L",
                    "0P0001FE43.L",
                    "0P0001GZXO.L",
                    "0P0000W36K.L",
                    "0P0001CBJA.L",
                ],
            )

    def test_company_data_prefers_explicit_symbols(self) -> None:
        with patch.dict(os.environ, {"YF_SYMBOLS": "msft, ba.l"}, clear=True):
            self.assertEqual(ingest_company_data._symbols_to_ingest(), ["MSFT", "BA.L"])

    def test_market_calendar_defaults_include_bae_systems(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                ingest_market_calendar._symbols_to_ingest(),
                [
                    "GOOG",
                    "MSFT",
                    "AAPL",
                    "AMZN",
                    "NVDA",
                    "RGTI",
                    "ASC.L",
                    "BA.L",
                    "GSK.L",
                    "HLN.L",
                    "ISF.L",
                    "IUKD.L",
                    "LLOY.L",
                    "NWG.L",
                    "VOD.L",
                    "0P0000RU81.L",
                    "0P0001FE43.L",
                    "0P0001GZXO.L",
                    "0P0000W36K.L",
                    "0P0001CBJA.L",
                ],
            )

    def test_market_calendar_prefers_explicit_symbols(self) -> None:
        with patch.dict(os.environ, {"MARKET_CALENDAR_SYMBOLS": "nvda, ba.l"}, clear=True):
            self.assertEqual(ingest_market_calendar._symbols_to_ingest(), ["NVDA", "BA.L"])


if __name__ == "__main__":
    unittest.main()
