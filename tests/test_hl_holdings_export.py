from datetime import datetime
from decimal import Decimal
from pathlib import Path
import sys
import tempfile
from unittest import TestCase

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "data_pipeline"))

from data_pipeline.personal_portfolios import (
    _normalize_json,
    _prepare_holdings_frame,
    hl_export_created_at,
    is_hl_export,
    read_hl_holdings_export,
)
from data_pipeline.yahoo_symbols import YAHOO_SYMBOLS, is_sedol

FIXTURE = Path(__file__).resolve().parents[1] / "data" / "fixtures" / "hl_portfolio_summary_sample.csv"


class HlHoldingsExportTests(TestCase):
    def test_detects_hl_export_and_rejects_plain_csv(self) -> None:
        self.assertTrue(is_hl_export(FIXTURE))
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as handle:
            handle.write("company,ticker,quantity\nApple,AAPL,1\n")
        self.assertFalse(is_hl_export(handle.name))

    def test_reads_created_at(self) -> None:
        self.assertEqual(hl_export_created_at(FIXTURE), datetime(2026, 1, 15, 9, 5))

    def test_skips_summary_block_totals_and_footnotes_and_adds_cash(self) -> None:
        holdings = read_hl_holdings_export(FIXTURE)
        self.assertEqual(list(holdings["ticker"]), ["GOOG", "LLOY", "B41YBW7", "CASH"])
        cash = holdings[holdings["ticker"] == "CASH"].iloc[0]
        self.assertEqual(cash["company"], "Cash")
        self.assertEqual(cash["market_value"], "1,000.50")

    def test_prepared_values_are_pounds_with_separators_removed(self) -> None:
        prepared = _prepare_holdings_frame(read_hl_holdings_export(FIXTURE)).set_index("ticker")
        self.assertEqual(prepared.loc["GOOG", "price"], Decimal("200.00"))
        self.assertEqual(prepared.loc["GOOG", "company"], "Alphabet Inc NPV C")
        self.assertEqual(prepared.loc["LLOY", "quantity"], Decimal("1500"))
        self.assertEqual(prepared.loc["LLOY", "price"], Decimal("0.985"))
        self.assertEqual(prepared.loc["B41YBW7", "quantity"], Decimal("1196.350"))
        self.assertEqual(prepared.loc["B41YBW7", "market_value"], Decimal("8882.37"))
        self.assertEqual(prepared.loc["B41YBW7", "gain_loss_value"], Decimal("1882.37"))
        self.assertEqual(prepared.loc["B41YBW7", "gain_loss_pct"], Decimal("26.89"))
        self.assertEqual(prepared.loc["CASH", "market_value"], Decimal("1000.50"))
        self.assertEqual(prepared.loc["GOOG", "quantity_label"], "Units")
        self.assertEqual(str(prepared.loc["GOOG", "as_of_date"]), "2026-01-15")

    def test_source_rows_are_json_serialisable(self) -> None:
        prepared = _prepare_holdings_frame(read_hl_holdings_export(FIXTURE))
        for source_row in prepared["source_row"]:
            self.assertIsInstance(_normalize_json(source_row), str)

    def test_sedol_detection_and_fund_mapping(self) -> None:
        self.assertTrue(is_sedol("B41YBW7"))
        self.assertTrue(is_sedol("BQVVWL8"))
        for code in ["GOOG", "BA.", "IUKD", "CASH", "", None]:
            self.assertFalse(is_sedol(code))
        self.assertEqual(YAHOO_SYMBOLS["B41YBW7"], "0P0000RU81.L")
