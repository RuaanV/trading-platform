"""Workflow tests for holding analytics and recommender backtesting."""

from __future__ import annotations

import os
from pathlib import Path
import unittest

import pandas as pd

from src.analytics.evaluate_holdings_history import build_holding_performance_report
from src.backtesting.backtest_recommender import build_backtest_outputs
from src.recommender.generate_recommendations import _load_optional_holdings


class BacktestingWorkflowTest(unittest.TestCase):
    def test_reports_and_backtest_from_fixture_inputs(self) -> None:
        holdings_fixture = (
            Path(__file__).resolve().parents[1] / "data" / "fixtures" / "recommender_holdings.csv"
        )
        dates = pd.bdate_range("2025-08-01", periods=170, tz="UTC")
        price_rows: list[dict[str, object]] = []

        for index, date in enumerate(dates):
            price_rows.extend(
                [
                    {"date": date.isoformat(), "symbol": "MSFT", "close": 100 + (index * 0.60)},
                    {"date": date.isoformat(), "symbol": "AAPL", "close": 100 + (index * 0.18)},
                    {"date": date.isoformat(), "symbol": "GOOG", "close": 140 - (index * 0.30)},
                    {"date": date.isoformat(), "symbol": "SPY", "close": 100 + (index * 0.12)},
                ]
            )

        history = pd.DataFrame(price_rows)

        original_holdings_env = os.environ.get("RECOMMENDER_HOLDINGS_PATH")
        try:
            os.environ["RECOMMENDER_HOLDINGS_PATH"] = str(holdings_fixture)
            holdings = _load_optional_holdings()
        finally:
            if original_holdings_env is None:
                os.environ.pop("RECOMMENDER_HOLDINGS_PATH", None)
            else:
                os.environ["RECOMMENDER_HOLDINGS_PATH"] = original_holdings_env

        report = build_holding_performance_report(holdings, history)
        backtest_history, backtest_summary = build_backtest_outputs(holdings, history)

        self.assertFalse(report.empty)
        self.assertIn("return_3m", report.columns)
        self.assertIn("relative_return_6m", report.columns)

        goog_row = report.loc[report["symbol"] == "GOOG"].iloc[0]
        self.assertLess(float(goog_row["return_6m"]), 0.0)

        self.assertFalse(backtest_history.empty)
        self.assertFalse(backtest_summary.empty)
        self.assertIn("max_drawdown", backtest_summary.columns)
        self.assertTrue(backtest_history["top_actions"].str.contains(r"GOOG:(?:TRIM|EXIT)", regex=True).any())


if __name__ == "__main__":
    unittest.main()
