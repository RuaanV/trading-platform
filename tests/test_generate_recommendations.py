"""Unit tests for the batch recommender."""

from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from src.recommender import generate_recommendations as recommender


class GenerateRecommendationsTest(unittest.TestCase):
    def test_alias_holding_symbol_matches_recommendation_symbol(self) -> None:
        scores = pd.DataFrame([{"symbol": "BA.L", "score": 0.56}])
        candidates = pd.DataFrame([{"symbol": "BA.L", "rank": 1}])
        holdings = pd.DataFrame(
            [
                {
                    "ticker": "BA.",
                    "market_value": 100.0,
                    "company": "BAE Systems",
                    "portfolio_name": "SIPP",
                    "snapshot_at": "2026-03-19T09:00:00+00:00",
                }
            ]
        )

        normalized_holdings = recommender._normalize_holdings(holdings)
        self.assertEqual(normalized_holdings.iloc[0]["ticker"], "BA.L")

        recommendations = recommender.build_recommendations(
            scores=scores,
            candidates=candidates,
            holdings=normalized_holdings,
            generated_at="2026-03-19T09:00:00+00:00",
        )

        self.assertEqual(recommendations.iloc[0]["symbol"], "BA.L")
        self.assertEqual(recommendations.iloc[0]["action"], "TRIM")
        self.assertIn("existing weight 100.0%", recommendations.iloc[0]["rationale"])

    def test_fixture_holdings_trim_oversized_mid_conviction_position(self) -> None:
        fixture_path = (
            Path(__file__).resolve().parents[1] / "data" / "fixtures" / "recommender_holdings.csv"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            scores_path = tmpdir_path / "scores.csv"
            candidates_path = tmpdir_path / "candidates.csv"
            recommendations_path = tmpdir_path / "recommendations.csv"

            scores_path.write_text(
                "symbol,score\nMSFT,0.62\nAMZN,0.58\nAAPL,0.57\nBA.L,0.56\nGOOG,0.55\n",
                encoding="utf-8",
            )
            candidates_path.write_text(
                "symbol,side,score,rank\nMSFT,BUY,0.62,1\nAMZN,BUY,0.58,2\nAAPL,BUY,0.57,3\nBA.L,BUY,0.56,4\nGOOG,BUY,0.55,5\n",
                encoding="utf-8",
            )

            original_scores_path = recommender.SCORES_PATH
            original_candidates_path = recommender.CANDIDATES_PATH
            original_recommendations_path = recommender.RECOMMENDATIONS_PATH
            original_fixture_env = os.environ.get(recommender.HOLDINGS_FIXTURE_ENV)

            try:
                recommender.SCORES_PATH = scores_path
                recommender.CANDIDATES_PATH = candidates_path
                recommender.RECOMMENDATIONS_PATH = recommendations_path
                os.environ[recommender.HOLDINGS_FIXTURE_ENV] = str(fixture_path)

                recommender.generate_recommendations()

                results = pd.read_csv(recommendations_path)
                actions = dict(zip(results["symbol"], results["action"], strict=False))
                target_weights = dict(zip(results["symbol"], results["target_weight"], strict=False))

                self.assertEqual(actions["MSFT"], "ADD")
                self.assertEqual(actions["AMZN"], "HOLD")
                self.assertEqual(actions["AAPL"], "HOLD")
                self.assertEqual(actions["BA.L"], "WATCH")
                self.assertEqual(actions["GOOG"], "TRIM")
                self.assertAlmostEqual(target_weights["BA.L"], 0.02, places=4)
                self.assertAlmostEqual(target_weights["AMZN"], 0.08, places=4)
                self.assertAlmostEqual(target_weights["GOOG"], 0.08, places=4)
            finally:
                recommender.SCORES_PATH = original_scores_path
                recommender.CANDIDATES_PATH = original_candidates_path
                recommender.RECOMMENDATIONS_PATH = original_recommendations_path
                if original_fixture_env is None:
                    os.environ.pop(recommender.HOLDINGS_FIXTURE_ENV, None)
                else:
                    os.environ[recommender.HOLDINGS_FIXTURE_ENV] = original_fixture_env


if __name__ == "__main__":
    unittest.main()
