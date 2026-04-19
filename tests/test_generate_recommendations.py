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

    def test_fund_name_aliases_match_recommendation_symbols(self) -> None:
        scores = pd.DataFrame(
            [
                {"symbol": "0P0000W36K.L", "score": 0.54},
                {"symbol": "0P0001CBJA.L", "score": 0.54},
            ]
        )
        candidates = pd.DataFrame(
            [
                {"symbol": "0P0000W36K.L", "rank": 1},
                {"symbol": "0P0001CBJA.L", "rank": 2},
            ]
        )
        holdings = pd.DataFrame(
            [
                {
                    "ticker": "Artemis Global Income",
                    "market_value": 100.0,
                    "company": "Artemis Global Income",
                    "portfolio_name": "SIPP",
                    "snapshot_at": "2026-04-03T11:10:00+00:00",
                },
                {
                    "ticker": "Troy Trojan (Class X)",
                    "market_value": 100.0,
                    "company": "Troy Trojan (Class X)",
                    "portfolio_name": "SIPP",
                    "snapshot_at": "2026-04-03T11:10:00+00:00",
                },
                {
                    "ticker": "MSFT",
                    "market_value": 1000.0,
                    "company": "MSFT",
                    "portfolio_name": "SIPP",
                    "snapshot_at": "2026-04-03T11:10:00+00:00",
                },
            ]
        )

        normalized_holdings = recommender._normalize_holdings(holdings)
        self.assertIn("0P0000W36K.L", set(normalized_holdings["ticker"]))
        self.assertIn("0P0001CBJA.L", set(normalized_holdings["ticker"]))

        recommendations = recommender.build_recommendations(
            scores=scores,
            candidates=candidates,
            holdings=normalized_holdings,
            generated_at="2026-04-03T11:10:00+00:00",
        )

        actions = dict(zip(recommendations["symbol"], recommendations["action"], strict=False))
        self.assertEqual(actions["0P0000W36K.L"], "HOLD")
        self.assertEqual(actions["0P0001CBJA.L"], "HOLD")

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
                (
                    "symbol,score\n"
                    "MSFT,0.62\n"
                    "NVDA,0.61\n"
                    "AMZN,0.58\n"
                    "AAPL,0.57\n"
                    "BA.L,0.56\n"
                    "GOOG,0.55\n"
                    "0P0000RU81.L,0.55\n"
                    "0P0001FE43.L,0.54\n"
                    "0P0001GZXO.L,0.54\n"
                    "0P0000W36K.L,0.54\n"
                    "0P0001CBJA.L,0.54\n"
                    "IUKD.L,0.54\n"
                    "ISF.L,0.53\n"
                    "GSK.L,0.53\n"
                    "HLN.L,0.53\n"
                    "LLOY.L,0.52\n"
                    "NWG.L,0.52\n"
                    "VOD.L,0.51\n"
                    "ASC.L,0.48\n"
                    "RGTI,0.46\n"
                ),
                encoding="utf-8",
            )
            candidates_path.write_text(
                (
                    "symbol,side,score,rank\n"
                    "MSFT,BUY,0.62,1\n"
                    "NVDA,BUY,0.61,2\n"
                    "AMZN,BUY,0.58,2\n"
                    "AAPL,BUY,0.57,3\n"
                    "BA.L,BUY,0.56,4\n"
                    "GOOG,BUY,0.55,5\n"
                    "0P0000RU81.L,BUY,0.55,6\n"
                    "0P0001FE43.L,BUY,0.54,7\n"
                    "0P0001GZXO.L,BUY,0.54,8\n"
                    "0P0000W36K.L,BUY,0.54,9\n"
                    "0P0001CBJA.L,BUY,0.54,10\n"
                    "IUKD.L,BUY,0.54,11\n"
                    "ISF.L,BUY,0.53,12\n"
                    "GSK.L,BUY,0.53,13\n"
                    "HLN.L,BUY,0.53,14\n"
                    "LLOY.L,BUY,0.52,15\n"
                    "NWG.L,BUY,0.52,16\n"
                    "VOD.L,BUY,0.51,17\n"
                    "ASC.L,BUY,0.48,18\n"
                    "RGTI,BUY,0.46,19\n"
                ),
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
                self.assertEqual(actions["BA.L"], "HOLD")
                self.assertEqual(actions["GOOG"], "TRIM")
                self.assertEqual(actions["0P0000RU81.L"], "HOLD")
                self.assertEqual(actions["0P0001FE43.L"], "HOLD")
                self.assertEqual(actions["0P0001GZXO.L"], "HOLD")
                self.assertEqual(actions["0P0000W36K.L"], "HOLD")
                self.assertEqual(actions["0P0001CBJA.L"], "HOLD")
                self.assertAlmostEqual(target_weights["BA.L"], 0.0505, places=4)
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
