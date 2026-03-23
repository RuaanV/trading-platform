"""Unit tests for trade candidate generation."""

from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from src.strategies import generate_trade_candidates as candidate_generator


class GenerateTradeCandidatesTest(unittest.TestCase):
    def test_fixture_holdings_produce_trim_and_add_sides(self) -> None:
        fixture_path = (
            Path(__file__).resolve().parents[1] / "data" / "fixtures" / "recommender_holdings.csv"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            scores_path = tmpdir_path / "scores.csv"
            candidates_path = tmpdir_path / "candidates.csv"

            scores_path.write_text(
                "symbol,score\nMSFT,0.62\nAMZN,0.58\nAAPL,0.57\nBA.L,0.56\nGOOG,0.55\n",
                encoding="utf-8",
            )

            original_scores_path = candidate_generator._load_scores.__globals__["SCORES_PATH"]
            original_candidates_path = candidate_generator.CANDIDATES_PATH
            original_fixture_env = os.environ.get("RECOMMENDER_HOLDINGS_PATH")

            try:
                candidate_generator._load_scores.__globals__["SCORES_PATH"] = scores_path
                candidate_generator.CANDIDATES_PATH = candidates_path
                os.environ["RECOMMENDER_HOLDINGS_PATH"] = str(fixture_path)

                candidate_generator.generate_trade_candidates()

                results = pd.read_csv(candidates_path)
                sides = dict(zip(results["symbol"], results["side"], strict=False))
                ranks = dict(zip(results["symbol"], results["rank"], strict=False))

                self.assertEqual(sides["MSFT"], "ADD")
                self.assertEqual(sides["AMZN"], "HOLD")
                self.assertEqual(sides["AAPL"], "HOLD")
                self.assertEqual(sides["BA.L"], "WATCH")
                self.assertEqual(sides["GOOG"], "TRIM")
                self.assertEqual(ranks["MSFT"], 1)
                self.assertEqual(ranks["GOOG"], 5)
            finally:
                candidate_generator._load_scores.__globals__["SCORES_PATH"] = original_scores_path
                candidate_generator.CANDIDATES_PATH = original_candidates_path
                if original_fixture_env is None:
                    os.environ.pop("RECOMMENDER_HOLDINGS_PATH", None)
                else:
                    os.environ["RECOMMENDER_HOLDINGS_PATH"] = original_fixture_env


if __name__ == "__main__":
    unittest.main()
