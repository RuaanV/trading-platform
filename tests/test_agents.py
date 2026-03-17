"""Tests for the baseline agent scaffolding."""

from __future__ import annotations

import unittest

import pandas as pd

from src.agents.base import AgentContext
from src.agents.market_analysis import MarketAnalysisAgent
from src.agents.registry import build_default_registry


class MarketAnalysisAgentTest(unittest.TestCase):
    def test_agent_returns_buy_review_and_trim_actions(self) -> None:
        scores = pd.DataFrame(
            [
                {"symbol": "MSFT", "score": 0.63},
                {"symbol": "AAPL", "score": 0.56},
                {"symbol": "GOOG", "score": 0.42},
                {"symbol": "AMZN", "score": 0.39},
            ]
        )
        candidates = pd.DataFrame(
            [
                {"symbol": "MSFT", "rank": 1},
                {"symbol": "AAPL", "rank": 2},
                {"symbol": "GOOG", "rank": 3},
                {"symbol": "AMZN", "rank": 4},
            ]
        )
        holdings = pd.DataFrame(
            [
                {"ticker": "GOOG", "market_value": 2200},
                {"ticker": "AAPL", "market_value": 800},
            ]
        )

        agent = MarketAnalysisAgent()
        result = agent.run(AgentContext(scores=scores, candidates=candidates, holdings=holdings))

        actions = {action["symbol"]: action["action"] for action in result.actions}
        self.assertEqual(actions["MSFT"], "BUY")
        self.assertEqual(actions["AAPL"], "REVIEW")
        self.assertEqual(actions["GOOG"], "TRIM")
        self.assertNotIn("AMZN", actions)
        self.assertIn("Priority actions", result.summary)

    def test_registry_exposes_market_analysis_agent(self) -> None:
        registry = build_default_registry()
        self.assertIn("market_analysis", registry)


if __name__ == "__main__":
    unittest.main()
