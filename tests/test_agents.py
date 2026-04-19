"""Tests for the baseline agent scaffolding."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from src.agents.base import AgentContext
from src.agents.holding_news_sentiment import HoldingNewsSentimentAgent
from src.agents.market_analysis import MarketAnalysisAgent
from src.agents.registry import build_default_registry
from data_pipeline.holding_news import (
    SymbolNewsSentiment,
    refresh_symbol_news_sentiment,
    score_headline_sentiment,
    sentiment_label,
)
from data_pipeline.yahoo_symbols import resolve_yahoo_symbol


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
        self.assertIn("holding_news_sentiment", registry)

    @patch("src.agents.holding_news_sentiment.refresh_symbol_news_sentiment")
    def test_holding_news_sentiment_agent_returns_headline_actions(self, mock_refresh) -> None:
        mock_refresh.return_value = (
            pd.DataFrame(
                [
                    {
                        "symbol": "GOOG",
                        "published_at": pd.Timestamp("2026-03-25T09:00:00Z"),
                        "article_title": "Google beats expectations on cloud growth",
                        "article_link": "https://example.com/google-cloud",
                        "publisher_name": "Yahoo Finance",
                        "sentiment_score": 0.4,
                        "sentiment_label": "bullish",
                    }
                ]
            ),
            SymbolNewsSentiment(
                symbol="GOOG",
                headline_count=1,
                average_sentiment_score=0.4,
                sentiment_label="bullish",
                as_of_date="2026-03-25",
            ),
        )

        agent = HoldingNewsSentimentAgent()
        result = agent.run(AgentContext(scores=pd.DataFrame(), metadata={"symbol": "GOOG"}))

        self.assertEqual(result.agent_name, "holding_news_sentiment")
        self.assertEqual(result.metrics["headline_count"], 1.0)
        self.assertEqual(result.metadata["sentiment_label"], "bullish")
        self.assertEqual(result.actions[0]["symbol"], "GOOG")
        self.assertEqual(result.actions[0]["sentiment_label"], "bullish")


class HoldingNewsSentimentHelpersTest(unittest.TestCase):
    def test_positive_headline_scores_above_negative_headline(self) -> None:
        positive_score = score_headline_sentiment("Google beats expectations and raises guidance")
        negative_score = score_headline_sentiment("Google faces antitrust probe and cuts guidance")

        self.assertGreater(positive_score, 0.0)
        self.assertLess(negative_score, 0.0)
        self.assertEqual(sentiment_label(positive_score), "bullish")
        self.assertEqual(sentiment_label(negative_score), "bearish")

    def test_resolve_yahoo_symbol_maps_london_listings(self) -> None:
        self.assertEqual(resolve_yahoo_symbol("LLOY"), "LLOY.L")
        self.assertEqual(resolve_yahoo_symbol("MSFT"), "MSFT")

    @patch("data_pipeline.holding_news.store_symbol_news_sentiment")
    @patch("data_pipeline.holding_news.fetch_yahoo_finance_headlines")
    def test_refresh_symbol_news_sentiment_uses_provider_symbol_but_stores_holding_symbol(
        self,
        mock_fetch,
        mock_store,
    ) -> None:
        mock_fetch.return_value = pd.DataFrame(
            [
                {
                    "symbol": "LLOY.L",
                    "article_id": "abc",
                    "published_at": pd.Timestamp("2026-03-26T08:00:00Z"),
                    "article_title": "Lloyds updates outlook",
                    "article_summary": None,
                    "article_link": "https://example.com/lloyds",
                    "source_name": "Yahoo Finance",
                    "publisher_name": "Yahoo Finance",
                    "sentiment_score": 0.1,
                    "sentiment_label": "neutral",
                    "sentiment_version": "headline_lexicon_v1",
                    "article_payload": {},
                }
            ]
        )

        headlines, summary = refresh_symbol_news_sentiment("LLOY")

        mock_fetch.assert_called_once_with(
            "LLOY.L",
            target_date=None,
            timezone_name="Europe/London",
        )
        mock_store.assert_called_once()
        self.assertEqual(headlines.iloc[0]["symbol"], "LLOY")
        self.assertIsNotNone(summary)
        self.assertEqual(summary.symbol, "LLOY")


if __name__ == "__main__":
    unittest.main()
