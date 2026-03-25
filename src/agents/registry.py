"""Agent registry helpers."""

from __future__ import annotations

from .base import BaseAgent
from .holding_news_sentiment import HoldingNewsSentimentAgent
from .market_analysis import MarketAnalysisAgent


def build_default_registry() -> dict[str, BaseAgent]:
    """Return the default agent set exposed by the platform."""

    market_analysis = MarketAnalysisAgent()
    holding_news_sentiment = HoldingNewsSentimentAgent()
    return {
        market_analysis.name: market_analysis,
        holding_news_sentiment.name: holding_news_sentiment,
    }
