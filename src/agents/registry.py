"""Agent registry helpers."""

from __future__ import annotations

from .base import BaseAgent
from .market_analysis import MarketAnalysisAgent


def build_default_registry() -> dict[str, BaseAgent]:
    """Return the default agent set exposed by the platform."""

    market_analysis = MarketAnalysisAgent()
    return {market_analysis.name: market_analysis}
