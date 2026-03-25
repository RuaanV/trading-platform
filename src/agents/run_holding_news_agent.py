"""CLI entrypoint for refreshing holding headline sentiment."""

from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.agents.base import AgentContext
from src.agents.holding_news_sentiment import HoldingNewsSentimentAgent


def run_holding_news_agent(symbol: str = "GOOG") -> None:
    agent = HoldingNewsSentimentAgent()
    result = agent.run(AgentContext(scores=pd.DataFrame(), metadata={"symbol": symbol}))
    print(json.dumps(asdict(result), default=str, indent=2))


if __name__ == "__main__":
    requested_symbol = sys.argv[1].strip().upper() if len(sys.argv) > 1 else "GOOG"
    run_holding_news_agent(requested_symbol)
