"""CLI entrypoint for running a baseline agent over local artifacts."""

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
from src.agents.registry import build_default_registry
from src.recommender.generate_recommendations import _load_optional_holdings


SCORES_PATH = Path("models/trained_models/latest_scores.csv")
CANDIDATES_PATH = Path("models/trained_models/trade_candidates.csv")


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def run_default_agent() -> None:
    registry = build_default_registry()
    agent = registry["market_analysis"]
    context = AgentContext(
        scores=_load_csv(SCORES_PATH),
        candidates=_load_csv(CANDIDATES_PATH),
        holdings=_load_optional_holdings(),
        metadata={"source": "local_artifacts"},
    )
    result = agent.run(context)
    print(json.dumps(asdict(result), default=str, indent=2))


if __name__ == "__main__":
    run_default_agent()
