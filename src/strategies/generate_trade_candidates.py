"""Generate portfolio-aware trade candidates from the latest score artifact."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.recommender.generate_recommendations import _build_universe, _determine_action, _load_optional_holdings
from src.recommender.generate_recommendations import _load_scores


CANDIDATES_PATH = Path("models/trained_models/trade_candidates.csv")


def build_trade_candidates(scores: pd.DataFrame, holdings: pd.DataFrame) -> pd.DataFrame:
    universe = _build_universe(scores, pd.DataFrame(columns=["symbol", "rank"]), holdings)
    universe["side"] = universe.apply(_determine_action, axis=1)
    universe = universe.sort_values(["score", "symbol"], ascending=[False, True]).reset_index(drop=True)
    universe["rank"] = range(1, len(universe) + 1)
    return universe[["symbol", "side", "score", "rank"]]


def generate_trade_candidates() -> None:
    scores = _load_scores()
    holdings = _load_optional_holdings()
    candidates = build_trade_candidates(scores=scores, holdings=holdings)

    CANDIDATES_PATH.parent.mkdir(parents=True, exist_ok=True)
    candidates.to_csv(CANDIDATES_PATH, index=False)
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] generate_trade_candidates: wrote {CANDIDATES_PATH}"
    )


if __name__ == "__main__":
    generate_trade_candidates()
