"""Generate portfolio-aware trade recommendations from scores and candidates."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import sys

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

SCORES_PATH = Path("models/trained_models/latest_scores.csv")
CANDIDATES_PATH = Path("models/trained_models/trade_candidates.csv")
RECOMMENDATIONS_PATH = Path("models/trained_models/latest_recommendations.csv")
HOLDINGS_COLUMNS = ["ticker", "market_value", "company", "portfolio_name", "snapshot_at", "current_weight"]
HOLDINGS_FIXTURE_ENV = "RECOMMENDER_HOLDINGS_PATH"

ACTION_PRIORITY = {
    "BUY": 0,
    "ADD": 1,
    "WATCH": 2,
    "HOLD": 3,
    "TRIM": 4,
    "EXIT": 5,
}


def _normalize_holdings(holdings: pd.DataFrame) -> pd.DataFrame:
    if holdings.empty or "ticker" not in holdings.columns:
        return pd.DataFrame(columns=HOLDINGS_COLUMNS)

    holdings["ticker"] = holdings["ticker"].fillna("").astype(str).str.strip().str.upper()
    holdings = holdings[holdings["ticker"] != ""].copy()
    if holdings.empty:
        return pd.DataFrame(columns=HOLDINGS_COLUMNS)

    holdings["market_value"] = pd.to_numeric(holdings.get("market_value"), errors="coerce").fillna(0.0)
    holdings = (
        holdings.sort_values(["ticker", "snapshot_at"], ascending=[True, False])
        .groupby("ticker", as_index=False)
        .agg(
            market_value=("market_value", "sum"),
            company=("company", "first"),
            portfolio_name=("portfolio_name", "first"),
            snapshot_at=("snapshot_at", "first"),
        )
    )

    total_value = float(holdings["market_value"].sum())
    if total_value > 0:
        holdings["current_weight"] = holdings["market_value"] / total_value
    else:
        holdings["current_weight"] = 0.0
    return holdings


def _load_fixture_holdings() -> pd.DataFrame:
    fixture_path = os.getenv(HOLDINGS_FIXTURE_ENV, "").strip()
    if not fixture_path:
        return pd.DataFrame(columns=HOLDINGS_COLUMNS)

    holdings = pd.read_csv(fixture_path)
    return _normalize_holdings(holdings)


def _load_optional_holdings() -> pd.DataFrame:
    fixture_holdings = _load_fixture_holdings()
    if not fixture_holdings.empty:
        return fixture_holdings

    try:
        from data_pipeline.personal_portfolios import fetch_portfolio_holdings
    except ImportError:
        return pd.DataFrame(columns=HOLDINGS_COLUMNS)

    try:
        holdings = fetch_portfolio_holdings().copy()
    except Exception:  # noqa: BLE001
        return pd.DataFrame(columns=HOLDINGS_COLUMNS)

    return _normalize_holdings(holdings)


def _load_scores() -> pd.DataFrame:
    if not SCORES_PATH.exists():
        raise FileNotFoundError(f"Missing score artifact: {SCORES_PATH}")

    scores = pd.read_csv(SCORES_PATH)
    if "symbol" not in scores.columns or "score" not in scores.columns:
        raise ValueError("latest_scores.csv must contain symbol and score columns")

    return _enrich_scores(scores)


def _enrich_scores(scores: pd.DataFrame) -> pd.DataFrame:
    enriched = scores.copy()
    enriched["symbol"] = enriched["symbol"].astype(str).str.strip().str.upper()
    enriched["score"] = pd.to_numeric(enriched["score"], errors="coerce")
    enriched = enriched.dropna(subset=["symbol", "score"]).copy()
    if "expected_return" not in enriched.columns:
        enriched["expected_return"] = enriched["score"]
    if "risk_score" not in enriched.columns:
        enriched["risk_score"] = (1 - enriched["score"]).clip(lower=0.0, upper=1.0)
    if "confidence" not in enriched.columns:
        enriched["confidence"] = (0.55 + (enriched["score"] - 0.5).abs() * 0.9).clip(upper=0.95)
    return enriched


def _load_candidates() -> pd.DataFrame:
    if CANDIDATES_PATH.exists():
        candidates = pd.read_csv(CANDIDATES_PATH)
        candidates["symbol"] = candidates["symbol"].astype(str).str.strip().str.upper()
        if "rank" not in candidates.columns:
            candidates["rank"] = range(1, len(candidates) + 1)
        return candidates
    return pd.DataFrame(columns=["symbol", "side", "score", "rank"])


def _build_universe(scores: pd.DataFrame, candidates: pd.DataFrame, holdings: pd.DataFrame) -> pd.DataFrame:
    universe = scores.merge(
        candidates[["symbol", "rank"]].drop_duplicates(),
        on="symbol",
        how="left",
    )
    universe = universe.merge(
        holdings.rename(columns={"ticker": "symbol"}),
        on="symbol",
        how="left",
    )
    if "market_value" in universe.columns:
        universe["market_value"] = pd.to_numeric(universe["market_value"], errors="coerce").fillna(0.0)
    else:
        universe["market_value"] = 0.0

    if "current_weight" in universe.columns:
        universe["current_weight"] = pd.to_numeric(
            universe["current_weight"], errors="coerce"
        ).fillna(0.0)
    else:
        universe["current_weight"] = 0.0
    universe["is_held"] = universe["market_value"] > 0
    universe["rank"] = universe["rank"].fillna(len(universe) + 1).astype(int)
    return universe


def _determine_action(row: pd.Series) -> str:
    score = float(row["score"])
    is_held = bool(row["is_held"])
    current_weight = float(row["current_weight"])

    if is_held and score < 0.4:
        return "EXIT"
    if is_held and current_weight >= 0.18 and score < 0.58:
        return "TRIM"
    if is_held and current_weight >= 0.12 and score < 0.5:
        return "TRIM"
    if score >= 0.6:
        return "ADD" if is_held else "BUY"
    if score >= 0.54:
        return "HOLD" if is_held else "WATCH"
    if is_held and current_weight >= 0.08 and score < 0.45:
        return "TRIM"
    return "WATCH"


def _target_weight(row: pd.Series) -> float:
    score = float(row["score"])
    is_held = bool(row["is_held"])
    current_weight = float(row["current_weight"])

    if is_held:
        base = max(current_weight, 0.03)
        if current_weight >= 0.18 and score < 0.58:
            return 0.08
        if current_weight >= 0.12 and score < 0.5:
            return 0.06
        if score >= 0.6:
            return min(base + 0.02, 0.12)
        if score < 0.45:
            return max(base - 0.03, 0.0)
        return min(base, 0.08)

    if score >= 0.6:
        return 0.05
    if score >= 0.54:
        return 0.02
    return 0.0


def _recommendation_score(row: pd.Series) -> float:
    turnover_penalty = 0.0 if bool(row["is_held"]) else 0.03
    concentration_penalty = max(float(row["current_weight"]) - 0.08, 0.0)
    return round(
        float(row["expected_return"]) * float(row["confidence"])
        - float(row["risk_score"]) * 0.25
        - turnover_penalty
        - concentration_penalty,
        4,
    )


def _build_rationale(row: pd.Series) -> str:
    action = row["action"]
    current_weight = float(row["current_weight"])
    position_text = (
        f"existing weight {current_weight:.1%}"
        if bool(row["is_held"])
        else "not currently held"
    )
    concentration_text = ""
    if action == "TRIM":
        concentration_text = " concentration is above the soft limit,"
    return (
        f"{action} because expected return is {float(row['expected_return']):.2f}, "
        f"risk score is {float(row['risk_score']):.2f}, confidence is {float(row['confidence']):.2f},{concentration_text} "
        f"and the symbol is {position_text}."
    )


def build_recommendations(
    scores: pd.DataFrame,
    candidates: pd.DataFrame,
    holdings: pd.DataFrame,
    *,
    generated_at: str,
) -> pd.DataFrame:
    universe = _build_universe(_enrich_scores(scores), candidates, holdings)
    universe["action"] = universe.apply(_determine_action, axis=1)
    universe["target_weight"] = universe.apply(_target_weight, axis=1).round(4)
    universe["recommendation_score"] = universe.apply(_recommendation_score, axis=1)
    universe["generated_at"] = generated_at
    universe["rationale"] = universe.apply(_build_rationale, axis=1)

    recommendations = universe[
        [
            "symbol",
            "action",
            "recommendation_score",
            "target_weight",
            "expected_return",
            "risk_score",
            "confidence",
            "rank",
            "company",
            "portfolio_name",
            "generated_at",
            "rationale",
        ]
    ].copy()
    recommendations["company"] = recommendations["company"].fillna("")
    recommendations["portfolio_name"] = recommendations["portfolio_name"].fillna("")
    recommendations["expected_return"] = recommendations["expected_return"].round(4)
    recommendations["risk_score"] = recommendations["risk_score"].round(4)
    recommendations["confidence"] = recommendations["confidence"].round(4)
    recommendations = recommendations.sort_values(
        by=["recommendation_score", "rank", "action"],
        ascending=[False, True, True],
        key=lambda column: column.map(ACTION_PRIORITY) if column.name == "action" else column,
    )
    return recommendations


def generate_recommendations() -> None:
    generated_at = datetime.now(timezone.utc).isoformat()
    scores = _load_scores()
    candidates = _load_candidates()
    holdings = _load_optional_holdings()
    recommendations = build_recommendations(
        scores=scores,
        candidates=candidates,
        holdings=holdings,
        generated_at=generated_at,
    )

    RECOMMENDATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    recommendations.to_csv(RECOMMENDATIONS_PATH, index=False)
    print(
        f"[{generated_at}] generate_recommendations: wrote {RECOMMENDATIONS_PATH} "
        f"with {len(recommendations)} rows"
    )


if __name__ == "__main__":
    generate_recommendations()
