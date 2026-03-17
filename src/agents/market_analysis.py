"""Baseline agent that translates model outputs into review actions."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .base import AgentContext, AgentResult


@dataclass(slots=True)
class MarketAnalysisAgent:
    """Simple default agent for portfolio review workflows."""

    name: str = "market_analysis"
    strong_buy_threshold: float = 0.6
    review_threshold: float = 0.54
    trim_threshold: float = 0.45

    def run(self, context: AgentContext) -> AgentResult:
        universe = self._build_universe(context)
        if universe.empty:
            return AgentResult(
                agent_name=self.name,
                summary="No symbols available for analysis.",
                actions=[],
                metrics={"symbol_count": 0.0},
            )

        universe["agent_action"] = universe.apply(self._classify_action, axis=1)
        actions = [
            {
                "symbol": row.symbol,
                "action": row.agent_action,
                "score": round(float(row.score), 4),
                "held": bool(row.is_held),
                "current_weight": round(float(row.current_weight), 4),
            }
            for row in universe.itertuples(index=False)
            if row.agent_action != "IGNORE"
        ]

        metrics = {
            "symbol_count": float(len(universe)),
            "buy_count": float(sum(action["action"] == "BUY" for action in actions)),
            "review_count": float(sum(action["action"] == "REVIEW" for action in actions)),
            "trim_count": float(sum(action["action"] == "TRIM" for action in actions)),
        }
        summary = self._build_summary(actions, int(metrics["symbol_count"]))
        return AgentResult(
            agent_name=self.name,
            summary=summary,
            actions=actions,
            metrics=metrics,
            metadata={"top_symbol": actions[0]["symbol"] if actions else None},
        )

    def _build_universe(self, context: AgentContext) -> pd.DataFrame:
        if context.scores.empty:
            return pd.DataFrame(columns=["symbol", "score", "rank", "current_weight", "is_held"])

        scores = context.scores.copy()
        scores["symbol"] = scores["symbol"].astype(str).str.strip().str.upper()
        scores["score"] = pd.to_numeric(scores["score"], errors="coerce")
        scores = scores.dropna(subset=["symbol", "score"]).copy()

        candidates = context.candidates.copy()
        if not candidates.empty and "symbol" in candidates.columns:
            candidates["symbol"] = candidates["symbol"].astype(str).str.strip().str.upper()
            candidates = candidates[["symbol", "rank"]].drop_duplicates()
        else:
            candidates = pd.DataFrame(columns=["symbol", "rank"])

        holdings = self._normalize_holdings(context.holdings)

        universe = scores.merge(candidates, on="symbol", how="left")
        universe = universe.merge(holdings, on="symbol", how="left")
        universe["rank"] = pd.to_numeric(universe["rank"], errors="coerce").fillna(len(universe) + 1)
        universe["current_weight"] = pd.to_numeric(
            universe.get("current_weight"), errors="coerce"
        ).fillna(0.0)
        universe["is_held"] = universe["current_weight"] > 0
        return universe.sort_values(["score", "rank"], ascending=[False, True]).reset_index(drop=True)

    def _normalize_holdings(self, holdings: pd.DataFrame) -> pd.DataFrame:
        if holdings.empty or "ticker" not in holdings.columns:
            return pd.DataFrame(columns=["symbol", "current_weight"])

        normalized = holdings.copy()
        normalized["ticker"] = normalized["ticker"].astype(str).str.strip().str.upper()
        normalized["market_value"] = pd.to_numeric(
            normalized.get("market_value"), errors="coerce"
        ).fillna(0.0)
        normalized = normalized[normalized["ticker"] != ""].copy()
        if normalized.empty:
            return pd.DataFrame(columns=["symbol", "current_weight"])

        aggregated = normalized.groupby("ticker", as_index=False).agg(market_value=("market_value", "sum"))
        total_market_value = float(aggregated["market_value"].sum())
        aggregated["current_weight"] = (
            aggregated["market_value"] / total_market_value if total_market_value > 0 else 0.0
        )
        return aggregated.rename(columns={"ticker": "symbol"})[["symbol", "current_weight"]]

    def _classify_action(self, row: pd.Series) -> str:
        score = float(row["score"])
        current_weight = float(row["current_weight"])
        is_held = bool(row["is_held"])

        if is_held and current_weight >= 0.1 and score < self.trim_threshold:
            return "TRIM"
        if score >= self.strong_buy_threshold:
            return "BUY"
        if score >= self.review_threshold:
            return "REVIEW"
        return "IGNORE"

    def _build_summary(self, actions: list[dict[str, object]], symbol_count: int) -> str:
        if not actions:
            return f"Reviewed {symbol_count} symbols and found no actions."

        top_actions = ", ".join(f"{action['symbol']}:{action['action']}" for action in actions[:3])
        return f"Reviewed {symbol_count} symbols. Priority actions: {top_actions}."
