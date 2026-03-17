"""Run a simple walk-forward backtest of the batch recommender rules."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

import numpy as np
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.backtesting.load_price_history import load_price_history
from src.recommender.generate_recommendations import _load_optional_holdings, build_recommendations


BACKTEST_HISTORY_PATH = Path("models/trained_models/recommender_backtest_history.csv")
BACKTEST_SUMMARY_PATH = Path("models/trained_models/recommender_backtest_summary.csv")
BENCHMARK_SYMBOL = "SPY"
MOMENTUM_3M = 63
MOMENTUM_6M = 126
REBALANCE_STEP = 21


def _price_table(history: pd.DataFrame) -> pd.DataFrame:
    return (
        history.sort_values(["date", "symbol"])
        .pivot_table(index="date", columns="symbol", values="close", aggfunc="last")
        .sort_index()
        .ffill()
    )


def _score_universe(price_table: pd.DataFrame, as_of_idx: int, symbols: list[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    returns = price_table.pct_change()

    for symbol in symbols:
        series = price_table[symbol].dropna()
        eligible = series[series.index <= price_table.index[as_of_idx]]
        if len(eligible) <= MOMENTUM_6M:
            continue

        current_price = float(eligible.iloc[-1])
        ret_3m = current_price / float(eligible.iloc[-MOMENTUM_3M - 1]) - 1
        ret_6m = current_price / float(eligible.iloc[-MOMENTUM_6M - 1]) - 1
        symbol_returns = returns[symbol].loc[eligible.index].dropna()
        vol_3m = float(symbol_returns.tail(MOMENTUM_3M).std(ddof=0) * np.sqrt(252))
        score = float(np.clip(0.5 + (ret_3m * 0.7) + (ret_6m * 0.5) - (vol_3m * 0.25), 0, 1))
        rows.append({"symbol": symbol, "score": round(score, 4)})

    scores = pd.DataFrame(rows)
    if scores.empty:
        return scores
    return scores.sort_values("score", ascending=False).reset_index(drop=True)


def _candidate_frame(scores: pd.DataFrame) -> pd.DataFrame:
    if scores.empty:
        return pd.DataFrame(columns=["symbol", "side", "score", "rank"])

    candidates = scores.copy()
    candidates["side"] = "BUY"
    candidates["rank"] = range(1, len(candidates) + 1)
    return candidates[["symbol", "side", "score", "rank"]]


def _holdings_frame(weights: dict[str, float], as_of: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for symbol, weight in weights.items():
        if weight <= 0:
            continue
        rows.append(
            {
                "ticker": symbol,
                "market_value": weight,
                "company": symbol,
                "portfolio_name": "SIMULATED",
                "snapshot_at": as_of,
            }
        )
    return pd.DataFrame(rows)


def _normalize_target_weights(recommendations: pd.DataFrame, symbols: list[str]) -> dict[str, float]:
    target_map = {symbol: 0.0 for symbol in symbols}
    for _, row in recommendations.iterrows():
        target_map[str(row["symbol"]).upper()] = max(float(row["target_weight"]), 0.0)

    total_weight = sum(target_map.values())
    if total_weight > 1:
        return {symbol: weight / total_weight for symbol, weight in target_map.items()}
    return target_map


def _portfolio_period_return(
    price_table: pd.DataFrame,
    start_idx: int,
    end_idx: int,
    weights: dict[str, float],
) -> float:
    portfolio_return = 0.0
    for symbol, weight in weights.items():
        if weight <= 0 or symbol not in price_table.columns:
            continue
        start_price = float(price_table.iloc[start_idx][symbol])
        end_price = float(price_table.iloc[end_idx][symbol])
        if start_price <= 0:
            continue
        portfolio_return += weight * ((end_price / start_price) - 1)
    return portfolio_return


def build_backtest_outputs(holdings: pd.DataFrame, history: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if holdings.empty:
        raise ValueError("No holdings available to backtest.")

    history = history.copy()
    history["date"] = pd.to_datetime(history["date"], utc=True)
    history["symbol"] = history["symbol"].astype(str).str.upper()
    history["close"] = pd.to_numeric(history["close"], errors="coerce")

    symbols = [symbol for symbol in holdings["ticker"].astype(str).str.upper() if symbol != "CASH"]
    price_table = _price_table(history)
    symbols = [symbol for symbol in symbols if symbol in price_table.columns]
    if len(symbols) < 2:
        raise ValueError("Need at least two price series for a meaningful backtest.")

    initial_weights = {
        str(row["ticker"]).upper(): float(row["current_weight"])
        for _, row in holdings.iterrows()
        if str(row["ticker"]).upper() in symbols
    }
    total_initial = sum(initial_weights.values())
    current_weights = {symbol: weight / total_initial for symbol, weight in initial_weights.items() if total_initial > 0}

    date_index = price_table.index
    rebalance_indices = list(range(MOMENTUM_6M, len(date_index) - 1, REBALANCE_STEP))
    if len(rebalance_indices) < 2:
        raise ValueError("Not enough history for walk-forward backtest.")

    history_rows: list[dict[str, object]] = []
    portfolio_value = 1.0
    benchmark_value = 1.0

    for start_idx, end_idx in zip(rebalance_indices[:-1], rebalance_indices[1:]):
        as_of = date_index[start_idx]
        next_as_of = date_index[end_idx]
        scores = _score_universe(price_table, start_idx, symbols)
        candidates = _candidate_frame(scores)
        holdings_frame = _holdings_frame(current_weights, as_of)
        recommendations = build_recommendations(
            scores=scores,
            candidates=candidates,
            holdings=holdings_frame,
            generated_at=as_of.isoformat(),
        )
        target_weights = _normalize_target_weights(recommendations, symbols)
        portfolio_return = _portfolio_period_return(price_table, start_idx, end_idx, target_weights)

        benchmark_return = 0.0
        if BENCHMARK_SYMBOL in price_table.columns:
            benchmark_return = _portfolio_period_return(
                price_table,
                start_idx,
                end_idx,
                {BENCHMARK_SYMBOL: 1.0},
            )

        portfolio_value *= 1 + portfolio_return
        benchmark_value *= 1 + benchmark_return
        current_weights = target_weights

        top_actions = ";".join(
            f"{row.symbol}:{row.action}" for row in recommendations.itertuples(index=False)
        )
        history_rows.append(
            {
                "rebalance_at": as_of.isoformat(),
                "next_rebalance_at": next_as_of.isoformat(),
                "portfolio_return": round(portfolio_return, 4),
                "benchmark_return": round(benchmark_return, 4),
                "portfolio_value": round(portfolio_value, 4),
                "benchmark_value": round(benchmark_value, 4),
                "cash_weight": round(max(0.0, 1 - sum(target_weights.values())), 4),
                "top_actions": top_actions,
            }
        )

    history_frame = pd.DataFrame(history_rows)
    if history_frame.empty:
        raise ValueError("Backtest produced no rebalance periods.")

    portfolio_returns = history_frame["portfolio_return"]
    benchmark_returns = history_frame["benchmark_return"]
    cumulative_curve = (1 + portfolio_returns).cumprod()
    drawdown = cumulative_curve / cumulative_curve.cummax() - 1

    summary = pd.DataFrame(
        [
            {
                "periods": len(history_frame),
                "cumulative_return": round(float(history_frame["portfolio_value"].iloc[-1] - 1), 4),
                "benchmark_cumulative_return": round(float(history_frame["benchmark_value"].iloc[-1] - 1), 4),
                "excess_return": round(
                    float(history_frame["portfolio_value"].iloc[-1] - history_frame["benchmark_value"].iloc[-1]),
                    4,
                ),
                "mean_period_return": round(float(portfolio_returns.mean()), 4),
                "mean_benchmark_return": round(float(benchmark_returns.mean()), 4),
                "volatility": round(float(portfolio_returns.std(ddof=0)), 4),
                "max_drawdown": round(float(drawdown.min()), 4),
            }
        ]
    )
    return history_frame, summary


def backtest_recommender() -> None:
    holdings = _load_optional_holdings()
    history = load_price_history()
    backtest_history, backtest_summary = build_backtest_outputs(holdings, history)

    BACKTEST_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    backtest_history.to_csv(BACKTEST_HISTORY_PATH, index=False)
    backtest_summary.to_csv(BACKTEST_SUMMARY_PATH, index=False)
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] backtest_recommender: wrote "
        f"{BACKTEST_HISTORY_PATH} and {BACKTEST_SUMMARY_PATH}"
    )


if __name__ == "__main__":
    backtest_recommender()
