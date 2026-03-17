"""Evaluate 3M/6M performance for the latest portfolio holdings."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

import numpy as np
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.backtesting.load_price_history import HISTORICAL_PRICES_PATH, load_price_history
from src.recommender.generate_recommendations import _load_optional_holdings


HOLDING_PERFORMANCE_PATH = Path("models/trained_models/holding_performance_report.csv")
BENCHMARK_SYMBOL = "SPY"
WINDOW_3M = 63
WINDOW_6M = 126


def _trailing_return(series: pd.Series, window: int) -> float | None:
    clean = series.dropna()
    if len(clean) <= window:
        return None
    return float(clean.iloc[-1] / clean.iloc[-window - 1] - 1)


def _annualized_volatility(series: pd.Series, window: int) -> float | None:
    clean = series.dropna().pct_change().dropna()
    if len(clean) < min(window, 20):
        return None
    return float(clean.tail(window).std(ddof=0) * np.sqrt(252))


def _max_drawdown(series: pd.Series, window: int) -> float | None:
    clean = series.dropna()
    if len(clean) < min(window, 20):
        return None
    tail = clean.tail(window)
    drawdown = tail / tail.cummax() - 1
    return float(drawdown.min())


def build_holding_performance_report(
    holdings: pd.DataFrame,
    history: pd.DataFrame,
    *,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
) -> pd.DataFrame:
    if holdings.empty:
        raise ValueError("No holdings available to evaluate.")

    history = history.copy()
    history["date"] = pd.to_datetime(history["date"], utc=True)
    history["symbol"] = history["symbol"].astype(str).str.upper()
    history["close"] = pd.to_numeric(history["close"], errors="coerce")
    price_table = (
        history.sort_values(["date", "symbol"])
        .pivot_table(index="date", columns="symbol", values="close", aggfunc="last")
        .sort_index()
        .ffill()
    )
    benchmark_series = price_table.get(benchmark_symbol)
    rows: list[dict[str, object]] = []

    for _, holding in holdings.iterrows():
        symbol = str(holding["ticker"]).upper()
        if symbol == "CASH" or symbol not in price_table.columns:
            continue

        series = price_table[symbol]
        return_3m = _trailing_return(series, WINDOW_3M)
        return_6m = _trailing_return(series, WINDOW_6M)
        vol_3m = _annualized_volatility(series, WINDOW_3M)
        vol_6m = _annualized_volatility(series, WINDOW_6M)
        drawdown_6m = _max_drawdown(series, WINDOW_6M)

        benchmark_3m = _trailing_return(benchmark_series, WINDOW_3M) if benchmark_series is not None else None
        benchmark_6m = _trailing_return(benchmark_series, WINDOW_6M) if benchmark_series is not None else None

        rows.append(
            {
                "symbol": symbol,
                "company": holding.get("company", symbol),
                "portfolio_name": holding.get("portfolio_name", ""),
                "current_weight": round(float(holding.get("current_weight", 0.0)), 4),
                "return_3m": None if return_3m is None else round(return_3m, 4),
                "return_6m": None if return_6m is None else round(return_6m, 4),
                "volatility_3m": None if vol_3m is None else round(vol_3m, 4),
                "volatility_6m": None if vol_6m is None else round(vol_6m, 4),
                "max_drawdown_6m": None if drawdown_6m is None else round(drawdown_6m, 4),
                "relative_return_3m": None
                if return_3m is None or benchmark_3m is None
                else round(return_3m - benchmark_3m, 4),
                "relative_return_6m": None
                if return_6m is None or benchmark_6m is None
                else round(return_6m - benchmark_6m, 4),
                "as_of": price_table.index.max().isoformat(),
            }
        )

    report = pd.DataFrame(rows)
    if report.empty:
        return report
    return report.sort_values(["return_6m", "return_3m"], ascending=[False, False], na_position="last")


def evaluate_holdings_history() -> None:
    holdings = _load_optional_holdings()
    history = load_price_history()
    report = build_holding_performance_report(holdings, history)
    HOLDING_PERFORMANCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(HOLDING_PERFORMANCE_PATH, index=False)
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] evaluate_holdings_history: wrote {HOLDING_PERFORMANCE_PATH} "
        f"with {len(report)} rows"
    )


if __name__ == "__main__":
    evaluate_holdings_history()
