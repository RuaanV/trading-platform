"""Load daily historical prices for portfolio holdings and benchmarks."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import sys

import pandas as pd
import yfinance as yf


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

HISTORICAL_PRICES_PATH = Path("models/trained_models/historical_prices.csv")
HISTORICAL_PRICES_FIXTURE_ENV = "HISTORICAL_PRICES_FIXTURE_PATH"
HISTORICAL_PROVIDER_ENV = "HISTORICAL_PRICE_PROVIDER"
DEFAULT_PERIOD = "9mo"
DEFAULT_INTERVAL = "1d"
DEFAULT_BENCHMARKS = ("SPY",)


def _load_holdings() -> pd.DataFrame:
    try:
        from src.recommender.generate_recommendations import _load_optional_holdings
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Could not import holdings loader") from exc

    return _load_optional_holdings()


def _symbol_mapping() -> dict[str, str]:
    try:
        from data_pipeline.load_personal_portfolio import YAHOO_SYMBOLS
    except ImportError:
        return {}
    return {str(key).upper(): str(value).upper() for key, value in YAHOO_SYMBOLS.items()}


def resolve_price_symbols() -> list[str]:
    holdings = _load_holdings()
    if holdings.empty:
        raise ValueError("No holdings available to resolve price history symbols.")

    symbol_map = _symbol_mapping()
    raw_symbols = holdings["ticker"].fillna("").astype(str).str.strip().str.upper().tolist()
    resolved_symbols = [symbol_map.get(symbol, symbol) for symbol in raw_symbols if symbol and symbol != "CASH"]

    extra_symbols = [
        item.strip().upper()
        for item in os.getenv("HISTORICAL_EXTRA_SYMBOLS", "").split(",")
        if item.strip()
    ]
    benchmark_symbols = list(DEFAULT_BENCHMARKS)
    return sorted(set(resolved_symbols + extra_symbols + benchmark_symbols))


def fetch_yfinance_history(
    symbols: list[str],
    *,
    period: str = DEFAULT_PERIOD,
    interval: str = DEFAULT_INTERVAL,
) -> pd.DataFrame:
    if not symbols:
        raise ValueError("At least one symbol is required to fetch historical prices.")

    downloaded = yf.download(
        tickers=symbols,
        period=period,
        interval=interval,
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=False,
    )
    if downloaded.empty:
        raise ValueError(f"No historical prices returned for symbols: {symbols}")

    rows: list[pd.DataFrame] = []
    if isinstance(downloaded.columns, pd.MultiIndex):
        for symbol in symbols:
            if symbol not in downloaded.columns.get_level_values(0):
                continue
            frame = downloaded[symbol].reset_index()
            if "Close" not in frame.columns:
                continue
            frame = frame.rename(columns={"Date": "date", "Close": "close"})
            frame["symbol"] = symbol
            rows.append(frame[["date", "symbol", "close"]])
    else:
        frame = downloaded.reset_index()
        frame = frame.rename(columns={"Date": "date", "Close": "close"})
        frame["symbol"] = symbols[0]
        rows.append(frame[["date", "symbol", "close"]])

    history = pd.concat(rows, ignore_index=True)
    history["date"] = pd.to_datetime(history["date"], utc=True).dt.normalize()
    history["symbol"] = history["symbol"].astype(str).str.upper()
    history["close"] = pd.to_numeric(history["close"], errors="coerce")
    history = history.dropna(subset=["date", "symbol", "close"]).sort_values(["symbol", "date"])
    return history


def fetch_massive_history(
    symbols: list[str],
    *,
    period: str = DEFAULT_PERIOD,
    interval: str = DEFAULT_INTERVAL,
) -> pd.DataFrame:
    if not symbols:
        raise ValueError("At least one symbol is required to fetch historical prices.")

    try:
        from data_pipeline.price_providers import get_massive_history
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Could not import Massive historical provider") from exc

    rows: list[dict[str, object]] = []
    failures: list[str] = []
    for symbol in symbols:
        try:
            rows.extend(get_massive_history(symbol, period=period, interval=interval))
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{symbol}: {exc}")

    if not rows:
        raise ValueError(f"No Massive historical prices returned for symbols: {symbols}. Failures: {failures}")

    history = pd.DataFrame(rows)
    history["date"] = pd.to_datetime(history["date"], utc=True).dt.normalize()
    history["symbol"] = history["symbol"].astype(str).str.upper()
    history["close"] = pd.to_numeric(history["close"], errors="coerce")
    return history.dropna(subset=["date", "symbol", "close"]).sort_values(["symbol", "date"])


def load_price_history() -> pd.DataFrame:
    fixture_path = os.getenv(HISTORICAL_PRICES_FIXTURE_ENV, "").strip()
    if fixture_path:
        history = pd.read_csv(fixture_path)
        history["date"] = pd.to_datetime(history["date"], utc=True).dt.normalize()
        history["symbol"] = history["symbol"].astype(str).str.upper()
        history["close"] = pd.to_numeric(history["close"], errors="coerce")
        return history.dropna(subset=["date", "symbol", "close"]).sort_values(["symbol", "date"])

    provider = os.getenv(HISTORICAL_PROVIDER_ENV, "yfinance").strip().lower()
    symbols = resolve_price_symbols()
    if provider == "massive":
        return fetch_massive_history(symbols)
    if provider in {"yfinance", "yahoo"}:
        return fetch_yfinance_history(symbols)
    raise ValueError(f"Unsupported historical provider '{provider}'")


def write_price_history() -> None:
    history = load_price_history()
    HISTORICAL_PRICES_PATH.parent.mkdir(parents=True, exist_ok=True)
    history.to_csv(HISTORICAL_PRICES_PATH, index=False)
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] write_price_history: wrote {HISTORICAL_PRICES_PATH} "
        f"with {len(history)} rows across {history['symbol'].nunique()} symbols"
    )


if __name__ == "__main__":
    write_price_history()
