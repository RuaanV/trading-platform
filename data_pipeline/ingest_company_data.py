"""Ingest latest company-level datasets from yfinance into Postgres raw schema."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
from sqlalchemy import text

try:
    from .db import postgres_engine
except ImportError:
    from db import postgres_engine

DEFAULT_SYMBOLS = ("AAPL", "AMZN", "GOOG", "MSFT", "BA.L")


def _to_snake_case(name: str) -> str:
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    return name.strip("_").lower()


def _normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame.columns = [_to_snake_case(str(col)) for col in frame.columns]
    return frame


def _symbols_to_ingest() -> list[str]:
    raw_symbols = os.getenv("YF_SYMBOLS", "").strip()
    if raw_symbols:
        return [symbol.strip().upper() for symbol in raw_symbols.split(",") if symbol.strip()]

    fallback_symbol = os.getenv("YF_SYMBOL", "").strip().upper()
    if fallback_symbol:
        return [fallback_symbol]

    return list(DEFAULT_SYMBOLS)


def _info_frame(ticker: yf.Ticker, symbol: str, extracted_at: str) -> pd.DataFrame:
    info = ticker.info or {}
    frame = pd.json_normalize(info)
    frame = _normalize_columns(frame)
    # Normalize nested objects (lists/dicts) so Postgres inserts don't fail.
    frame = frame.map(
        lambda value: json.dumps(value) if isinstance(value, (dict, list)) else value
    )
    frame["symbol"] = symbol
    frame["extracted_at"] = extracted_at
    return frame


def _major_holders_frame(ticker: yf.Ticker, symbol: str, extracted_at: str) -> pd.DataFrame:
    frame = ticker.major_holders
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["holder_metric", "holder_value", "symbol", "extracted_at"])
    frame = frame.copy()

    # yfinance can return major_holders in multiple layouts depending on upstream response.
    if {0, 1}.issubset(set(frame.columns)):
        frame = frame.rename(columns={0: "holder_value", 1: "holder_metric"})
    elif {"Value", "Breakdown"}.issubset(set(frame.columns)):
        frame = frame.rename(columns={"Value": "holder_value", "Breakdown": "holder_metric"})
    elif frame.shape[1] == 1:
        # Single-column format where index carries the metric labels.
        only_col = frame.columns[0]
        frame = frame.reset_index().rename(columns={"index": "holder_metric", only_col: "holder_value"})
    else:
        # Last-resort normalization by positional columns.
        if frame.shape[1] >= 2:
            frame = frame.iloc[:, :2]
            frame.columns = ["holder_value", "holder_metric"]
        else:
            return pd.DataFrame(
                columns=["holder_metric", "holder_value", "symbol", "extracted_at"]
            )

    frame["holder_metric"] = frame["holder_metric"].astype(str)
    frame["holder_value"] = frame["holder_value"].astype(str)
    frame["symbol"] = symbol
    frame["extracted_at"] = extracted_at
    return frame[["holder_metric", "holder_value", "symbol", "extracted_at"]]


def _institutional_holders_frame(ticker: yf.Ticker, symbol: str, extracted_at: str) -> pd.DataFrame:
    frame = ticker.institutional_holders
    if frame is None or frame.empty:
        return pd.DataFrame(
            columns=["holder", "shares", "date_reported", "pct_out", "value", "symbol", "extracted_at"]
        )

    frame = _normalize_columns(frame)
    frame = frame.rename(columns={"pct_held": "pct_out"})

    for col in ["holder", "shares", "date_reported", "pct_out", "value"]:
        if col not in frame.columns:
            frame[col] = None

    frame["symbol"] = symbol
    frame["extracted_at"] = extracted_at
    return frame[["holder", "shares", "date_reported", "pct_out", "value", "symbol", "extracted_at"]]


def _balance_sheet_frame(ticker: yf.Ticker, symbol: str, extracted_at: str) -> pd.DataFrame:
    frame = ticker.balancesheet
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["metric", "as_of_date", "value", "symbol", "extracted_at"])

    normalized = (
        frame.transpose()
        .stack(future_stack=True)
        .reset_index()
        .rename(columns={"level_0": "as_of_date", "level_1": "metric", 0: "value"})
    )
    normalized["symbol"] = symbol
    normalized["extracted_at"] = extracted_at
    return normalized[["metric", "as_of_date", "value", "symbol", "extracted_at"]]


def ingest_company_data() -> None:
    extracted_at = datetime.now(timezone.utc).isoformat()
    symbols = _symbols_to_ingest()
    info_frames: list[pd.DataFrame] = []
    major_holders_frames: list[pd.DataFrame] = []
    institutional_holders_frames: list[pd.DataFrame] = []
    balance_sheet_frames: list[pd.DataFrame] = []

    for symbol in symbols:
        ticker = yf.Ticker(symbol)
        info_frames.append(_info_frame(ticker, symbol, extracted_at))
        major_holders_frames.append(_major_holders_frame(ticker, symbol, extracted_at))
        institutional_holders_frames.append(_institutional_holders_frame(ticker, symbol, extracted_at))
        balance_sheet_frames.append(_balance_sheet_frame(ticker, symbol, extracted_at))

    info_df = pd.concat(info_frames, ignore_index=True)
    major_holders_df = pd.concat(major_holders_frames, ignore_index=True)
    institutional_holders_df = pd.concat(institutional_holders_frames, ignore_index=True)
    balance_sheet_df = pd.concat(balance_sheet_frames, ignore_index=True)

    engine = postgres_engine()

    with engine.begin() as conn:
        conn.execute(text("create schema if not exists raw"))
        # Drop transformed schemas so raw tables can be replaced even when dbt views depend on them.
        conn.execute(text("drop schema if exists analytics_staging cascade"))
        conn.execute(text("drop schema if exists analytics_features cascade"))

    info_df.to_sql("company_info", engine, schema="raw", if_exists="replace", index=False)
    major_holders_df.to_sql("major_holders", engine, schema="raw", if_exists="replace", index=False)
    institutional_holders_df.to_sql(
        "institutional_holders", engine, schema="raw", if_exists="replace", index=False
    )
    balance_sheet_df.to_sql("balance_sheet", engine, schema="raw", if_exists="replace", index=False)

    print(
        f"[{extracted_at}] Ingested yfinance company datasets for {', '.join(symbols)} "
        "into Postgres raw schema."
    )


if __name__ == "__main__":
    ingest_company_data()
