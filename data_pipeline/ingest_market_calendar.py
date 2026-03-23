"""Ingest current-year market calendar events for tracked symbols."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import yfinance as yf
from sqlalchemy import text

try:
    from .db import postgres_engine
except ImportError:
    from db import postgres_engine


DEFAULT_SYMBOLS = ("GOOG", "MSFT", "AAPL", "AMZN", "NVDA", "BA.L")
DEFAULT_PROVIDER = "yfinance"
CALENDAR_ARTIFACT_PATH = Path("models/trained_models/current_year_market_calendar.csv")
RAW_SCHEMA = "raw"
RAW_TABLE = "market_calendar_events"
FIXTURE_ENV = "MARKET_CALENDAR_FIXTURE_PATH"


def _current_year() -> int:
    return datetime.now(timezone.utc).year


def _symbols_to_ingest() -> list[str]:
    raw_symbols = os.getenv("MARKET_CALENDAR_SYMBOLS", "").strip()
    if raw_symbols:
        return [symbol.strip().upper() for symbol in raw_symbols.split(",") if symbol.strip()]
    return list(DEFAULT_SYMBOLS)


def _calendar_year() -> int:
    raw_year = os.getenv("MARKET_CALENDAR_YEAR", "").strip()
    return int(raw_year) if raw_year else _current_year()


def _json_safe(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:  # noqa: BLE001
            return str(value)
    return value


def _normalize_event_timestamp(value: object) -> pd.Timestamp | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        for item in value:
            normalized = _normalize_event_timestamp(item)
            if normalized is not None:
                return normalized
        return None
    if isinstance(value, pd.DataFrame):
        for column in value.columns:
            normalized = _normalize_event_timestamp(value[column].tolist())
            if normalized is not None:
                return normalized
        return None
    if isinstance(value, pd.Series):
        return _normalize_event_timestamp(value.tolist())

    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    if isinstance(timestamp, pd.DatetimeIndex):
        return timestamp[0] if len(timestamp) else None
    return timestamp


def _add_event_row(
    rows: list[dict[str, object]],
    *,
    symbol: str,
    company: str | None,
    event_type: str,
    event_name: str,
    event_value: object,
    source_provider: str,
    source_payload: object,
    extracted_at: str,
    calendar_year: int,
) -> None:
    event_timestamp = _normalize_event_timestamp(event_value)
    if event_timestamp is None or event_timestamp.year != calendar_year:
        return

    rows.append(
        {
            "symbol": symbol.upper(),
            "company": company or symbol.upper(),
            "event_type": event_type,
            "event_name": event_name,
            "event_date": event_timestamp.date().isoformat(),
            "event_timestamp": event_timestamp.isoformat(),
            "calendar_year": calendar_year,
            "source_provider": source_provider,
            "source_payload": json.dumps(_json_safe(source_payload), allow_nan=False),
            "extracted_at": extracted_at,
        }
    )


def _company_name(ticker: yf.Ticker, symbol: str) -> str:
    try:
        info = ticker.info or {}
    except Exception:  # noqa: BLE001
        return symbol.upper()
    return str(info.get("shortName") or info.get("longName") or symbol.upper())


def _calendar_mapping(calendar: object) -> dict[str, object]:
    if calendar is None:
        return {}
    if isinstance(calendar, dict):
        return {str(key): value for key, value in calendar.items()}
    if isinstance(calendar, pd.DataFrame):
        if "Value" in calendar.columns:
            return {str(index): calendar.loc[index, "Value"] for index in calendar.index}
        if {"Event", "Value"}.issubset(set(calendar.columns)):
            return {
                str(row["Event"]): row["Value"]
                for _, row in calendar.iterrows()
            }
        if calendar.shape[1] == 1:
            only_col = calendar.columns[0]
            return {str(index): calendar.loc[index, only_col] for index in calendar.index}
    return {}


def _load_fixture_events() -> pd.DataFrame | None:
    fixture_path = os.getenv(FIXTURE_ENV, "").strip()
    if not fixture_path:
        return None

    frame = pd.read_csv(fixture_path)
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["calendar_year"] = pd.to_numeric(frame["calendar_year"], errors="coerce").fillna(0).astype(int)
    frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce").dt.date.astype(str)
    if "event_timestamp" in frame.columns:
        frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True, errors="coerce").astype(str)
    return frame


def fetch_yfinance_market_calendar(symbol: str, *, calendar_year: int, extracted_at: str) -> pd.DataFrame:
    ticker = yf.Ticker(symbol)
    company = _company_name(ticker, symbol)
    rows: list[dict[str, object]] = []

    calendar_items = _calendar_mapping(getattr(ticker, "calendar", None))
    for event_name, event_value in calendar_items.items():
        normalized_name = str(event_name).strip()
        lower_name = normalized_name.lower()
        if "earnings" in lower_name:
            event_type = "earnings"
        elif "dividend" in lower_name:
            event_type = "dividend"
        elif "split" in lower_name:
            event_type = "split"
        else:
            event_type = "calendar"
        _add_event_row(
            rows,
            symbol=symbol,
            company=company,
            event_type=event_type,
            event_name=normalized_name,
            event_value=event_value,
            source_provider="yfinance",
            source_payload={normalized_name: _json_safe(event_value)},
            extracted_at=extracted_at,
            calendar_year=calendar_year,
        )

    try:
        actions = ticker.actions
    except Exception:  # noqa: BLE001
        actions = pd.DataFrame()

    if actions is not None and not actions.empty:
        normalized_actions = actions.reset_index().rename(columns={"Date": "event_timestamp"})
        normalized_actions["event_timestamp"] = pd.to_datetime(
            normalized_actions["event_timestamp"], utc=True, errors="coerce"
        )
        normalized_actions = normalized_actions[
            normalized_actions["event_timestamp"].dt.year == calendar_year
        ].copy()

        for row in normalized_actions.itertuples(index=False):
            event_timestamp = getattr(row, "event_timestamp", None)
            dividends = float(getattr(row, "Dividends", 0.0) or 0.0)
            splits = float(getattr(row, "Stock_Splits", 0.0) or 0.0)

            if event_timestamp is None or pd.isna(event_timestamp):
                continue
            if dividends:
                rows.append(
                    {
                        "symbol": symbol.upper(),
                        "company": company,
                        "event_type": "dividend",
                        "event_name": "Dividend",
                        "event_date": event_timestamp.date().isoformat(),
                        "event_timestamp": event_timestamp.isoformat(),
                        "calendar_year": calendar_year,
                        "source_provider": "yfinance",
                        "source_payload": json.dumps(
                            {"dividends": dividends, "timestamp": event_timestamp.isoformat()},
                            allow_nan=False,
                        ),
                        "extracted_at": extracted_at,
                    }
                )
            if splits:
                rows.append(
                    {
                        "symbol": symbol.upper(),
                        "company": company,
                        "event_type": "split",
                        "event_name": "Stock Split",
                        "event_date": event_timestamp.date().isoformat(),
                        "event_timestamp": event_timestamp.isoformat(),
                        "calendar_year": calendar_year,
                        "source_provider": "yfinance",
                        "source_payload": json.dumps(
                            {"stock_splits": splits, "timestamp": event_timestamp.isoformat()},
                            allow_nan=False,
                        ),
                        "extracted_at": extracted_at,
                    }
                )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "company",
                "event_type",
                "event_name",
                "event_date",
                "event_timestamp",
                "calendar_year",
                "source_provider",
                "source_payload",
                "extracted_at",
            ]
        )

    return (
        frame.drop_duplicates(subset=["symbol", "event_type", "event_name", "event_timestamp"])
        .sort_values(["symbol", "event_timestamp", "event_type", "event_name"])
        .reset_index(drop=True)
    )


def build_market_calendar(*, provider: str, symbols: Iterable[str], calendar_year: int) -> pd.DataFrame:
    fixture_frame = _load_fixture_events()
    if fixture_frame is not None:
        filtered = fixture_frame[
            (fixture_frame["calendar_year"] == calendar_year)
            & (fixture_frame["symbol"].isin([item.upper() for item in symbols]))
        ].copy()
        return filtered.sort_values(["symbol", "event_timestamp", "event_type"]).reset_index(drop=True)

    extracted_at = datetime.now(timezone.utc).isoformat()
    provider_name = provider.strip().lower()
    if provider_name not in {"yfinance", "yahoo"}:
        raise ValueError(f"Unsupported market calendar provider '{provider}'")

    frames = [
        fetch_yfinance_market_calendar(symbol, calendar_year=calendar_year, extracted_at=extracted_at)
        for symbol in symbols
    ]
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    if combined.empty:
        return combined
    return combined.sort_values(["symbol", "event_timestamp", "event_type"]).reset_index(drop=True)


def write_market_calendar(frame: pd.DataFrame) -> None:
    CALENDAR_ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(CALENDAR_ARTIFACT_PATH, index=False)

    engine = postgres_engine()
    with engine.begin() as conn:
        conn.execute(text(f"create schema if not exists {RAW_SCHEMA}"))
    frame.to_sql(RAW_TABLE, engine, schema=RAW_SCHEMA, if_exists="replace", index=False)


def ingest_market_calendar() -> pd.DataFrame:
    symbols = _symbols_to_ingest()
    calendar_year = _calendar_year()
    provider = os.getenv("MARKET_CALENDAR_PROVIDER", DEFAULT_PROVIDER)
    frame = build_market_calendar(provider=provider, symbols=symbols, calendar_year=calendar_year)
    write_market_calendar(frame)
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] ingest_market_calendar: wrote "
        f"{len(frame)} rows for {len(symbols)} symbols to {CALENDAR_ARTIFACT_PATH} "
        f"and {RAW_SCHEMA}.{RAW_TABLE}"
    )
    return frame


if __name__ == "__main__":
    ingest_market_calendar()
