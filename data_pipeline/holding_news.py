"""Fetch and persist holding-level news headlines with basic sentiment scoring."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import math
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text
import yfinance as yf

try:
    from .db import postgres_engine
    from .yahoo_symbols import resolve_yahoo_symbol
except ImportError:
    from db import postgres_engine
    from yahoo_symbols import resolve_yahoo_symbol


PORTFOLIO_SCHEMA = "app"
HOLDING_NEWS_TABLE = "holding_news_sentiment"
DEFAULT_SENTIMENT_VERSION = "headline_lexicon_v1"
DEFAULT_SOURCE = "Yahoo Finance"
DEFAULT_LOCAL_TIMEZONE = "Europe/London"

POSITIVE_PHRASE_WEIGHTS = {
    "beats expectations": 2.0,
    "beat expectations": 2.0,
    "raises guidance": 1.8,
    "record revenue": 1.8,
    "strong demand": 1.6,
    "price target raised": 1.4,
}

NEGATIVE_PHRASE_WEIGHTS = {
    "misses expectations": -2.0,
    "missed expectations": -2.0,
    "cuts guidance": -1.8,
    "antitrust probe": -1.6,
    "price target cut": -1.4,
    "slowing demand": -1.6,
}

TOKEN_WEIGHTS = {
    "beat": 1.0,
    "beats": 1.0,
    "bullish": 1.2,
    "buyback": 0.8,
    "expands": 0.8,
    "expansion": 0.8,
    "gain": 0.8,
    "gains": 0.8,
    "growth": 1.0,
    "higher": 0.5,
    "outperform": 1.0,
    "outperforms": 1.0,
    "profit": 1.0,
    "profits": 1.0,
    "rally": 1.0,
    "raises": 0.8,
    "record": 0.8,
    "rebound": 0.8,
    "strong": 0.8,
    "surge": 1.0,
    "upside": 0.8,
    "upgrade": 1.2,
    "upgrades": 1.2,
    "warning": -1.0,
    "weak": -0.8,
    "weaker": -0.8,
    "cuts": -0.8,
    "cut": -0.8,
    "decline": -1.0,
    "declines": -1.0,
    "downgrade": -1.2,
    "downgrades": -1.2,
    "drop": -1.0,
    "drops": -1.0,
    "fall": -1.0,
    "falls": -1.0,
    "investigation": -1.0,
    "lawsuit": -1.2,
    "miss": -1.0,
    "misses": -1.0,
    "probe": -1.2,
    "risk": -0.6,
    "risks": -0.6,
    "selloff": -1.0,
    "slump": -1.0,
}


@dataclass(slots=True)
class SymbolNewsSentiment:
    symbol: str
    headline_count: int
    average_sentiment_score: float
    sentiment_label: str
    as_of_date: str


def _coerce_datetime(value: object) -> datetime | None:
    if value in (None, "", 0):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)) and not math.isnan(float(value)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            return None
    return None


def _extract_nested(item: dict[str, object], *path: str) -> object:
    current: object = item
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _article_link(item: dict[str, object]) -> str | None:
    candidates = (
        _extract_nested(item, "content", "clickThroughUrl", "url"),
        _extract_nested(item, "content", "canonicalUrl", "url"),
        _extract_nested(item, "clickThroughUrl", "url"),
        _extract_nested(item, "canonicalUrl", "url"),
        item.get("link"),
        item.get("url"),
    )
    for candidate in candidates:
        value = str(candidate or "").strip()
        if value:
            return value
    return None


def _article_title(item: dict[str, object]) -> str | None:
    candidates = (
        _extract_nested(item, "content", "title"),
        item.get("title"),
    )
    for candidate in candidates:
        value = str(candidate or "").strip()
        if value:
            return value
    return None


def _article_summary(item: dict[str, object]) -> str | None:
    candidates = (
        _extract_nested(item, "content", "summary"),
        item.get("summary"),
    )
    for candidate in candidates:
        value = str(candidate or "").strip()
        if value:
            return value
    return None


def _article_publisher(item: dict[str, object]) -> str | None:
    candidates = (
        _extract_nested(item, "content", "provider", "displayName"),
        _extract_nested(item, "content", "provider", "name"),
        item.get("publisher"),
        item.get("provider"),
    )
    for candidate in candidates:
        value = str(candidate or "").strip()
        if value:
            return value
    return None


def _article_id(symbol: str, title: str, link: str) -> str:
    payload = f"{symbol.upper()}|{title.strip()}|{link.strip()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def score_headline_sentiment(text: str) -> float:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return 0.0

    score = 0.0
    for phrase, weight in POSITIVE_PHRASE_WEIGHTS.items():
        if phrase in normalized:
            score += weight
    for phrase, weight in NEGATIVE_PHRASE_WEIGHTS.items():
        if phrase in normalized:
            score += weight

    cleaned = "".join(char if char.isalnum() else " " for char in normalized)
    tokens = [token for token in cleaned.split() if token]
    for token in tokens:
        score += TOKEN_WEIGHTS.get(token, 0.0)

    if not tokens:
        return 0.0

    normalized_score = score / max(3.0, len(tokens) / 2.0)
    return max(-1.0, min(1.0, round(normalized_score, 4)))


def sentiment_label(score: float) -> str:
    if score >= 0.15:
        return "bullish"
    if score <= -0.15:
        return "bearish"
    return "neutral"


def _normalize_news_item(
    symbol: str,
    item: dict[str, object],
    *,
    target_date: date,
    timezone_name: str = DEFAULT_LOCAL_TIMEZONE,
) -> dict[str, object] | None:
    title = _article_title(item)
    link = _article_link(item)
    if not title or not link:
        return None

    published_at = _coerce_datetime(
        item.get("providerPublishTime")
        or _extract_nested(item, "content", "pubDate")
        or item.get("pubDate")
        or item.get("published_at")
    )
    if published_at is None:
        return None

    local_date = published_at.astimezone(ZoneInfo(timezone_name)).date()
    if local_date != target_date:
        return None

    summary = _article_summary(item)
    score_input = title if not summary else f"{title}. {summary}"
    score = score_headline_sentiment(score_input)

    return {
        "symbol": symbol.upper(),
        "article_id": _article_id(symbol, title, link),
        "published_at": published_at.isoformat(),
        "article_title": title,
        "article_summary": summary,
        "article_link": link,
        "source_name": DEFAULT_SOURCE,
        "publisher_name": _article_publisher(item) or DEFAULT_SOURCE,
        "sentiment_score": score,
        "sentiment_label": sentiment_label(score),
        "sentiment_version": DEFAULT_SENTIMENT_VERSION,
        "article_payload": item,
    }


def fetch_yahoo_finance_headlines(
    symbol: str,
    *,
    target_date: date | None = None,
    timezone_name: str = DEFAULT_LOCAL_TIMEZONE,
) -> pd.DataFrame:
    if target_date is None:
        target_date = datetime.now(ZoneInfo(timezone_name)).date()

    ticker = yf.Ticker(symbol.upper())
    raw_items = getattr(ticker, "news", None) or []
    normalized_items = [
        _normalize_news_item(symbol, item, target_date=target_date, timezone_name=timezone_name)
        for item in raw_items
        if isinstance(item, dict)
    ]
    rows = [item for item in normalized_items if item is not None]
    if not rows:
        return pd.DataFrame(
            columns=[
                "symbol",
                "article_id",
                "published_at",
                "article_title",
                "article_summary",
                "article_link",
                "source_name",
                "publisher_name",
                "sentiment_score",
                "sentiment_label",
                "sentiment_version",
                "article_payload",
            ]
        )

    frame = pd.DataFrame(rows).drop_duplicates(subset=["symbol", "article_id"]).copy()
    frame["published_at"] = pd.to_datetime(frame["published_at"], utc=True, errors="coerce")
    frame = frame.sort_values("published_at", ascending=False).reset_index(drop=True)
    return frame


def summarize_news_sentiment(frame: pd.DataFrame, *, timezone_name: str = DEFAULT_LOCAL_TIMEZONE) -> SymbolNewsSentiment | None:
    if frame.empty:
        return None

    working = frame.copy()
    working["symbol"] = working["symbol"].astype(str).str.upper()
    working["sentiment_score"] = pd.to_numeric(working["sentiment_score"], errors="coerce")
    working["published_at"] = pd.to_datetime(working["published_at"], utc=True, errors="coerce")
    working = working.dropna(subset=["symbol", "sentiment_score", "published_at"])
    if working.empty:
        return None

    working["local_date"] = working["published_at"].dt.tz_convert(ZoneInfo(timezone_name)).dt.date
    latest_date = working["local_date"].max()
    working = working.loc[working["local_date"] == latest_date].copy()

    symbol = str(working.iloc[0]["symbol"])
    average_score = round(float(working["sentiment_score"].mean()), 4)
    return SymbolNewsSentiment(
        symbol=symbol,
        headline_count=int(len(working)),
        average_sentiment_score=average_score,
        sentiment_label=sentiment_label(average_score),
        as_of_date=latest_date.isoformat(),
    )


def ensure_holding_news_table() -> None:
    engine = postgres_engine()
    create_sql = f"""
    create schema if not exists {PORTFOLIO_SCHEMA};

    create table if not exists {PORTFOLIO_SCHEMA}.{HOLDING_NEWS_TABLE} (
        id bigserial primary key,
        symbol text not null,
        article_id text not null,
        published_at timestamptz not null,
        fetched_at timestamptz not null default now(),
        article_title text not null,
        article_summary text,
        article_link text not null,
        source_name text not null,
        publisher_name text,
        sentiment_score numeric(10, 4) not null,
        sentiment_label text not null,
        sentiment_version text not null,
        article_payload jsonb,
        unique (symbol, article_id)
    );

    create index if not exists idx_{HOLDING_NEWS_TABLE}_symbol_published_at
        on {PORTFOLIO_SCHEMA}.{HOLDING_NEWS_TABLE} (symbol, published_at desc);
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def store_symbol_news_sentiment(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0

    ensure_holding_news_table()
    engine = postgres_engine()
    insert_sql = text(
        f"""
        insert into {PORTFOLIO_SCHEMA}.{HOLDING_NEWS_TABLE} (
            symbol,
            article_id,
            published_at,
            article_title,
            article_summary,
            article_link,
            source_name,
            publisher_name,
            sentiment_score,
            sentiment_label,
            sentiment_version,
            article_payload
        ) values (
            :symbol,
            :article_id,
            :published_at,
            :article_title,
            :article_summary,
            :article_link,
            :source_name,
            :publisher_name,
            :sentiment_score,
            :sentiment_label,
            :sentiment_version,
            cast(:article_payload as jsonb)
        )
        on conflict (symbol, article_id) do update
        set published_at = excluded.published_at,
            fetched_at = now(),
            article_title = excluded.article_title,
            article_summary = excluded.article_summary,
            article_link = excluded.article_link,
            source_name = excluded.source_name,
            publisher_name = excluded.publisher_name,
            sentiment_score = excluded.sentiment_score,
            sentiment_label = excluded.sentiment_label,
            sentiment_version = excluded.sentiment_version,
            article_payload = excluded.article_payload
        """
    )
    records = [
        {
            "symbol": str(row["symbol"]).upper(),
            "article_id": str(row["article_id"]),
            "published_at": pd.to_datetime(row["published_at"], utc=True, errors="coerce").to_pydatetime(),
            "article_title": str(row["article_title"]),
            "article_summary": None if pd.isna(row.get("article_summary")) else str(row.get("article_summary")),
            "article_link": str(row["article_link"]),
            "source_name": str(row.get("source_name") or DEFAULT_SOURCE),
            "publisher_name": None if pd.isna(row.get("publisher_name")) else str(row.get("publisher_name")),
            "sentiment_score": float(row["sentiment_score"]),
            "sentiment_label": str(row["sentiment_label"]),
            "sentiment_version": str(row.get("sentiment_version") or DEFAULT_SENTIMENT_VERSION),
            "article_payload": json.dumps(row.get("article_payload") or {}, allow_nan=False),
        }
        for row in frame.to_dict(orient="records")
    ]
    with engine.begin() as conn:
        conn.execute(insert_sql, records)
    return len(records)


def load_symbol_news_sentiment(symbol: str, *, limit: int = 10) -> pd.DataFrame:
    ensure_holding_news_table()
    engine = postgres_engine()
    query = text(
        f"""
        select
            symbol,
            published_at,
            fetched_at,
            article_title,
            article_summary,
            article_link,
            source_name,
            publisher_name,
            sentiment_score,
            sentiment_label,
            sentiment_version
        from {PORTFOLIO_SCHEMA}.{HOLDING_NEWS_TABLE}
        where upper(symbol) = :symbol
        order by published_at desc, fetched_at desc
        limit :limit
        """
    )
    frame = pd.read_sql(query, engine, params={"symbol": symbol.upper(), "limit": int(limit)})
    if frame.empty:
        return frame
    frame["published_at"] = pd.to_datetime(frame["published_at"], utc=True, errors="coerce")
    frame["fetched_at"] = pd.to_datetime(frame["fetched_at"], utc=True, errors="coerce")
    frame["sentiment_score"] = pd.to_numeric(frame["sentiment_score"], errors="coerce")
    return frame


def refresh_symbol_news_sentiment(
    symbol: str,
    *,
    target_date: date | None = None,
    timezone_name: str = DEFAULT_LOCAL_TIMEZONE,
) -> tuple[pd.DataFrame, SymbolNewsSentiment | None]:
    holding_symbol = str(symbol).strip().upper()
    provider_symbol = resolve_yahoo_symbol(holding_symbol)
    headlines = fetch_yahoo_finance_headlines(
        provider_symbol,
        target_date=target_date,
        timezone_name=timezone_name,
    )
    if not headlines.empty:
        headlines = headlines.copy()
        headlines["symbol"] = holding_symbol
    store_symbol_news_sentiment(headlines)
    return headlines, summarize_news_sentiment(headlines, timezone_name=timezone_name)
