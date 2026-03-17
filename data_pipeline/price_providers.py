"""Price data provider methods for yfinance, Massive, and Finnhub."""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable
from collections import deque

import requests
import yfinance as yf

try:
    from .env import load_local_env
except ImportError:
    from env import load_local_env

load_local_env()


@dataclass
class PriceQuote:
    provider: str
    symbol: str
    price: float
    currency: str
    as_of: str


class PriceProviderError(RuntimeError):
    """Raised when a price provider cannot return a quote."""


@dataclass
class SymbolMatch:
    provider: str
    query: str
    symbol: str
    name: str
    exchange: str | None = None
    currency: str | None = None


_RATE_LIMIT_LOCK = threading.Lock()
_RATE_LIMIT_WINDOWS: dict[str, deque[float]] = {}
_DEFAULT_RATE_LIMIT = 60
_RATE_LIMIT_PERIOD_SECONDS = 60.0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _provider_rate_limit(provider: str) -> int:
    specific = os.getenv(f"{provider.strip().upper()}_RATE_LIMIT_PER_MINUTE")
    if specific:
        return max(1, int(specific))
    shared = os.getenv("MARKET_DATA_RATE_LIMIT_PER_MINUTE")
    if shared:
        return max(1, int(shared))
    return _DEFAULT_RATE_LIMIT


def _wait_for_rate_limit(provider: str) -> None:
    normalized = provider.strip().lower()
    limit = _provider_rate_limit(normalized)

    while True:
        wait_time = 0.0
        with _RATE_LIMIT_LOCK:
            now = time.monotonic()
            window = _RATE_LIMIT_WINDOWS.setdefault(normalized, deque())
            while window and now - window[0] >= _RATE_LIMIT_PERIOD_SECONDS:
                window.popleft()

            if len(window) < limit:
                window.append(now)
                return

            wait_time = _RATE_LIMIT_PERIOD_SECONDS - (now - window[0])

        if wait_time > 0:
            time.sleep(wait_time)


def _rate_limited_get(provider: str, url: str, **kwargs) -> requests.Response:
    _wait_for_rate_limit(provider)
    return requests.get(url, **kwargs)


def _parse_timestamp(value: str | int | float | None) -> str:
    if value is None:
        return _utc_now().isoformat()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
        except ValueError:
            return _utc_now().isoformat()
    return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()


def _period_to_dates(period: str) -> tuple[str, str]:
    normalized = period.strip().lower()
    now = _utc_now()
    mapping = {
        "3mo": 95,
        "6mo": 190,
        "9mo": 285,
        "1y": 370,
        "2y": 740,
    }
    days = mapping.get(normalized)
    if days is None:
        raise PriceProviderError(f"Unsupported historical period '{period}' for Massive provider")
    start = (now - timedelta(days=days)).date().isoformat()
    end = now.date().isoformat()
    return start, end


def get_yfinance_latest_price(symbol: str) -> PriceQuote:
    ticker = yf.Ticker(symbol)

    fast_info = getattr(ticker, "fast_info", None)
    if fast_info:
        last_price = fast_info.get("lastPrice")
        currency = fast_info.get("currency", "USD")
        if last_price is not None:
            return PriceQuote(
                provider="yfinance",
                symbol=symbol.upper(),
                price=float(last_price),
                currency=str(currency),
                as_of=datetime.now(timezone.utc).isoformat(),
            )

    history = ticker.history(period="1d", interval="1m")
    if history.empty:
        raise PriceProviderError(f"yfinance returned no price history for {symbol}")

    price = float(history["Close"].iloc[-1])
    as_of = history.index[-1].to_pydatetime().astimezone(timezone.utc).isoformat()
    currency = str((ticker.info or {}).get("currency", "USD"))

    return PriceQuote(
        provider="yfinance",
        symbol=symbol.upper(),
        price=price,
        currency=currency,
        as_of=as_of,
    )


def get_massive_latest_price(
    symbol: str,
    api_key: str | None = None,
    base_url: str | None = None,
) -> PriceQuote:
    key = api_key or os.getenv("MASSIVE_API_KEY")
    if not key:
        raise PriceProviderError("Missing MASSIVE_API_KEY for Massive API provider")

    api_base = (base_url or os.getenv("MASSIVE_BASE_URL") or "https://api.polygon.io").rstrip("/")

    # Prefer a near real-time last trade endpoint.
    realtime_response = _rate_limited_get(
        "massive",
        f"{api_base}/v2/last/trade/{symbol.upper()}",
        params={"apiKey": key},
        timeout=30,
    )

    if realtime_response.ok:
        realtime_payload = realtime_response.json()
        result = realtime_payload.get("results") or {}
        trade_price = result.get("p")
        timestamp_ns = result.get("t")
        if trade_price is not None and timestamp_ns is not None:
            as_of = datetime.fromtimestamp(float(timestamp_ns) / 1_000_000_000, tz=timezone.utc).isoformat()
            return PriceQuote(
                provider="massive",
                symbol=symbol.upper(),
                price=float(trade_price),
                currency="USD",
                as_of=as_of,
            )

    # Fallback: previous close if real-time endpoint is unavailable for the plan/ticker.
    prev_response = _rate_limited_get(
        "massive",
        f"{api_base}/v2/aggs/ticker/{symbol.upper()}/prev",
        params={"adjusted": "true", "apiKey": key},
        timeout=30,
    )
    prev_response.raise_for_status()
    prev_payload = prev_response.json()

    prev_results = prev_payload.get("results") or []
    if not prev_results:
        raise PriceProviderError(f"Massive API returned no results for {symbol}: {prev_payload}")

    prev = prev_results[0]
    close_price = prev.get("c")
    timestamp_ms = prev.get("t")
    if close_price is None or timestamp_ms is None:
        raise PriceProviderError(f"Massive API result missing price/timestamp for {symbol}: {prev}")

    as_of = datetime.fromtimestamp(float(timestamp_ms) / 1000, tz=timezone.utc).isoformat()
    return PriceQuote(
        provider="massive",
        symbol=symbol.upper(),
        price=float(close_price),
        currency="USD",
        as_of=as_of,
    )


def get_latest_price(symbol: str, provider: str) -> PriceQuote:
    normalized = provider.strip().lower()
    if normalized in {"yfinance", "yahoo"}:
        return get_yfinance_latest_price(symbol)
    if normalized == "massive":
        return get_massive_latest_price(symbol)
    if normalized == "finnhub":
        return get_finnhub_latest_price(symbol)

    raise PriceProviderError(f"Unsupported price provider '{provider}'")


def get_massive_history(
    symbol: str,
    *,
    period: str = "9mo",
    interval: str = "1d",
    api_key: str | None = None,
    base_url: str | None = None,
) -> list[dict[str, object]]:
    if interval != "1d":
        raise PriceProviderError(f"Unsupported interval '{interval}' for Massive provider")

    key = api_key or os.getenv("MASSIVE_API_KEY")
    if not key:
        raise PriceProviderError("Missing MASSIVE_API_KEY for Massive historical provider")

    api_base = (base_url or os.getenv("MASSIVE_BASE_URL") or "https://api.polygon.io").rstrip("/")
    start, end = _period_to_dates(period)
    response = _rate_limited_get(
        "massive",
        f"{api_base}/v2/aggs/ticker/{symbol.upper()}/range/1/day/{start}/{end}",
        params={"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": key},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    results = payload.get("results") or []
    if not results:
        raise PriceProviderError(f"Massive historical API returned no rows for {symbol}: {payload}")

    rows: list[dict[str, object]] = []
    for item in results:
        close_price = item.get("c")
        timestamp_ms = item.get("t")
        if close_price is None or timestamp_ms is None:
            continue
        rows.append(
            {
                "date": datetime.fromtimestamp(float(timestamp_ms) / 1000, tz=timezone.utc).isoformat(),
                "symbol": symbol.upper(),
                "close": float(close_price),
            }
        )
    if not rows:
        raise PriceProviderError(f"Massive historical rows missing close/timestamp for {symbol}")
    return rows


def get_finnhub_latest_price(symbol: str, api_key: str | None = None) -> PriceQuote:
    key = api_key or os.getenv("FINNHUB_API_KEY")
    if not key:
        raise PriceProviderError("Missing FINNHUB_API_KEY for Finnhub provider")

    response = _rate_limited_get(
        "finnhub",
        "https://finnhub.io/api/v1/quote",
        params={"symbol": symbol.upper(), "token": key},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    current = payload.get("c")
    timestamp = payload.get("t")
    if current in (None, 0) or not timestamp:
        raise PriceProviderError(f"Finnhub returned no quote for {symbol}: {payload}")

    return PriceQuote(
        provider="finnhub",
        symbol=symbol.upper(),
        price=float(current),
        currency="USD",
        as_of=_parse_timestamp(timestamp),
    )


def search_yfinance_symbols(query: str) -> list[SymbolMatch]:
    search = yf.Search(query=query, max_results=10)
    quotes = getattr(search, "quotes", []) or []
    matches: list[SymbolMatch] = []
    for quote in quotes:
        symbol = str(quote.get("symbol") or "").strip()
        if not symbol:
            continue
        matches.append(
            SymbolMatch(
                provider="yahoo",
                query=query,
                symbol=symbol,
                name=str(quote.get("shortname") or quote.get("longname") or symbol),
                exchange=quote.get("exchange"),
                currency=quote.get("currency"),
            )
        )
    return matches


def search_massive_symbols(query: str, api_key: str | None = None) -> list[SymbolMatch]:
    key = api_key or os.getenv("MASSIVE_API_KEY")
    if not key:
        return []

    response = _rate_limited_get(
        "massive",
        f"{(os.getenv('MASSIVE_BASE_URL') or 'https://api.polygon.io').rstrip('/')}/v3/reference/tickers",
        params={"search": query, "active": "true", "limit": 10, "apiKey": key},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    results = payload.get("results") or []
    matches: list[SymbolMatch] = []
    for result in results:
        ticker = str(result.get("ticker") or "").strip()
        if not ticker:
            continue
        matches.append(
            SymbolMatch(
                provider="massive",
                query=query,
                symbol=ticker,
                name=str(result.get("name") or ticker),
                exchange=result.get("primary_exchange"),
                currency=result.get("currency_name"),
            )
        )
    return matches


def search_finnhub_symbols(query: str, api_key: str | None = None) -> list[SymbolMatch]:
    key = api_key or os.getenv("FINNHUB_API_KEY")
    if not key:
        return []

    response = _rate_limited_get(
        "finnhub",
        "https://finnhub.io/api/v1/search",
        params={"q": query, "token": key},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    results = payload.get("result") or []
    matches: list[SymbolMatch] = []
    for result in results:
        symbol = str(result.get("symbol") or "").strip()
        if not symbol:
            continue
        matches.append(
            SymbolMatch(
                provider="finnhub",
                query=query,
                symbol=symbol,
                name=str(result.get("description") or symbol),
                exchange=result.get("mic"),
                currency=result.get("currency"),
            )
        )
    return matches


def search_symbols(query: str, providers: Iterable[str]) -> list[SymbolMatch]:
    results: list[SymbolMatch] = []
    for provider in providers:
        normalized = provider.strip().lower()
        try:
            if normalized == "yahoo":
                results.extend(search_yfinance_symbols(query))
            elif normalized == "massive":
                results.extend(search_massive_symbols(query))
            elif normalized == "finnhub":
                results.extend(search_finnhub_symbols(query))
        except Exception:
            continue
    return results


def convert_quote_to_gbp(quote: PriceQuote, usd_to_gbp: Decimal) -> Decimal:
    price = Decimal(str(quote.price))
    raw_currency = (quote.currency or "").strip()
    upper_currency = raw_currency.upper()

    if raw_currency == "GBp" or upper_currency == "GBX":
        return price / Decimal("100")
    if upper_currency == "GBP":
        return price
    return price * usd_to_gbp
