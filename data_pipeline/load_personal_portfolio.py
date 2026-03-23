"""Load the personal SIPP portfolio and refresh market values from Yahoo Finance."""

from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd

from price_providers import (
    PriceProviderError,
    convert_quote_to_gbp,
    get_finnhub_latest_price,
    get_latest_price,
    search_symbols,
)
from personal_portfolios import (
    add_personal_portfolio,
    ensure_personal_portfolio_tables,
    fetch_portfolio_holdings,
    fetch_portfolio_snapshots,
    import_portfolio_holdings_from_csv,
    insert_portfolio_holdings_snapshot,
)


PORTFOLIO_NAME = "SIPP"
PORTFOLIO_HOLDER = "Ruaan Venter"
PORTFOLIO_TYPE = "SIPP"
SEED_CSV_PATH = Path(__file__).resolve().parents[1] / "data" / "personal_portfolios" / "sipp_seed_2026-03-08.csv"
SEED_SNAPSHOT_AT = datetime.fromisoformat("2026-03-08T15:32:00")
SEED_SOURCE_UPDATED_AT = datetime.fromisoformat("2026-03-08T15:31:00")
SEED_QUOTE_DELAY_NOTE = "Delayed by at least 15 minutes"
SEED_SOURCE_NAME = "Interactive Investor"
SEED_FX_NOTE = "USD values converted to GBP at indicative FX rate"
SEARCH_PROVIDERS = ("yahoo", "massive", "finnhub")
MANUAL_SYMBOL_OVERRIDES = {
    "Artemis High Income": {
        "symbol": "0P0001GZXO.L",
        "provider": "yahoo",
        "name": "Artemis High Income I Acc",
    },
    "Fundsmith Equity": {
        "symbol": "0P0000RU81.L",
        "provider": "yahoo",
        "name": "Fundsmith Equity I Acc",
    },
    "Rathbone Global Opportunities": {
        "symbol": "0P0001FE43.L",
        "provider": "yahoo",
        "name": "Rathbone Global Opportunities Fund S Acc",
    },
}

YAHOO_SYMBOLS = {
    "AAPL": "AAPL",
    "AMZN": "AMZN",
    "ASC": "ASC.L",
    "BA.": "BA.L",
    "GOOG": "GOOG",
    "GSK": "GSK.L",
    "HLN": "HLN.L",
    "ISF": "ISF.L",
    "IUKD": "IUKD.L",
    "LLOY": "LLOY.L",
    "MSFT": "MSFT",
    "NWG": "NWG.L",
    "NVDA": "NVDA",
    "RGTI": "RGTI",
    "VOD": "VOD.L",
}


def _to_decimal(value: object) -> Decimal:
    return Decimal(str(value))


def _fetch_usd_to_gbp() -> tuple[Decimal, datetime]:
    quote = get_latest_price("GBPUSD=X", "yfinance")
    gbpusd = Decimal(str(quote.price))
    if gbpusd == 0:
        raise ValueError("GBPUSD=X returned 0")
    return Decimal("1") / gbpusd, datetime.fromisoformat(quote.as_of)


def _portfolio_price_source() -> str:
    return os.getenv("PORTFOLIO_PRICE_SOURCE", "yahoo").strip().lower()


def _try_provider_quote(symbol: str, provider: str):
    normalized = provider.strip().lower()
    if normalized == "yahoo":
        return get_latest_price(symbol, "yfinance")
    if normalized == "massive":
        return get_latest_price(symbol, "massive")
    if normalized == "finnhub":
        return get_finnhub_latest_price(symbol)
    raise PriceProviderError(f"Unsupported provider {provider}")


def _candidate_queries(company: str, instrument_name: object) -> list[str]:
    queries = [company]
    instrument = str(instrument_name).strip() if instrument_name else ""
    if instrument:
        queries.append(f"{company} {instrument}")
        queries.append(instrument)
    return queries


def _resolve_symbol_from_apis(company: str, instrument_name: object) -> dict[str, object] | None:
    manual_override = MANUAL_SYMBOL_OVERRIDES.get(company)
    if manual_override:
        return manual_override

    for query in _candidate_queries(company, instrument_name):
        matches = search_symbols(query, SEARCH_PROVIDERS)
        if not matches:
            continue
        first = matches[0]
        return {
            "symbol": first.symbol,
            "provider": first.provider,
            "query": query,
            "name": first.name,
            "exchange": first.exchange,
            "currency": first.currency,
        }
    return None


def seed_reference_snapshot() -> tuple[int, int]:
    snapshots = fetch_portfolio_snapshots()
    snapshot_times = pd.to_datetime(snapshots["snapshot_at"], utc=True).dt.tz_localize(None)
    existing_seed = snapshots[
        (snapshots["portfolio_name"] == PORTFOLIO_NAME)
        & (snapshots["holder"] == PORTFOLIO_HOLDER)
        & (snapshots["portfolio_type"] == PORTFOLIO_TYPE)
        & (snapshot_times == pd.Timestamp(SEED_SNAPSHOT_AT))
    ]
    if not existing_seed.empty:
        return int(existing_seed.iloc[0]["snapshot_id"]), 0

    return import_portfolio_holdings_from_csv(
        portfolio_name=PORTFOLIO_NAME,
        holder=PORTFOLIO_HOLDER,
        portfolio_type=PORTFOLIO_TYPE,
        csv_path=str(SEED_CSV_PATH),
        snapshot_at=SEED_SNAPSHOT_AT,
        source_updated_at=SEED_SOURCE_UPDATED_AT,
        quote_delay_note=SEED_QUOTE_DELAY_NOTE,
        source_name=SEED_SOURCE_NAME,
        fx_note=SEED_FX_NOTE,
    )


def refresh_latest_snapshot_from_yahoo() -> tuple[int, int]:
    latest_holdings = fetch_portfolio_holdings()
    latest_holdings = latest_holdings[
        (latest_holdings["portfolio_name"] == PORTFOLIO_NAME)
        & (latest_holdings["holder"] == PORTFOLIO_HOLDER)
        & (latest_holdings["portfolio_type"] == PORTFOLIO_TYPE)
    ].copy()

    if latest_holdings.empty:
        raise ValueError("No existing holdings found to refresh.")

    portfolio_id = add_personal_portfolio(
        name=PORTFOLIO_NAME,
        holder=PORTFOLIO_HOLDER,
        portfolio_type=PORTFOLIO_TYPE,
    )

    usd_to_gbp, fx_timestamp = _fetch_usd_to_gbp()
    latest_quote_at = fx_timestamp
    selected_provider = _portfolio_price_source()
    refreshed_rows: list[dict[str, object]] = []

    for _, row in latest_holdings.iterrows():
        record = {
            "company": row["company"],
            "instrument_name": row["instrument_name"],
            "ticker": row["ticker"],
            "quantity": row["quantity"],
            "quantity_label": row["quantity_label"],
            "price": row["price"],
            "market_value": row["market_value"],
            "total_cost": row["total_cost"],
            "gain_loss_value": row["gain_loss_value"],
            "gain_loss_pct": row["gain_loss_pct"],
            "currency": row["currency"] or "GBP",
            "sector": row["sector"],
            "as_of_date": row["as_of_date"],
            "source_row": {
                "refresh_status": "carried_forward",
                "source_snapshot_at": str(row["snapshot_at"]),
            },
        }

        ticker = row["ticker"]
        if ticker == "CASH" or row["company"] == "Cash":
            record["source_row"] = {
                "refresh_status": "manual_cash_carried_forward",
                "source_snapshot_at": str(row["snapshot_at"]),
            }
            refreshed_rows.append(record)
            continue

        provider_symbol = YAHOO_SYMBOLS.get(ticker) if selected_provider == "yahoo" else ticker
        resolved_symbol = None
        resolved_by = None

        if not provider_symbol:
            resolved_symbol = _resolve_symbol_from_apis(row["company"], row["instrument_name"])
            if resolved_symbol:
                provider_symbol = str(resolved_symbol["symbol"])
                resolved_by = str(resolved_symbol["provider"])

        if provider_symbol and row["quantity"] is not None:
            try:
                quote = _try_provider_quote(provider_symbol, selected_provider if ticker else (resolved_by or selected_provider))
                quote_timestamp = datetime.fromisoformat(quote.as_of)
                latest_quote_at = max(latest_quote_at, quote_timestamp)
                gbp_price = convert_quote_to_gbp(quote, usd_to_gbp)

                quantity = _to_decimal(row["quantity"])
                total_cost = _to_decimal(row["total_cost"])
                market_value = (quantity * gbp_price).quantize(Decimal("0.01"))
                gain_loss_value = (market_value - total_cost).quantize(Decimal("0.01"))
                gain_loss_pct = (
                    ((gain_loss_value / total_cost) * Decimal("100")).quantize(Decimal("0.01"))
                    if total_cost != 0
                    else None
                )

                record.update(
                    {
                        "price": gbp_price.quantize(Decimal("0.000001")),
                        "market_value": market_value,
                        "gain_loss_value": gain_loss_value,
                        "gain_loss_pct": gain_loss_pct,
                        "as_of_date": quote_timestamp.date(),
                        "source_row": {
                            "refresh_status": f"{quote.provider}_updated",
                            "selected_provider": selected_provider,
                            "resolved_provider": resolved_by,
                            "provider_symbol": provider_symbol,
                            "quote_timestamp": quote_timestamp.isoformat(),
                            "resolved_symbol": resolved_symbol,
                        },
                    }
                )
            except Exception as exc:  # noqa: BLE001
                record["source_row"] = {
                    "refresh_status": "provider_unavailable",
                    "selected_provider": selected_provider,
                    "provider_symbol": provider_symbol,
                    "resolved_symbol": resolved_symbol,
                    "error": str(exc),
                }

        refreshed_rows.append(record)

    refreshed_frame = pd.DataFrame(refreshed_rows)
    snapshot_at = datetime.now()
    snapshot_id, row_count = insert_portfolio_holdings_snapshot(
        portfolio_id=portfolio_id,
        snapshot_at=snapshot_at,
        holdings_frame=refreshed_frame,
        source_updated_at=latest_quote_at,
        quote_delay_note=f"Latest available quote from {selected_provider}",
        source_name=selected_provider,
        fx_note=f"USD holdings converted to GBP using GBPUSD=X as of {fx_timestamp.isoformat()}",
    )
    return snapshot_id, row_count


def main() -> None:
    ensure_personal_portfolio_tables()
    seed_snapshot_id, seed_rows = seed_reference_snapshot()
    refresh_snapshot_id, refresh_rows = refresh_latest_snapshot_from_yahoo()
    print(
        "Loaded personal portfolio snapshots: "
        f"seed_snapshot_id={seed_snapshot_id} seed_rows={seed_rows} "
        f"refresh_snapshot_id={refresh_snapshot_id} refresh_rows={refresh_rows}"
    )


if __name__ == "__main__":
    main()
