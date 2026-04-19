"""Load the personal ISA portfolio and refresh market values from Yahoo Finance.

Broker: Hargreaves Lansdown (HL)

## Importing the first snapshot

When you have a CSV export from HL, import it using the generic add-portfolio target:

    PORTFOLIO_NAME="ISA" \\
    PORTFOLIO_HOLDER="Ruaan Venter" \\
    PORTFOLIO_TYPE=ISA \\
    PORTFOLIO_CSV_PATH=/absolute/path/to/isa_export.csv \\
    PORTFOLIO_SNAPSHOT_AT=2026-04-16T17:00:00 \\
    PORTFOLIO_SOURCE_UPDATED_AT=2026-04-16T16:35:00 \\
    PORTFOLIO_QUOTE_DELAY_NOTE="Delayed by at least 15 minutes" \\
    PORTFOLIO_SOURCE_NAME="Hargreaves Lansdown" \\
    make add-portfolio

Once a snapshot is in the database, `make load-isa-portfolio` will refresh prices
on the standard daily schedule.

## Hargreaves Lansdown CSV notes

HL account exports typically include these columns (normalised names in parentheses):
  - Stock          → company
  - Epic           → ticker  (LSE epic code, e.g. "VOD", "LLOY")
  - Units          → quantity
  - Price (p)      → price   (NOTE: UK equity prices are in PENCE — divide by 100 for £)
  - Value (£)      → market_value
  - Book cost (£)  → total_cost
  - Gain/loss (£)  → gain_loss_value
  - Gain/loss (%)  → gain_loss_pct

These column names are handled by the platform's flexible column mapper. If an HL
export uses different column names, add them to the candidates in
`personal_portfolios._prepare_holdings_frame`.

## Symbol mapping

After importing the first snapshot, inspect unresolved symbols with:

    SELECT ticker, company, source_row->>'refresh_status'
    FROM app.latest_portfolio_holdings
    WHERE portfolio_name = 'ISA';

For any holding that resolves as "carried_forward" or "provider_unavailable",
add an entry to MANUAL_SYMBOL_OVERRIDES below.
"""

from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal

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
    insert_portfolio_holdings_snapshot,
)
from yahoo_symbols import YAHOO_SYMBOLS


PORTFOLIO_NAME = "ISA"
PORTFOLIO_HOLDER = "Ruaan Venter"
PORTFOLIO_TYPE = "ISA"
SEARCH_PROVIDERS = ("yahoo", "massive", "finnhub")

# Add ISA-specific overrides here once you know which holdings don't resolve
# automatically. Use the same format as load_personal_portfolio.py.
# Example:
#   "Fundsmith Equity": {
#       "symbol": "0P0000RU81.L",
#       "provider": "yahoo",
#       "name": "Fundsmith Equity I Acc",
#   },
MANUAL_SYMBOL_OVERRIDES: dict[str, dict[str, object]] = {}


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


def refresh_latest_snapshot() -> tuple[int, int] | None:
    """Refresh ISA portfolio prices from Yahoo Finance.

    Returns (snapshot_id, row_count) on success, or None if no holdings
    exist yet (i.e. no CSV has been imported yet).
    """
    latest_holdings = fetch_portfolio_holdings()
    isa_holdings = latest_holdings[
        (latest_holdings["portfolio_name"] == PORTFOLIO_NAME)
        & (latest_holdings["holder"] == PORTFOLIO_HOLDER)
        & (latest_holdings["portfolio_type"] == PORTFOLIO_TYPE)
    ].copy()

    if isa_holdings.empty:
        print(
            f"No holdings found for {PORTFOLIO_NAME} ({PORTFOLIO_HOLDER}). "
            "Import a CSV snapshot first — see the docstring at the top of this file."
        )
        return None

    portfolio_id = add_personal_portfolio(
        name=PORTFOLIO_NAME,
        holder=PORTFOLIO_HOLDER,
        portfolio_type=PORTFOLIO_TYPE,
    )

    usd_to_gbp, fx_timestamp = _fetch_usd_to_gbp()
    latest_quote_at = fx_timestamp
    selected_provider = _portfolio_price_source()
    refreshed_rows: list[dict[str, object]] = []

    for _, row in isa_holdings.iterrows():
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
                quote = _try_provider_quote(
                    provider_symbol,
                    selected_provider if ticker else (resolved_by or selected_provider),
                )
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

                record.update({
                    "ticker": provider_symbol if not ticker else ticker,
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
                })
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
    result = refresh_latest_snapshot()
    if result is None:
        return
    refresh_snapshot_id, refresh_rows = result
    print(
        f"Refreshed ISA portfolio snapshot: "
        f"refresh_snapshot_id={refresh_snapshot_id} refresh_rows={refresh_rows}"
    )


if __name__ == "__main__":
    main()
