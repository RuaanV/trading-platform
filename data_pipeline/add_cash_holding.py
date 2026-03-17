"""Append a cash holding to the latest SIPP portfolio snapshot."""

from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal

import pandas as pd

from personal_portfolios import (
    add_personal_portfolio,
    fetch_portfolio_holdings,
    insert_portfolio_holdings_snapshot,
)


PORTFOLIO_NAME = "SIPP"
PORTFOLIO_HOLDER = "Ruaan Venter"
PORTFOLIO_TYPE = "SIPP"


def _cash_value() -> Decimal:
    raw = os.getenv("PORTFOLIO_CASH_VALUE", "7900.53").strip()
    return Decimal(raw)


def add_cash_holding() -> tuple[int, Decimal]:
    latest_holdings = fetch_portfolio_holdings()
    latest_holdings = latest_holdings[
        (latest_holdings["portfolio_name"] == PORTFOLIO_NAME)
        & (latest_holdings["holder"] == PORTFOLIO_HOLDER)
        & (latest_holdings["portfolio_type"] == PORTFOLIO_TYPE)
    ].copy()

    if latest_holdings.empty:
        raise ValueError("No existing holdings found to clone for the cash snapshot.")

    latest_holdings = latest_holdings.drop(
        columns=[
            "portfolio_name",
            "holder",
            "portfolio_type",
            "snapshot_at",
            "source_updated_at",
            "quote_delay_note",
            "created_at",
        ]
    )
    latest_holdings = latest_holdings[latest_holdings["company"] != "Cash"].copy()
    latest_holdings["source_row"] = latest_holdings["ticker"].map(
        lambda _: {"refresh_status": "carried_forward_from_latest_snapshot"}
    )

    cash_value = _cash_value()
    cash_row = {
        "company": "Cash",
        "instrument_name": "Available to invest",
        "ticker": "CASH",
        "quantity": cash_value,
        "quantity_label": "GBP",
        "price": Decimal("1"),
        "market_value": cash_value,
        "total_cost": cash_value,
        "gain_loss_value": Decimal("0"),
        "gain_loss_pct": Decimal("0"),
        "currency": "GBP",
        "sector": None,
        "as_of_date": datetime.now().date(),
        "source_row": {
            "refresh_status": "manual_cash_added",
            "cash_value": str(cash_value),
        },
    }

    updated_holdings = pd.concat([latest_holdings, pd.DataFrame([cash_row])], ignore_index=True)

    portfolio_id = add_personal_portfolio(
        name=PORTFOLIO_NAME,
        holder=PORTFOLIO_HOLDER,
        portfolio_type=PORTFOLIO_TYPE,
    )
    snapshot_id, _ = insert_portfolio_holdings_snapshot(
        portfolio_id=portfolio_id,
        snapshot_at=datetime.now(),
        holdings_frame=updated_holdings,
        source_updated_at=datetime.now(),
        quote_delay_note="Latest portfolio snapshot with manual cash balance",
        source_name="manual_cash",
        fx_note=None,
    )
    return snapshot_id, cash_value


def main() -> None:
    snapshot_id, cash_value = add_cash_holding()
    print(f"Added cash holding to snapshot {snapshot_id}: cash_value={cash_value}")


if __name__ == "__main__":
    main()
