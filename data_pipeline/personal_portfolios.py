"""Create, query, and import personal portfolio snapshots in Postgres."""

from __future__ import annotations

import csv
import io
import os
import re
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
from sqlalchemy import text

try:
    from .db import postgres_engine
except ImportError:
    from db import postgres_engine


PORTFOLIO_SCHEMA = "app"
PORTFOLIO_TABLE = "personal_portfolios"
SNAPSHOT_TABLE = "portfolio_snapshots"
HOLDINGS_TABLE = "portfolio_holdings"


def _normalize_column_name(name: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9]+", "_", str(name).strip().lower())
    return value.strip("_")


def _normalize_string(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_json(value: object) -> str | None:
    if value is None or (hasattr(pd, "isna") and pd.isna(value)):
        return None
    return json.dumps(_json_safe_value(value), allow_nan=False)


def _json_safe_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, dict):
        return {key: _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if hasattr(pd, "isna") and pd.isna(value):
        return None
    if isinstance(value, Decimal):
        return str(value)
    return value


def ensure_personal_portfolio_tables() -> None:
    engine = postgres_engine()
    create_sql = f"""
    create schema if not exists {PORTFOLIO_SCHEMA};

    create table if not exists {PORTFOLIO_SCHEMA}.{PORTFOLIO_TABLE} (
        id bigserial primary key,
        name text not null,
        holder text not null,
        portfolio_type text not null,
        created_at timestamptz not null default now(),
        unique (name, holder, portfolio_type)
    );

    create table if not exists {PORTFOLIO_SCHEMA}.{SNAPSHOT_TABLE} (
        id bigserial primary key,
        portfolio_id bigint not null references {PORTFOLIO_SCHEMA}.{PORTFOLIO_TABLE}(id) on delete cascade,
        snapshot_at timestamptz not null,
        source_updated_at timestamptz,
        quote_delay_note text,
        source_name text,
        fx_note text,
        created_at timestamptz not null default now()
    );

    create index if not exists idx_{SNAPSHOT_TABLE}_portfolio_snapshot_at
        on {PORTFOLIO_SCHEMA}.{SNAPSHOT_TABLE} (portfolio_id, snapshot_at desc);

    create table if not exists {PORTFOLIO_SCHEMA}.{HOLDINGS_TABLE} (
        id bigserial primary key,
        portfolio_id bigint not null references {PORTFOLIO_SCHEMA}.{PORTFOLIO_TABLE}(id) on delete cascade,
        snapshot_id bigint references {PORTFOLIO_SCHEMA}.{SNAPSHOT_TABLE}(id) on delete cascade,
        company text not null,
        instrument_name text,
        ticker text,
        quantity numeric(20, 6),
        quantity_label text,
        price numeric(20, 6),
        market_value numeric(20, 2),
        total_cost numeric(20, 2),
        gain_loss_value numeric(20, 2),
        gain_loss_pct numeric(10, 4),
        currency text,
        sector text,
        as_of_date date,
        source_row jsonb,
        created_at timestamptz not null default now()
    );

    alter table {PORTFOLIO_SCHEMA}.{HOLDINGS_TABLE}
        add column if not exists snapshot_id bigint references {PORTFOLIO_SCHEMA}.{SNAPSHOT_TABLE}(id) on delete cascade,
        add column if not exists instrument_name text,
        add column if not exists quantity numeric(20, 6),
        add column if not exists quantity_label text,
        add column if not exists total_cost numeric(20, 2),
        add column if not exists gain_loss_value numeric(20, 2),
        add column if not exists gain_loss_pct numeric(10, 4);

    alter table {PORTFOLIO_SCHEMA}.{HOLDINGS_TABLE}
        alter column portfolio_id drop not null;

    create or replace view {PORTFOLIO_SCHEMA}.latest_portfolio_holdings as
    with latest_snapshot as (
        select distinct on (portfolio_id)
            id,
            portfolio_id,
            snapshot_at,
            source_updated_at,
            quote_delay_note,
            source_name,
            fx_note
        from {PORTFOLIO_SCHEMA}.{SNAPSHOT_TABLE}
        order by portfolio_id, snapshot_at desc, id desc
    )
    select
        p.id as portfolio_id,
        p.name as portfolio_name,
        p.holder,
        p.portfolio_type,
        ls.id as snapshot_id,
        ls.snapshot_at,
        ls.source_updated_at,
        ls.quote_delay_note,
        ls.source_name,
        ls.fx_note,
        h.company,
        h.instrument_name,
        h.ticker,
        h.quantity,
        h.quantity_label,
        h.price,
        h.market_value,
        h.total_cost,
        h.gain_loss_value,
        h.gain_loss_pct,
        h.currency,
        h.sector,
        h.as_of_date,
        h.source_row
    from latest_snapshot ls
    join {PORTFOLIO_SCHEMA}.{PORTFOLIO_TABLE} p
      on p.id = ls.portfolio_id
    join {PORTFOLIO_SCHEMA}.{HOLDINGS_TABLE} h
      on h.snapshot_id = ls.id;

    create or replace view {PORTFOLIO_SCHEMA}.latest_portfolio_resolved_symbols as
    select
        portfolio_id,
        portfolio_name,
        holder,
        portfolio_type,
        snapshot_id,
        snapshot_at,
        company,
        ticker,
        source_row ->> 'refresh_status' as refresh_status,
        source_row ->> 'selected_provider' as selected_provider,
        source_row ->> 'resolved_provider' as resolved_provider,
        source_row ->> 'provider_symbol' as provider_symbol,
        source_row ->> 'quote_timestamp' as quote_timestamp,
        market_value,
        as_of_date
    from {PORTFOLIO_SCHEMA}.latest_portfolio_holdings
    where source_row ? 'provider_symbol';
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))


def add_personal_portfolio(name: str, holder: str, portfolio_type: str) -> int:
    engine = postgres_engine()
    insert_sql = f"""
    insert into {PORTFOLIO_SCHEMA}.{PORTFOLIO_TABLE} (name, holder, portfolio_type)
    values (:name, :holder, :portfolio_type)
    on conflict (name, holder, portfolio_type) do update
    set name = excluded.name
    returning id;
    """

    with engine.begin() as conn:
        portfolio_id = conn.execute(
            text(insert_sql),
            {
                "name": name.strip(),
                "holder": holder.strip(),
                "portfolio_type": portfolio_type.strip(),
            },
        ).scalar_one()

    return int(portfolio_id)


def create_portfolio_snapshot(
    *,
    portfolio_id: int,
    snapshot_at: datetime,
    source_updated_at: datetime | None,
    quote_delay_note: str | None,
    source_name: str | None,
    fx_note: str | None,
) -> int:
    engine = postgres_engine()
    insert_sql = f"""
    insert into {PORTFOLIO_SCHEMA}.{SNAPSHOT_TABLE} (
        portfolio_id,
        snapshot_at,
        source_updated_at,
        quote_delay_note,
        source_name,
        fx_note
    ) values (
        :portfolio_id,
        :snapshot_at,
        :source_updated_at,
        :quote_delay_note,
        :source_name,
        :fx_note
    )
    returning id;
    """

    with engine.begin() as conn:
        snapshot_id = conn.execute(
            text(insert_sql),
            {
                "portfolio_id": portfolio_id,
                "snapshot_at": snapshot_at,
                "source_updated_at": source_updated_at,
                "quote_delay_note": quote_delay_note,
                "source_name": source_name,
                "fx_note": fx_note,
            },
        ).scalar_one()

    return int(snapshot_id)


def fetch_personal_portfolios() -> pd.DataFrame:
    engine = postgres_engine()
    query = f"""
    with latest_snapshot as (
        select distinct on (portfolio_id)
            id,
            portfolio_id,
            snapshot_at,
            source_updated_at,
            quote_delay_note,
            source_name,
            fx_note
        from {PORTFOLIO_SCHEMA}.{SNAPSHOT_TABLE}
        order by portfolio_id, snapshot_at desc, id desc
    )
    select
        p.id,
        p.name,
        p.holder,
        p.portfolio_type,
        ls.snapshot_at as latest_snapshot_at,
        ls.source_updated_at,
        ls.quote_delay_note,
        count(h.id) as holdings_count,
        coalesce(sum(h.market_value), 0) as total_market_value,
        coalesce(sum(h.total_cost), 0) as total_cost,
        coalesce(sum(h.gain_loss_value), 0) as gain_loss_value,
        p.created_at
    from {PORTFOLIO_SCHEMA}.{PORTFOLIO_TABLE} p
    left join latest_snapshot ls
      on ls.portfolio_id = p.id
    left join {PORTFOLIO_SCHEMA}.{HOLDINGS_TABLE} h
      on h.snapshot_id = ls.id
    group by
        p.id,
        p.name,
        p.holder,
        p.portfolio_type,
        ls.snapshot_at,
        ls.source_updated_at,
        ls.quote_delay_note,
        p.created_at
    order by p.holder, p.name, p.portfolio_type;
    """
    return pd.read_sql(text(query), engine)


def fetch_portfolio_holdings() -> pd.DataFrame:
    engine = postgres_engine()
    # previous_price is the last price recorded on a *different* (earlier) calendar
    # day (UTC), so the Move indicator on the dashboard always reflects the change
    # from the previous trading day's close — not an earlier run on the same day.
    query = f"""
    with latest_snapshot as (
        select distinct on (portfolio_id)
            id,
            portfolio_id,
            snapshot_at,
            (snapshot_at at time zone 'UTC')::date as snapshot_day,
            source_updated_at,
            quote_delay_note,
            source_name,
            fx_note
        from {PORTFOLIO_SCHEMA}.{SNAPSHOT_TABLE}
        order by portfolio_id, snapshot_at desc, id desc
    ),
    prev_day_snapshot as (
        select distinct on (s.portfolio_id)
            s.id,
            s.portfolio_id
        from {PORTFOLIO_SCHEMA}.{SNAPSHOT_TABLE} s
        join latest_snapshot ls on ls.portfolio_id = s.portfolio_id
        where (s.snapshot_at at time zone 'UTC')::date < ls.snapshot_day
        order by s.portfolio_id, s.snapshot_at desc, s.id desc
    ),
    prev_prices as (
        select
            h.portfolio_id,
            upper(coalesce(h.ticker, '')) as ticker_key,
            h.price                        as previous_price
        from {PORTFOLIO_SCHEMA}.{HOLDINGS_TABLE} h
        join prev_day_snapshot ps on ps.id = h.snapshot_id
    )
    select
        p.name          as portfolio_name,
        p.holder,
        p.portfolio_type,
        ls.snapshot_at,
        ls.source_updated_at,
        ls.quote_delay_note,
        h.company,
        h.instrument_name,
        h.ticker,
        h.quantity,
        h.quantity_label,
        h.price,
        pp.previous_price,
        h.market_value,
        h.total_cost,
        h.gain_loss_value,
        h.gain_loss_pct,
        h.currency,
        h.sector,
        h.as_of_date,
        h.created_at
    from latest_snapshot ls
    join {PORTFOLIO_SCHEMA}.{PORTFOLIO_TABLE} p
      on p.id = ls.portfolio_id
    join {PORTFOLIO_SCHEMA}.{HOLDINGS_TABLE} h
      on h.snapshot_id = ls.id
    left join prev_prices pp
      on pp.portfolio_id = ls.portfolio_id
     and pp.ticker_key = upper(coalesce(h.ticker, ''))
    order by p.holder, p.name, h.market_value desc nulls last, h.company;
    """
    return pd.read_sql(text(query), engine)


def fetch_portfolio_snapshots() -> pd.DataFrame:
    engine = postgres_engine()
    query = f"""
    select
        p.name as portfolio_name,
        p.holder,
        p.portfolio_type,
        s.id as snapshot_id,
        s.snapshot_at,
        s.source_updated_at,
        s.quote_delay_note,
        s.source_name,
        s.fx_note,
        count(h.id) as holdings_count,
        coalesce(sum(h.market_value), 0) as total_market_value,
        coalesce(sum(h.total_cost), 0) as total_cost,
        coalesce(sum(h.gain_loss_value), 0) as gain_loss_value
    from {PORTFOLIO_SCHEMA}.{SNAPSHOT_TABLE} s
    join {PORTFOLIO_SCHEMA}.{PORTFOLIO_TABLE} p
      on p.id = s.portfolio_id
    left join {PORTFOLIO_SCHEMA}.{HOLDINGS_TABLE} h
      on h.snapshot_id = s.id
    group by
        p.name,
        p.holder,
        p.portfolio_type,
        s.id,
        s.snapshot_at,
        s.source_updated_at,
        s.quote_delay_note,
        s.source_name,
        s.fx_note
    order by s.snapshot_at desc, p.holder, p.name;
    """
    return pd.read_sql(text(query), engine)


def _resolve_source_column(columns: list[str], candidates: list[str]) -> str | None:
    normalized_map = {_normalize_column_name(column): column for column in columns}
    for candidate in candidates:
        original = normalized_map.get(candidate)
        if original:
            return original
    return None


def _to_decimal(value: object) -> Decimal | None:
    if value is None or pd.isna(value):
        return None
    cleaned = (
        str(value)
        .strip()
        .replace(",", "")
        .replace("£", "")
        .replace("$", "")
        .replace("%", "")
        .replace("+", "")
    )
    if cleaned in {"", "-", "nan", "None"}:
        return None
    return Decimal(cleaned)


def _prepare_holdings_frame(frame: pd.DataFrame) -> pd.DataFrame:
    source_records = frame.to_dict(orient="records")
    frame = frame.copy()
    columns = list(frame.columns)

    mappings = {
        # "stock" covers Hargreaves Lansdown exports which use "Stock" as the company column
        "company": ["company", "name", "instrument", "security", "holding", "display_name", "stock"],
        "instrument_name": ["instrument_name", "description", "full_name", "security_description"],
        # "epic" covers Hargreaves Lansdown exports which use "Epic" for the LSE ticker code
        "ticker": ["ticker", "symbol", "epic"],
        # "units" is already covered; listed explicitly for clarity (HL uses "Units")
        "quantity": ["quantity", "shares", "units"],
        "quantity_label": ["quantity_label", "holding_type", "quantity_type"],
        # HL UK equity prices are in pence ("Price (p)") — divide by 100 when loading if needed
        "price": ["price", "share_price", "last_price", "current_price", "price_p"],
        # "value" covers HL "Value (£)" column
        "market_value": ["market_value", "value", "market_val", "current_value"],
        # "book_cost" covers HL "Book cost (£)" column
        "total_cost": ["total_cost", "cost_basis", "book_cost", "cost", "total_book_cost"],
        "gain_loss_value": ["gain_loss_value", "gain_loss", "profit_loss", "pnl_value"],
        "gain_loss_pct": ["gain_loss_pct", "gain_loss_percent", "return_pct", "profit_loss_pct"],
        "currency": ["currency", "ccy"],
        "sector": ["sector", "industry"],
        "as_of_date": ["as_of_date", "date", "valuation_date", "priced_date"],
    }

    selected: dict[str, str | None] = {
        target: _resolve_source_column(columns, candidates)
        for target, candidates in mappings.items()
    }

    if selected["company"] is None:
        raise ValueError("Could not find a company column in the holdings file.")

    prepared = pd.DataFrame()
    for target, source_column in selected.items():
        prepared[target] = frame[source_column] if source_column else None

    prepared["company"] = prepared["company"].astype(str).str.strip()
    prepared = prepared[prepared["company"] != ""]

    quantity_source = selected["quantity"]
    if selected["quantity_label"] is None and quantity_source:
        normalized_quantity = _normalize_column_name(quantity_source)
        if normalized_quantity == "units":
            prepared["quantity_label"] = "Units"
        elif normalized_quantity == "shares":
            prepared["quantity_label"] = "Shares"

    for numeric_column in [
        "quantity",
        "price",
        "market_value",
        "total_cost",
        "gain_loss_value",
        "gain_loss_pct",
    ]:
        prepared[numeric_column] = prepared[numeric_column].map(_to_decimal)

    for string_column in ["instrument_name", "ticker", "quantity_label", "currency", "sector"]:
        prepared[string_column] = prepared[string_column].map(_normalize_string)

    prepared["as_of_date"] = pd.to_datetime(prepared["as_of_date"], errors="coerce").dt.date
    prepared["source_row"] = [source_records[index] for index in prepared.index]
    return prepared


def insert_portfolio_holdings_snapshot(
    *,
    portfolio_id: int,
    snapshot_at: datetime,
    holdings_frame: pd.DataFrame,
    source_updated_at: datetime | None,
    quote_delay_note: str | None,
    source_name: str | None,
    fx_note: str | None,
) -> tuple[int, int]:
    engine = postgres_engine()
    # Normalise every record before writing anything, so a row that can't be stored
    # fails the import without leaving an empty snapshot behind.
    records = []
    for record in holdings_frame.to_dict(orient="records"):
        normalized_record = record.copy()
        normalized_record["as_of_date"] = (
            None if pd.isna(normalized_record["as_of_date"]) else normalized_record["as_of_date"]
        )
        normalized_record["source_row"] = _normalize_json(normalized_record["source_row"])
        records.append(normalized_record)

    snapshot_id = create_portfolio_snapshot(
        portfolio_id=portfolio_id,
        snapshot_at=snapshot_at,
        source_updated_at=source_updated_at,
        quote_delay_note=quote_delay_note,
        source_name=source_name,
        fx_note=fx_note,
    )

    insert_sql = f"""
    insert into {PORTFOLIO_SCHEMA}.{HOLDINGS_TABLE} (
        portfolio_id,
        snapshot_id,
        company,
        instrument_name,
        ticker,
        quantity,
        quantity_label,
        price,
        market_value,
        total_cost,
        gain_loss_value,
        gain_loss_pct,
        currency,
        sector,
        as_of_date,
        source_row
    ) values (
        :portfolio_id,
        :snapshot_id,
        :company,
        :instrument_name,
        :ticker,
        :quantity,
        :quantity_label,
        :price,
        :market_value,
        :total_cost,
        :gain_loss_value,
        :gain_loss_pct,
        :currency,
        :sector,
        :as_of_date,
        cast(:source_row as jsonb)
    );
    """

    with engine.begin() as conn:
        for normalized_record in records:
            conn.execute(
                text(insert_sql),
                {"portfolio_id": portfolio_id, "snapshot_id": snapshot_id, **normalized_record},
            )

    return snapshot_id, len(records)


HL_EXPORT_ENCODING = "latin-1"
HL_HEADER_PREFIX = "Code,Stock,"
HL_COLUMN_MAP = {
    "code": "ticker",
    "stock": "company",
    "units_held": "quantity",
    "price_pence": "price_pence",
    "value": "market_value",
    "cost": "total_cost",
    "gain_loss": "gain_loss_value",
    "gain_loss_pct": "gain_loss_pct",
}


def _hl_export_lines(csv_path: str | Path) -> list[str]:
    return Path(csv_path).expanduser().read_text(encoding=HL_EXPORT_ENCODING).splitlines()


def is_hl_export(csv_path: str | Path) -> bool:
    """True for Hargreaves Lansdown 'portfolio summary' CSV downloads."""
    lines = _hl_export_lines(csv_path)
    return bool(lines) and lines[0].startswith("HL ") and any(
        line.startswith(HL_HEADER_PREFIX) for line in lines
    )


def _hl_summary_value(lines: list[str], label: str) -> str | None:
    for line in lines:
        if line.startswith(label):
            parts = next(csv.reader([line]))
            return parts[1].strip() if len(parts) > 1 else None
    return None


def hl_export_created_at(csv_path: str | Path) -> datetime | None:
    raw = _hl_summary_value(_hl_export_lines(csv_path), "Spreadsheet created at")
    return datetime.strptime(raw, "%d-%m-%Y %H:%M") if raw else None


def read_hl_holdings_export(csv_path: str | Path) -> pd.DataFrame:
    """Parse an HL portfolio summary CSV into the platform's holdings columns.

    HL downloads are Latin-1 encoded, start with an account summary block, quote
    numbers with thousands separators, give prices in pence and end with a totals
    row plus footnotes. Funds are identified by SEDOL in the Code column. Cash is
    only reported in the summary block, so it is appended as a CASH holding.
    """
    lines = _hl_export_lines(csv_path)
    header_index = next(i for i, line in enumerate(lines) if line.startswith(HL_HEADER_PREFIX))
    table_lines = [lines[header_index]]
    for line in lines[header_index + 1:]:
        if not line.strip():
            break
        table_lines.append(line)

    frame = pd.read_csv(io.StringIO("\n".join(table_lines)), thousands=",", dtype=str)
    # "Gain/loss (£)" and "Gain/loss (%)" would otherwise normalise to the same name.
    frame.columns = [_normalize_column_name(column.replace("%", "pct")) for column in frame.columns]
    frame = frame.rename(columns=HL_COLUMN_MAP)
    frame = frame[frame["ticker"].fillna("").str.strip() != ""]

    holdings = pd.DataFrame({
        "company": frame["company"].str.replace(r"\s*\*R$", "", regex=True).str.strip(),
        "instrument_name": frame["company"].str.strip(),
        "ticker": frame["ticker"].str.strip(),
        "quantity": frame["quantity"],
        "quantity_label": "Units",
        "price": frame["price_pence"].map(_to_decimal).map(
            lambda pence: (pence / Decimal("100")) if pence is not None else None
        ),
        "market_value": frame["market_value"],
        "total_cost": frame["total_cost"],
        "gain_loss_value": frame["gain_loss_value"],
        "gain_loss_pct": frame["gain_loss_pct"],
        "currency": "GBP",
    })

    cash = _hl_summary_value(lines, "Total cash:")
    if cash and _to_decimal(cash):
        holdings = pd.concat([holdings, pd.DataFrame([{
            "company": "Cash",
            "instrument_name": "Cash",
            "ticker": "CASH",
            "quantity": None,
            "quantity_label": None,
            "price": None,
            "market_value": cash,
            "total_cost": cash,
            "gain_loss_value": "0",
            "gain_loss_pct": "0",
            "currency": "GBP",
        }])], ignore_index=True)

    created_at = hl_export_created_at(csv_path)
    holdings["as_of_date"] = created_at.date().isoformat() if created_at else None
    return holdings.reset_index(drop=True)


def import_portfolio_holdings_from_csv(
    *,
    portfolio_name: str,
    holder: str,
    portfolio_type: str,
    csv_path: str,
    snapshot_at: datetime,
    source_updated_at: datetime | None,
    quote_delay_note: str | None,
    source_name: str | None,
    fx_note: str | None,
) -> tuple[int, int]:
    ensure_personal_portfolio_tables()
    portfolio_id = add_personal_portfolio(
        name=portfolio_name,
        holder=holder,
        portfolio_type=portfolio_type,
    )

    if is_hl_export(csv_path):
        frame = read_hl_holdings_export(csv_path)
    else:
        frame = pd.read_csv(Path(csv_path).expanduser())
    prepared = _prepare_holdings_frame(frame)
    return insert_portfolio_holdings_snapshot(
        portfolio_id=portfolio_id,
        snapshot_at=snapshot_at,
        holdings_frame=prepared,
        source_updated_at=source_updated_at,
        quote_delay_note=quote_delay_note,
        source_name=source_name,
        fx_note=fx_note,
    )


def _parse_datetime_env(name: str) -> datetime | None:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    return datetime.fromisoformat(raw)


def ingest_personal_portfolio() -> None:
    name = os.getenv("PORTFOLIO_NAME", "").strip()
    holder = os.getenv("PORTFOLIO_HOLDER", "").strip()
    portfolio_type = os.getenv("PORTFOLIO_TYPE", "").strip()
    csv_path = os.getenv("PORTFOLIO_CSV_PATH", "").strip()
    snapshot_at = _parse_datetime_env("PORTFOLIO_SNAPSHOT_AT") or datetime.now()
    source_updated_at = _parse_datetime_env("PORTFOLIO_SOURCE_UPDATED_AT")
    quote_delay_note = _normalize_string(os.getenv("PORTFOLIO_QUOTE_DELAY_NOTE", ""))
    source_name = _normalize_string(os.getenv("PORTFOLIO_SOURCE_NAME", ""))
    fx_note = _normalize_string(os.getenv("PORTFOLIO_FX_NOTE", ""))

    if not name or not holder or not portfolio_type:
        raise ValueError(
            "PORTFOLIO_NAME, PORTFOLIO_HOLDER, and PORTFOLIO_TYPE must all be set."
        )

    ensure_personal_portfolio_tables()

    if csv_path:
        if is_hl_export(csv_path):
            # Default the snapshot time and source to what the HL download itself states.
            created_at = hl_export_created_at(csv_path)
            if not os.getenv("PORTFOLIO_SNAPSHOT_AT", "").strip() and created_at:
                snapshot_at = created_at
            source_updated_at = source_updated_at or created_at
            source_name = source_name or "Hargreaves Lansdown"
            quote_delay_note = quote_delay_note or "Shares at bid price delayed 15 minutes; funds at latest bid price"
            fx_note = fx_note or "US holdings converted to GBP by HL at the export-date rate"
        snapshot_id, row_count = import_portfolio_holdings_from_csv(
            portfolio_name=name,
            holder=holder,
            portfolio_type=portfolio_type,
            csv_path=csv_path,
            snapshot_at=snapshot_at,
            source_updated_at=source_updated_at,
            quote_delay_note=quote_delay_note,
            source_name=source_name,
            fx_note=fx_note,
        )
        print(
            "Stored portfolio snapshot: "
            f"name={name} holder={holder} type={portfolio_type} snapshot_id={snapshot_id} rows={row_count}"
        )
        return

    portfolio_id = add_personal_portfolio(name=name, holder=holder, portfolio_type=portfolio_type)
    print(f"Stored portfolio: id={portfolio_id} name={name} holder={holder} type={portfolio_type}")


if __name__ == "__main__":
    ingest_personal_portfolio()
