# Data Pipeline

This folder contains the platform's ingestion and portfolio-loading scripts. These scripts are responsible for pulling data into the platform, refreshing portfolio state, and maintaining a usable portfolio history in Postgres.

At a high level, the pipelines in this folder fall into five groups:

- market data pipelines
- company and fundamentals pipelines
- market calendar pipelines
- portfolio ingestion and refresh pipelines
- portfolio maintenance utilities

## Prerequisites

Most scripts in this folder expect the local virtual environment and environment variables to be available.

Recommended setup:

```bash
make setup
set -a && source .env && set +a
```

Default Python executable:

```bash
.venv/bin/python
```

Database-backed scripts expect a reachable Postgres database, typically via:

- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`

API-backed scripts may also need:

- `MASSIVE_API_KEY`
- `MASSIVE_BASE_URL`
- `FINNHUB_API_KEY`

The local `.env` loader is implemented in [`env.py`](/Users/ruaan.venter/code/trading-platform/data_pipeline/env.py), and the shared Postgres connection helper is in [`db.py`](/Users/ruaan.venter/code/trading-platform/data_pipeline/db.py).

## Pipeline Types

### 1. Market data pipelines

These pipelines fetch latest or historical market prices from external providers.

#### `ingest_prices.py`

Purpose:
- fetch the latest price for a symbol from `yfinance`, `massive`, or both
- validate provider connectivity and quote retrieval
- act as a simple price-ingestion smoke test

What it does:
- reads `PRICE_SYMBOL`
- reads `PRICE_PROVIDER`
- fetches the latest quote from the selected provider or providers
- prints the result to stdout

Notes:
- this script does not persist prices into Postgres
- it is mainly a lightweight ingestion and provider-check pipeline

How to run:

```bash
make run-prices-apple
```

or:

```bash
PRICE_SYMBOL=AAPL PRICE_PROVIDER=both .venv/bin/python data_pipeline/ingest_prices.py
```

Common environment variables:

- `PRICE_SYMBOL`, for example `AAPL`
- `PRICE_PROVIDER`, one of `yfinance`, `massive`, or `both`
- `MASSIVE_API_KEY`
- `MASSIVE_BASE_URL`

#### `price_providers.py`

Purpose:
- provide the shared provider layer used by other scripts

What it does:
- fetches latest prices from yfinance, Massive, and Finnhub
- fetches Massive historical prices
- performs provider search and symbol matching
- applies provider-specific rate limiting
- supports FX conversion helpers used by portfolio refresh flows

How it is used:
- imported by `ingest_prices.py`
- imported by `load_personal_portfolio.py`
- indirectly supports historical price workflows elsewhere in the repo

How to run:
- this is normally not executed directly
- it is a shared library module for the other pipeline scripts

### 2. Company and fundamentals pipelines

These pipelines ingest company-level or fundamentals-style data into the warehouse side of the platform.

#### `ingest_company_data.py`

Purpose:
- pull company-level datasets from yfinance into Postgres raw tables

What it does:
- loads company info
- loads major holders
- loads institutional holders
- loads balance sheet data
- writes the results to the `raw` schema in Postgres
- drops the transformed analytics schemas first so dbt views can be rebuilt cleanly

Output tables:

- `raw.company_info`
- `raw.major_holders`
- `raw.institutional_holders`
- `raw.balance_sheet`

How to run:

```bash
make run-company
```

or:

```bash
.venv/bin/python data_pipeline/ingest_company_data.py
```

Optional environment variables:

- `YF_SYMBOLS`, comma-separated list such as `AAPL,AMZN,GOOG,MSFT,BA.L`
- `YF_SYMBOL`, single fallback symbol

Use this pipeline when:
- you want to refresh raw company-level datasets before running dbt
- you are testing the warehouse ingestion path

#### `ingest_fundamentals.py`

Purpose:
- placeholder for a richer fundamentals or filings ingestion pipeline

What it currently does:
- logs a stub completion message

Current status:
- scaffold only
- intended to become the real fundamentals ingestion entrypoint later

How to run:

```bash
.venv/bin/python data_pipeline/ingest_fundamentals.py
```

or as part of:

```bash
make run-ingestion
```

### 3. Market calendar pipelines

These pipelines scaffold event ingestion for monitoring workflows such as earnings dates, dividend events, splits, and other calendar-driven review triggers.

#### `ingest_market_calendar.py`

Purpose:
- ingest current-year market calendar events for a tracked symbol set

What it does:
- defaults to `GOOG`, `MSFT`, `AAPL`, `AMZN`, `NVDA`, and `BA.L`
- defaults to the current calendar year
- uses yfinance as the initial scaffold provider
- normalises available calendar and actions data into a single event table
- writes a CSV artifact to `models/trained_models/current_year_market_calendar.csv`
- writes a raw Postgres table to `raw.market_calendar_events`

How to run:

```bash
make run-market-calendar
```

or:

```bash
MARKET_CALENDAR_SYMBOLS=GOOG,MSFT,AAPL,AMZN,NVDA,BA.L \
MARKET_CALENDAR_YEAR=2026 \
.venv/bin/python data_pipeline/ingest_market_calendar.py
```

Common environment variables:

- `MARKET_CALENDAR_SYMBOLS`
- `MARKET_CALENDAR_YEAR`
- `MARKET_CALENDAR_PROVIDER`
- `MARKET_CALENDAR_FIXTURE_PATH`

Important notes:
- this is scaffold-first, so provider support is currently implemented for yfinance only
- the pipeline is intended to become a monitoring input for alerts, dashboard reviews, and later automation

### 4. Portfolio ingestion and refresh pipelines

These pipelines are the most important for the portfolio-management side of the platform. They create and refresh portfolio state in Postgres.

#### `personal_portfolios.py`

Purpose:
- manage portfolio metadata, snapshot creation, holdings import, and portfolio queries

What it does:
- creates the `app` schema portfolio tables if needed
- creates `personal_portfolios`, `portfolio_snapshots`, and `portfolio_holdings`
- provides helper functions to add portfolios
- imports holdings from CSV exports
- inserts holdings snapshots
- exposes query helpers for portfolios, snapshots, and holdings
- defines views for latest holdings and resolved symbols

Use this pipeline when:
- you need to initialise portfolio storage
- you want to add a new portfolio
- you want to import a new holdings snapshot from a CSV export

How to run:

Initialise portfolio tables:

```bash
make init-portfolios
```

Add a portfolio or import a snapshot:

```bash
make add-portfolio
```

Typical import example:

```bash
PORTFOLIO_NAME="My ISA" \
PORTFOLIO_HOLDER="Ruaan Venter" \
PORTFOLIO_TYPE=ISA \
PORTFOLIO_CSV_PATH=/absolute/path/to/portfolio.csv \
PORTFOLIO_SNAPSHOT_AT=2026-03-08T15:32:00 \
PORTFOLIO_SOURCE_UPDATED_AT=2026-03-08T15:31:00 \
PORTFOLIO_QUOTE_DELAY_NOTE="Delayed by at least 15 minutes" \
PORTFOLIO_SOURCE_NAME="Interactive Investor" \
PORTFOLIO_FX_NOTE="USD values converted to GBP at indicative FX rate" \
make add-portfolio
```

Important behaviour:
- repeated imports append new snapshots rather than replacing old ones
- this enables portfolio history tracking over time

#### `load_personal_portfolio.py`

Purpose:
- seed and refresh the default personal SIPP portfolio

What it does:
- ensures the portfolio tables exist
- loads a reference SIPP snapshot from a seed CSV if it is missing
- resolves provider symbols for holdings
- refreshes the latest holdings using live price data
- converts USD quotes into GBP where needed
- inserts a new portfolio snapshot with updated values

Use this pipeline when:
- you want to bootstrap the SIPP portfolio in the database
- you want to refresh the latest SIPP snapshot from live providers

How to run:

```bash
make load-personal-portfolio
```

or:

```bash
.venv/bin/python data_pipeline/load_personal_portfolio.py
```

Important notes:
- this flow is currently SIPP-specific
- it is designed as a convenience loader for the default personal portfolio setup
- it depends on working market-data providers and database connectivity

### 5. Portfolio maintenance utilities

These scripts maintain or adjust stored portfolio snapshots after ingestion.

#### `add_cash_holding.py`

Purpose:
- append a manual cash position to the latest SIPP snapshot

What it does:
- clones the latest SIPP holdings
- removes any existing `Cash` row
- adds a `CASH` holding using `PORTFOLIO_CASH_VALUE`
- writes a fresh snapshot into Postgres

Use this utility when:
- your exported holdings snapshot does not include investable cash
- you want the latest recommendation or analytics runs to include current cash

How to run:

```bash
PORTFOLIO_CASH_VALUE=7900.53 make add-cash-holding
```

or:

```bash
PORTFOLIO_CASH_VALUE=7900.53 .venv/bin/python data_pipeline/add_cash_holding.py
```

#### `cleanup_portfolio_snapshots.py`

Purpose:
- remove intermediate SIPP snapshots while keeping the seed snapshot and latest snapshot

What it does:
- queries all stored snapshots for the default SIPP portfolio
- identifies the original seed snapshot
- keeps the seed and most recent snapshot
- deletes intermediate snapshots from Postgres

Use this utility when:
- you want to reduce duplicate or temporary refresh snapshots in the default SIPP workflow

How to run:

```bash
.venv/bin/python data_pipeline/cleanup_portfolio_snapshots.py
```

There is also a Make target listed in the root [`Makefile`](/Users/ruaan.venter/code/trading-platform/Makefile):

```bash
make cleanup-portfolio-snapshots
```

Important note:
- this is a cleanup script, so use it carefully
- it is intentionally narrow and currently only targets the default SIPP portfolio

## Execution Summary

The most common execution paths are:

### Market and company ingestion

```bash
make run-prices-apple
make run-company
make run-market-calendar
make run-ingestion
```

### Portfolio setup and refresh

```bash
make init-portfolios
make add-portfolio
make load-personal-portfolio
PORTFOLIO_CASH_VALUE=7900.53 make add-cash-holding
```

### Warehouse shaping

After company ingestion, run dbt:

```bash
make dbt-run
```

## Suggested Usage Order

For a typical local workflow:

1. set up the environment and database
2. initialise the portfolio tables with `make init-portfolios`
3. import a portfolio snapshot with `make add-portfolio` or seed the SIPP with `make load-personal-portfolio`
4. optionally add cash with `make add-cash-holding`
5. ingest company data with `make run-company`
6. optionally ingest market calendar events with `make run-market-calendar`
7. shape warehouse models with `make dbt-run`
8. run downstream scoring, recommendation, analytics, or dashboard workflows from the repo root

## Current Gaps

The `data_pipeline` area is still evolving. Important current gaps:

- `ingest_fundamentals.py` is still a stub
- `ingest_market_calendar.py` is an initial scaffold and currently uses yfinance only
- there is no dedicated news ingestion pipeline yet
- there is no general multi-portfolio scheduling or automation pipeline yet
- some workflows are still specialised for the default SIPP portfolio instead of being fully generic
