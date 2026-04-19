# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make setup               # Create .venv and install dependencies
make install             # Update requirements into existing .venv
```

Run pipeline stages:
```bash
make run-pipeline        # Full batch: ingestion → features → training → scoring → strategy → recommendations
make run-company         # Ingest company fundamentals into Postgres raw schema
make run-market-calendar # Ingest current-year market calendar events
make run-recommender     # Generate portfolio-aware recommendations
make run-holding-news    # Fetch and score Yahoo Finance headlines for holdings
make run-history         # Fetch historical price history
make run-holdings-report # Build 3M/6M holding performance report
make run-backtest        # Walk-forward recommender backtest
make run-agent           # Run market analysis agent
make run-dashboard       # Start Streamlit dashboard (http://localhost:8501)
make dbt-run             # Execute dbt warehouse models
make load-personal-portfolio # Seed/refresh SIPP portfolio from CSV + live prices
```

Run tests:
```bash
make test-recommender    # Recommender rules unit tests
make test-analytics      # Analytics and backtesting unit tests
make test-providers      # Price provider rate-limiting unit tests
```

Run a single test file directly:
```bash
.venv/bin/python -m unittest tests.test_generate_recommendations
```

All tests use the `unittest` framework. Always use `.venv/bin/python` to run scripts, not the system Python.

## Architecture

This is a **batch analytics platform** for personal portfolio management. The main data flow:

```
External APIs (yfinance, Polygon/Massive, Finnhub, Yahoo Finance)
  → data_pipeline/ (ingestion)
  → Postgres (raw.* schema)
  → dbt/models/ (staging + feature transformations → analytics.* schema)
  → src/ (features → training → scoring → strategies → recommender)
  → models/trained_models/ (CSV artifacts)
  → dashboard/streamlit_app.py (Streamlit UI)
```

Orchestration runs weekdays via an Airflow DAG (`airflow/dags/trading_pipeline_dag.py`).

### Key subsystems

**`data_pipeline/`** — Ingestion and portfolio management
- `personal_portfolios.py` — Core portfolio/snapshot/holdings schema and all DB queries (app.* tables)
- `load_personal_portfolio.py` — SIPP-specific refresh: seeds from CSV, fetches live prices
- `price_providers.py` — Abstraction over yfinance, Massive (Polygon), and Finnhub
- `holding_news.py` — News headline fetching and sentiment scoring
- `yahoo_symbols.py` — Symbol resolution and ticker aliases (handles UK `.L` suffixes, etc.)

**`dbt/models/`** — Warehouse transformations
- `staging/stg_*.sql` — Raw data cleaning
- `features/fct_*.sql` — Business-logic feature models
- Profiles config in `dbt/profiles.yml` (Postgres, `raw` → `analytics` schema)

**`src/recommender/generate_recommendations.py`** — The primary functional analytics output. Reads portfolio holdings and scored candidates, applies rules, writes `latest_recommendations.csv`.

**`src/agents/`** — Higher-level agent layer built on top of the pipeline
- `base.py` defines `AgentContext`, `AgentResult`, and `BaseAgent` protocol
- `market_analysis.py` and `holding_news_sentiment.py` are the two implemented agents
- `registry.py` handles agent registration

**`dashboard/streamlit_app.py`** — Multi-page viewer: portfolio summaries, holding details with movement indicators, news sentiment, recommendations.

### Database schema (Postgres)

- `app.personal_portfolios` / `app.portfolio_snapshots` / `app.portfolio_holdings` — Portfolio domain
- `raw.company_info`, `raw.market_calendar_events`, `raw.balance_sheet`, etc. — Ingested raw data
- `analytics.*` — dbt-generated views

### Pipeline stubs

`src/features/`, `src/training/`, `src/scoring/`, and `src/strategies/` are functional stubs that produce placeholder artifacts. The recommender reads from these artifacts, so the pipeline is end-to-end but not yet using real ML signals.

## Working Style

- Use `Makefile` targets instead of reconstructing command sequences manually.
- Keep changes narrow and task-focused; preserve existing architecture unless asked to change it.
- Do not introduce new dependencies unless necessary.
- Prefer local consistency over new patterns.
- Treat `models/trained_models/` files as generated artifacts — do not edit them directly.
- Be careful with scripts that write to Postgres or refresh portfolio snapshots.
- Do not assume external API credentials are available unless the task confirms they are configured.
- When touching recommendation logic, preserve `rationale` fields and artifact CSV compatibility.
- Run the narrowest relevant test target after changes; state what wasn't tested and why if full verification isn't possible.
- Do not confuse `src/agents/` (application code) with `AGENTS.md` (coding-agent guidance).
