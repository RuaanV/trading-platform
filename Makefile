UV ?= uv
PY := $(UV) run python
STREAMLIT := $(UV) run streamlit
DBT := $(UV) run dbt
DBT_PROFILES_DIR := dbt

.DEFAULT_GOAL := help

help:
	@echo "Available targets:"
	@echo "  make setup              - create .venv and install locked dependencies with uv (alias of install)"
	@echo "  make install            - sync .venv to uv.lock (uv sync)"
	@echo "  make install-airflow    - also install the optional Airflow dependency group"
	@echo "  make lock               - re-resolve uv.lock after editing pyproject.toml"
	@echo "  make outdated           - list direct dependencies with newer versions available"
	@echo "  make upgrade            - upgrade all locked dependencies to latest allowed and sync"
	@echo "  make test               - run the full unittest suite"
	@echo "  make run-prices-apple   - fetch AAPL latest price using yfinance + Massive"
	@echo "  make run-ingestion      - run price + fundamentals ingestion"
	@echo "  make run-company        - ingest company data for AAPL, AMZN, GOOG, MSFT, and BA.L into Postgres raw tables"
	@echo "  make run-market-calendar - ingest current-year market calendar events for tracked symbols"
	@echo "  make run-features       - build feature table artifacts"
	@echo "  make run-training       - train return model artifact"
	@echo "  make run-scoring        - score universe"
	@echo "  make run-strategy       - generate ranked trade candidates"
	@echo "  make run-recommender    - generate portfolio-aware recommendations"
	@echo "  make run-agent          - run the baseline market analysis agent"
	@echo "  make run-holding-news   - fetch and score today's Yahoo Finance headlines for a holding"
	@echo "  make run-history        - fetch historical price history for holdings and benchmarks"
	@echo "  make run-holdings-report - build 3M/6M holding performance report"
	@echo "  make run-backtest       - run a simple walk-forward recommender backtest"
	@echo "  make test-recommender   - run unit tests for the recommender rules"
	@echo "  make test-analytics     - run analytics and backtesting unit tests"
	@echo "  make test-providers     - run provider and rate-limiter unit tests"
	@echo "  make run-pipeline       - run full local pipeline sequence"
	@echo "  make run-dashboard      - start Streamlit dashboard"
	@echo "  make init-portfolios    - create the personal portfolios table in Postgres"
	@echo "  make add-portfolio      - insert a portfolio or import holdings using PORTFOLIO_* env vars"
	@echo "  make load-personal-portfolio - seed the SIPP portfolio and refresh live Yahoo Finance prices"
	@echo "  make load-isa-portfolio - refresh the ISA portfolio prices (no-op until first CSV is imported)"
	@echo "  make add-cash-holding   - append the current cash balance as a portfolio holding"
	@echo "  make cleanup-portfolio-snapshots - keep only the seed and latest SIPP snapshots"
	@echo "  make dbt-run            - execute dbt models"
	@echo "  make test-aapl-pipeline - run company ingestion + dbt shaping for the AAPL/AMZN/GOOG/MSFT/BA.L set"
	@echo "  make clean-artifacts    - remove generated model artifacts"
	@echo "  make db-up              - start the local Postgres container (docker compose) and wait until healthy"
	@echo "  make db-down            - stop the Postgres container (data is kept in the volume)"
	@echo "  make db-status          - show container status and readiness"
	@echo "  make db-shell           - open psql inside the container"
	@echo "  make db-init            - start Postgres and create the portfolios table"

setup: install

# Kept as a prerequisite name for the run-* targets: just checks uv is available.
# `uv run` creates/syncs .venv automatically from uv.lock.
venv:
	@command -v $(UV) >/dev/null 2>&1 || { echo "uv is not installed - see https://docs.astral.sh/uv/getting-started/installation/"; exit 1; }

install: venv
	$(UV) sync

install-airflow: venv
	$(UV) sync --group airflow

lock: venv
	$(UV) lock

outdated: venv
	$(UV) tree --outdated --depth 1

upgrade: venv
	$(UV) lock --upgrade
	$(UV) sync

test: venv
	$(PY) -m unittest $(basename $(subst /,.,$(wildcard tests/test_*.py)))

run-ingestion: venv
	$(PY) data_pipeline/ingest_prices.py
	$(PY) data_pipeline/ingest_fundamentals.py

run-prices-apple: venv
	PRICE_SYMBOL=AAPL PRICE_PROVIDER=both $(PY) data_pipeline/ingest_prices.py

run-company: venv
	$(PY) data_pipeline/ingest_company_data.py

run-market-calendar: venv
	$(PY) data_pipeline/ingest_market_calendar.py

run-features: venv
	$(PY) src/features/build_features.py

run-training: venv
	$(PY) src/training/train_return_model.py

run-scoring: venv
	$(PY) src/scoring/score_universe.py

run-strategy: venv
	$(PY) src/strategies/generate_trade_candidates.py

run-recommender: venv
	$(PY) src/recommender/generate_recommendations.py

run-agent: venv
	$(PY) src/agents/run_agent.py

run-holding-news: venv
	$(PY) src/agents/run_holding_news_agent.py

refresh-holding-news: venv
	$(PY) data_pipeline/refresh_holding_news.py

run-history: venv
	$(PY) src/backtesting/load_price_history.py

run-holdings-report: venv
	$(PY) src/analytics/evaluate_holdings_history.py

run-backtest: venv
	$(PY) src/backtesting/backtest_recommender.py

test-recommender: venv
	$(PY) -m unittest tests.test_generate_recommendations

test-analytics: venv
	$(PY) -m unittest tests.test_backtesting_workflow

test-providers: venv
	$(PY) -m unittest tests.test_price_provider_rate_limit

run-pipeline: run-ingestion run-features run-training run-scoring run-strategy run-recommender

run-dashboard: venv
	$(STREAMLIT) run dashboard/streamlit_app.py

init-portfolios: venv
	$(PY) -c "from data_pipeline.personal_portfolios import ensure_personal_portfolio_tables; ensure_personal_portfolio_tables()"

add-portfolio: venv
	$(PY) data_pipeline/personal_portfolios.py

load-personal-portfolio: venv
	$(PY) data_pipeline/load_personal_portfolio.py

load-isa-portfolio: venv
	$(PY) data_pipeline/load_isa_portfolio.py

add-cash-holding: venv
	$(PY) data_pipeline/add_cash_holding.py

cleanup-portfolio-snapshots: venv
	$(PY) data_pipeline/cleanup_portfolio_snapshots.py

dbt-run: venv
	DBT_PROFILES_DIR=$(DBT_PROFILES_DIR) $(DBT) run --project-dir dbt

test-aapl-pipeline: run-company dbt-run

clean-artifacts:
	rm -f models/trained_models/return_model.txt
	rm -f models/trained_models/latest_scores.csv
	rm -f models/trained_models/trade_candidates.csv
	rm -f models/trained_models/latest_recommendations.csv
	rm -f models/trained_models/historical_prices.csv
	rm -f models/trained_models/holding_performance_report.csv
	rm -f models/trained_models/recommender_backtest_history.csv
	rm -f models/trained_models/recommender_backtest_summary.csv

db-up:
	docker compose up -d postgres
	@echo "Waiting for Postgres to become healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' trading-postgres 2>/dev/null)" = "healthy" ]; do sleep 1; done
	@echo "Postgres is ready on localhost:$${POSTGRES_PORT:-5432}"

db-down:
	docker compose stop postgres

db-status:
	docker compose ps postgres
	docker exec trading-postgres pg_isready

db-shell:
	docker exec -it trading-postgres psql -U $${POSTGRES_USER:-postgres} -d $${POSTGRES_DB:-trading_platform}

db-init: db-up init-portfolios

.PHONY: setup venv install install-airflow lock outdated upgrade test db-up db-down db-status db-shell db-init
