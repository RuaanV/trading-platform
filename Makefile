PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python
STREAMLIT := $(VENV)/bin/streamlit
DBT := $(VENV)/bin/dbt
DBT_PROFILES_DIR := dbt

.DEFAULT_GOAL := help

help:
	@echo "Available targets:"
	@echo "  make setup              - create venv and install dependencies"
	@echo "  make venv               - create .venv if missing"
	@echo "  make install            - install/update requirements into .venv"
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
	@echo "  make add-cash-holding   - append the current cash balance as a portfolio holding"
	@echo "  make cleanup-portfolio-snapshots - keep only the seed and latest SIPP snapshots"
	@echo "  make dbt-run            - execute dbt models"
	@echo "  make test-aapl-pipeline - run company ingestion + dbt shaping for the AAPL/AMZN/GOOG/MSFT/BA.L set"
	@echo "  make clean-artifacts    - remove generated model artifacts"

setup: venv install

venv:
	@if [ ! -d "$(VENV)" ]; then $(PYTHON) -m venv $(VENV); fi

install: venv
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements.txt

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
