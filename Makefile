PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python
STREAMLIT := $(VENV)/bin/streamlit
DBT := $(VENV)/bin/dbt

.DEFAULT_GOAL := help

help:
	@echo "Available targets:"
	@echo "  make setup              - create venv and install dependencies"
	@echo "  make venv               - create .venv if missing"
	@echo "  make install            - install/update requirements into .venv"
	@echo "  make run-ingestion      - run price + fundamentals ingestion"
	@echo "  make run-features       - build feature table artifacts"
	@echo "  make run-training       - train return model artifact"
	@echo "  make run-scoring        - score universe"
	@echo "  make run-strategy       - generate ranked trade candidates"
	@echo "  make run-pipeline       - run full local pipeline sequence"
	@echo "  make run-dashboard      - start Streamlit dashboard"
	@echo "  make dbt-run            - execute dbt models"
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

run-features: venv
	$(PY) src/features/build_features.py

run-training: venv
	$(PY) src/training/train_return_model.py

run-scoring: venv
	$(PY) src/scoring/score_universe.py

run-strategy: venv
	$(PY) src/strategies/generate_trade_candidates.py

run-pipeline: run-ingestion run-features run-training run-scoring run-strategy

run-dashboard: venv
	$(STREAMLIT) run dashboard/streamlit_app.py

dbt-run: venv
	$(DBT) run --project-dir dbt

clean-artifacts:
	rm -f models/trained_models/return_model.txt
	rm -f models/trained_models/latest_scores.csv
	rm -f models/trained_models/trade_candidates.csv
