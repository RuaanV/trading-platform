# Trading Platform (Agreed Architecture Scaffold)

This repository now matches the agreed ML-first architecture for a personal trading platform.

## Directory layout

```text
trading-platform/
├── data_pipeline/
│   ├── ingest_prices.py
│   └── ingest_fundamentals.py
├── dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── features/
│       │   └── fct_feature_snapshot.sql
│       └── staging/
│           └── stg_prices.sql
├── src/
│   ├── features/
│   │   └── build_features.py
│   ├── training/
│   │   └── train_return_model.py
│   ├── scoring/
│   │   └── score_universe.py
│   └── strategies/
│       └── generate_trade_candidates.py
├── notebooks/
│   ├── research_modeling.ipynb
│   └── factor_analysis.ipynb
├── airflow/
│   └── dags/
│       └── trading_pipeline_dag.py
├── models/
│   └── trained_models/
└── dashboard/
    └── streamlit_app.py
```

## Recommended ML architecture

```text
                ┌──────────────────────┐
                │   Data Warehouse      │
                │  Postgres + dbt       │
                └──────────┬───────────┘
                           │
                     feature tables
                           │
                ┌──────────▼───────────┐
                │   ML Training Layer   │
                │ Python / notebooks    │
                └──────────┬───────────┘
                           │
                       trained model
                           │
                ┌──────────▼───────────┐
                │   Airflow Scheduler   │
                │ retrain / scoring     │
                └──────────┬───────────┘
                           │
                     predictions
                           │
                ┌──────────▼───────────┐
                │  Trade Suggestions    │
                │ strategy ranking      │
                └──────────────────────┘
```

## Overall architecture

```text
                ┌──────────────────────────────┐
                │        User Interface         │
                │  Notebook / Streamlit / App  │
                └──────────────┬───────────────┘
                               │
                     Trade suggestions / review
                               │
                ┌──────────────▼───────────────┐
                │        Strategy Layer         │
                │ signals / models / ranking    │
                └──────────────┬───────────────┘
                               │
                       feature tables
                               │
                ┌──────────────▼───────────────┐
                │       Data Engineering        │
                │ prices / fundamentals / news  │
                └──────────────┬───────────────┘
                               │
                        ingestion
                               │
                ┌──────────────▼───────────────┐
                │        Raw Data Sources       │
                │ APIs / PDFs / filings / news  │
                └──────────────────────────────┘
```

## Quick start (virtual environment + Makefile)

```bash
make setup
make run-pipeline
make run-dashboard
```

If you prefer manual activation:

```bash
source .venv/bin/activate
```

Useful commands:

```bash
make help
make run-ingestion
make run-features
make run-training
make run-scoring
make run-strategy
make dbt-run
```
