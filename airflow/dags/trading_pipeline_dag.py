"""Airflow DAG for end-to-end trading pipeline orchestration."""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_pipeline.ingest_fundamentals import ingest_fundamentals
from data_pipeline.ingest_prices import ingest_prices
from src.features.build_features import build_features
from src.scoring.score_universe import score_universe
from src.strategies.generate_trade_candidates import generate_trade_candidates
from src.training.train_return_model import train_return_model


default_args = {
    "owner": "trading-platform",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="trading_pipeline",
    description="Ingestion -> features -> train -> score -> candidates",
    schedule="0 6 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["trading", "ml"],
) as dag:
    t_ingest_prices = PythonOperator(task_id="ingest_prices", python_callable=ingest_prices)
    t_ingest_fundamentals = PythonOperator(
        task_id="ingest_fundamentals", python_callable=ingest_fundamentals
    )
    t_build_features = PythonOperator(task_id="build_features", python_callable=build_features)
    t_train = PythonOperator(task_id="train_return_model", python_callable=train_return_model)
    t_score = PythonOperator(task_id="score_universe", python_callable=score_universe)
    t_candidates = PythonOperator(
        task_id="generate_trade_candidates", python_callable=generate_trade_candidates
    )

    [t_ingest_prices, t_ingest_fundamentals] >> t_build_features >> t_train >> t_score >> t_candidates
