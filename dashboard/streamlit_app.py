"""Minimal Streamlit dashboard for viewing model outputs, trade candidates, and portfolios."""

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_PIPELINE_DIR = ROOT_DIR / "data_pipeline"
if str(DATA_PIPELINE_DIR) not in sys.path:
    sys.path.append(str(DATA_PIPELINE_DIR))

from personal_portfolios import (
    ensure_personal_portfolio_tables,
    fetch_personal_portfolios,
    fetch_portfolio_holdings,
    fetch_portfolio_snapshots,
)


st.set_page_config(page_title="Trading Platform Dashboard", layout="wide")
st.title("Trading Platform Dashboard")

scores_path = Path("models/trained_models/latest_scores.csv")
candidates_path = Path("models/trained_models/trade_candidates.csv")
recommendations_path = Path("models/trained_models/latest_recommendations.csv")

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Latest Scores")
    if scores_path.exists():
        st.dataframe(pd.read_csv(scores_path), use_container_width=True)
    else:
        st.info("No scores yet. Run src/scoring/score_universe.py first.")

with col2:
    st.subheader("Trade Candidates")
    if candidates_path.exists():
        st.dataframe(pd.read_csv(candidates_path), use_container_width=True)
    else:
        st.info("No candidates yet. Run src/strategies/generate_trade_candidates.py first.")

with col3:
    st.subheader("Recommendations")
    if recommendations_path.exists():
        st.dataframe(pd.read_csv(recommendations_path), use_container_width=True)
    else:
        st.info("No recommendations yet. Run src/recommender/generate_recommendations.py first.")

st.subheader("Personal Portfolios")
try:
    ensure_personal_portfolio_tables()
    portfolios_df = fetch_personal_portfolios()
    snapshots_df = fetch_portfolio_snapshots()
    holdings_df = fetch_portfolio_holdings()
except Exception as exc:  # noqa: BLE001
    st.warning(f"Could not load portfolios from Postgres: {exc}")
else:
    if portfolios_df.empty:
        st.info("No portfolios yet. Add one with `make add-portfolio`.")
    else:
        st.dataframe(portfolios_df, use_container_width=True)

    st.subheader("Portfolio Snapshots")
    if snapshots_df.empty:
        st.info("No snapshots yet. Import holdings with `PORTFOLIO_CSV_PATH=... make add-portfolio`.")
    else:
        st.dataframe(snapshots_df, use_container_width=True)

    st.subheader("Portfolio Holdings")
    if holdings_df.empty:
        st.info("No holdings yet. Import a CSV with `PORTFOLIO_CSV_PATH=... make add-portfolio`.")
    else:
        st.dataframe(holdings_df, use_container_width=True)
