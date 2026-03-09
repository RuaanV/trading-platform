"""Minimal Streamlit dashboard for viewing model outputs and trade candidates."""

from pathlib import Path

import pandas as pd
import streamlit as st


st.set_page_config(page_title="Trading Platform Dashboard", layout="wide")
st.title("Trading Platform Dashboard")

scores_path = Path("models/trained_models/latest_scores.csv")
candidates_path = Path("models/trained_models/trade_candidates.csv")

col1, col2 = st.columns(2)

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
