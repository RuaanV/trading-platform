"""Minimal Streamlit dashboard for viewing model outputs, trade candidates, and portfolios."""

from calendar import month_name, monthcalendar
from html import escape
from pathlib import Path
import sys
from urllib.parse import urlencode

import pandas as pd
import streamlit as st
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_PIPELINE_DIR = ROOT_DIR / "data_pipeline"
if str(DATA_PIPELINE_DIR) not in sys.path:
    sys.path.append(str(DATA_PIPELINE_DIR))

from db import postgres_engine
from holding_news import load_symbol_news_sentiment, summarize_news_sentiment
from personal_portfolios import (
    ensure_personal_portfolio_tables,
    fetch_personal_portfolios,
    fetch_portfolio_holdings,
    fetch_portfolio_snapshots,
)


st.set_page_config(page_title="Trading Platform Dashboard", layout="wide")

scores_path = Path("models/trained_models/latest_scores.csv")
candidates_path = Path("models/trained_models/trade_candidates.csv")
recommendations_path = Path("models/trained_models/latest_recommendations.csv")
market_calendar_path = Path("models/trained_models/current_year_market_calendar.csv")

SYMBOL_ALIASES = {
    "BA.": "BA.L",
}

HOLDING_DETAIL_QUERY_KEYS = (
    "holding_symbol",
    "holding_portfolio_name",
    "holding_holder",
    "holding_portfolio_type",
)


def _normalize_symbol(symbol: object) -> str:
    normalized = str(symbol or "").strip().upper()
    return SYMBOL_ALIASES.get(normalized, normalized)


def _format_currency(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "N/A"
    return f"{float(numeric):,.2f}"


def _format_percent(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "N/A"
    return f"{float(numeric) * 100:.1f}%"


def _format_number(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "N/A"
    return f"{float(numeric):,.2f}"


def _get_query_params() -> dict[str, list[str]]:
    if hasattr(st, "experimental_get_query_params"):
        return st.experimental_get_query_params()
    if hasattr(st, "query_params"):
        params = dict(st.query_params)
        normalized: dict[str, list[str]] = {}
        for key, value in params.items():
            if isinstance(value, list):
                normalized[key] = [str(item) for item in value]
            else:
                normalized[key] = [str(value)]
        return normalized
    return {}


def _set_query_params(**params: str) -> None:
    if hasattr(st, "experimental_set_query_params"):
        st.experimental_set_query_params(**params)
        return
    if hasattr(st, "query_params"):
        st.query_params.clear()
        for key, value in params.items():
            st.query_params[key] = value


def _clear_query_params() -> None:
    if hasattr(st, "experimental_set_query_params"):
        st.experimental_set_query_params()
        return
    if hasattr(st, "query_params"):
        st.query_params.clear()


def _get_single_query_param(name: str) -> str:
    values = _get_query_params().get(name, [])
    return values[0] if values else ""


def _get_holding_route() -> dict[str, str]:
    return {key: _get_single_query_param(key) for key in HOLDING_DETAIL_QUERY_KEYS}


def _has_holding_route(route: dict[str, str]) -> bool:
    return all(route.values())


def _build_holding_detail_url(selected_holding: pd.Series) -> str:
    params = {
        "holding_symbol": str(selected_holding.get("detail_symbol", "")),
        "holding_portfolio_name": str(selected_holding.get("portfolio_name", "")),
        "holding_holder": str(selected_holding.get("holder", "")),
        "holding_portfolio_type": str(selected_holding.get("portfolio_type", "")),
    }
    return f"?{urlencode(params)}"


@st.cache_data(show_spinner=False)
def _load_recommendations() -> pd.DataFrame:
    if not recommendations_path.exists():
        return pd.DataFrame()

    recommendations = pd.read_csv(recommendations_path)
    if "symbol" in recommendations.columns:
        recommendations["symbol"] = recommendations["symbol"].map(_normalize_symbol)
    return recommendations


@st.cache_data(show_spinner=False)
def _load_company_snapshot() -> pd.DataFrame:
    query = """
    select
        symbol,
        short_name,
        currency,
        market_cap,
        enterprise_value,
        trailing_pe,
        forward_pe,
        trailing_eps,
        forward_eps,
        total_revenue,
        gross_profits,
        ebitda,
        total_assets,
        total_debt,
        stockholders_equity,
        cash_and_short_term_investments
    from analytics_features.fct_company_snapshot
    """
    try:
        return pd.read_sql(text(query), postgres_engine())
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False)
def _load_holding_history(
    portfolio_name: str,
    holder: str,
    portfolio_type: str,
    ticker: str,
) -> pd.DataFrame:
    query = """
    select
        s.snapshot_at,
        h.price,
        h.market_value,
        h.total_cost,
        h.gain_loss_value,
        h.gain_loss_pct
    from app.portfolio_holdings h
    join app.portfolio_snapshots s
      on s.id = h.snapshot_id
    join app.personal_portfolios p
      on p.id = h.portfolio_id
    where p.name = :portfolio_name
      and p.holder = :holder
      and p.portfolio_type = :portfolio_type
      and upper(coalesce(h.ticker, '')) = :ticker
    order by s.snapshot_at asc, h.id asc
    """
    try:
        history = pd.read_sql(
            text(query),
            postgres_engine(),
            params={
                "portfolio_name": portfolio_name,
                "holder": holder,
                "portfolio_type": portfolio_type,
                "ticker": _normalize_symbol(ticker),
            },
        )
    except Exception:
        return pd.DataFrame()

    if history.empty:
        return history

    history["snapshot_at"] = pd.to_datetime(history["snapshot_at"], errors="coerce")
    for column in ["price", "market_value", "total_cost", "gain_loss_value", "gain_loss_pct"]:
        history[column] = pd.to_numeric(history[column], errors="coerce")
    return history


def _attach_previous_prices(holdings: pd.DataFrame) -> pd.DataFrame:
    if holdings.empty:
        return holdings
    if "previous_price" in holdings.columns and not holdings["previous_price"].isna().all():
        return holdings

    query = """
    with latest_snapshot as (
        select distinct on (portfolio_id)
            id,
            portfolio_id,
            snapshot_at
        from app.portfolio_snapshots
        order by portfolio_id, snapshot_at desc, id desc
    ),
    holdings_with_prev as (
        select
            h.portfolio_id,
            h.snapshot_id,
            h.company,
            h.instrument_name,
            h.ticker,
            h.price,
            lag(h.price) over (
                partition by h.portfolio_id, upper(coalesce(h.ticker, ''))
                order by s.snapshot_at asc, s.id asc, h.id asc
            ) as previous_price
        from app.portfolio_holdings h
        join app.portfolio_snapshots s
          on s.id = h.snapshot_id
    )
    select
        p.name as portfolio_name,
        p.holder,
        p.portfolio_type,
        ls.snapshot_at,
        h.company,
        h.instrument_name,
        h.ticker,
        h.price,
        h.previous_price
    from latest_snapshot ls
    join app.personal_portfolios p
      on p.id = ls.portfolio_id
    join holdings_with_prev h
      on h.snapshot_id = ls.id
    """
    try:
        previous_prices = pd.read_sql(text(query), postgres_engine())
    except Exception:
        return holdings

    if previous_prices.empty:
        return holdings

    merged = holdings.copy()
    merge_keys = [
        "portfolio_name",
        "holder",
        "portfolio_type",
        "snapshot_at",
        "company",
        "instrument_name",
        "ticker",
        "price",
    ]
    if "previous_price" in merged.columns:
        merged = merged.drop(columns=["previous_price"])
    merged = merged.merge(previous_prices, on=merge_keys, how="left")
    return merged


@st.cache_data(show_spinner=False)
def _load_holding_news_sentiment(ticker: str, limit: int = 8) -> pd.DataFrame:
    try:
        return load_symbol_news_sentiment(_normalize_symbol(ticker), limit=limit)
    except Exception:
        return pd.DataFrame()


def _build_holding_detail_frame(holdings: pd.DataFrame) -> pd.DataFrame:
    detail_frame = holdings.copy()
    detail_frame["detail_symbol"] = detail_frame["ticker"].map(_normalize_symbol)
    detail_frame["detail_link"] = detail_frame.apply(_build_holding_detail_url, axis=1)
    return detail_frame


def _build_portfolio_detail_frame(portfolios: pd.DataFrame) -> pd.DataFrame:
    detail_frame = portfolios.copy()
    detail_frame["portfolio_label"] = (
        detail_frame["holder"].fillna("").astype(str).str.strip()
        + " | "
        + detail_frame["name"].fillna("").astype(str).str.strip()
        + " | "
        + detail_frame["portfolio_type"].fillna("").astype(str).str.strip()
    )
    return detail_frame


def _render_metric_grid(metrics: list[tuple[str, str]]) -> None:
    columns = st.columns(len(metrics))
    for column, (label, value) in zip(columns, metrics, strict=False):
        column.metric(label, value)


def _movement_direction(current_value: object, comparison_value: object) -> str | None:
    current_numeric = pd.to_numeric(pd.Series([current_value]), errors="coerce").iloc[0]
    comparison_numeric = pd.to_numeric(pd.Series([comparison_value]), errors="coerce").iloc[0]
    if pd.isna(current_numeric) or pd.isna(comparison_numeric):
        return None
    if float(current_numeric) > float(comparison_numeric):
        return "up"
    if float(current_numeric) < float(comparison_numeric):
        return "down"
    return "unchanged"


def _movement_indicator_html(direction: str | None, title: str | None = None, label: str | None = None) -> str:
    if direction == "up":
        arrow = "&uarr;"
        css_class = "holding-move holding-move--up"
    elif direction == "down":
        arrow = "&darr;"
        css_class = "holding-move holding-move--down"
    elif direction == "unchanged":
        arrow = "&rarr;"
        css_class = "holding-move holding-move--flat"
    else:
        return ""
    title_attribute = f' title="{escape(title)}"' if title else ""
    label_html = f"<span class='holding-move-label'>{escape(label)}</span>" if label else ""
    return f"<span class='{css_class}'{title_attribute}>{arrow}</span>{label_html}"


def _format_price_move(price: object, previous_price: object) -> str:
    direction = _movement_direction(price, previous_price)
    return _movement_indicator_html(direction, title="Price movement versus previous snapshot")


def _render_holding_table(holding_frame: pd.DataFrame) -> None:
    if holding_frame.empty:
        st.info("No holdings found.")
        return

    trailing_metadata_columns = ["snapshot_at", "source_updated_at", "quote_delay_note"]
    display_frame = holding_frame.copy()
    if "Company / Ticker" in display_frame.columns:
        display_frame = display_frame.drop(columns=["Company / Ticker"])
    display_frame["Company / Ticker"] = display_frame.apply(
        lambda row: (
            f"<a href=\"{escape(str(row.get('detail_link', '#')))}\" target=\"_self\">"
            f"{escape(str(row.get('company', '')))} ({escape(str(row.get('detail_symbol', '')))}"
            f")</a>"
        ),
        axis=1,
    )
    if "price" in display_frame.columns and "previous_price" in display_frame.columns:
        display_frame["Movement"] = display_frame.apply(
            lambda row: _format_price_move(row.get("price"), row.get("previous_price")),
            axis=1,
        )

    ordered_columns = display_frame.columns.tolist()
    company_index = ordered_columns.index("company") if "company" in ordered_columns else 0
    for column in ["company", "ticker", "detail_link", "detail_symbol"]:
        if column in ordered_columns:
            ordered_columns.remove(column)
    if "previous_price" in ordered_columns:
        ordered_columns.remove("previous_price")
    if "Company / Ticker" in ordered_columns:
        ordered_columns.remove("Company / Ticker")
    ordered_columns.insert(company_index, "Company / Ticker")
    if "Movement" in ordered_columns and "price" in ordered_columns:
        ordered_columns.remove("Movement")
        price_index = ordered_columns.index("price")
        ordered_columns.insert(price_index + 1, "Movement")
    for column in trailing_metadata_columns:
        if column in ordered_columns:
            ordered_columns.remove(column)
            ordered_columns.append(column)
    display_frame = display_frame.loc[:, ordered_columns]

    html_table = display_frame.to_html(index=False, escape=False)
    st.markdown(
        """
        <style>
        .holding-table-wrap {
            overflow-x: auto;
            border: 1px solid rgba(250, 250, 250, 0.08);
            border-radius: 12px;
        }
        .holding-table-wrap table {
            border-collapse: collapse;
            width: 100%;
        }
        .holding-table-wrap th,
        .holding-table-wrap td {
            border-bottom: 1px solid rgba(250, 250, 250, 0.08);
            padding: 0.65rem 0.75rem;
            text-align: left;
            white-space: nowrap;
        }
        .holding-table-wrap th {
            background: rgba(250, 250, 250, 0.04);
            font-weight: 600;
        }
        .holding-table-wrap a {
            color: #83b8ff;
            text-decoration: none;
            font-weight: 600;
        }
        .holding-table-wrap a:hover {
            text-decoration: underline;
        }
        .holding-move {
            display: inline-block;
            font-size: 1rem;
            font-weight: 700;
            line-height: 1;
        }
        .holding-move--up {
            color: #38b000;
        }
        .holding-move--down {
            color: #d62828;
        }
        .holding-move--flat {
            color: #a0a4ab;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(f"<div class='holding-table-wrap'>{html_table}</div>", unsafe_allow_html=True)


def _render_holding_history_chart(history_df: pd.DataFrame, gross_profit_indicator: str = "") -> None:
    st.markdown("##### Profitability And Cost")
    if history_df.empty:
        st.info("No holding history is available yet for this symbol.")
        return

    chart_df = history_df.dropna(subset=["snapshot_at"]).copy()
    if chart_df.empty:
        st.info("Holding history exists, but snapshot timestamps are missing.")
        return

    chart_df["snapshot_day"] = chart_df["snapshot_at"].dt.floor("D")
    chart_df = (
        chart_df.sort_values("snapshot_at")
        .groupby("snapshot_day", as_index=False)
        .last()
    )
    chart_df = chart_df.rename(
        columns={
            "snapshot_day": "Snapshot",
            "market_value": "Market Value",
            "total_cost": "Total Cost",
            "gain_loss_value": "Profit / Loss",
        }
    )
    chart_df["Snapshot"] = chart_df["Snapshot"].dt.strftime("%Y-%m-%d")
    chart_df = chart_df.set_index("Snapshot")

    latest_history = history_df.iloc[-1]
    latest_return_pct = pd.to_numeric(
        pd.Series([latest_history.get("gain_loss_pct")]), errors="coerce"
    ).iloc[0]
    summary_columns = st.columns(3)
    summary_columns[0].metric("Latest Cost", _format_currency(latest_history.get("total_cost")))
    with summary_columns[1]:
        st.markdown("**Gross Profit**")
        st.markdown(
            f"""
            <style>
            .holding-history-profit {{
                align-items: center;
                display: inline-flex;
                gap: 0.45rem;
                margin-top: 0.15rem;
            }}
            .holding-history-profit__value {{
                color: rgba(250, 250, 250, 0.96);
                font-size: 1.7rem;
                font-weight: 600;
                line-height: 1.2;
            }}
            .holding-history-profit .holding-move {{
                font-size: 1.2rem;
                font-weight: 700;
                line-height: 1;
            }}
            .holding-history-profit .holding-move--up {{
                color: #38b000;
            }}
            .holding-history-profit .holding-move--down {{
                color: #d62828;
            }}
            .holding-history-profit .holding-move--flat {{
                color: #a0a4ab;
            }}
            </style>
            <div class="holding-history-profit">
                <span class="holding-history-profit__value">{escape(_format_currency(latest_history.get("gain_loss_value")))}</span>
                {gross_profit_indicator}
            </div>
            """,
            unsafe_allow_html=True,
        )
    summary_columns[2].metric(
        "Margin",
        _format_percent(latest_return_pct / 100 if pd.notna(latest_return_pct) else None),
    )
    st.line_chart(chart_df[["Market Value", "Total Cost", "Profit / Loss"]], use_container_width=True)


def _render_holding_detail(
    selected_holding: pd.Series,
    company_snapshot: pd.DataFrame,
    recommendations: pd.DataFrame,
    total_market_value: float,
    holding_history: pd.DataFrame | None = None,
) -> None:
    detail_symbol = selected_holding["detail_symbol"]
    company_row = pd.Series(dtype="object")
    recommendation_row = pd.Series(dtype="object")
    news_frame = _load_holding_news_sentiment(detail_symbol)
    news_summary = summarize_news_sentiment(news_frame)

    if not company_snapshot.empty:
        company_matches = company_snapshot.loc[company_snapshot["symbol"] == detail_symbol]
        if not company_matches.empty:
            company_row = company_matches.iloc[0]

    if not recommendations.empty:
        recommendation_matches = recommendations.loc[recommendations["symbol"] == detail_symbol]
        if not recommendation_matches.empty:
            recommendation_row = recommendation_matches.iloc[0]

    st.markdown(f"#### Holding Detail: `{detail_symbol}`")
    st.caption(
        f"{selected_holding.get('instrument_name', selected_holding.get('company', detail_symbol))} | "
        f"{selected_holding.get('portfolio_name', '')}"
    )

    market_value = pd.to_numeric(pd.Series([selected_holding.get("market_value")]), errors="coerce").iloc[0]
    total_cost = pd.to_numeric(pd.Series([selected_holding.get("total_cost")]), errors="coerce").iloc[0]
    portfolio_weight = (
        float(market_value) / float(total_market_value)
        if pd.notna(market_value) and total_market_value > 0
        else 0.0
    )

    gross_profit_direction = _movement_direction(market_value, total_cost)
    gross_profit_indicator = _movement_indicator_html(
        gross_profit_direction,
        title="Compares current market value with total purchase cost",
        label="Gross P/L vs cost",
    )
    metric_columns = st.columns(4)
    metric_columns[0].metric("Market Value", _format_currency(selected_holding.get("market_value")))
    metric_columns[1].metric("Portfolio Weight", _format_percent(portfolio_weight))
    metric_columns[2].metric("Quantity", str(selected_holding.get("quantity", "N/A")))
    metric_columns[3].metric("Price", _format_currency(selected_holding.get("price")))

    _render_holding_history_chart(
        holding_history if holding_history is not None else pd.DataFrame(),
        gross_profit_indicator=gross_profit_indicator,
    )

    fundamentals_col, recommendation_col = st.columns([1.2, 1])

    with fundamentals_col:
        st.markdown("##### Fundamentals")
        if company_row.empty:
            st.info("No company fundamentals available for this holding.")
        else:
            _render_metric_grid(
                [
                    ("Trailing EPS", _format_number(company_row.get("trailing_eps"))),
                    ("Forward EPS", _format_number(company_row.get("forward_eps"))),
                    ("Trailing P/E", _format_number(company_row.get("trailing_pe"))),
                    ("Forward P/E", _format_number(company_row.get("forward_pe"))),
                ]
            )
            st.dataframe(
                pd.DataFrame(
                    [
                        {"Metric": "Revenue", "Value": _format_currency(company_row.get("total_revenue"))},
                        {"Metric": "Gross Profit", "Value": _format_currency(company_row.get("gross_profits"))},
                        {"Metric": "EBITDA", "Value": _format_currency(company_row.get("ebitda"))},
                        {"Metric": "Total Assets", "Value": _format_currency(company_row.get("total_assets"))},
                        {"Metric": "Total Debt", "Value": _format_currency(company_row.get("total_debt"))},
                        {
                            "Metric": "Shareholders Equity",
                            "Value": _format_currency(company_row.get("stockholders_equity")),
                        },
                        {
                            "Metric": "Cash & Short-Term Investments",
                            "Value": _format_currency(company_row.get("cash_and_short_term_investments")),
                        },
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )

    with recommendation_col:
        st.markdown("##### Recommendation")
        if recommendation_row.empty:
            st.info("No recommendation available for this holding.")
        else:
            _render_metric_grid(
                [
                    ("Action", str(recommendation_row.get("action", "N/A"))),
                    ("Target Weight", _format_percent(recommendation_row.get("target_weight"))),
                    ("Confidence", _format_percent(recommendation_row.get("confidence"))),
                ]
            )
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "Metric": "Recommendation Score",
                            "Value": _format_number(recommendation_row.get("recommendation_score")),
                        },
                        {
                            "Metric": "Expected Return",
                            "Value": _format_percent(recommendation_row.get("expected_return")),
                        },
                        {"Metric": "Risk Score", "Value": _format_number(recommendation_row.get("risk_score"))},
                        {"Metric": "Rank", "Value": str(recommendation_row.get("rank", "N/A"))},
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )
            st.caption(str(recommendation_row.get("rationale", "")))

    st.markdown("##### News Sentiment")
    if news_frame.empty or news_summary is None:
        st.info("No stored daily news sentiment is available for this holding yet.")
    else:
        _render_metric_grid(
            [
                ("Headlines", str(news_summary.headline_count)),
                ("Average Sentiment", _format_number(news_summary.average_sentiment_score)),
                ("Market Tone", news_summary.sentiment_label.title()),
                ("As Of", news_summary.as_of_date),
            ]
        )
        display_news = news_frame.copy()
        display_news["local_date"] = (
            pd.to_datetime(display_news["published_at"], utc=True, errors="coerce")
            .dt.tz_convert("Europe/London")
            .dt.date
        )
        display_news = display_news.loc[display_news["local_date"].astype(str) == news_summary.as_of_date].copy()
        display_news["published_at"] = (
            pd.to_datetime(display_news["published_at"], utc=True, errors="coerce")
            .dt.tz_convert("Europe/London")
            .dt.strftime("%Y-%m-%d %H:%M")
        )
        display_news["headline"] = display_news.apply(
            lambda row: (
                f"<a href=\"{escape(str(row.get('article_link', '#')))}\" target=\"_blank\">"
                f"{escape(str(row.get('article_title', '')))}</a>"
            ),
            axis=1,
        )
        article_columns = ["published_at", "publisher_name", "headline", "sentiment_label", "sentiment_score"]
        article_frame = display_news.loc[:, article_columns].rename(
            columns={
                "published_at": "Published",
                "publisher_name": "Publisher",
                "headline": "Headline",
                "sentiment_label": "Sentiment",
                "sentiment_score": "Score",
            }
        )
        st.markdown(article_frame.to_html(index=False, escape=False), unsafe_allow_html=True)


def _find_holding_from_route(holding_detail_df: pd.DataFrame, route: dict[str, str]) -> pd.Series:
    if holding_detail_df.empty or not _has_holding_route(route):
        return pd.Series(dtype="object")

    matches = holding_detail_df.loc[
        (holding_detail_df["detail_symbol"].astype(str) == route["holding_symbol"])
        & (holding_detail_df["portfolio_name"].astype(str) == route["holding_portfolio_name"])
        & (holding_detail_df["holder"].astype(str) == route["holding_holder"])
        & (holding_detail_df["portfolio_type"].astype(str) == route["holding_portfolio_type"])
    ]
    if matches.empty:
        return pd.Series(dtype="object")
    return matches.iloc[0]


def _render_portfolio_detail(
    selected_portfolio: pd.Series,
    holdings_df: pd.DataFrame,
    snapshots_df: pd.DataFrame,
) -> pd.DataFrame:
    portfolio_name = selected_portfolio.get("name", "")
    holder = selected_portfolio.get("holder", "")
    portfolio_type = selected_portfolio.get("portfolio_type", "")

    st.markdown("#### Active Portfolio")
    st.caption(f"{portfolio_name} | {holder} | {portfolio_type}")

    _render_metric_grid(
        [
            ("Total Market Value", _format_currency(selected_portfolio.get("total_market_value"))),
            ("Total Cost", _format_currency(selected_portfolio.get("total_cost"))),
            ("Gain / Loss", _format_currency(selected_portfolio.get("gain_loss_value"))),
            ("Holdings", str(selected_portfolio.get("holdings_count", "0"))),
        ]
    )

    portfolio_snapshots = snapshots_df.loc[
        (snapshots_df["portfolio_name"] == portfolio_name)
        & (snapshots_df["holder"] == holder)
        & (snapshots_df["portfolio_type"] == portfolio_type)
    ].copy()
    latest_snapshot = portfolio_snapshots.iloc[0] if not portfolio_snapshots.empty else pd.Series(dtype="object")

    snapshot_col, metadata_col = st.columns([1.1, 1])

    with snapshot_col:
        st.markdown("##### Latest Snapshot")
        if latest_snapshot.empty:
            st.info("No snapshot metadata available for this portfolio yet.")
        else:
            st.dataframe(
                pd.DataFrame(
                    [
                        {"Metric": "Latest Snapshot", "Value": str(latest_snapshot.get("snapshot_at", "N/A"))},
                        {"Metric": "Source Updated", "Value": str(latest_snapshot.get("source_updated_at", "N/A"))},
                        {"Metric": "Source Name", "Value": str(latest_snapshot.get("source_name", "N/A"))},
                        {"Metric": "Quote Delay", "Value": str(latest_snapshot.get("quote_delay_note", "N/A"))},
                        {"Metric": "FX Note", "Value": str(latest_snapshot.get("fx_note", "N/A"))},
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )

    with metadata_col:
        st.markdown("##### Portfolio Metadata")
        st.dataframe(
            pd.DataFrame(
                [
                    {"Metric": "Holder", "Value": str(holder or "N/A")},
                    {"Metric": "Portfolio Name", "Value": str(portfolio_name or "N/A")},
                    {"Metric": "Portfolio Type", "Value": str(portfolio_type or "N/A")},
                    {"Metric": "Created At", "Value": str(selected_portfolio.get("created_at", "N/A"))},
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )

    portfolio_holdings = holdings_df.loc[
        (holdings_df["portfolio_name"] == portfolio_name)
        & (holdings_df["holder"] == holder)
        & (holdings_df["portfolio_type"] == portfolio_type)
    ].copy()

    st.markdown("##### Portfolio Holdings")
    if portfolio_holdings.empty:
        st.info("No holdings found for the selected portfolio.")
        return pd.DataFrame()

    portfolio_holding_detail_df = _build_holding_detail_frame(portfolio_holdings)
    _render_holding_table(portfolio_holding_detail_df)
    st.caption("Use the company or ticker link to open its dedicated detail page.")
    return pd.DataFrame()


def _build_market_calendar_html(frame: pd.DataFrame, selected_month: int) -> str:
    """Render an Outlook-style month calendar with compact event cards and hover details."""
    display_frame = frame.copy()
    display_frame["event_date"] = pd.to_datetime(display_frame["event_date"], errors="coerce")
    display_frame = display_frame.dropna(subset=["event_date"]).sort_values(
        ["event_date", "symbol", "event_type", "event_name"]
    )
    display_frame["event_day"] = display_frame["event_date"].dt.day
    display_frame["event_month"] = display_frame["event_date"].dt.month

    month_frame = display_frame.loc[display_frame["event_month"] == selected_month].copy()
    if month_frame.empty:
        return (
            "<div class='calendar-empty'>"
            f"No market calendar events found for {escape(month_name[selected_month])}."
            "</div>"
        )

    calendar_year = int(month_frame["event_date"].dt.year.mode().iloc[0])
    events_by_day = {
        day: rows.to_dict("records")
        for day, rows in month_frame.groupby("event_day", sort=True)
    }
    weekday_headers = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    weeks = monthcalendar(calendar_year, selected_month)

    html_parts = [
        """
        <style>
        .market-calendar {
            background: linear-gradient(180deg, #2c233c 0%, #1c1b24 18%, #17181f 100%);
            border: 1px solid #3c3f4e;
            border-radius: 18px;
            display: grid;
            grid-template-columns: repeat(7, minmax(0, 1fr));
            gap: 1px;
            margin-top: 0.75rem;
            overflow: hidden;
            padding: 1px;
        }
        .market-calendar__header {
            background: #232325;
            color: #d4d7de;
            font-size: 0.84rem;
            font-weight: 700;
            letter-spacing: 0.02em;
            min-height: 48px;
            padding: 0.8rem 0.75rem;
            text-align: left;
        }
        .market-calendar__day {
            background: linear-gradient(180deg, #222326 0%, #1f2023 100%);
            min-height: 120px;
            padding: 0.55rem;
            position: relative;
        }
        .market-calendar__day--empty {
            background: linear-gradient(180deg, #1d1e22 0%, #191a1d 100%);
        }
        .market-calendar__day-number {
            color: #f2f4f8;
            font-size: 1rem;
            font-weight: 700;
            margin-bottom: 0.4rem;
        }
        .market-calendar__events {
            display: flex;
            flex-direction: column;
            gap: 0.3rem;
        }
        .market-calendar__event {
            background: linear-gradient(180deg, #bb5a0a 0%, #9e4703 100%);
            border-left: 5px solid #ff7a1a;
            border-radius: 7px;
            box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06);
            color: #fff4eb;
            cursor: default;
            padding: 0.42rem 0.58rem;
            position: relative;
        }
        .market-calendar__event-line {
            display: block;
            line-height: 1.2;
        }
        .market-calendar__event-date {
            color: #ffd7b5;
            font-size: 0.68rem;
            font-weight: 700;
            opacity: 0.96;
        }
        .market-calendar__event-symbol {
            font-size: 0.82rem;
            font-weight: 800;
            margin-top: 0.12rem;
        }
        .market-calendar__event-type {
            color: #ffe3cc;
            font-size: 0.72rem;
            opacity: 0.88;
            text-transform: capitalize;
        }
        .market-calendar__tooltip {
            background: #20232b;
            border-radius: 12px;
            bottom: calc(100% + 10px);
            border: 1px solid #464b5d;
            box-shadow: 0 20px 45px rgba(0, 0, 0, 0.4);
            color: #f5f7fb;
            left: 0;
            opacity: 0;
            padding: 0.8rem 0.9rem;
            pointer-events: none;
            position: absolute;
            transform: translateY(6px);
            transition: opacity 0.16s ease, transform 0.16s ease;
            visibility: hidden;
            width: 240px;
            z-index: 20;
        }
        .market-calendar__tooltip::after {
            border-left: 8px solid transparent;
            border-right: 8px solid transparent;
            border-top: 8px solid #20232b;
            content: "";
            left: 18px;
            position: absolute;
            top: 100%;
        }
        .market-calendar__event:hover .market-calendar__tooltip {
            opacity: 1;
            transform: translateY(0);
            visibility: visible;
        }
        .market-calendar__tooltip-title {
            color: #ffffff;
            font-size: 0.9rem;
            font-weight: 800;
            margin-bottom: 0.35rem;
        }
        .market-calendar__tooltip-line {
            display: block;
            font-size: 0.77rem;
            line-height: 1.35;
            margin-top: 0.14rem;
        }
        .calendar-empty {
            background: linear-gradient(180deg, #222326 0%, #1d1e22 100%);
            border: 1px solid #3c3f4e;
            border-radius: 12px;
            color: #d6dae3;
            padding: 1rem;
        }
        </style>
        """
    ]
    html_parts.extend(
        f"<div class='market-calendar__header'>{header}</div>" for header in weekday_headers
    )

    for week in weeks:
        for day in week:
            if day == 0:
                html_parts.append("<div class='market-calendar__day market-calendar__day--empty'></div>")
                continue

            day_events = events_by_day.get(day, [])
            event_cards = []
            for event in day_events:
                tooltip_lines = [
                    f"<span class='market-calendar__tooltip-title'>{escape(str(event.get('event_name', 'Event')))}</span>",
                    f"<span class='market-calendar__tooltip-line'><strong>Date:</strong> {escape(event['event_date'].strftime('%A, %d %B %Y'))}</span>",
                    f"<span class='market-calendar__tooltip-line'><strong>Symbol:</strong> {escape(str(event.get('symbol', '')))}</span>",
                    f"<span class='market-calendar__tooltip-line'><strong>Type:</strong> {escape(str(event.get('event_type', ''))).title()}</span>",
                    f"<span class='market-calendar__tooltip-line'><strong>Company:</strong> {escape(str(event.get('company', '')))}</span>",
                ]
                if pd.notna(event.get("event_timestamp")):
                    tooltip_lines.append(
                        f"<span class='market-calendar__tooltip-line'><strong>Timestamp:</strong> {escape(str(event.get('event_timestamp', '')))}</span>"
                    )
                event_cards.append(
                    "<div class='market-calendar__event'>"
                    f"<span class='market-calendar__event-line market-calendar__event-date'>{escape(event['event_date'].strftime('%d %b'))} {escape(str(event.get('event_name', '')))}</span>"
                    f"<span class='market-calendar__event-line market-calendar__event-symbol'>{escape(str(event.get('symbol', '')))}</span>"
                    f"<span class='market-calendar__event-line market-calendar__event-type'>{escape(str(event.get('event_type', '')))}</span>"
                    "<div class='market-calendar__tooltip'>"
                    f"{''.join(tooltip_lines)}"
                    "</div>"
                    "</div>"
                )

            html_parts.append(
                "<div class='market-calendar__day'>"
                f"<div class='market-calendar__day-number'>{day}</div>"
                f"<div class='market-calendar__events'>{''.join(event_cards)}</div>"
                "</div>"
            )

    return f"<div class='market-calendar'>{''.join(html_parts)}</div>"

try:
    ensure_personal_portfolio_tables()
    portfolios_df = fetch_personal_portfolios()
    snapshots_df = fetch_portfolio_snapshots()
    holdings_df = fetch_portfolio_holdings()
except Exception as exc:  # noqa: BLE001
    portfolios_error = exc
    portfolios_df = pd.DataFrame()
    snapshots_df = pd.DataFrame()
    holdings_df = pd.DataFrame()
else:
    portfolios_error = None
    holdings_df = _attach_previous_prices(holdings_df)

company_snapshot_df = _load_company_snapshot()
recommendations_df = _load_recommendations()
holding_route = _get_holding_route()
holding_detail_df = _build_holding_detail_frame(holdings_df) if not holdings_df.empty else pd.DataFrame()

if _has_holding_route(holding_route):
    st.title("Holding Details")
    if st.button("Back To Dashboard", type="primary"):
        _clear_query_params()
        st.rerun()

    if portfolios_error is not None:
        st.warning(f"Could not load portfolios from Postgres: {portfolios_error}")
    elif holding_detail_df.empty:
        st.info("No holdings are available for the selected detail page.")
    else:
        selected_holding = _find_holding_from_route(holding_detail_df, holding_route)
        if selected_holding.empty:
            st.warning("The selected holding could not be found in the latest portfolio snapshot.")
        else:
            portfolio_holdings = holding_detail_df.loc[
                (holding_detail_df["portfolio_name"] == selected_holding.get("portfolio_name"))
                & (holding_detail_df["holder"] == selected_holding.get("holder"))
                & (holding_detail_df["portfolio_type"] == selected_holding.get("portfolio_type"))
            ]
            total_market_value = float(
                pd.to_numeric(portfolio_holdings["market_value"], errors="coerce").fillna(0).sum()
            )
            holding_history_df = _load_holding_history(
                portfolio_name=str(selected_holding.get("portfolio_name", "")),
                holder=str(selected_holding.get("holder", "")),
                portfolio_type=str(selected_holding.get("portfolio_type", "")),
                ticker=str(selected_holding.get("ticker", "")),
            )
            _render_holding_detail(
                selected_holding,
                company_snapshot=company_snapshot_df,
                recommendations=recommendations_df,
                total_market_value=total_market_value,
                holding_history=holding_history_df,
            )

            st.markdown("##### Snapshot History")
            if holding_history_df.empty:
                st.info("No holding snapshot history is available for this symbol yet.")
            else:
                display_history = holding_history_df.copy()
                st.dataframe(display_history, use_container_width=True, hide_index=True)
else:
    st.title("Trading Platform Dashboard")

    st.subheader("Personal Portfolios")
    selected_portfolio = pd.Series(dtype="object")

    if portfolios_error is not None:
        st.warning(f"Could not load portfolios from Postgres: {portfolios_error}")
    elif portfolios_df.empty:
        st.info("No portfolios yet. Add one with `make add-portfolio`.")
    else:
        portfolio_detail_df = _build_portfolio_detail_frame(portfolios_df)
        portfolio_selection_event = st.dataframe(
            portfolio_detail_df.drop(columns=["portfolio_label"]),
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key="personal_portfolios_table",
        )
        st.caption("Select a portfolio row to activate its detail view and holdings list below.")

        selected_rows = portfolio_selection_event.selection.rows
        if selected_rows:
            selected_portfolio = portfolio_detail_df.iloc[selected_rows[0]]

    st.subheader("Portfolio Holdings")
    total_market_value = 0.0

    if portfolios_error is not None:
        st.info("Portfolio holdings are unavailable because portfolio data could not be loaded.")
    elif holdings_df.empty:
        st.info("No holdings yet. Import a CSV with `PORTFOLIO_CSV_PATH=... make add-portfolio`.")
    else:
        total_market_value = float(
            pd.to_numeric(holding_detail_df["market_value"], errors="coerce").fillna(0).sum()
        )
        _render_holding_table(holding_detail_df)
        st.caption("Use the company or ticker link to open the dedicated holding detail page.")

    st.subheader("Market Calendar")
    if market_calendar_path.exists():
        market_calendar_df = pd.read_csv(market_calendar_path)
        market_calendar_df["event_date"] = pd.to_datetime(market_calendar_df["event_date"], errors="coerce")
        available_months = sorted(
            month
            for month in market_calendar_df["event_date"].dropna().dt.month.unique().tolist()
            if month
        )
        if not available_months:
            st.info("Market calendar data exists, but no valid event dates were found.")
        else:
            current_month = pd.Timestamp.today().month
            default_month = current_month if current_month in available_months else available_months[0]
            selected_month = st.selectbox(
                "Month",
                options=available_months,
                index=available_months.index(default_month),
                format_func=lambda month: month_name[month],
                key="market_calendar_month",
            )
            st.markdown(
                _build_market_calendar_html(market_calendar_df, selected_month),
                unsafe_allow_html=True,
            )
    else:
        st.info("No market calendar yet. Run data_pipeline/ingest_market_calendar.py first.")

    st.subheader("Latest Scores / Recommendations / Candidates")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("##### Latest Scores")
        if scores_path.exists():
            st.dataframe(pd.read_csv(scores_path), use_container_width=True)
        else:
            st.info("No scores yet. Run src/scoring/score_universe.py first.")

    with col2:
        st.markdown("##### Recommendations")
        if recommendations_path.exists():
            st.dataframe(pd.read_csv(recommendations_path), use_container_width=True)
        else:
            st.info("No recommendations yet. Run src/recommender/generate_recommendations.py first.")

    with col3:
        st.markdown("##### Trade Candidates")
        if candidates_path.exists():
            st.dataframe(pd.read_csv(candidates_path), use_container_width=True)
        else:
            st.info("No candidates yet. Run src/strategies/generate_trade_candidates.py first.")

    st.subheader("Snapshots")
    if portfolios_error is not None:
        st.info("Snapshots are unavailable because portfolio data could not be loaded.")
    elif snapshots_df.empty:
        st.info("No snapshots yet. Import holdings with `PORTFOLIO_CSV_PATH=... make add-portfolio`.")
    elif not selected_portfolio.empty:
        filtered_snapshots = snapshots_df.loc[
            (snapshots_df["portfolio_name"] == selected_portfolio.get("name"))
            & (snapshots_df["holder"] == selected_portfolio.get("holder"))
            & (snapshots_df["portfolio_type"] == selected_portfolio.get("portfolio_type"))
        ]
        st.dataframe(filtered_snapshots, use_container_width=True, hide_index=True)
    else:
        st.dataframe(snapshots_df, use_container_width=True, hide_index=True)
