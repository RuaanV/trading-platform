"""Agent that fetches daily holding news headlines and scores sentiment."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from data_pipeline.holding_news import refresh_symbol_news_sentiment

from .base import AgentContext, AgentResult


@dataclass(slots=True)
class HoldingNewsSentimentAgent:
    """Fetch recent headlines for a holding and persist a simple sentiment view."""

    name: str = "holding_news_sentiment"
    default_symbol: str = "GOOG"

    def run(self, context: AgentContext) -> AgentResult:
        target_symbol = str(context.metadata.get("symbol") or self.default_symbol).strip().upper()
        headlines, summary = refresh_symbol_news_sentiment(target_symbol)
        if headlines.empty or summary is None:
            return AgentResult(
                agent_name=self.name,
                summary=f"No Yahoo Finance headlines found today for {target_symbol}.",
                actions=[],
                metrics={"headline_count": 0.0, "average_sentiment_score": 0.0},
                metadata={
                    "symbol": target_symbol,
                    "source": "Yahoo Finance",
                    "as_of": datetime.now(timezone.utc).isoformat(),
                },
            )

        actions = [
            {
                "symbol": target_symbol,
                "headline": str(row.article_title),
                "publisher": str(row.publisher_name),
                "link": str(row.article_link),
                "sentiment_score": round(float(row.sentiment_score), 4),
                "sentiment_label": str(row.sentiment_label),
                "published_at": row.published_at.isoformat(),
            }
            for row in headlines.itertuples(index=False)
        ]
        return AgentResult(
            agent_name=self.name,
            summary=(
                f"Captured {summary.headline_count} Yahoo Finance headlines for {target_symbol} on "
                f"{summary.as_of_date} with {summary.sentiment_label} sentiment."
            ),
            actions=actions,
            metrics={
                "headline_count": float(summary.headline_count),
                "average_sentiment_score": float(summary.average_sentiment_score),
            },
            metadata={
                "symbol": target_symbol,
                "sentiment_label": summary.sentiment_label,
                "source": "Yahoo Finance",
                "as_of_date": summary.as_of_date,
            },
        )
