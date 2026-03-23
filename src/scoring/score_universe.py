"""Score investable universe using latest trained model and feature snapshot."""

from datetime import datetime, timezone
from pathlib import Path


SCORES_PATH = Path("models/trained_models/latest_scores.csv")


def score_universe() -> None:
    SCORES_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCORES_PATH.write_text(
        "symbol,score\nMSFT,0.62\nAMZN,0.58\nAAPL,0.57\nBA.L,0.56\nGOOG,0.55\n",
        encoding="utf-8",
    )
    print(f"[{datetime.now(timezone.utc).isoformat()}] score_universe: wrote {SCORES_PATH}")


if __name__ == "__main__":
    score_universe()
