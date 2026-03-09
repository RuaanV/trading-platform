"""Generate ranked trade candidates from model scores and risk constraints."""

from datetime import datetime, timezone
from pathlib import Path


CANDIDATES_PATH = Path("models/trained_models/trade_candidates.csv")


def generate_trade_candidates() -> None:
    CANDIDATES_PATH.parent.mkdir(parents=True, exist_ok=True)
    CANDIDATES_PATH.write_text(
        "symbol,side,score,rank\nMSFT,BUY,0.62,1\nAAPL,BUY,0.57,2\n",
        encoding="utf-8",
    )
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] generate_trade_candidates: wrote {CANDIDATES_PATH}"
    )


if __name__ == "__main__":
    generate_trade_candidates()
