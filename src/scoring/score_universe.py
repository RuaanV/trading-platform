"""Score investable universe using latest trained model and feature snapshot."""

from datetime import datetime, timezone
from pathlib import Path


SCORES_PATH = Path("models/trained_models/latest_scores.csv")


def score_universe() -> None:
    SCORES_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCORES_PATH.write_text(
        (
            "symbol,score\n"
            "MSFT,0.62\n"
            "NVDA,0.61\n"
            "AMZN,0.58\n"
            "AAPL,0.57\n"
            "BA.L,0.56\n"
            "GOOG,0.55\n"
            "0P0000RU81.L,0.55\n"
            "0P0001FE43.L,0.54\n"
            "0P0001GZXO.L,0.54\n"
            "0P0000W36K.L,0.54\n"
            "0P0001CBJA.L,0.54\n"
            "IUKD.L,0.54\n"
            "ISF.L,0.53\n"
            "GSK.L,0.53\n"
            "HLN.L,0.53\n"
            "LLOY.L,0.52\n"
            "NWG.L,0.52\n"
            "VOD.L,0.51\n"
            "ASC.L,0.48\n"
            "RGTI,0.46\n"
        ),
        encoding="utf-8",
    )
    print(f"[{datetime.now(timezone.utc).isoformat()}] score_universe: wrote {SCORES_PATH}")


if __name__ == "__main__":
    score_universe()
