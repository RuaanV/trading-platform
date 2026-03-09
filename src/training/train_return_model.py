"""Train return prediction model and persist artifact to models/trained_models."""

from datetime import datetime, timezone
from pathlib import Path


MODEL_PATH = Path("models/trained_models/return_model.txt")


def train_return_model() -> None:
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    MODEL_PATH.write_text("stub-model-artifact\n", encoding="utf-8")
    print(f"[{datetime.now(timezone.utc).isoformat()}] train_return_model: wrote {MODEL_PATH}")


if __name__ == "__main__":
    train_return_model()
