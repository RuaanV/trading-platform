"""Build feature tables from staged market and fundamentals data."""

from datetime import datetime, timezone


def build_features() -> None:
    # TODO: Pull from warehouse and write feature table snapshots.
    print(f"[{datetime.now(timezone.utc).isoformat()}] build_features: stub run complete")


if __name__ == "__main__":
    build_features()
