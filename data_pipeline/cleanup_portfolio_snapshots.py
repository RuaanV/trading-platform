"""Remove intermediate portfolio snapshots while keeping the seed and latest run."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text

from personal_portfolios import ensure_personal_portfolio_tables

try:
    from .db import postgres_engine
except ImportError:
    from db import postgres_engine


PORTFOLIO_NAME = "SIPP"
PORTFOLIO_HOLDER = "Ruaan Venter"
PORTFOLIO_TYPE = "SIPP"
SEED_SNAPSHOT_AT = datetime.fromisoformat("2026-03-08T15:32:00")


def cleanup_sipp_snapshots() -> tuple[list[int], list[int]]:
    ensure_personal_portfolio_tables()
    engine = postgres_engine()

    with engine.begin() as conn:
        snapshot_rows = conn.execute(
            text(
                """
                select
                    s.id,
                    s.snapshot_at
                from app.portfolio_snapshots s
                join app.personal_portfolios p
                  on p.id = s.portfolio_id
                where p.name = :name
                  and p.holder = :holder
                  and p.portfolio_type = :portfolio_type
                order by s.snapshot_at asc, s.id asc
                """
            ),
            {
                "name": PORTFOLIO_NAME,
                "holder": PORTFOLIO_HOLDER,
                "portfolio_type": PORTFOLIO_TYPE,
            },
        ).mappings().all()

        if not snapshot_rows:
            return [], []

        seed_id = None
        latest_id = int(snapshot_rows[-1]["id"])

        for row in snapshot_rows:
            snapshot_at = row["snapshot_at"]
            if snapshot_at is not None and snapshot_at.replace(tzinfo=None) == SEED_SNAPSHOT_AT:
                seed_id = int(row["id"])
                break

        keep_ids = [latest_id]
        if seed_id is not None and seed_id != latest_id:
            keep_ids.insert(0, seed_id)

        delete_ids = [int(row["id"]) for row in snapshot_rows if int(row["id"]) not in keep_ids]

        if delete_ids:
            conn.execute(
                text("delete from app.portfolio_snapshots where id = any(:snapshot_ids)"),
                {"snapshot_ids": delete_ids},
            )

    return keep_ids, delete_ids


def main() -> None:
    keep_ids, delete_ids = cleanup_sipp_snapshots()
    print(f"Kept snapshot ids: {keep_ids}")
    print(f"Deleted snapshot ids: {delete_ids}")


if __name__ == "__main__":
    main()
