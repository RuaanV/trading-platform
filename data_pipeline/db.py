"""Shared Postgres connection helpers for local scripts."""

from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

try:
    from .env import load_local_env
except ImportError:
    from env import load_local_env

load_local_env()


def postgres_url() -> str:
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "trading_platform")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"


def postgres_engine() -> Engine:
    return create_engine(postgres_url())
