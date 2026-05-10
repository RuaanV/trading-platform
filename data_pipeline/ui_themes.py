"""Persisted UI theme definitions for the Streamlit dashboard."""

from __future__ import annotations

import json
from copy import deepcopy

from sqlalchemy import text

try:
    from .db import postgres_engine
except ImportError:
    from db import postgres_engine


THEME_SCHEMA = "app"
THEME_TABLE = "ui_themes"
THEME_SETTINGS_TABLE = "ui_theme_settings"
ACTIVE_THEME_SETTING_KEY = "active_dashboard_theme"

_SEED_THEMES: tuple[dict[str, object], ...] = (
    {
        "name": "base",
        "label": "Default",
        "description": "Current Trading Platform palette.",
        "tokens": {
            "bg": "#FFFFFF",
            "bg2": "#F5F3EE",
            "bg_soft": "#FBF9F6",
            "card": "#FFFFFF",
            "border": "#E2DDD5",
            "primary": "#C41E3A",
            "primary_dk": "#A01830",
            "accent": "#B08A5C",
            "text": "#1A1A1A",
            "text_muted": "#7A7269",
            "up": "#2D7A50",
            "down": "#C41E3A",
            "flat": "#B0A99E",
            "link": "#C41E3A",
            "banner_bg": "#F2EBE3",
            "banner_border": "#E4D8CC",
            "banner_top": "#C4573A",
            "badge_bg": "#F8F2EC",
            "badge_border": "#DDB9AC",
            "badge_text": "#C4573A",
            "badge_muted": "#9C8D83",
            "table_header_bg": "#F5F3EE",
            "table_row_hover": "#FBF9F6",
            "calendar_header_bg": "#EDE9E2",
            "calendar_empty_bg": "#FAF8F4",
            "event_border": "#8C0F22",
            "tooltip_shadow": "rgba(26, 26, 26, 0.14)",
        },
    },
    {
        "name": "spring",
        "label": "Spring",
        "description": "Forest, lime, daffodil, and rose accents.",
        "tokens": {
            "bg": "#FFFCF7",
            "bg2": "#E0EEDB",
            "bg_soft": "#EEF4EA",
            "card": "#FFFFFF",
            "border": "#C6D7BF",
            "primary": "#256238",
            "primary_dk": "#722E6F",
            "accent": "#E29A2D",
            "text": "#1A1A1A",
            "text_muted": "#667161",
            "up": "#70B472",
            "down": "#F07D72",
            "flat": "#B8C1B0",
            "link": "#256238",
            "banner_bg": "#EEF5EA",
            "banner_border": "#C6D7BF",
            "banner_top": "#E29A2D",
            "badge_bg": "#F8FBF6",
            "badge_border": "#C6D7BF",
            "badge_text": "#256238",
            "badge_muted": "#6F7A69",
            "table_header_bg": "#DCE9D5",
            "table_row_hover": "#F6FAF3",
            "calendar_header_bg": "#D4E4CD",
            "calendar_empty_bg": "#F2F7EF",
            "event_border": "#1C4B2A",
            "tooltip_shadow": "rgba(37, 98, 56, 0.18)",
        },
    },
    {
        "name": "summer",
        "label": "Summer",
        "description": "Rain blue, lemon, strawberry, and sky tones.",
        "tokens": {
            "bg": "#FFFDFC",
            "bg2": "#FFF0C7",
            "bg_soft": "#EEF1F6",
            "card": "#FFFFFF",
            "border": "#D6DBE7",
            "primary": "#1D418E",
            "primary_dk": "#843922",
            "accent": "#FFD032",
            "text": "#1A1A1A",
            "text_muted": "#687289",
            "up": "#4A9857",
            "down": "#E8472F",
            "flat": "#AAB4C8",
            "link": "#1D418E",
            "banner_bg": "#F3F4FA",
            "banner_border": "#D6DBE7",
            "banner_top": "#FFD032",
            "badge_bg": "#FBFBFE",
            "badge_border": "#D6DBE7",
            "badge_text": "#1D418E",
            "badge_muted": "#7A86A2",
            "table_header_bg": "#E7ECF6",
            "table_row_hover": "#F7F9FD",
            "calendar_header_bg": "#DCE5F2",
            "calendar_empty_bg": "#F6F8FC",
            "event_border": "#162F67",
            "tooltip_shadow": "rgba(29, 65, 142, 0.18)",
        },
    },
    {
        "name": "autumn",
        "label": "Autumn",
        "description": "Brick, postbox, ash, and orange tones.",
        "tokens": {
            "bg": "#FFF9F5",
            "bg2": "#FCD9CA",
            "bg_soft": "#F2E8E6",
            "card": "#FFFFFF",
            "border": "#E9C9BF",
            "primary": "#69222E",
            "primary_dk": "#722E6F",
            "accent": "#E8472F",
            "text": "#1A1A1A",
            "text_muted": "#7E655E",
            "up": "#4A9857",
            "down": "#E8472F",
            "flat": "#C8B3AB",
            "link": "#69222E",
            "banner_bg": "#F8ECE6",
            "banner_border": "#E7C9BD",
            "banner_top": "#843922",
            "badge_bg": "#FCF6F2",
            "badge_border": "#E7C9BD",
            "badge_text": "#69222E",
            "badge_muted": "#9B7C74",
            "table_header_bg": "#F3DDD2",
            "table_row_hover": "#FEF5F0",
            "calendar_header_bg": "#EFD5CB",
            "calendar_empty_bg": "#FBF1EC",
            "event_border": "#4D1720",
            "tooltip_shadow": "rgba(105, 34, 46, 0.18)",
        },
    },
    {
        "name": "winter",
        "label": "Winter",
        "description": "Rain blue, brick, forest, and blueberry tones.",
        "tokens": {
            "bg": "#FCFEFF",
            "bg2": "#D5ECF4",
            "bg_soft": "#F2E8E6",
            "card": "#FFFFFF",
            "border": "#C7DCE4",
            "primary": "#1D418E",
            "primary_dk": "#69222E",
            "accent": "#256238",
            "text": "#1A1A1A",
            "text_muted": "#67727C",
            "up": "#70B472",
            "down": "#69222E",
            "flat": "#AAB7BE",
            "link": "#1D418E",
            "banner_bg": "#EEF6FA",
            "banner_border": "#C7DCE4",
            "banner_top": "#256238",
            "badge_bg": "#F8FCFE",
            "badge_border": "#C7DCE4",
            "badge_text": "#1D418E",
            "badge_muted": "#72808D",
            "table_header_bg": "#E1EFF4",
            "table_row_hover": "#F6FBFD",
            "calendar_header_bg": "#D9E9EF",
            "calendar_empty_bg": "#F4F9FB",
            "event_border": "#162F67",
            "tooltip_shadow": "rgba(29, 65, 142, 0.18)",
        },
    },
)

DEFAULT_THEME_NAME = "base"


def get_seed_themes() -> list[dict[str, object]]:
    """Return a copy of the seeded dashboard themes."""
    return deepcopy(list(_SEED_THEMES))


def get_seed_theme_map() -> dict[str, dict[str, object]]:
    """Return seeded themes keyed by their internal name."""
    return {theme["name"]: theme for theme in get_seed_themes()}


def ensure_ui_theme_tables() -> None:
    """Create and seed the persisted theme catalog."""
    engine = postgres_engine()
    create_sql = f"""
    create schema if not exists {THEME_SCHEMA};

    create table if not exists {THEME_SCHEMA}.{THEME_TABLE} (
        name text primary key,
        label text not null,
        description text,
        tokens jsonb not null,
        created_at timestamptz not null default now(),
        updated_at timestamptz not null default now()
    );

    create table if not exists {THEME_SCHEMA}.{THEME_SETTINGS_TABLE} (
        setting_key text primary key,
        active_theme_name text not null references {THEME_SCHEMA}.{THEME_TABLE}(name),
        updated_at timestamptz not null default now()
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))
        for theme in get_seed_themes():
            conn.execute(
                text(
                    f"""
                    insert into {THEME_SCHEMA}.{THEME_TABLE} (
                        name,
                        label,
                        description,
                        tokens
                    ) values (
                        :name,
                        :label,
                        :description,
                        cast(:tokens as jsonb)
                    )
                    on conflict (name) do update
                    set
                        label = excluded.label,
                        description = excluded.description,
                        tokens = excluded.tokens,
                        updated_at = now();
                    """
                ),
                {
                    "name": str(theme["name"]),
                    "label": str(theme["label"]),
                    "description": str(theme["description"]),
                    "tokens": json.dumps(theme["tokens"], sort_keys=True),
                },
            )

        conn.execute(
            text(
                f"""
                insert into {THEME_SCHEMA}.{THEME_SETTINGS_TABLE} (
                    setting_key,
                    active_theme_name
                ) values (
                    :setting_key,
                    :active_theme_name
                )
                on conflict (setting_key) do nothing;
                """
            ),
            {
                "setting_key": ACTIVE_THEME_SETTING_KEY,
                "active_theme_name": DEFAULT_THEME_NAME,
            },
        )
        conn.execute(
            text(
                f"""
                update {THEME_SCHEMA}.{THEME_SETTINGS_TABLE}
                set
                    active_theme_name = :active_theme_name,
                    updated_at = now()
                where setting_key = :setting_key
                  and active_theme_name not in (
                      select name from {THEME_SCHEMA}.{THEME_TABLE}
                  );
                """
            ),
            {
                "setting_key": ACTIVE_THEME_SETTING_KEY,
                "active_theme_name": DEFAULT_THEME_NAME,
            },
        )


def fetch_ui_themes() -> list[dict[str, object]]:
    """Load persisted themes in selector order."""
    engine = postgres_engine()
    query = f"""
    select
        name,
        label,
        description,
        tokens
    from {THEME_SCHEMA}.{THEME_TABLE}
    order by
        case name
            when 'base' then 0
            when 'spring' then 1
            when 'summer' then 2
            when 'autumn' then 3
            when 'winter' then 4
            else 99
        end,
        label;
    """
    with engine.begin() as conn:
        rows = conn.execute(text(query)).mappings().all()

    return [
        {
            "name": row["name"],
            "label": row["label"],
            "description": row["description"],
            "tokens": dict(row["tokens"]) if isinstance(row["tokens"], dict) else json.loads(row["tokens"]),
        }
        for row in rows
    ]


def fetch_active_ui_theme_name() -> str:
    """Return the persisted active theme name."""
    engine = postgres_engine()
    query = f"""
    select active_theme_name
    from {THEME_SCHEMA}.{THEME_SETTINGS_TABLE}
    where setting_key = :setting_key
    """
    with engine.begin() as conn:
        active_theme_name = conn.execute(
            text(query),
            {"setting_key": ACTIVE_THEME_SETTING_KEY},
        ).scalar_one_or_none()

    return str(active_theme_name or DEFAULT_THEME_NAME)


def load_active_ui_theme() -> dict[str, object]:
    """Return the active theme record, falling back to the seeded base theme."""
    theme_map = {theme["name"]: theme for theme in fetch_ui_themes()}
    active_theme_name = fetch_active_ui_theme_name()
    return deepcopy(theme_map.get(active_theme_name, get_seed_theme_map()[DEFAULT_THEME_NAME]))


def save_active_ui_theme(theme_name: str) -> None:
    """Persist the selected dashboard theme."""
    valid_theme_names = {theme["name"] for theme in fetch_ui_themes()}
    if theme_name not in valid_theme_names:
        raise ValueError(f"Unknown theme: {theme_name}")

    engine = postgres_engine()
    query = f"""
    insert into {THEME_SCHEMA}.{THEME_SETTINGS_TABLE} (
        setting_key,
        active_theme_name
    ) values (
        :setting_key,
        :active_theme_name
    )
    on conflict (setting_key) do update
    set
        active_theme_name = excluded.active_theme_name,
        updated_at = now();
    """
    with engine.begin() as conn:
        conn.execute(
            text(query),
            {
                "setting_key": ACTIVE_THEME_SETTING_KEY,
                "active_theme_name": theme_name,
            },
        )
