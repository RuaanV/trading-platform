"""Unit tests for persisted dashboard theme definitions."""

from __future__ import annotations

import unittest

from data_pipeline import ui_themes


class UiThemesTest(unittest.TestCase):
    def test_seed_themes_include_base_and_all_seasons(self) -> None:
        theme_names = [theme["name"] for theme in ui_themes.get_seed_themes()]
        self.assertEqual(theme_names, ["base", "spring", "summer", "autumn", "winter"])

    def test_base_theme_preserves_existing_dashboard_core_palette(self) -> None:
        base_theme = ui_themes.get_seed_theme_map()["base"]
        tokens = base_theme["tokens"]

        self.assertEqual(tokens["primary"], "#C41E3A")
        self.assertEqual(tokens["primary_dk"], "#A01830")
        self.assertEqual(tokens["bg2"], "#F5F3EE")
        self.assertEqual(tokens["text"], "#1A1A1A")

    def test_all_seed_themes_share_the_same_token_shape(self) -> None:
        themes = ui_themes.get_seed_themes()
        expected_keys = set(themes[0]["tokens"])

        for theme in themes[1:]:
            self.assertEqual(set(theme["tokens"]), expected_keys, msg=f"Unexpected token set for {theme['name']}")


if __name__ == "__main__":
    unittest.main()
