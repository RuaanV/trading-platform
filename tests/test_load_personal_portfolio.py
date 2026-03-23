from pathlib import Path
import sys
from unittest import TestCase

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "data_pipeline"))

from data_pipeline.load_personal_portfolio import _resolve_symbol_from_apis


class LoadPersonalPortfolioTests(TestCase):
    def test_fundsmith_uses_manual_override(self) -> None:
        resolved = _resolve_symbol_from_apis(
            "Fundsmith Equity",
            "Class I - Accumulation (GBP)",
        )

        self.assertEqual(
            resolved,
            {
                "symbol": "0P0000RU81.L",
                "provider": "yahoo",
                "name": "Fundsmith Equity I Acc",
            },
        )
