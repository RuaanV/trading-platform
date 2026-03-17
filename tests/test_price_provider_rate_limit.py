"""Tests for market-data rate limiting."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from data_pipeline import price_providers


class PriceProviderRateLimitTest(unittest.TestCase):
    def setUp(self) -> None:
        price_providers._RATE_LIMIT_WINDOWS.clear()

    def tearDown(self) -> None:
        price_providers._RATE_LIMIT_WINDOWS.clear()
        os.environ.pop("MARKET_DATA_RATE_LIMIT_PER_MINUTE", None)
        os.environ.pop("FINNHUB_RATE_LIMIT_PER_MINUTE", None)

    def test_specific_provider_limit_overrides_shared_limit(self) -> None:
        os.environ["MARKET_DATA_RATE_LIMIT_PER_MINUTE"] = "60"
        os.environ["FINNHUB_RATE_LIMIT_PER_MINUTE"] = "12"

        self.assertEqual(price_providers._provider_rate_limit("finnhub"), 12)
        self.assertEqual(price_providers._provider_rate_limit("massive"), 60)

    def test_wait_for_rate_limit_sleeps_when_window_is_full(self) -> None:
        os.environ["FINNHUB_RATE_LIMIT_PER_MINUTE"] = "2"
        monotonic_values = iter([0.0, 0.0, 0.0, 60.1])
        sleep_calls: list[float] = []

        def fake_monotonic() -> float:
            return next(monotonic_values)

        def fake_sleep(value: float) -> None:
            sleep_calls.append(value)

        with (
            patch("data_pipeline.price_providers.time.monotonic", side_effect=fake_monotonic),
            patch("data_pipeline.price_providers.time.sleep", side_effect=fake_sleep),
        ):
            price_providers._wait_for_rate_limit("finnhub")
            price_providers._wait_for_rate_limit("finnhub")
            price_providers._wait_for_rate_limit("finnhub")

        self.assertEqual(len(sleep_calls), 1)
        self.assertAlmostEqual(sleep_calls[0], 60.0, places=3)


if __name__ == "__main__":
    unittest.main()
