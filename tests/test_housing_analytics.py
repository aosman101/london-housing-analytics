import importlib.util
import unittest
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "housing-analytics" / "build_analytics.py"
spec = importlib.util.spec_from_file_location("build_analytics", MODULE_PATH)
build_analytics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build_analytics)


def ashe_fixture() -> pd.DataFrame:
    return pd.DataFrame({
        "reference_year": [2025, 2025],
        "area_name": ["Camden", "London"],
        "area_code": ["E09000007", build_analytics.LONDON_REGION],
        "median_gross_annual_pay": [53133.0, 46414.0],
        "earnings_yoy_pct": [19.1, 3.6],
        "median_pay_cv": [9.4, 1.2],
        "earnings_quality": ["Reasonably precise", "Precise"],
    })


class AttachEarningsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.months = pd.DataFrame({
            "date_month": pd.to_datetime(["1995-01-01", "2024-12-01", "2025-06-01", "2026-02-01", "2027-01-01"]),
            "area_code": ["E09000007"] * 5,
        })

    def test_earnings_only_attached_within_ashe_window(self) -> None:
        out = build_analytics.attach_earnings(self.months, ashe_fixture())
        pay = dict(zip(out.date_month.dt.year, out.median_gross_annual_pay, strict=True))
        self.assertTrue(pd.isna(pay[1995]), "1995 must not be divided by 2025 pay")
        self.assertTrue(pd.isna(pay[2024]), "months before the reference year get no earnings")
        self.assertEqual(pay[2025], 53133.0)
        self.assertEqual(pay[2026], 53133.0)
        self.assertTrue(pd.isna(pay[2027]), "more than one year after the reference year is out of window")

    def test_suppressed_borough_falls_back_to_london(self) -> None:
        city = pd.DataFrame({"date_month": pd.to_datetime(["2026-02-01"]), "area_code": ["E09000001"]})
        out = build_analytics.attach_earnings(city, ashe_fixture()).iloc[0]
        self.assertEqual(out.median_gross_annual_pay, 46414.0)
        self.assertTrue(out.earnings_fallback_used)
        self.assertEqual(out.earnings_quality, "Precise")

    def test_row_order_and_count_preserved(self) -> None:
        shuffled = self.months.sample(frac=1, random_state=1)
        out = build_analytics.attach_earnings(shuffled, ashe_fixture())
        self.assertEqual(len(out), len(shuffled))
        self.assertListEqual(list(out.date_month), list(shuffled.date_month))


class JsonableTests(unittest.TestCase):
    def test_nan_becomes_null_and_floats_round(self) -> None:
        self.assertEqual(build_analytics.jsonable({"a": float("nan"), "b": 1.23456}), {"a": None, "b": 1.23})


if __name__ == "__main__":
    unittest.main()
