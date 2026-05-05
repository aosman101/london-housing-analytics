import unittest

from src.common import config


class ConfigTests(unittest.TestCase):
    def test_raw_filename_renders_hpi_vintage_dash_context(self) -> None:
        self.assertEqual(
            config.render("Average-prices-{hpi_vintage_dash}.csv", {"hpi_vintage": "2026_01"}),
            "Average-prices-2026-01.csv",
        )

    def test_config_sources_have_required_templates(self) -> None:
        cfg = config.load()
        expected_sources = {
            "hpi_average_prices",
            "hpi_property_type_prices",
            "hpi_sales",
            "pipr_monthly_price_statistics",
            "ashe_table8",
        }

        self.assertEqual(set(cfg["sources"]), expected_sources)
        for source_key in expected_sources:
            spec = config.source_spec(source_key)
            self.assertIn("filename_template", spec)
            self.assertIn("url_template", spec)
            self.assertTrue(config.raw_filename(source_key))


if __name__ == "__main__":
    unittest.main()
