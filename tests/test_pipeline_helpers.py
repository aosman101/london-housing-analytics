import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.common import db
from src.load import load_to_postgres
from src.transform.normalise_sources import clean_col, normalise_property_type_label


class PipelineHelperTests(unittest.TestCase):
    def test_database_url_preserves_special_characters(self) -> None:
        env = {
            "PGUSER": "analytics",
            "PGPASSWORD": "p@ss:word/with symbols",
            "PGHOST": "localhost",
            "PGPORT": "5432",
            "PGDATABASE": "housing_warehouse",
        }
        with patch.dict(os.environ, env, clear=True):
            url = db.database_url()

        self.assertEqual(url.username, "analytics")
        self.assertEqual(url.password, "p@ss:word/with symbols")
        self.assertEqual(url.host, "localhost")
        self.assertEqual(url.port, 5432)
        self.assertEqual(url.database, "housing_warehouse")

    def test_load_refuses_partial_normalised_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.object(load_to_postgres, "NORM", Path(tmp_dir)):
                with self.assertRaisesRegex(FileNotFoundError, "Missing normalised files"):
                    load_to_postgres.ensure_inputs_exist()

    def test_property_type_labels_are_canonicalised(self) -> None:
        self.assertEqual(normalise_property_type_label(" F "), "Flat/Maisonette")
        self.assertEqual(normalise_property_type_label("Flat or maisonette"), "Flat/Maisonette")
        self.assertEqual(normalise_property_type_label("Detached"), "Detached")

    def test_clean_col_normalises_spreadsheet_headers(self) -> None:
        self.assertEqual(clean_col(" Rental Price\r\n "), "rental price")


if __name__ == "__main__":
    unittest.main()
