import sys
import unittest
from unittest.mock import patch

from ingestion.cli import main


class CliTests(unittest.TestCase):
    def test_extract_parameters_reach_extractor(self):
        with patch.object(sys, "argv", ["cli", "extract", "--season", "2025", "--round", "1",
                                       "--session", "R", "--cutoff", "2025-03-18"]), \
             patch("ingestion.cli.extract_session", return_value="manifest.json") as extract:
            main()
        extract.assert_called_once_with(season=2025, round_number=1, session_type="R", cutoff="2025-03-18")
