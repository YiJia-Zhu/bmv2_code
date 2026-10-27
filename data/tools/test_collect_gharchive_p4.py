#!/usr/bin/env python3
"""Compatibility test entry point for the isolated B pipeline."""

import sys
import unittest
from pathlib import Path


TEST_ROOT = (
    Path(__file__).resolve().parents[1]
    / "pipelines"
    / "bigquery_gharchive_p4"
    / "tests"
)
sys.path.insert(0, str(TEST_ROOT))

from test_collector import *  # noqa: E402,F401,F403


if __name__ == "__main__":
    unittest.main()
