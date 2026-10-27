#!/usr/bin/env python3
"""Compatibility entry point for the isolated BigQuery/GHArchive pipeline."""

import sys
from pathlib import Path


PIPELINE_ROOT = Path(__file__).resolve().parents[1] / "pipelines" / "bigquery_gharchive_p4"
sys.path.insert(0, str(PIPELINE_ROOT))

from collector import *  # noqa: E402,F401,F403


if __name__ == "__main__":
    sys.exit(main())
