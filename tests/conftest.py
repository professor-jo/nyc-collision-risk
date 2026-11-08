"""Shared pytest setup.

The ingestion modules read BRONZE_BUCKET when they are imported, so it must
exist before pytest collects any test file. Tests mock S3, so the value is
never used to reach AWS. setdefault keeps a value you export yourself.
"""

import os

os.environ.setdefault("BRONZE_BUCKET", "test-bronze-bucket")
