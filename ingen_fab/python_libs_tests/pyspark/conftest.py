"""Every test in this tree starts a local Spark session, which needs a JVM."""

import pytest


def pytest_collection_modifyitems(items):
    for item in items:
        item.add_marker(pytest.mark.spark)
