import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: tests that require live API connections (Bloomberg, Timescale, Oracle). "
        "Skip with: pytest -m 'not integration'",
    )
