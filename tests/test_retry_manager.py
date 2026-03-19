"""
Test per retry con fallbacks (versione semplificata).
"""

import pytest
from interface.bshdata import BshData


@pytest.fixture(scope="module")
def api():
    return BshData(cache=False, log_level="DEBUG")


def test_retry_with_fallbacks(api):
    """Testa retry con fallbacks su source diversi."""
    result = api.market.get()

    print(f"\nResult: {result}")
    assert result is not None


def test_retry_multiple_fields(api):
    """Testa retry quando alcuni field mancano."""
    result = api.market.get()

    print(f"\nResult: {result}")
    assert result is not None



def test_info_with_fallbacks(api):
    """Testa fallbacks su InfoDataAPI."""
    result = api.info.get()

    print(f"\nResult: {result}")
    assert result is not None


def test_ter_with_fallbacks(api):

    result = api.info.get_ter(ticker=["XBRMIB","IHYU"], source="oracle", fallbacks=[{"source": "bloomberg"}])