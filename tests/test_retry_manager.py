"""
Test per retry con fallbacks (versione semplificata).
"""
from datetime import time, timedelta

import pytest
from dateutil.utils import today

from sfm_data_provider.interface.bshdata import BshData


@pytest.fixture
def sample_isins():
    """Two example ETFs for testing."""
    with open('tests\sample_isin.txt', 'r') as f:
        isins = f.readlines()
    return [i.replace('\n', '') for i in isins]


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
    result = api.info.get_ter(ticker=["XBRMIB", "IHYU"], source="oracle", fallbacks=[{"source": "bloomberg"}])


def test_retry_etfs(api, sample_isins):
    sample_isins = ['DE000A0Q4R85',
                    'IE00B0M63516',
                    'LU0292109344',
                    "IE0002OP0LA0",
                    'LU1900066207',
                    'IE00B02KXK85',
                    'IE00BM8QS095',
                    'LU2265794276',
                    'LU2265794946',
                    'LU2376679564',
                    'IE000K9Z3SF5',
                    'IE00BF4NQ904',
                    'LU0779800910',
                    'LU0875160326',
                    'IE00099GAJC6',
                    'LU1900067940',
                    'LU1900068914',
                    'IE00BHZRR147',
                    'IE0007P4PBU1',
                    'IE00B44T3H88',
                    'IE00BK80XL30',
                    'LU0514695690',
                    'LU1841731745',
                    'LU1953188833',
                    'LU2314312849',
                    'LU2456436083',
                    'LU0292109856']

    results = api.market.get_daily_etf(
        id=sample_isins, start="2026-04-08",
        snapshot_time=time(10, 45), timeout=10,
        fallbacks=[{"source": "bloomberg", "market": mkt} for mkt in ["IM", "FP", "NA"]],
    )

    a = 0
