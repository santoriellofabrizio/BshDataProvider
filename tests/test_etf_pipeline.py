from sfm_data_provider.analytics.pipeline import DataPipeline
import pytest

from sfm_data_provider.interface.bshdata import BshData


@pytest.fixture(scope="module")
def api():
    return BshData(cache=False, log_level="DEBUG")

@pytest.fixture
def sample_isins():
    """Two example ETFs for testing."""
    with open('tests\sample_isin.txt','r') as f:
        isins = f.readlines()
    return [i.replace('\n','') for i in isins]


def test_etf_pipeline(api, sample_isins):


    isins = sample_isins[:10]
    start = '2026-04-20'
    end = '2026-04-27'
    snapshot_time = '17:00:00'
    frequency = '1D'

    loader = DataPipeline(api, isins, start, end)

    loader.load_all()

    a = 0