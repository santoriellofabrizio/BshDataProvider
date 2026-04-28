from datetime import time

import pytest
from dateutil.tz import enfold
from matplotlib import pyplot as plt

from sfm_data_provider.analytics.adjustments import Adjuster, TerComponent, FxSpotComponent, FxForwardCarryComponent, \
    DividendComponent, YtmComponent, RepoComponent
from sfm_data_provider.analytics.adjustments.outlier import OutlierDetector
from sfm_data_provider.analytics.pipeline import DataPipeline
from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.interface.bshdata import BshData
from tests.test_adjuster_performance import instruments


@pytest.fixture(scope="module")
def api():
    return BshData(cache=False, log_level="DEBUG")

@pytest.fixture
def sample_isins():
    """Two example ETFs for testing."""
    with open('tests\sample_isin.txt','r') as f:
        isins = f.readlines()
    return [i.replace('\n','') for i in isins]

def full_adjustments_pipeline(api):

    ids = ["C50", "VGA INDEX"]
    start = '2026-02-20'
    end = '2026-04-27'
    snapshot_time = time(17)
    frequency = '1D'

    data = DataPipeline(api, ids, start, end, frequency, snapshot_time)
    data.load_all()

    instruments = data.get_instruments()

    adj = (((Adjuster(
            data.prices, instruments)
           .add(TerComponent(data.ter)))
           .add(FxSpotComponent(data.fx_composition, data.fx_prices)))
           .add(DividendComponent(data.dividends, data.prices, data.fx_prices)))

    if (ytm := data.ytm) is not None:
        adj = adj.add(YtmComponent(ytm))

    if futures := [i for i in instruments.values() if i.type == InstrumentType.FUTURE]:
        adj.add(RepoComponent(data.repo, 'currency', {f.id: f.currency.value for f in futures}))

    debug = adj.get_breakdown()

    for name, val in debug.items():
        val.plot(kind='bar', title=name)
        plt.show()

    clean_returns = adj.get_clean_returns()

    rebase = ((1+clean_returns).cumprod(axis=1) - 1)

    clean_returns.plot(kind='bar', title=f"Clean Simple Returns")

    plt.show()

if __name__ == "__main__":
    api = BshData(config_path=r'C:\AFMachineLearning\Libraries\SFMDataProvider\config\bshdata_config.yaml',
                  cache=False, log_level="DEBUG")
    full_adjustments_pipeline(api)