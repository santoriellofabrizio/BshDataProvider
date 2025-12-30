import datetime as dt
import time
import pytest

from interface.bshdata import BshData


@pytest.fixture(scope="module")
def api():
    """Shared BSH API interface."""
    return BshData(config_path="config/bshdata_config.yaml", log_level="ERROR")


@pytest.fixture
def sample_isins():
    """Two example ETFs for testing."""
    return ["LU0292107645", "LU1681045370"]


# ============================================================
# UNIT TESTS
# ============================================================

def test_timescale_intraday_best_sampled(api, sample_isins):
    """Single intraday ETF request (1m best sampled)."""
    res = api.market.get_intraday_etf(
        isin=sample_isins[0],
        date="2025-12-01",
        frequency="1m",
        fields="mid",
        market="EURONEXT",
        source="timescale",
    )
    print(res.head())
    assert res is not None
    assert not res.empty, "Expected non-empty intraday DataFrame"

def test_timescale_multi_intraday_best_sampled(api, sample_isins):
    """Single intraday ETF request (1m best sampled)."""
    res = api.market.get_intraday_etf(
        isin=sample_isins[:10],
        date="2025-10-01",
        frequency="1m",
        fields="mid",
        market="EURONEXT",
        source="timescale",
    )
    print(res.head())
    assert res is not None
    assert not res.empty, "Expected non-empty intraday DataFrame"


def test_timescale_daily_fairvalue(api, sample_isins):
    """Single daily ETF fairvalue snapshot."""
    res = api.market.get_daily_etf(
        isin=sample_isins[0],
        start="2025-10-01",
        fields="mid",
        market="ETFP",
        source="timescale",
        snapshot_time=dt.time(11, 0),
    )
    print(res.head())
    assert res is not None
    assert not res.empty, "Expected non-empty daily fairvalue series"


def test_timescale_batch_fairvalue(api, sample_isins):
    """Batch (multi-instrument) daily ETF fairvalues."""
    res = api.market.get_daily_etf(
        isin=sample_isins,
        start=dt.datetime(2025, 9, 22),
        end=dt.datetime(2025, 9, 26),
        fields="mid",
        market="EURONEXT",
        source="timescale",
        snapshot_time=dt.time(11, 0),
    )
    df = res
    print(res.head())
    assert not df.empty, "Expected non-empty batch DataFrame"
    assert df.shape[1] == len(sample_isins), "Each instrument should produce one column"


def test_timescale_batch_intraday(api, sample_isins):
    """Batch (multi-instrument) intraday ETF data."""
    res = api.market.get_intraday_etf(
        isin=sample_isins,
        date=dt.datetime(2025, 9, 25),
        fields="mid",
        market="EURONEXT",
        frequency="1m",
        source="timescale",
    )
    df = res
    print(res.head())
    assert not df.empty, "Expected non-empty intraday batch DataFrame"


# ============================================================
# PERFORMANCE TESTS
# ============================================================

@pytest.mark.performance
def test_timescale_daily_fairvalue_batch_perf(api):
    """Performance test: batch 100+ ETF fairvalues."""
    isin_list = [
        'LU0292109856', 'IE00B02KXK85', 'IE00BM8QS095', 'LU2265794276', 'LU2376679564',
        'LU2265794946', 'LU0779800910', 'LU0875160326', 'IE00BF4NQ904', 'IE00099GAJC6',
        'IE000K9Z3SF5', 'LU1841731745', 'IE00B44T3H88', 'IE0007P4PBU1', 'LU0514695690',
        'LU2456436083', 'LU1900068914', 'LU1900067940', 'LU2314312849', 'LU1953188833',
        'IE00BHZRR147', 'IE00BK80XL30', 'FR0011720911', 'IE00BKFB6K94', 'LU2469465822',
        'FR0011660927', 'IE00B441G979', 'IE00BK5BQV03', 'IE00BKX55T58', 'LU1681043599'
    ]

    t0 = time.time()
    res = api.market.get_daily_etf(
        isin=isin_list,
        start=dt.datetime(2025, 9, 22),
        end=dt.datetime(2025, 9, 26),
        fields="mid",
        market="EURONEXT",
        source="timescale",
        snapshot_time=dt.time(11, 0),
    )
    elapsed = time.time() - t0
    df = res
    print(res.head())

    print(f"\n✅ Batch fetched {len(isin_list)} ISINs in {elapsed:.2f}s ({df.shape[1]} columns)")
    assert elapsed < 10, f"Batch too slow: {elapsed:.2f}s"
    assert not df.empty


@pytest.mark.performance
def test_timescale_daily_fairvalue_single_perf(api):
    """Sequential single ETF requests performance."""
    isin_list = [
        'LU0292109856', 'IE00B02KXK85', 'IE00BM8QS095', 'LU2265794276', 'LU2376679564', "pippo"
    ]

    t0 = time.time()
    results = [
        api.market.get_daily_etf(
            isin=isin,
            start=dt.datetime(2025, 9, 22),
            end=dt.datetime(2025, 12, 26),
            fields="mid",
            market="EURONEXT",
            source="timescale",
            snapshot_time=dt.time(11, 0),
        )
        for isin in isin_list
    ]
    elapsed = time.time() - t0

    print(f"\n✅ Fetched {len(isin_list)} singles in {elapsed:.2f}s")
    assert elapsed < 30, f"Single requests too slow: {elapsed:.2f}s"



@pytest.mark.performance
def test_timescale_daily_currency_perf(api):
    """Daily FX_COMPOSITION currency pairs."""
    fx_pairs = ["EURUSD", "EURGBP", "EURJPY"]

    t0 = time.time()
    res = api.market.get_daily_currency(
        id=fx_pairs,
        start=dt.datetime(2025, 9, 22),
        end=dt.datetime(2025, 9, 26),
        fields="mid",
        source="timescale",
        snapshot_time=dt.time(11, 0),
    )
    elapsed = time.time() - t0

    print(f"\n✅ [CURRENCY DAILY] {len(fx_pairs)} pairs in {elapsed:.2f}s")
    print(res.head())
    assert elapsed < 30
    assert not res.empty


@pytest.mark.performance
def test_timescale_currency_batch_perf(api):
    """Batch FX_COMPOSITION currency pairs."""
    fx_pairs = ["EURUSD", "EURGBP", "EURJPY", "USDCAD"]

    t0 = time.time()
    res = api.market.get_daily_currency(
        id=fx_pairs,
        start=dt.datetime(2025, 9, 22),
        end=dt.datetime(2025, 9, 26),
        fields="mid",
        snapshot_time=dt.time(11, 0),
    )
    elapsed = time.time() - t0
    print(res.head())

    print(f"\n✅ [CURRENCY BATCH] {len(fx_pairs)} pairs in {elapsed:.2f}s")
    assert elapsed < 30
    assert not res.empty
