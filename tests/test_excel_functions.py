# tests/test_bsh_xlwings_udfs_integration.py

import os
import pytest
import pandas as pd
import bsh_addin.bsh_udf as udfs

# =====================================================================
# Config test: strumenti "reali" (override via variabili ambiente)
# =====================================================================

TEST_ETF_ISIN = os.getenv("TEST_ETF_ISIN", "IE00B4L5Y983")    # iShares Core MSCI World (esempio)
TEST_FX_PAIR = os.getenv("TEST_FX_PAIR", "eurusd")
TEST_FUTURE_TICKER = os.getenv("TEST_FUTURE_TICKER", "VG")

TEST_START_DATE = os.getenv("TEST_START_DATE", "2025-10-01")
TEST_END_DATE = os.getenv("TEST_END_DATE", "2025-10-10")


# =====================================================================
# Helper
# =====================================================================

def _non_empty(res):
    """Controllo soft sul risultato, indipendente dal formato."""
    if res is None:
        return False
    if isinstance(res, (pd.DataFrame, pd.Series)):
        return not res.empty
    if isinstance(res, (list, tuple, dict)):
        return len(res) > 0
    return True


# =====================================================================
# Test base: infrastruttura / helpers
# =====================================================================

@pytest.mark.unit
def test_get_api_singleton():
    a1 = udfs.get_api()
    a2 = udfs.get_api()
    print("\nget_api_singleton:", a1, a2)
    assert a1 is a2, "get_api() deve restituire sempre la stessa istanza (singleton BshData)"


@pytest.mark.unit
def test_split_ids_isin_ticker_mixed():
    vals, isins, tickers = udfs._split_ids_isin_ticker(
        ["IE00B4L5Y983", "SPY", "LU0000000001"]
    )
    print("\nsplit_ids_isin_ticker_mixed:", vals, isins, tickers)
    assert vals == ["IE00B4L5Y983", "SPY", "LU0000000001"]
    assert isins == ["IE00B4L5Y983", None, "LU0000000001"]
    assert tickers == [None, "SPY", None]


@pytest.mark.integration
def test_intraday_etf():
    etfs = udfs.get_intraday_etf(["CIT", "CITE"], "2025-11-14", source="bloomberg")

    assert not etfs.empty

# =====================================================================
# InfoData UDF (Excel-style)
# =====================================================================

@pytest.mark.integration
def test_udf_get_nav_real():
    res = udfs.get_nav(TEST_ETF_ISIN, TEST_START_DATE, TEST_END_DATE)
    if not _non_empty(res):
        pytest.skip("Nessun NAV disponibile per l'ISIN di test nel periodo indicato.")
    print("\nudf_get_nav_real:", type(res), res)
    assert _non_empty(res)


@pytest.mark.integration
def test_udf_get_dividends_real():
    res = udfs.get_dividends(TEST_ETF_ISIN, TEST_START_DATE, TEST_END_DATE, source="oracle")
    if not _non_empty(res):
        pytest.skip("Nessun dividendo disponibile per l'ISIN di test.")
    print("\nudf_get_dividends_real:", type(res), res)
    assert _non_empty(res)


@pytest.mark.integration
def test_udf_get_etp_fields_real():
    res = udfs.get_etp_fields(TEST_ETF_ISIN, ["TER", "FUND_CURRENCY"])
    if not _non_empty(res):
        pytest.skip("Nessun dato ETP_FIELDS per l'ISIN di test.")
    print("\nudf_get_etp_fields_real:", type(res), res)
    assert _non_empty(res)


@pytest.mark.integration
def test_udf_get_fx_composition_real():
    # Test sia full che filtrato per una valuta specifica
    res_all = udfs.get_fx_composition(TEST_ETF_ISIN,fx_fxfwrd="fx", options="reference_date=latest")
    if not _non_empty(res_all):
        pytest.skip("Nessuna FX composition per l'ISIN di test.")
    print("\nudf_get_fx_composition_real_all:", type(res_all), res_all)
    assert _non_empty(res_all)

    res_eur = udfs.get_fx_composition(TEST_ETF_ISIN, options="reference_date=latest")
    print("\nudf_get_fx_composition_real_EUR:", type(res_eur), res_eur)


# =====================================================================
# MarketData UDF - Daily
# =====================================================================

@pytest.mark.integration
def test_udf_get_daily_etf_real():
    res = udfs.get_daily_etf(
        TEST_ETF_ISIN,
        TEST_START_DATE,
        TEST_END_DATE,
        fields="MID",
        source="timescale",
        market="ETFP",
        currency_et_etp="EUR",
        snapshot_time="17:00:00",
    )
    if not _non_empty(res):
        pytest.skip("Nessun dato daily ETF per l'ISIN/periodo scelto nel tuo ambiente.")
    print("\nudf_get_daily_etf_real:", type(res), res)
    assert _non_empty(res)


@pytest.mark.integration
def test_udf_get_daily_currency_real():
    res = udfs.get_daily_currency(
        TEST_FX_PAIR,
        TEST_START_DATE,
        TEST_END_DATE,
        fields="MID",
        source="timescale",
    )
    if not _non_empty(res):
        pytest.skip("Nessun dato daily FX per la coppia scelta nel tuo ambiente.")
    print("\nudf_get_daily_currency_real:", type(res), res)
    assert _non_empty(res)


@pytest.mark.integration
def test_udf_get_daily_future_real():
    if not TEST_FUTURE_TICKER:
        pytest.skip("Imposta TEST_FUTURE_TICKER per testare i future.")
    res = udfs.get_daily_future(
        TEST_FUTURE_TICKER,
        TEST_START_DATE,
        TEST_END_DATE,
        fields="MID",
        source="timescale",
        snapshot_time="17:00:00"
    )
    if not _non_empty(res):
        pytest.skip("Nessun dato daily future per il ticker scelto nel tuo ambiente.")
    print("\nudf_get_daily_future_real:", type(res), res)
    assert _non_empty(res)


# =====================================================================
# MarketData UDF - Intraday
# =====================================================================

@pytest.mark.integration
def test_udf_get_intraday_etf_real():
    res = udfs.get_intraday_etf(
        TEST_ETF_ISIN,
        TEST_START_DATE,
        start_time="09:00:00",
        end_time="10:00:00",
        fields="MID",
        frequency="15m",
        curr_of_etp="EUR",
        source="timescale",
        market="ETFP",
    )
    if not _non_empty(res):
        pytest.skip("Nessun dato intraday ETF per ISIN/fascia oraria scelti.")
    print("\nudf_get_intraday_etf_real:", type(res), res)
    assert _non_empty(res)


@pytest.mark.integration
def test_udf_get_intraday_fx_real():
    res = udfs.get_intraday_currency(
        TEST_FX_PAIR,
        TEST_START_DATE,
        start_time="13:00:00",
    )
    if not _non_empty(res):
        pytest.skip("Nessun dato intraday FX per coppia/fascia oraria scelta.")
    print("\nudf_get_intraday_fx_real:", type(res), res)
    assert _non_empty(res)


@pytest.mark.integration
def test_udf_get_intraday_future_real():
    if not TEST_FUTURE_TICKER:
        pytest.skip("Imposta TEST_FUTURE_TICKER per testare i future.")
    res = udfs.get_intraday_future(
        TEST_FUTURE_TICKER,
        TEST_START_DATE,
        start_time="09:00:00",
        end_time="09:10:00",
        fields="MID",
        frequency="1m",
        source="timescale",
    )
    if not _non_empty(res):
        pytest.skip("Nessun dato intraday future per ticker/fascia oraria scelti.")
    print("\nudf_get_intraday_future_real:", type(res), res)
    assert _non_empty(res)
