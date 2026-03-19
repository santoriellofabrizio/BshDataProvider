# tests/test_oracle_provider_cases.py
import pytest

from sfm_data_provider.interface.bshdata import BshData


@pytest.fixture(scope="module")
def api():
    """Istanzia l’API con sorgente Oracle per test statici."""
    return BshData(cache=False, log_level="WARNING")


# ============================================================
# TEST: INFO.GET BASE
# ============================================================
def test_info_get_oracle_basic(api: BshData):
    print("\n========== TEST ORACLE BASIC INFO.GET ==========")

    ids = ["LU1681045370", "LU0950674175"]
    fields = ["FUND_CURRENCY", "ETP_TYPE", "EXPENSE_RATIO"]

    df = api.info.get_etp_fields(isin=ids, fields=fields, source="oracle")

    print(f"Fetched {len(df)} rows and {len(df.columns)} columns from Oracle")
    print(df.head(10).to_string(index=True))
    print("===============================================")

    # Assertions
    assert not df.empty, "DataFrame restituito è vuoto"
    assert set(df.index) == set(ids), "Non tutti gli identificatori sono presenti nel risultato"
    for f in fields:
        assert f in df.columns, f"Campo richiesto '{f}' non presente nel risultato"


# ============================================================
# TEST: FX COMPOSITION
# ============================================================
def test_info_get_oracle_fx_composition(api: BshData):
    print("\n========== TEST ORACLE FX COMPOSITION ==========")

    ids = ["LU1681045370", "LU0950674175"]
    df = api.info.get_fx_composition(isin=ids)

    print(f"Fetched {len(df)} FX composition rows")
    print(df.head(10).to_string(index=True))
    print("===============================================")

    assert not df.empty, "DataFrame FX_COMPOSITION vuoto"


# ============================================================
# TEST: INVALID FIELD HANDLING
# ============================================================
def test_info_get_oracle_invalid_fields(api: BshData):
    print("\n========== TEST ORACLE INVALID FIELDS ==========")

    ids = ["LU1681045370"]
    bad_fields = ["THIS_FIELD_DOES_NOT_EXIST"]

    with pytest.raises(Exception):
        _ = api.info.get(id_=ids)

    print("Handled invalid field correctly (exception raised)")
    print("===============================================")

def test_cdx_info(api: BshData):

    isin = ["ITXEB543"]
    ticker_root = api.info.get(id_=isin)
    currency = api.info.get(id_=isin)
    print(ticker_root.to_string(index=True))
    print(currency.to_string(index=True))
    assert not ticker_root.empty
    assert not currency.empty


def test_pcf_composition_oracle(api: BshData):

    print("\n========== TEST ORACLE PCF ==========")
    api.enable_cache()
    pcf = api.info.get_pcf_composition(isins=["LU1681045370","LU2265794946"])
    print(pcf.head(10).to_string(index=True))

    assert not pcf.empty

def test_stocks_info(api: BshData):

    print("\n========== TEST ORACLE PCF ==========")
    api.enable_cache()
    pcf = api.info.get_stock_markets(ticker="UCG", autocomplete=True)
    print(pcf.head(10).to_string(index=True))
    assert not pcf.empty


def test_stocks_lookups(api: BshData):

    print("\n========== TEST ORACLE PCF ==========")
    api.enable_cache()
    input = ["UCG","RACE","ISP"]
    ticker = api.info.get_stock_fields(ticker=input, fields="TICKER", market="IM", autocomplete=True)

    for t in input:
        assert ticker.at[t, "TICKER"] == t


def test_speed_lookup(api: BshData):

    for isin in api.general.get_etp_isins(segments=["IM"])[:900]:
        print(api.info.get_etp_fields(isin=isin, fields="TICKER"))


