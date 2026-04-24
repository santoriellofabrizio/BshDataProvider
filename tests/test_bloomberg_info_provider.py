# tests/test_bloomberg_provider_cases.py
import pytest

from sfm_data_provider.interface.bshdata import BshData


@pytest.fixture(scope="module")
def api():
    return BshData(cache=False, log_level="WARNING", autocomplete=False)


ISIN_LIST = [
    'LU0292109856',
    'IE00B02KXK85',
    'IE00BM8QS095',
    'LU2265794276',
    'LU2376679564',
    'LU2265794946',
    'LU0779800910',
    'LU0875160326',
    'IE00BF4NQ904',
    'IE00099GAJC6',
    'IE000K9Z3SF5',
    'LU1841731745',
    'IE00B44T3H88',
    'IE0007P4PBU1',
    'LU0514695690',
    'LU2456436083',
    'LU1900068914',
    'LU1900067940',
    'LU2314312849',
    'LU1953188833',
    'IE00BHZRR147',
    'IE00BK80XL30',
    'FR0011720911',
    'IE00BKFB6K94',
    'LU2469465822',
    'FR0011660927',
    'IE00B441G979',
    'IE00BK5BQV03',
    'IE00BKX55T58',
    'LU1681043599',
    'IE000BI8OT95',
    'FR0010315770',
    'FR0014003IY1',
    'IE00B4X9L533',
    'IE000QMIHY81',
    'IE000UQND7H4',
    'IE00B60SX394',
    'IE00BFY0GT14',
    'IE00BD4TXV59',
    'LU0340285161',
    'LU0659579733',
    'LU0274208692',
    'IE00BJ0KDQ92',
    'IE00B4L5Y983',
    'IE00B0M62Q58',
    'IE00BFNM3J75',
    'IE00BHZPJ569',
    'IE00BCHWNQ94',
    'IE00BZ02LR44',
    'IE00BMY76136',
    'IE000E4BATC9',
    'IE00BL25JP72',
    'IE00BP3QZ825',
    'IE000PB4LRO2',
    'IE0001GSQ2O9',
    'IE000SU7USQ3',
    'IE000TT7HZ88',
    'IE00BP2C1V62',
    'IE00BYTH5594',
    'IE00BL25JL35',
    'IE00BP3QZ601',
    'IE00BK72HJ67',
    'LU0950674332',
    'LU0629459743',
    'IE00BK72HM96',
    'IE000Y77LGG9',
    'IE00BL25JM42',
    'IE00BP3QZB59',
    'LU1681047236',
    'FR0007054358',
    'LU0136234068',
    'LU0380865021',
    'LU0274211217',
    'IE0008471009',
    'IE00B53L3W79',
    'IE00B53QG562',
    'IE00BYXZ2585',
    'IE00BCLWRF22',
    'IE00B910VR50',
    'LU0950668870',
    'LU0147308422',
    'LU0846194776',
    'LU1291098827',
    'IE00BHZPJ015',
    'IE00BDGN9Z19',
    'IE00BNC1G699',
    'LU1931974429',
    'LU1437017350',
    'LU1737652583',
    'LU1681045370',
    'FR0010429068',
    'LU2573966905',
    'LU2573967036',
    'IE000KCS7J59',
    'IE00B5SSQT16',
    'IE00B3DWVS88',
    'IE00B469F816',
    'LU0480132876',
    'LU0950674175',
    'LU0292107645',
    'IE00BTJRMP35',
    'IE00B4L5YC18',
    'IE00B0M63177',
    'LU1900068161',
    'IE00BKM4GZ66',
    'LU2109787049',
    'LU1291097779',
    'IE00BHZPJ239',
    'IE00BLRPN388',
    'LU1681044480',
    'IE00B466KX20',
    'IE00B5L8K969',
    'LU1781541849',
    'IE00BK5BR733',
    'IE00B3VVMM84',

]


# =============================================================
# TESTS
# ============================================================


def test_bloomberg_yas(api: BshData):
    print("\n========== TEST BLOOMBERG YAS ==========")
    df = api.info.get_etp_fields(ticker="IHYG", source="bloomberg", fields="YAS_YLD_SPREAD",
                                 subscriptions="IHYG LN EQUITY")

    print(f"Fetched {len(df)} rows for YAS_YLD_SPREAD on IHYG")
    print(df.head(10).to_string(index=True))
    print("=======================================\n")

    assert df is not None
    assert not df.empty


def test_bloomberg_dividends(api: BshData):
    print("\n========== TEST BLOOMBERG DIVIDENDS ==========")
    dvd = api.info.get_dividends(isin="IE00B66F4759", start="2024-10-01", source="bloomberg")
    print(f"Fetched {len(dvd)} dividend rows for IE00B66F4759")
    print(dvd.head(10).to_string(index=True))
    print("==============================================\n")

    assert dvd is not None
    assert not dvd.empty


def test_bloomberg_nav(api: BshData):
    print("\n========== TEST BLOOMBERG NAV ==========")
    nav = api.info.get_nav(ticker="IHYG", source="bloomberg", subscriptions="IHYG IM EQUITY", start="2025-10-01")
    print(f"Fetched {len(nav)} NAV rows for IHYG")
    print(nav.head(10).to_string(index=True))
    print("=========================================\n")

    assert nav is not None
    assert not nav.empty


def test_ter(api: BshData):
    ter = api.info.get_ter(ISIN_LIST)
    assert len(ter) > 0
