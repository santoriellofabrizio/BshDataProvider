import pytest

from sfm_data_provider.core.instruments.instrument_factory import InstrumentFactory
from sfm_data_provider.core.instruments.instruments import CurrencyPairInstrument, EtfInstrument, EquityFuture, \
    SwapInstrument
from sfm_data_provider.interface.bshdata import BshData


@pytest.fixture(scope="module")
def instrument_factory():
    helper = BshData(cache=False, log_level="WARNING").client
    return InstrumentFactory(helper)

def test_currency_factory(instrument_factory):
    USD = instrument_factory.create("USD", autocomplete=True)
    GBp = instrument_factory.create("GBp", autocomplete=True)
    USDGBp: CurrencyPairInstrument = instrument_factory.create("USDGBp", autocomplete=True)
    USDGBp2: CurrencyPairInstrument = CurrencyPairInstrument(id="USDGBp", base_currency=USD, quoted_currency=GBp)
    assert USDGBp == USDGBp2
    assert USDGBp.currency_pair_multiplier == 0.01


def test_instrument_factory_etf_from_ticker_type(instrument_factory):
    instrument = instrument_factory.create(type="ETP", ticker="IHYG", autocomplete=True)

    print(instrument.isin)

    assert instrument.isin is not None


def test_instrument_factory_etf_from_isin_type(instrument_factory):
    instrument = instrument_factory.create("IE00B66F4759", type="ETP", autocomplete=True)

    print(instrument.isin)
    print(instrument.ticker)

    assert instrument.ticker is not None


def test_instrument_factory_etf_from_isin(instrument_factory):
    instrument = instrument_factory.create("IE00B66F4759", type="ETP", autocomplete=True)

    print(instrument.isin)
    print(instrument.ticker)

    assert instrument.ticker is not None

def test_swap_factory(instrument_factory):
    instrument = instrument_factory.create("EUZCISWAP1", autocomplete=True)
    assert isinstance(instrument, SwapInstrument)
    assert instrument.tenor == "1Y"



def test_instrument_factory_currency(instrument_factory):
    usd = instrument_factory.create("AUDUSD", autocomplete=True)
    eur = instrument_factory.create("EURAUD", autocomplete=True)

    assert isinstance(usd, CurrencyPairInstrument)
    assert isinstance(eur, CurrencyPairInstrument)


def test_instrument_factory_etf(instrument_factory):

    for id in ["AEEM", "IHYG", "LU2265794946"]:
        assert isinstance(instrument_factory.create(id, autocomplete=True), EtfInstrument)


def test_oracle_future_anagraphic(instrument_factory):

    for eq in ["VGA", "vga index"]:
        f = instrument_factory.create(ticker=eq, autocomplete=True)
        assert isinstance(f, EquityFuture)
        assert f.is_active_form

    for eq in ["FESX","fxxp202512"]:
        f = instrument_factory.create(ticker=eq, autocomplete=True)
        assert isinstance(f, EquityFuture)
        print(f.is_active_form)
        print(f.root)

    for eq in ["VGA INDEX"]:
        f = instrument_factory.create(ticker=eq, autocomplete=True)
        assert isinstance(f, EquityFuture)


def test_oracle_irs(instrument_factory):
    estron = instrument_factory.create("ESTRON", autocomplete=True)
    estr = instrument_factory.create("ESTR3M", autocomplete=True)


def test_credit_futures(instrument_factory):
    fehy = instrument_factory.create("FEHY202606", autocomplete=True)
    a = 0

def test_swaps(instrument_factory):
    EUSWI10 = instrument_factory.create("EUZCISWAP1", autocomplete=True)


