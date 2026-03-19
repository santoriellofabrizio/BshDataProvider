"""
Type hints for InfoDataAPI methods.

These TypedDict classes provide IDE autocomplete and type checking for API parameters
without enforcing runtime validation.

Usage:
    from interface.api.type_hints import InfoDataGetParams

    def get(self, **kwargs):
        # Type hint for IDE support (no runtime overhead)
        params: InfoDataGetParams = kwargs

        # Access with autocomplete
        instrument_id = params.get('id')
        market = params.get('market')
"""
from typing import TypedDict, Optional, Union, List, Dict, Any
from datetime import date


class InfoDataGetParams(TypedDict, total=False):
    """
    Type hints for InfoDataAPI.get() parameters.

    All fields are optional (total=False).

    Dict mode support:
        Parameters like currency, market, source can be passed as:
        - Single value: "USD" (replicated to all instruments)
        - List: ["USD", "EUR", "GBP"] (aligned with instruments)
        - Dict: {"AAPL": "USD", "MSFT": "EUR"} (mapped by instrument ID)
    """
    type: str
    id: Union[str, List[str]]
    isin: Union[str, List[str]]
    ticker: Union[str, List[str]]
    instruments: List[Any]
    market: Union[str, List[str], Dict[str, str]]
    currency: Union[str, List[str], Dict[str, str]]
    source: Union[str, List[str], Dict[str, str]]
    autocomplete: bool
    # Additional optional parameters
    start_date: date
    end_date: date
    fields: List[str]


class FXCompositionParams(TypedDict, total=False):
    """Type hints for InfoDataAPI.get_fx_composition() parameters."""
    id: Union[str, List[str]]
    isin: Union[str, List[str]]
    ticker: Union[str, List[str]]
    instruments: List[Any]
    market: Union[str, List[str], Dict[str, str]]
    currency: Union[str, List[str], Dict[str, str]]
    source: Union[str, List[str], Dict[str, str]]
    autocomplete: bool
    start_date: date
    end_date: date


class PCFCompositionParams(TypedDict, total=False):
    """Type hints for InfoDataAPI.get_pcf_composition() parameters."""
    id: Union[str, List[str]]
    isin: Union[str, List[str]]
    ticker: Union[str, List[str]]
    instruments: List[Any]
    market: Union[str, List[str], Dict[str, str]]
    currency: Union[str, List[str], Dict[str, str]]
    source: Union[str, List[str], Dict[str, str]]
    autocomplete: bool
    start_date: date
    end_date: date


class DividendsParams(TypedDict, total=False):
    """Type hints for InfoDataAPI.get_dividends() parameters."""
    id: Union[str, List[str]]
    isin: Union[str, List[str]]
    ticker: Union[str, List[str]]
    instruments: List[Any]
    market: Union[str, List[str], Dict[str, str]]
    currency: Union[str, List[str], Dict[str, str]]
    source: Union[str, List[str], Dict[str, str]]
    autocomplete: bool
    start_date: date
    end_date: date
    ex_date: bool
    pay_date: bool


class TERParams(TypedDict, total=False):
    """Type hints for InfoDataAPI.get_ter() parameters."""
    id: Union[str, List[str]]
    isin: Union[str, List[str]]
    ticker: Union[str, List[str]]
    instruments: List[Any]
    market: Union[str, List[str], Dict[str, str]]
    source: Union[str, List[str], Dict[str, str]]
    autocomplete: bool


class NAVParams(TypedDict, total=False):
    """Type hints for InfoDataAPI.get_nav() parameters."""
    id: Union[str, List[str]]
    isin: Union[str, List[str]]
    ticker: Union[str, List[str]]
    instruments: List[Any]
    market: Union[str, List[str], Dict[str, str]]
    currency: Union[str, List[str], Dict[str, str]]
    source: Union[str, List[str], Dict[str, str]]
    autocomplete: bool
    start_date: date
    end_date: date


class PricesParams(TypedDict, total=False):
    """Type hints for InfoDataAPI.get_prices() parameters."""
    id: Union[str, List[str]]
    isin: Union[str, List[str]]
    ticker: Union[str, List[str]]
    instruments: List[Any]
    market: Union[str, List[str], Dict[str, str]]
    currency: Union[str, List[str], Dict[str, str]]
    source: Union[str, List[str], Dict[str, str]]
    autocomplete: bool
    start_date: date
    end_date: date
    price_type: Union[str, List[str], Dict[str, str]]
