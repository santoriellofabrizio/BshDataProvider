# instrument_classifier.py
from typing import Optional

from core.enums.instrument_types import InstrumentType

from .etp_classifier import ETPClassifier
from .future_classifier import FutureClassifier
from .fx_forward_classifier import FXForwardClassifier
from .index_classifier import IndexClassifier
from .stock_classifier import StockClassifier
from .swap_classifier import SwapClassifier
from .cdx_classifier import CDSClassifier
from .fx_classifier import FXClassifier


class InstrumentClassifier:

    def __init__(self, oracle):
        self.etp = ETPClassifier(oracle)
        self.future = FutureClassifier(oracle)
        self.swap = SwapClassifier(oracle)
        self.cds = CDSClassifier(oracle)
        self.fx = FXClassifier(oracle)
        self.stock = StockClassifier(oracle)
        self.index = IndexClassifier(oracle)
        self.fx_forward = FXForwardClassifier(oracle)

    # ------------------------------------------------------------
    # Type inference
    # ------------------------------------------------------------
    def infer_type(self, identifier: str) -> InstrumentType:

        if self.etp.matches(identifier):
            return InstrumentType.ETP

        if self.future.matches(identifier):
            return InstrumentType.FUTURE

        if self.fx.matches(identifier):
            return InstrumentType.CURRENCY

        if self.fx.matches_pair(identifier):
            return InstrumentType.CURRENCYPAIR

        if self.swap.matches(identifier):
            return InstrumentType.SWAP

        if self.cds.matches(identifier):
            return InstrumentType.CDXINDEX

        if self.index.matches(identifier):
            return InstrumentType.INDEX

        if self.fx_forward.matches(identifier):
            return InstrumentType.FXFWD

        return InstrumentType.STOCK

    # ------------------------------------------------------------
    # Deleghe verso i classificatori
    # ------------------------------------------------------------
    def auto_complete(self, isin=None, ticker=None, type=InstrumentType.ETP):
        match type:
            case InstrumentType.ETP:
                return isin or self.etp.as_isin(ticker), ticker or self.etp.as_ticker(isin)
            case InstrumentType.STOCK:
                return isin or self.stock.as_isin(ticker), ticker or self.stock.as_ticker(isin)
        return isin, ticker

    def get_ccy(self, isin: str, market: str, instrument_type: Optional[InstrumentType] = None):
        match instrument_type:
            case InstrumentType.ETP:
                return self.etp.get_ccy(isin, market)
            case InstrumentType.STOCK:
                return self.stock.get_ccy(isin, market)
            case InstrumentType.FUTURE:
                return self.future.get_ccy(isin, market)
            case InstrumentType.CDXINDEX:
                return self.cds.get_ccy(isin, market)
            case InstrumentType.SWAP:
                return self.swap.get_ccy(isin, market)
            case InstrumentType.INDEX:
                return self.index.get_ccy(isin)
        return None

    def get_future_metadata(self, identifier: str):
        return self.future.get_metadata(identifier)

    def get_cdx_field(self, ticker_root, field):
        return self.cds.get_field(ticker_root, field)

    def get_cdx_roots(self):
        return self.cds.roots()
