# bshdata/core/instruments/factory/instrument_factory.py

import logging
from threading import Lock
from typing import Optional, Type, Dict

from client import BSHDataClient
from core.enums.currencies import CurrencyEnum
from core.enums.instrument_types import InstrumentType

from core.instruments.classifier.instrument_classifier import InstrumentClassifier
from core.instruments.instruments import (
    Instrument, InstrumentRegistry,
    FutureInstrument, CDXIndexInstrument, EtfInstrument, RatesIndexInstrument, CurrencyPairInstrument, CurrencyInstrument
)
from core.utils.singleton import Singleton

logger = logging.getLogger(__name__)


class InstrumentFactory(Singleton):
    """
    Central factory for building Instrument objects.

    Delegates metadata lookup to InstrumentClassifier, handles:
        - identifier resolution
        - type inference
        - routing to builder methods
    """

    _instruments: Dict[str, Instrument] = {}
    _lock = Lock()  # ← Thread safety
    _classifier: Optional[InstrumentClassifier] = None
    _client: Optional[BSHDataClient] = None

    def __init__(self, client: Optional[BSHDataClient] = None):
        """
        Initialize factory (only first call configures).

        Args:
            client: BSHDataClient instance (optional)
        """
        # Check CLASS attribute (singleton-safe)
        if InstrumentFactory._client is None:
            self.client = client or self._safe_init_client()
            self.providers = getattr(self.client, "providers", {}) if self.client else {}

            oracle_provider = self.providers.get("oracle")

            # Set CLASS attributes
            InstrumentFactory._classifier = InstrumentClassifier(
                oracle_provider.query if oracle_provider else None
            )
            InstrumentFactory._client = self.client
    # ==================================================================
    # PUBLIC API: create()
    # ==================================================================
    def create(
            self,
            id: Optional[str] = None,
            type: InstrumentType | str | None = None,
            ticker: Optional[str] = None,
            isin: Optional[str] = None,
            currency: Optional[str] = None,
            autocomplete: bool = False,
            **kwargs,
    ) -> Instrument:

        # --- resolve ID / ISIN / ticker ---
        isin, ticker, id = self._resolve_identifiers(id, isin, ticker)

        # --- infer type ---
        if type is None:
            type = self._classifier.infer_type(isin or ticker or id)
        if isinstance(type, str):
            type = InstrumentType.from_str(type)

        instrument: Optional[Instrument] = None
        # --- dispatch ---
        match type:
            case InstrumentType.FUTURE:
                instrument = self._build_future(id, isin, ticker, currency, autocomplete, **kwargs)

            case InstrumentType.ETP:
                instrument = self._build_etp(id, isin, ticker, currency, autocomplete, **kwargs)

            case InstrumentType.CDXINDEX:
                instrument = self._build_cdx(id, ticker, autocomplete, **kwargs)

            case InstrumentType.CURRENCYPAIR:
                instrument = self._build_currency_pair(id, autocomplete, **kwargs)

            case InstrumentType.CURRENCY:
                instrument = self._build_currency(id, autocomplete, **kwargs)

            case InstrumentType.SWAP:
                instrument = self._build_swap(id, autocomplete, **kwargs)

            case InstrumentType.STOCK:
                instrument = self._build_stock(id, isin, ticker, currency, autocomplete, **kwargs)

            case InstrumentType.CDXINDEX:
                instrument = self._build_cdx(id, ticker, autocomplete, **kwargs)

            case InstrumentType.INDEX:
                instrument = self._build_index(id, ticker, autocomplete, currency=currency, **kwargs)

            case InstrumentType.FXFWD:
                instrument = self._build_fx_forward(id=id, ticker=ticker, autocomplete=autocomplete, **kwargs)

            case _:
                cls: Type[Instrument] = InstrumentRegistry.get_class(type)
                instrument = cls(type=type, ticker=ticker, isin=isin, id=id, currency=currency)

        self.register(instrument)
        return instrument

    # ==================================================================
    # BUILDERS
    # ==================================================================

    # ---------- FUTURE ----------
    def _build_future(self, id_, isin, ticker, currency, autocomplete, **kwargs) -> FutureInstrument:
        is_active = kwargs.get("is_active_form")
        if autocomplete:
            meta = self._classifier.get_future_metadata(id_)
            kwargs.update({k: v for k, v in meta.items() if v is not None})
            is_active = (not self._classifier.future.is_contract(id_)) if is_active is None else is_active

        fut = FutureInstrument(
            isin=isin,
            id=id_,
            ticker=ticker,
            currency=kwargs.pop("future_currency", currency),
            is_active_form=is_active,
            root=kwargs.get("root"),
            future_underlying=kwargs.get("future_underlying"),
            suffix=kwargs.get("suffix"),
            timescale_root=kwargs.get("timescale_root"),
        )

        underlying = fut.future_underlying or kwargs.get("future_underlying")
        return fut.set_future_underlying(underlying)

    # ---------- ETP ----------
    def _build_etp(self, id_, isin, ticker, currency, autocomplete, **kwargs) -> EtfInstrument:

        underlying = kwargs.get("etf_underlying")
        mkt = kwargs.get("market")
        payment_policy = kwargs.get("payment_policy")
        fund_currency = kwargs.get("fund_currency")

        if autocomplete:
            if not ticker and not isin:
                if id_ in self._classifier.etp.tickers:
                    ticker = id_
            isin, ticker = self._classifier.auto_complete(isin, ticker, InstrumentType.ETP)
            currency = currency or (self._classifier.get_ccy(isin, mkt, InstrumentType.ETP) if mkt else None)
            underlying = underlying or self._classifier.etp.get_undelying_type(isin)
            fund_currency = fund_currency or self._classifier.etp.get_fund_currency(isin)
            payment_policy = payment_policy or self._classifier.etp.get_payment_policy(isin)

        cls: Type[Instrument] = InstrumentRegistry.get_class(InstrumentType.ETP)
        return cls(type=InstrumentType.ETP, isin=isin, id=id_, ticker=ticker, currency=currency,
                   underlying_type=underlying, payment_policy=payment_policy, fund_currency=fund_currency)

    # ---------- FX ----------
    def _build_currency_pair(self, id_, autocomplete,  **kwargs) -> CurrencyPairInstrument:
        currency_pair_code = id_.split(" ")[0]
        assert len(currency_pair_code) == 6, "specify currency pair code as pair (EURUSD not just USD)"
        cls = InstrumentRegistry.get_class(InstrumentType.CURRENCYPAIR)
        quoted = self._build_currency(currency_pair_code[3:], autocomplete=autocomplete, **kwargs)
        base = self._build_currency(currency_pair_code[:3], autocomplete=autocomplete, **kwargs)

        return cls(type=InstrumentType.CURRENCYPAIR, id=id_,
                   base_currency=base,
                   quoted_currency=quoted
                   , **kwargs)

    def _build_currency(self, id_, autocomplete, **kwargs) -> CurrencyInstrument:
        currency_code = id_
        assert CurrencyEnum.exists(currency_code), "currency_code does not exist"
        currency_type = kwargs.get("currency_type")
        currency_multiplier = kwargs.get("currency_multiplier")
        reference_currency = kwargs.get("reference_currency", id_)

        if autocomplete:

            currency_type = currency_type or self._classifier.fx.get_currency_type(currency_code)
            currency_multiplier = currency_multiplier or self._classifier.fx.get_currency_multiplier(currency_code) or 1
            reference_currency = reference_currency or self._classifier.fx.get_reference_currency(currency_code)

        cls = InstrumentRegistry.get_class(InstrumentType.CURRENCY)
        return cls(type=InstrumentType.CURRENCY,
                   id=currency_code,
                   currency_type=currency_type,
                   currency_code=currency_code,
                   currency_multiplier=currency_multiplier,
                   reference_currency=reference_currency)

    # ---------- CDX ----------
    def _build_cdx(self, id_, ticker, autocomplete, **kwargs) -> CDXIndexInstrument:
        cls = InstrumentRegistry.get_class(InstrumentType.CDXINDEX)
        uid = id_.upper()

        # ------------------------------
        # FIX: ticker fallback = id
        # ------------------------------
        if ticker is None:
            ticker = id_

        ticker_root = kwargs.pop("ticker_root", None)

        if not ticker_root:
            if len(uid) == 8 and any(uid.endswith(str(s)) for s in range(35, 50)):
                ticker_root = uid[:-2]
            else:
                ticker_root = ticker.upper()

        # autocomplete metadata
        if autocomplete:
            for f in ("CURRENCY", "INDEX_NAME", "TENOR", "SERIES"):
                key = f.lower()
                kwargs[key] = kwargs.get(key) or self._classifier.get_cdx_field(ticker_root, f)

            if not self._classifier.cds.matches(ticker_root):
                logger.warning(f"{ticker_root} is not a recognized CDS root.")

        return cls(type=InstrumentType.CDXINDEX, ticker=id_, ticker_root=ticker_root, **kwargs)

    # ---------- STOCK ----------
    def _build_stock(self, id_, isin, ticker, currency, autocomplete, **kwargs):
        cls = InstrumentRegistry.get_class(InstrumentType.STOCK)
        market = kwargs.get("market")
        if autocomplete:
            isin, ticker = self._classifier.auto_complete(isin, ticker, InstrumentType.STOCK)
            currency = currency or (self._classifier.get_ccy(isin,
                                                             market=market,
                                                             instrument_type=InstrumentType.STOCK) if market else None)

        return cls(
            type=InstrumentType.STOCK,
            id=id_,
            currency=currency,
            ticker=ticker,
            isin=isin,
            market=market
        )

    # ---------- SWAP ----------
    def _build_swap(self, id_, autocomplete, **kwargs) -> Instrument:
        cls = InstrumentRegistry.get_class(InstrumentType.SWAP)

        tenor = (
                kwargs.pop("tenor", None)
                or (self._classifier.swap.extract_tenor(id_) if autocomplete else None)
        )

        ticker = (
                kwargs.pop("ticker", None)
                or (id_ if (self._classifier.swap.matches(id_) and autocomplete) else None)
        )

        return cls(id=id_, ticker=ticker, tenor=tenor)

    def _build_index(self, id_, ticker, autocomplete, **kwargs) -> Instrument:
        """
        Costruisce un Rates Index Instrument (EURIBOR, ESTR, SOFR...)
        usando ID / TICKER / FAMILY / TENOR + completamento automatico.
        """
        family = kwargs.get("family")
        tenor = kwargs.get("tenor")
        ccy = kwargs.get("currency")
        if not ticker:
            ticker = self._classifier.index.get_ticker_by_id(id_, tenor)

        if autocomplete:
            if not ticker and family:
                if not tenor:
                    logger.warning(f"Index specified only as family '{family}'. TENOR missing → using default 1D.")
                    tenor = "1D"

            if not family and ticker:
                family = self._classifier.index.get_family(ticker)
            if not tenor:
                tenor = self._classifier.index.get_tenor(ticker, family)
            ccy = ccy or self._classifier.index.get_currency_from_family(family)
        row = self._classifier.index.lookup_by_ticker(ticker) or {}

        # I kwargs devono OVERRIDE il dataset
        comp = kwargs.get("compounding", row.get("COMPOUNDING"))
        dcount = kwargs.get("day_count", row.get("DAY_COUNT"))
        bdc = kwargs.get("business_day_convention", row.get("BUSINESS_DAY_CONVENTION"))
        eom = kwargs.get("eom", row.get("EOM"))
        is_rate_index = any((comp, dcount, bdc, eom))
        if is_rate_index:
            return RatesIndexInstrument(
                id=id_,
                ticker=ticker,
                tenor=tenor,
                compounding=comp,
                currency=ccy,
                day_count=dcount,
                business_day_convention=bdc,
                eom=eom,
                family=family,
            )
        cls: Type[Instrument] = InstrumentRegistry.get_class(InstrumentType.INDEX)
        return cls(
            id=id_,
            name=ticker,
            tenor=tenor,
            compounding=comp,
            currency=ccy,
            day_count=dcount,
            business_day_convention=bdc,
            eom=eom,
            family=family,
        )

    # ==================================================================
    # INTERNAL HELPERS
    # ==================================================================
    def _resolve_identifiers(self, id_, isin, ticker):
        # ID is ISIN
        if id_ and self._classifier.etp.ISIN_RE.match(id_):
            isin = isin or id_

        if not id_:
            id_ = isin or ticker

        return isin, ticker, id_

    # ----------------------------------------------------------------------
    # Convenience wrappers
    # ----------------------------------------------------------------------
    def as_isin(self, *args: object, **kwargs: object) -> str:
        return self._classifier.etp.as_isin(*args, **kwargs)

    def as_ticker(self, *args, **kwargs) -> str:
        return self._classifier.etp.as_ticker(*args, **kwargs)

    def _build_fx_forward(self, id, ticker, quoted_currency, tenor, autocomplete, **param):

        cls = InstrumentRegistry.get_class(InstrumentType.FXFWD)
        base_currency = param.get("base_currency")

        if isinstance(base_currency, dict):
            base_currency = base_currency.get(id)
        if isinstance(quoted_currency, dict):
            quoted_currency = quoted_currency.get(id)

        if autocomplete:
            tenor = tenor or self._classifier.fx_forward.extract_tenor(id)
            base_currency = base_currency or self._classifier.fx_forward.get_base_currency(id)
            quoted_currency = quoted_currency or self._classifier.fx_forward.get_quoted_currency(id)

        return cls(id=id,
                   ticker=ticker,
                   quoted_currency=quoted_currency,
                   tenor=tenor,
                   base_currency=base_currency)

    @staticmethod
    def _safe_init_client():
        try:
            return BSHDataClient()
        except Exception as e:
            logger.warning(f"Failed to initialize BSHDataClient for {e}."
                           f" instantiate BSHDataClient instead or BDConnction singleton."
                           "auto metadata fetching is disabled for the session.")

    def register(self, instrument: Instrument) -> None:
        """Register instrument in internal cache"""
        with self._lock:
            self._instruments[instrument.id] = instrument

    def get(self, id_: str, **kwargs) -> Instrument:
        """
        Get or create instrument (lazy loading).

        Thread-safe with double-check pattern.
        """
        # Fast path (no lock)
        if id_ in self._instruments:
            return self._instruments[id_]

        # Slow path (with lock)
        with self._lock:
            # Double-check (altro thread potrebbe aver creato nel frattempo)
            if id_ not in self._instruments:
                logger.warning(f"Instrument {id_} not registered. Creating new one.")
                self._instruments[id_] = self.create(id=id_, autocomplete=True)
            return self._instruments[id_]

    def get_many(self, ids: list[str]) -> dict[str, Instrument]:
        """Batch get instruments"""
        return {id_: self.get(id_) for id_ in ids}

    def clear_cache(self) -> None:
        """Clear internal cache (testing utility)"""
        with self._lock:
            self._instruments.clear()

    @classmethod
    def configure(cls, client: BSHDataClient):
        """
        Configure the singleton instance with a client.

        Useful for manual configuration after initialization.

        Args:
            client: BSHDataClient instance
        """
        instance = cls()

        if instance._client is None:
            instance.client = client
            instance.providers = getattr(client, "providers", {})

            oracle_provider = instance.providers.get("oracle")
            instance._classifier = InstrumentClassifier(
                oracle_provider.query if oracle_provider else None
            )

            instance._client = client

        return instance
