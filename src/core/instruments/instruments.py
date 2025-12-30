from dataclasses import dataclass, field
from typing import Optional, Type, Dict, TypeVar, Union, Literal
import logging

from core.enums.currencies import CurrencyEnum
from core.enums.instrument_types import InstrumentType
from core.enums.issuers import IssuerGroup

logger = logging.getLogger(__name__)
T = TypeVar("T", bound="Instrument")


# ============================================================
# BASE
# ============================================================
@dataclass
class Instrument:
    ticker: Optional[str] = None
    isin: Optional[str] = None
    id: Optional[str] = None
    currency: Optional[CurrencyEnum | str] = None
    type: Optional[InstrumentType] = None
    market: Optional[str] = None

    def __post_init__(self):
        if not any([self.ticker, self.isin, self.id]):
            raise ValueError("Instrument requires at least one between ticker, isin or id.")

        if self.id is None:
            self.id = self.isin or self.ticker

        # Normalizza type e currency
        if isinstance(self.type, str):
            self.type = InstrumentType.from_str(self.type)

        if self.type != InstrumentType.CURRENCYPAIR:
            if isinstance(self.currency, str):
                self.currency = CurrencyEnum.from_str(self.currency)
            self.currency = self.currency or CurrencyEnum.EUR

    # ============================================================
    # 🔹 AUTOPROMOTION METHOD
    # ============================================================
    def set_type(self: T, new_type: InstrumentType | str) -> T:
        """
        Imposta il tipo e promuove automaticamente l'istanza
        alla sottoclasse corretta registrata nel registry.
        """
        if isinstance(new_type, str):
            new_type = InstrumentType.from_str(new_type)

        self.type = new_type
        subclass = InstrumentRegistry.get_class(new_type)

        if not isinstance(self, subclass):
            promoted = subclass.__new__(subclass)
            promoted.__dict__.update(self.__dict__)
            promoted.__post_init__()  # esegue validazioni della sottoclasse
            logger.debug(f"Promoted Instrument → {subclass.__name__} (type={new_type})")
            return promoted  # restituisce la sottoclasse coerente

        return self

    def __repr__(self):
        return f"{self.__class__.__name__}(id={self.id}, type={self.type}, currency={self.currency})"


# ============================================================
# ETF
# ============================================================
@dataclass
class EtfInstrument(Instrument):
    underlying_type: Optional[str] = None
    payment_policy: Literal["DIST", "ACC"] | None = None
    fund_currency: Optional[CurrencyEnum] = None
    underlying_index: Optional[Literal["EQUITY", "FIXED INCOME", "COMMODITY"]] = None
    index_provider: Optional[str] = None
    replication_method: Optional[str] = None
    issuer: Optional[IssuerGroup] = None

    def __post_init__(self):
        self.type = InstrumentType.ETP
        if self.underlying_type is not None:
            if self.underlying_type.upper() not in ["EQUITY", "FIXED INCOME", "COMMODITY"]:
                raise ValueError("Instrument requires underlying type to EQUITY or FIXED INCOME")
            self.underlying_type = self.underlying_type.upper()

        super().__post_init__()


@dataclass
class FxForwardInstrument(Instrument):
    tenor: Optional[str] = None
    base_currency: Optional[CurrencyEnum] = CurrencyEnum.EUR
    quoted_currency: Optional[CurrencyEnum] = None
    underlying: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()
        self.type = InstrumentType.FXFWD
        if isinstance(self.base_currency, str):
            self.base_currency = CurrencyEnum.from_str(self.base_currency)
        if isinstance(self.quoted_currency, str):
            self.quoted_currency = CurrencyEnum.from_str(self.quoted_currency)
        if isinstance(self.tenor, int):
            self.tenor = f"{self.tenor}M"
            logger.warning(f"assuming tenor of {self.id} is expressed in month")


# ============================================================
# STOCK
# ============================================================
@dataclass
class StockInstrument(Instrument):
    def __post_init__(self):
        self.type = InstrumentType.STOCK
        super().__post_init__()


@dataclass
class CurrencyInstrument(Instrument):
    currency_code: Optional[str] = id
    currency_type: Optional[Literal["STANDARD", "SUBUNIT", "FUNDS CODE"]] = "STANDARD"
    currency_multiplier: Optional[float] = 1.0
    reference_currency: Optional[CurrencyEnum] = None

    def __post_init__(self):
        super().__post_init__()

        # Validate and normalize currency_code
        if self.currency_code is None:
            raise ValueError("currency_code is required")

        if not CurrencyEnum.exists(self.currency_code):
            raise ValueError(
                f"Invalid currency code: '{self.currency_code}'. "
                f"Use one of: {', '.join([c.value for c in CurrencyEnum][:10])}..."
            )

        self.currency_code = CurrencyEnum.from_str(self.currency_code)

        # Normalize currency_type
        if self.currency_type:
            self.currency_type = self.currency_type

        # Validate reference_currency if provided
        if self.reference_currency is not None:
            if not CurrencyEnum.exists(self.reference_currency):
                raise ValueError(
                    f"Invalid reference currency: '{self.reference_currency}'"
                )
            self.reference_currency = CurrencyEnum.from_str(self.reference_currency)

        # SUBUNIT validation
        if self.currency_type == "SUBUNIT":
            if self.currency_multiplier is None or self.currency_multiplier == 1:
                raise ValueError(
                    f"SUBUNIT currency '{self.currency_code}' requires "
                    f"currency_multiplier different from 1"
                )

            if self.reference_currency is None:
                raise ValueError(
                    f"SUBUNIT currency '{self.currency_code}' requires "
                    f"reference_currency (e.g., GBX → GBP)"
                )


# ============================================================
# CURRENCYPAIR
# ============================================================
@dataclass
class CurrencyPairInstrument(Instrument):
    """CurrencyInstrument pair instrument (e.g., EURUSD, GBPJPY)."""

    base_currency: Optional[CurrencyInstrument] = field(default=None)
    quoted_currency: Optional[CurrencyInstrument] = field(default=None)
    currency_pair_multiplier: Optional[float] = field(default=None)

    def __post_init__(self):
        super().__post_init__()

        self.type = InstrumentType.CURRENCYPAIR
        self.currency = None

        # Validate
        if self.base_currency is None or self.quoted_currency is None:
            raise ValueError(f"CurrencyPair '{self.id}' requires base_currency and quote_currency")

        # Auto-calculate multiplier for subunits
        if (self.base_currency.currency_type == "SUBUNIT" or
                self.quoted_currency.currency_type == "SUBUNIT"):

            base_mult = self.base_currency.currency_multiplier or 1.0
            quote_mult = self.quoted_currency.currency_multiplier or 1.0

            if self.currency_pair_multiplier is None:
                self.currency_pair_multiplier = base_mult * quote_mult
            elif self.currency_pair_multiplier == 1.0:
                raise ValueError(
                    f"currency_pair_multiplier cannot be 1.0 for subunit pair "
                    f"{self.base_currency.currency_code}{self.quoted_currency.currency_code}"
                )
        else:
            self.currency_pair_multiplier = self.currency_pair_multiplier or 1.0

        # Auto-generate id
        if not self.id:
            self.id = f"{self.base_currency.currency_code}{self.quoted_currency.currency_code}"


# ============================================================
# FUTURE BASE
# ============================================================
@dataclass
class FutureInstrument(Instrument):
    is_active_form: bool = True
    root: Optional[str] = None
    future_underlying: Optional[str] = None
    suffix: Optional[str] = None
    timescale_root: Optional[str] = None

    def __post_init__(self):
        self.type = InstrumentType.FUTURE
        self.suffix = self.suffix.upper() if self.suffix else None
        if self.suffix not in ["INDEX", "COMDTY"]:
            raise ValueError("Future instrument requires a suffix like INDEX or COMDTY")
        super().__post_init__()

    def set_future_underlying(self: T, value: str):
        if not value:
            raise ValueError("future_underlying cannot be empty")

        value_upper = value.strip().upper()
        self.future_underlying = value_upper

        subclass = {
            "EQUITY": EquityFuture,
            "FIXED INCOME": FixedIncomeFuture,
        }.get(value_upper, FutureInstrument)

        if not isinstance(self, subclass):
            promoted = subclass.__new__(subclass)
            promoted.__dict__.update(self.__dict__)
            promoted.__post_init__()
            logger.debug(f"Promoted FutureInstrument → {subclass.__name__} (underlying={value_upper})")
            return promoted

        return self


# ============================================================
# FUTURE SUBTYPES
# ============================================================
@dataclass
class FixedIncomeFuture(FutureInstrument):
    def __post_init__(self):
        self.future_underlying = "FIXED INCOME"
        super().__post_init__()


@dataclass
class EquityFuture(FutureInstrument):
    def __post_init__(self):
        self.future_underlying = "EQUITY"
        super().__post_init__()


@dataclass
class SwapInstrument(Instrument):
    tenor: Optional[str] = None
    swap_type = Optional[str]

    def __post_init__(self):
        super().__post_init__()
        self.type = InstrumentType.SWAP


@dataclass
class CDXIndexInstrument(Instrument):
    ticker_root: Optional[str] = None
    index_name: Optional[str] = None
    series: Optional[str] = None
    tenor: Optional[Union[str, int]] = None
    is_active_form: Optional[bool] = True

    def __post_init__(self):
        super().__post_init__()
        self.type = InstrumentType.CDXINDEX
        if self.tenor:
            if isinstance(self.tenor, int):
                self.tenor = f"{self.tenor}y"


@dataclass
class IndexInstrument(Instrument):
    name: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()
        self.type = InstrumentType.INDEX


@dataclass
class RatesIndexInstrument(IndexInstrument):
    tenor: Optional[str] = None
    family: Optional[str] = None
    compounding: Optional[str] = None
    currency: Optional[str] = None
    day_count: Optional[str] = None
    business_day_convention: Optional[str] = None
    eom: Optional[bool] = None


# ============================================================
# REGISTRY
# ============================================================
class InstrumentRegistry:
    _registry: Dict[InstrumentType, Type[Instrument]] = {}

    @classmethod
    def register(cls, instrument_type: InstrumentType, klass: Type[Instrument]):
        cls._registry[instrument_type] = klass

    @classmethod
    def get_class(cls, instrument_type: InstrumentType) -> Type[Instrument]:
        return cls._registry.get(instrument_type, Instrument)


# Registrazione automatica
InstrumentRegistry.register(InstrumentType.ETP, EtfInstrument)
InstrumentRegistry.register(InstrumentType.STOCK, StockInstrument)
InstrumentRegistry.register(InstrumentType.CURRENCYPAIR, CurrencyPairInstrument)
InstrumentRegistry.register(InstrumentType.CURRENCY, CurrencyInstrument)
InstrumentRegistry.register(InstrumentType.FUTURE, FutureInstrument)
InstrumentRegistry.register(InstrumentType.SWAP, SwapInstrument)
InstrumentRegistry.register(InstrumentType.CDXINDEX, CDXIndexInstrument)
InstrumentRegistry.register(InstrumentType.FXFWD, FxForwardInstrument)
InstrumentRegistry.register(InstrumentType.INDEX, IndexInstrument)
