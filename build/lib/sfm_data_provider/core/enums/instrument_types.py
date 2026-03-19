from enum import Enum


class InstrumentType(str, Enum):

    INDEX = "INDEX"
    ETP = "ETP"
    STOCK = "STOCK"
    FUTURE = "FUTURE"
    CURRENCYPAIR = "CURRENCYPAIR"
    CURRENCY = "CURRENCY"
    FXFWD = "FXFWD"
    BOND = "BOND"
    CDXINDEX = "CDXINDEX"
    SWAP = "SWAP"
    IR = "IR"
    WARRANT = "WARRANT"
    CERTIFICATE = "CERTIFICATE"

    @classmethod
    def from_str(cls, value: str) -> "InstrumentType":
        try:
            return cls(value.strip().upper())
        except ValueError:
            raise ValueError(f"Invalid InstrumentType: {value}")

