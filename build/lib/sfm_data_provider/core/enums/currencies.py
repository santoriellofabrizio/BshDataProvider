from enum import Enum


class CurrencyEnum(str, Enum):
    """
    Enumerazione standardizzata delle valute (ISO 4217).
    Include valute principali, emergenti e alias comuni.
    """

    # --- Valute principali ---
    EUR = "EUR"  # Euro
    USD = "USD"  # US Dollar
    GBP = "GBP"  # British Pound
    CHF = "CHF"  # Swiss Franc
    JPY = "JPY"  # Japanese Yen
    CAD = "CAD"  # Canadian Dollar
    AUD = "AUD"  # Australian Dollar
    NZD = "NZD"  # New Zealand Dollar
    CNY = "CNY"  # Chinese Yuan
    CNH = "CNH"  # Chinese Yuan (offshore)
    HKD = "HKD"  # Hong Kong Dollar
    SEK = "SEK"  # Swedish Krona
    NOK = "NOK"  # Norwegian Krone
    DKK = "DKK"  # Danish Krone
    PLN = "PLN"  # Polish Zloty
    CZK = "CZK"  # Czech Koruna
    HUF = "HUF"  # Hungarian Forint
    RON = "RON"  # Romanian Leu
    HRK = "HRK"  # Croatian Kuna
    ISK = "ISK"  # Icelandic Krona

    # --- Valute extra-UE e globali ---
    RUB = "RUB"  # Russian Ruble
    TRY = "TRY"  # Turkish Lira
    ZAR = "ZAR"  # South African Rand
    INR = "INR"  # Indian Rupee
    IDR = "IDR"  # Indonesian Rupiah
    MYR = "MYR"  # Malaysian Ringgit
    SGD = "SGD"  # Singapore Dollar
    THB = "THB"  # Thai Baht
    PHP = "PHP"  # Philippine Peso
    KRW = "KRW"  # South Korean Won
    TWD = "TWD"  # Taiwan Dollar
    VND = "VND"  # Vietnamese Dong
    KZT = "KZT"  # Kazakhstani Tenge
    SAR = "SAR"  # Saudi Riyal
    QAR = "QAR"  # Qatari Riyal
    AED = "AED"  # UAE Dirham
    EGP = "EGP"  # Egyptian Pound
    NGN = "NGN"  # Nigerian Naira
    KES = "KES"  # Kenyan Shilling
    PKR = "PKR"  # Pakistani Rupee
    NPR = "NPR"  # Nepalese Rupee
    LKR = "LKR"  # Sri Lankan Rupee
    KWD = "KWD"

    # --- Americhe ---
    BRL = "BRL"  # Brazilian Real
    ARS = "ARS"  # Argentine Peso
    CLP = "CLP"  # Chilean Peso
    COP = "COP"  # Colombian Peso
    MXN = "MXN"  # Mexican Peso
    PEN = "PEN"  # Peruvian Sol
    UYU = "UYU"  # Uruguayan Peso
    DOP = "DOP"  # Dominican Peso

    # --- Altri casi particolari ---
    GBX = "GBX"  # Pence (1/100 di GBP)
    GBp = "GBp"  # Alias di GBX
    FJD = "FJD"  # Fijian Dollar
    JMD = "JMD"  # Jamaican Dollar
    RSD = "RSD"  # Serbian Dinar
    UAH = "UAH"  # Ukrainian Hryvnia
    MAD = "MAD"  # Moroccan Dirham
    ILS = "ILS"  # Israeli Shekel

    # --- Metodi helper ---
    @classmethod
    def from_str(cls, value: str) -> "CurrencyEnum":
        """Normalizza e converte una stringa in CurrencyEnum Enum."""
        if not value:
            raise ValueError("CurrencyEnum string cannot be empty")
        val = value.strip()
        # Alias comuni
        if val in {"GBp", "GBX"}:
            val = "GBp"
        try:
            return cls[val]
        except KeyError:
            raise ValueError(f"Unsupported currency: {value}")

    @classmethod
    def exists(cls, value: str) -> bool:
        """True se la valuta è supportata."""
        try:
            cls.from_str(value)
            return True
        except ValueError:
            return False

    # ============================================================
    # FX_COMPOSITION PAIR DETECTION
    # ============================================================

    @classmethod
    def is_currency_pair(cls, value: str) -> bool:
        """
        Determina se una stringa rappresenta una currency pair valida (es. 'EURUSD', 'USDCNH').
        Restituisce True solo se entrambe le valute sono riconosciute.
        """
        if not value or not isinstance(value, str):
            return False

        val = value.strip().upper()
        # lunghezza tipica 6 o 7 (es. USDCNH)
        if len(val) < 6 or len(val) > 7:
            return False

        # suddividi in base 3 + resto
        base, quote = val[:3], val[3:]
        return cls.exists(base) and cls.exists(quote)
