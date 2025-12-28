import re
from typing import Optional

from enum import Enum


class IssuerGroup(str, Enum):
    # Core global issuers
    ISHARES = "ISHARES"
    AMUNDI = "AMUNDI"
    VANGUARD = "VANGUARD"
    STATE_STREET = "STATE_STREET"
    INVESCO = "INVESCO"
    UBS = "UBS"
    BNP_PARIBAS = "BNP_PARIBAS"
    HSBC = "HSBC"
    GOLDMAN_SACHS = "GOLDMAN_SACHS"
    JPMORGAN = "JPMORGAN"
    FIDELITY = "FIDELITY"
    FRANKLIN_TEMPLETON = "FRANKLIN_TEMPLETON"
    AXA = "AXA"
    XTRACKERS = "XTRACKERS"
    LYXOR = "LYXOR"

    # Smart beta / thematic / alternatives
    WISDOMTREE = "WISDOMTREE"
    VANECK = "VANECK"
    GLOBAL_X = "GLOBAL_X"
    KRANESHARES = "KRANESHARES"
    ARK = "ARK"
    HANETF = "HANETF"
    TABULA = "TABULA"
    OSSIAM = "OSSIAM"
    ROBECO = "ROBECO"
    LEGAL_GENERAL = "LEGAL_GENERAL"
    GRANITESHARES = "GRANITESHARES"
    LEVERAGE_SHARES = "LEVERAGE_SHARES"
    MELANION = "MELANION"

    # Digital / crypto / ETP specialist
    COINSHARES = "COINSHARES"
    BITWISE = "BITWISE"
    TWENTYONE_SHARES = "21SHARES"
    DDA = "DDA"
    VANECK_ETP = "VANECK_ETP"

    # Other / special cases
    DEUTSCHE_BANK = "DEUTSCHE_BANK"
    CREDIT_SUISSE = "CREDIT_SUISSE"
    LISTED_FUNDS = "LISTED_FUNDS"
    SPDR_US = "SPDR_US"
    EDR = "EDR"
    YOURINDEX = "YOURINDEX"
    AILIS = "AILIS"
    INVESTLINX = "INVESTLINX"
    INDEXIQ = "INDEXIQ"
    FAM = "FAM"

    UNKNOWN = "UNKNOWN"


def normalize_issuer(raw_name: Optional[str]) -> IssuerGroup:
    if not raw_name:
        return IssuerGroup.UNKNOWN

    name = raw_name.upper().strip()

    # ------------------------------------------------------------------
    # 1. Match diretto (nomi noti, senza ambiguità)
    # ------------------------------------------------------------------
    DIRECT_MAP = {
        # ISHARES / ISHARES
        "ISHARES PLC": IssuerGroup.ISHARES,
        "ISHARES II PLC": IssuerGroup.ISHARES,
        "ISHARES III PLC": IssuerGroup.ISHARES,
        "ISHARES IV PLC": IssuerGroup.ISHARES,
        "ISHARES V PLC": IssuerGroup.ISHARES,
        "ISHARES VI PLC": IssuerGroup.ISHARES,
        "ISHARES VII PLC": IssuerGroup.ISHARES,
        "ISHARES FUNDING": IssuerGroup.ISHARES,
        "ISHARES AM (DE) AG": IssuerGroup.ISHARES,

        # AMUNDI / LYXOR
        "AMUNDI AM SA": IssuerGroup.AMUNDI,
        "AMUNDI ETF ICAV": IssuerGroup.AMUNDI,
        "AMUNDI INDEX SOLUTIONS SICAV": IssuerGroup.AMUNDI,
        "LYXOR INDEX FUND SICAV": IssuerGroup.LYXOR,

        # STATE STREET / SPDR
        "STATE STREET CORP": IssuerGroup.STATE_STREET,
        "SSGA SPDR ETFS EUROPE I PLC": IssuerGroup.STATE_STREET,
        "SSGA SPDR ETFS EUROPE II PLC": IssuerGroup.STATE_STREET,
        "SPDR DJ INDL AVERAGE ETF TRUST": IssuerGroup.SPDR_US,

        # INVESCO
        "INVESCO LTD": IssuerGroup.INVESCO,
        "INVESCO MKTS PLC": IssuerGroup.INVESCO,
        "INVESCO MKTS II PLC": IssuerGroup.INVESCO,
        "INVESCO MKTS III PLC": IssuerGroup.INVESCO,

        # UBS
        "UBS (LUX) FUND SOLUTIONS SICAV": IssuerGroup.UBS,
        "UBS (IRL) ETF PLC": IssuerGroup.UBS,
        "UBS (IRL) FUND SOLUTIONS PLC": IssuerGroup.UBS,

        # BNP
        "BNP PARIBAS SA": IssuerGroup.BNP_PARIBAS,
        "BNP PARIBAS EASY SICAV": IssuerGroup.BNP_PARIBAS,
        "BNP PARIBAS EASY (FR) SICAV": IssuerGroup.BNP_PARIBAS,
        "BNP PARIBAS EASY ICAV": IssuerGroup.BNP_PARIBAS,

        # DIGITAL
        "21SHARES AG": IssuerGroup.TWENTYONE_SHARES,
        "COINSHARES DIGITAL SECS LTD": IssuerGroup.COINSHARES,
        "BITWISE EUROPE GMBH": IssuerGroup.BITWISE,
    }

    if name in DIRECT_MAP:
        return DIRECT_MAP[name]

    # ------------------------------------------------------------------
    # 2. Keyword / regex matching (vero lavoro sporco)
    # ------------------------------------------------------------------
    RULES = [
        (r"ISHARES|ISHARES", IssuerGroup.ISHARES),
        (r"AMUNDI", IssuerGroup.AMUNDI),
        (r"LYXOR", IssuerGroup.LYXOR),
        (r"VANGUARD", IssuerGroup.VANGUARD),
        (r"STATE STREET|SSGA|SPDR", IssuerGroup.STATE_STREET),
        (r"INVESCO", IssuerGroup.INVESCO),
        (r"UBS", IssuerGroup.UBS),
        (r"BNP PARIBAS", IssuerGroup.BNP_PARIBAS),
        (r"HSBC", IssuerGroup.HSBC),
        (r"GOLDMAN SACHS", IssuerGroup.GOLDMAN_SACHS),
        (r"JPMORGAN", IssuerGroup.JPMORGAN),
        (r"FIDELITY", IssuerGroup.FIDELITY),
        (r"FRANKLIN TEMPLETON", IssuerGroup.FRANKLIN_TEMPLETON),
        (r"AXA", IssuerGroup.AXA),
        (r"XTRACKERS|DWS", IssuerGroup.XTRACKERS),
        (r"WISDOMTREE", IssuerGroup.WISDOMTREE),
        (r"VANECK", IssuerGroup.VANECK),
        (r"GLOBAL X", IssuerGroup.GLOBAL_X),
        (r"KRANESHARES", IssuerGroup.KRANESHARES),
        (r"HANETF", IssuerGroup.HANETF),
        (r"TABULA", IssuerGroup.TABULA),
        (r"OSSIAM", IssuerGroup.OSSIAM),
        (r"ROBECO", IssuerGroup.ROBECO),
        (r"LEGAL & GENERAL", IssuerGroup.LEGAL_GENERAL),
        (r"GRANITESHARES", IssuerGroup.GRANITESHARES),
        (r"LEVERAGE SHARES", IssuerGroup.LEVERAGE_SHARES),
        (r"MELANION", IssuerGroup.MELANION),
        (r"COINSHARES", IssuerGroup.COINSHARES),
        (r"BITWISE", IssuerGroup.BITWISE),
        (r"21SHARES", IssuerGroup.TWENTYONE_SHARES),
        (r"DDA", IssuerGroup.DDA),
        (r"DEUTSCHE", IssuerGroup.DEUTSCHE_BANK),
        (r"CREDIT SUISSE", IssuerGroup.CREDIT_SUISSE),
        (r"LISTED FUNDS", IssuerGroup.LISTED_FUNDS),
        (r"EDR", IssuerGroup.EDR),
        (r"YOURINDEX", IssuerGroup.YOURINDEX),
        (r"AILIS", IssuerGroup.AILIS),
        (r"INVESTLINX", IssuerGroup.INVESTLINX),
        (r"INDEXIQ", IssuerGroup.INDEXIQ),
        (r"FAM", IssuerGroup.FAM),
    ]

    for pattern, issuer in RULES:
        if re.search(pattern, name):
            return issuer

    # ------------------------------------------------------------------
    # 3. Fallback finale
    # ------------------------------------------------------------------
    return IssuerGroup.UNKNOWN
