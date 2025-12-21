import logging
from typing import List, Dict, Any, Optional
import pandas as pd

from core.enums.datasources import DataSource
from core.enums.fields import StaticField, MarketField
from core.enums.instrument_types import InstrumentType
from core.requests.request_builder.info_request_builder import StaticRequestBuilder
from interface.api.base_api import BaseAPI

logger = logging.getLogger(__name__)


class GeneralDataAPI(BaseAPI):
    """
    API per dati *statici e generali* (ETF, futures, valute, mercati...).
    Tutti i wrapper passano da un'unica `get()` che costruisce la GeneralRequest.
    """

    # ============================================================
    # GET GENERICO
    # ============================================================
    def get(self, fields: Optional[List[StaticField | str]] = None, **kwargs) -> Dict[str, Any]:
        """
        Costruisce e invia una GeneralRequest.

        Args:
            fields: lista di StaticField o stringhe identificative dei dataset richiesti.
                    Se None, recupera un set predefinito (ETF, futures, valute, mercati...).
            **kwargs: parametri addizionali passati alla GeneralRequest
                      (es. segments, currency, ticker_root, root_tickers, swap_type, tenor, ecc.)

        Returns:
            dict[str, Any]: mapping {field_name: dataset}
        """

        req = StaticRequestBuilder.build(
            fields=fields,
            source=DataSource.ORACLE.value,
            **kwargs,
        )

        return self.client.send(req).get("GENERAL")

    # ============================================================
    # WRAPPERS SPECIFICI
    # ============================================================

    def get_etp_isins(
            self, segments: Optional[List[str]] = None, currency: Optional[str] = None,
            underlying: Optional[str] = None,
    ):
        """Lista ISIN ETP attivi."""
        if not isinstance(segments, list):
            segments = [segments]
        return self.get(["etp_isins"],
                        segments=segments,
                        currency=currency,
                        underlying=underlying)["etp_isins"]

    def get_etf_markets(self, isins: Optional[List[str]] = None) -> pd.DataFrame:
        """Mercati e valute ETF."""
        return self.get(["etf_markets"], isins=isins)["etf_markets"]

    def get_futures_identifiers(self, id_type="root_ticker") -> list:
        """Mapping ISIN ↔ CONTRACT ↔ ROOT_TICKER."""
        return [root[id_type.upper()] for root in self.get(["futures_identifiers"])["futures_identifiers"]]

    @staticmethod
    def get_instrument_types() -> list:
        """Lista di tutti i tipi di strumenti (ETF, FUTURE, FX, ecc.)."""
        return [*InstrumentType.__members__.keys()]

    @staticmethod
    def get_available_info_fields():
        return StaticField.all()

    @staticmethod
    def get_available_market_fields():
        return MarketField.all()

    def provider_healthcheck(self):
        for name, provider in self.client.providers.items():
            print(f"provider {name} is {'ON' if provider.healthcheck() else 'OFF'}")

    @staticmethod
    def get_help():
        """Stampa una guida rapida alla configurazione e all'utilizzo di BshData."""
        help_text = """
        ============================================================
        📘 BSHAPI - Guida rapida
        ============================================================

        1️⃣  CONFIGURAZIONE DELLE CREDENZIALI
        ------------------------------------------------------------
        Crea un file YAML (es: bshdata_config.yaml) con le credenziali di accesso ai provider:

            ORACLE:
              USER: my_user
              PASSWORD: my_password
              HOST: my_host
              PORT: 1521
              SERVICE: MYDB

            TIMESCALE:
              HOST: localhost
              PORT: 5432
              USER: postgres
              PASSWORD: my_password
              DATABASE: marketdata

        ------------------------------------------------------------
        2️⃣  LOGGING
        ------------------------------------------------------------
        Il logging viene configurato automaticamente da BshData.

        Parametri principali:
            - log_level: livello per la console (es. "INFO", "DEBUG")
            - log_file:  percorso del file log (default: logs/bshapi.log)
            - log_level_file: livello di dettaglio per il file

        Esempio:
            api = BshData(log_level="DEBUG", log_file="logs/dev.log")

        ------------------------------------------------------------
        3️⃣  CACHE
        ------------------------------------------------------------
        La cache globale usa joblib.Memory per evitare query ripetute.

        Parametri:
            - api.enable_cache()  → attiva la cache
            - api.disable_cache() → disattiva completamente

        Esempio:
            api = BshData(cache=True)

        ------------------------------------------------------------
        4️⃣  TIPI DI CHIAMATE DISPONIBILI
        ------------------------------------------------------------

        🔹 1. Dati di mercato (dinamici)
            api.market

        🔹 2. Dati statici / anagrafici (Oracle)
            api.info

        🔹 3. Lookup globali (senza strumento)
            api.general

        ------------------------------------------------------------
        5️⃣  Fields
        ------------------------------------------------------------
        i campi sono censite (per oracle e timescale) in Fields.yaml. i campi qui presenti sono disponibili nell.API

        ============================================================
        """
        print(help_text)
