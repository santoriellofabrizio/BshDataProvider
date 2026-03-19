import logging
from datetime import datetime, date, time
from typing import Optional, Union, List

from client import BSHDataClient
from core.instruments.instrument_factory import InstrumentFactory
from core.instruments.instruments import Instrument


class BaseAPI:
    """
    Classe base comune per tutte le sotto-API (MarketDataAPI, InfoDataAPI).

    Gestisce:
      - Normalizzazione input e liste
      - Parsing date e orari
      - Creazione coerente degli strumenti
      - Dispatch generico per RequestBuilder
      - Aggregazione base dei risultati
      - Logging e gestione client/cache
      - Retry context per fallback automatici
    """

    def __init__(
        self,
        client: Optional[BSHDataClient] = None,
        autocomplete: bool = False,
    ):
        self.client = client
        self.autocomplete = autocomplete
        self.logger = logging.getLogger(self.__class__.__name__)
        self.instrument_builder = InstrumentFactory(client=self.client)

    # ------------------------------------------------------------
    #  Parsing date / datetime / time
    # ------------------------------------------------------------

    @staticmethod
    def _parse_date(value: Union[str, date, datetime, None]) -> Optional[date]:
        """Converte una stringa o datetime in date."""
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"):
                try:
                    return datetime.strptime(value, fmt).date()
                except Exception:
                    continue
        raise ValueError(f"Formato data non riconosciuto: {value}")

    @staticmethod
    def _parse_datetime(value: Union[str, datetime, None]) -> Optional[datetime]:
        """Converte ISO stringhe o date in datetime."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, time.min)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except Exception:
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                    try:
                        return datetime.strptime(value, fmt)
                    except Exception:
                        continue
        raise ValueError(f"Formato datetime non riconosciuto: {value}")

    @staticmethod
    def _parse_time(value) -> Optional[time]:
        """Parsing robusto per orari comuni."""
        if value is None:
            return None
        if isinstance(value, time):
            return value
        if isinstance(value, datetime):
            return value.time()
        if isinstance(value, str):
            value = value.strip()
            for fmt in ("%H:%M", "%H:%M:%S", "%H.%M", "%H%M"):
                try:
                    return datetime.strptime(value, fmt).time()
                except Exception:
                    continue
        raise ValueError(f"Formato orario non riconosciuto: {value}")

    # ============================================================
    # 🔹 Costruzione strumenti e risoluzione ID
    # ============================================================

    def _build_instrument(self, **kwargs) -> Instrument:
        return self.instrument_builder.create(**kwargs)

    def _resolve_identifiers(
            self,
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            autocomplete: bool = True,
    ) -> tuple[list[str | None], list[str | None], list[str | None]]:
        """
        Risolve coerentemente la combinazione di id / isin / ticker.
        Tutte le liste restituite hanno la stessa lunghezza.
        Converte automaticamente le stringhe in MAIUSCOLO.
        """
        import re
        ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")

        # Helper
        to_list = lambda x: [x] if isinstance(x, str) else list(x) if x is not None else []

        def norm_upper_list(lst):
            return [s.upper() if isinstance(s, str) else s for s in lst]

        # Converti in liste
        ids = to_list(id)
        isins = to_list(isin)
        tickers = to_list(ticker)

        # Normalizza MAIUSCOLO
        ids = norm_upper_list(ids)
        isins = norm_upper_list(isins)
        tickers = norm_upper_list(tickers)

        # Normalizzazione lunghezze
        n = max(len(ids), len(isins), len(tickers), 1)

        def norm_list(x):
            if not x:
                return [None] * n
            if len(x) == 1 and n > 1:
                return x * n
            if len(x) != n:
                raise ValueError("Lunghezze incoerenti tra id / isin / ticker.")
            return x

        ids, isins, tickers = map(norm_list, (ids, isins, tickers))

        # Risoluzione coerente
        for i in range(n):
            i_id, i_isin, i_ticker = ids[i], isins[i], tickers[i]

            # Caso 1: manca ID → usa isin o ticker
            if not i_id:
                i_id = i_isin or i_ticker
                ids[i] = i_id

            # Caso 2: ID è ISIN → copia su isin
            if i_id and ISIN_RE.match(i_id):
                isins[i] = i_isin or i_id


        return ids, isins, tickers

    # ============================================================
    # 🔹 Logging e cache
    # ============================================================

    def log_request(self, msg: str):
        self.logger.debug(f"[{self.__class__.__name__}] {msg}")

    # ============================================================
    # 🔹 Dispatcher generico
    # ============================================================

    def _dispatch(self, *args, **kwargs):
        pass

    # ============================================================
    # 🔹 Aggregatore base (override nei figli)
    # ============================================================

    def _aggregate(self, results):
        """
        Converte un dict annidato in DataFrame/Series secondo queste regole:

        Input: {instrument: {field: value}}

        1) Tutti i value interni sono scalari
           -> DataFrame con index=instrument, columns=fields.

        2) Un solo strumento, un solo field:
           - value = dict con chiavi "date-like"
               -> pandas.Series con index=date, name=field.
           - value = dict NON "date-like"
               -> DataFrame una riga, index=instrument, columns=chiavi del dict.
           - value = list[dict]
               -> DataFrame costruito dalla lista di dict.
           - altro -> NotImplementedError.

        3) Un solo strumento, più field:
           - se esiste un solo field con value = list[dict]
               -> DataFrame dalla lista di dict di quel field.
           - se TUTTI i field hanno value = dict
               -> DataFrame una riga,
                  index=instrument,
                  columns=MultiIndex (field, chiave_interna).
           - altro -> NotImplementedError.

        4) Più strumenti, più field (non tutti scalari)
           -> NotImplementedError.

        5) Più strumenti, un solo field:
           - se value per ogni strumento è dict con chiavi "date-like"
               -> DataFrame con index=date, columns=instrument.
           - altro -> NotImplementedError.
        """
        import numbers
        import numpy as np
        import pandas as pd
        from datetime import date, datetime, time

        if not results:
            return pd.DataFrame()

        def is_scalar(val) -> bool:
            if val is None:
                return True
            if isinstance(val, (str, numbers.Number, np.generic)):
                return True
            if isinstance(val, (date, datetime, time, pd.Timestamp, np.datetime64)):
                return True
            return False

        def is_date_key(k) -> bool:
            return isinstance(k, (date, datetime, pd.Timestamp, np.datetime64))

        def normalize_date_index(s: pd.Series) -> pd.Series:
            """
            Normalizza indici datetime.date → DatetimeIndex per evitare dtype='object'.

            Questo previene duplicati quando si fa concat di Series con chiavi date.
            Standard BshDataProvider:
            - datetime.date → DatetimeIndex (display: '2025-12-05')
            - datetime/Timestamp → DatetimeIndex con time
            """
            if len(s.index) == 0:
                return s

            first_key = s.index[0]

            # datetime.date → DatetimeIndex normalizzato
            if isinstance(first_key, date) and not isinstance(first_key, datetime):
                s.index = pd.DatetimeIndex(s.index)
                return s

            # datetime/Timestamp → assicura DatetimeIndex
            if isinstance(first_key, (datetime, pd.Timestamp)):
                if not isinstance(s.index, pd.DatetimeIndex):
                    s.index = pd.to_datetime(s.index)
                return s

            # Altri tipi date-like (np.datetime64, ecc)
            if is_date_key(first_key):
                s.index = pd.to_datetime(s.index)

            return s

        # -----------------------------------------------------------
        # 1) Tutti scalari -> tabella semplice
        # -----------------------------------------------------------
        all_scalar = True
        for fields in results.values():
            if not isinstance(fields, dict):
                all_scalar = False
                break
            if not all(is_scalar(v) for v in fields.values()):
                all_scalar = False
                break

        if all_scalar and results:
            return pd.DataFrame.from_dict(results, orient="index")

        # -----------------------------------------------------------
        # Setup info strumenti / campi
        # -----------------------------------------------------------
        instruments = list(results.keys())
        field_counts = [
            len(v) if isinstance(v, dict) else 0
            for v in results.values()
        ]

        is_single_instrument = (len(instruments) == 1)
        is_single_field = (len(field_counts) > 0 and all(c == 1 for c in field_counts))
        has_multi_field = any(c > 1 for c in field_counts)

        # -----------------------------------------------------------
        # 2) Un solo strumento, un solo field
        # -----------------------------------------------------------
        if is_single_instrument and is_single_field:
            instr, fields = next(iter(results.items()))
            if not isinstance(fields, dict) or len(fields) != 1:
                raise NotImplementedError("Struttura inattesa per singolo strumento/singolo field.")

            field, val = next(iter(fields.items()))

            # dict -> time series o dict piatto
            if isinstance(val, dict):
                if val and all(is_date_key(k) for k in val.keys()):
                    # Serie temporale: Series con index=date, name=field
                    s = pd.Series(val, name=field)
                    s = normalize_date_index(s)  # ✅ FIX: normalizza indice
                    s.index.name = "date"
                    return s
                else:
                    # Dict piatto: una riga, colonne = chiavi del dict
                    return pd.DataFrame({k: [v] for k, v in val.items()}, index=[instr])

            # lista di dict -> tabella diretta (es. PCF/FX)
            if isinstance(val, list) and val and isinstance(val[0], dict):
                return pd.DataFrame(val)

            # altro non supportato
            raise NotImplementedError(
                "Single instrument & single field con questo tipo di valore non è supportato."
            )

        # -----------------------------------------------------------
        # 3) Un solo strumento, più field
        # -----------------------------------------------------------
        if is_single_instrument and has_multi_field:
            instr, data = next(iter(results.items()))
            if not isinstance(data, dict):
                raise NotImplementedError("Single instrument con dati non-dict non supportato.")

            # 3a) Se esiste esattamente un field list[dict] -> usa quello
            list_dict_fields = [
                f for f, v in data.items()
                if isinstance(v, list) and v and isinstance(v[0], dict)
            ]
            if list_dict_fields:
                if len(list_dict_fields) > 1:
                    raise NotImplementedError(
                        "Più field con list[dict] per singolo strumento non supportati."
                    )
                f = list_dict_fields[0]
                return pd.DataFrame(data[f])

            # 3b) Tutti i field sono dict -> flatten MultiIndex (field, chiave_interna)
            if all(isinstance(v, dict) for v in data.values()):
                flat = {}
                for f, dct in data.items():
                    for k, v in dct.items():
                        flat[(k, f)] = v

                cols = pd.MultiIndex.from_tuples(flat.keys(), names=["field", "key"])
                df = pd.DataFrame([[flat[c] for c in flat.keys()]], index=[instr])
                df.columns = cols
                return df

            # altro non definito
            raise NotImplementedError(
                "Single instrument & multiple fields con tipi misti non supportato."
            )

        # -----------------------------------------------------------
        # 4) Multi-instrument & multi-field (non tutti scalari)
        # -----------------------------------------------------------
        if len(instruments) > 1 and has_multi_field:
            # Se fossimo stati tutti scalari, saremmo già usciti al punto (1)
            frames = []
            for ticker, sides in results.items():
                for side, ts in sides.items():
                    s = pd.Series(ts, name=(ticker, side))
                    s = normalize_date_index(s)  # ✅ FIX: normalizza indice
                    frames.append(s)
            df = pd.concat(frames, axis=1)
            df.columns = pd.MultiIndex.from_tuples(df.columns, names=["id", "field"])
            df = df.sort_index()
            return df

        # -----------------------------------------------------------
        # 5) Multi-instrument & single field
        # -----------------------------------------------------------
        if len(instruments) > 1 and is_single_field:
            first_instr, first_fields = next(iter(results.items()))
            field_name = next(iter(first_fields.keys()))

            # mappa instr -> value
            values = {}
            for instr, fields in results.items():
                if not isinstance(fields, dict) or len(fields) != 1:
                    raise NotImplementedError("Struttura non consistente tra strumenti.")
                f, v = next(iter(fields.items()))
                if f != field_name:
                    raise NotImplementedError(
                        "Field diversi tra strumenti non supportati in questo caso."
                    )
                values[instr] = v

            # 5a) Tutti dict con chiavi date-like -> DataFrame date x instrument
            if all(isinstance(v, dict) for v in values.values()):
                sample = next(iter(values.values()))
                if sample and all(is_date_key(k) for k in sample.keys()):
                    series_list = []
                    for instr, dct in values.items():
                        s = pd.Series(dct, name=instr)
                        s = normalize_date_index(s)  # ✅ FIX: normalizza indice
                        series_list.append(s)

                    df = pd.concat(series_list, axis=1)
                    df.index.name = "date"
                    df.columns.name = "instrument"
                    return df

            # 5b) Tutti list[dict] -> Concatena tutte le liste e crea DataFrame
            if all(isinstance(v, list) for v in values.values()):
                # Concatena tutte le liste (anche se vuote)
                all_data = []
                for instr, data_list in values.items():
                    all_data.extend(data_list)

                if all_data:
                    return pd.DataFrame(all_data)
                else:
                    # Tutte liste vuote
                    return pd.DataFrame()

            # 5c) Tutti dict flat scalari -> DataFrame con strumenti come colonne
            df = {}
            for id, inner in results.items():
                for field, data in inner.items():
                    if isinstance(data, dict):
                        if not data or all(is_scalar(k) and is_scalar(v) for k, v in data.items()):
                            df[id] = data
                        else:
                            raise NotImplementedError("inner data is not flat")

            return pd.DataFrame(df)

        # -----------------------------------------------------------
        # Fallback
        # -----------------------------------------------------------
        return pd.DataFrame(next(iter(results.values())))