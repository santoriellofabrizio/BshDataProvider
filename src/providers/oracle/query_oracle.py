import datetime as dt
import logging
from collections import defaultdict
from typing import List, Tuple, Optional, Dict, Any, Union

from dateutil.utils import today

from sfm_dbconnections.OracleConnection import OracleConnection

from core.utils.memory_provider import cache_bsh_data

logger = logging.getLogger(__name__)


class QueryOracle:
    """
    Wrapper centralizzato per tutte le query Oracle.
    Gestisce ETF, Futures, Swaps, mercati e lookup anagrafici.
    Restituisce sempre dati come dizionari o liste di dizionari.
    """

    # ===========================================================
    # INIT
    # ===========================================================
    def __init__(self, oracle_connection: OracleConnection):
        if not isinstance(oracle_connection, OracleConnection):
            raise TypeError("QueryOracle richiede un'istanza di OracleConnection")
        self.conn = oracle_connection
        logger.debug("QueryOracle initialized with active OracleConnection")

    # ===========================================================
    # HELPERS
    # ===========================================================
    def _in_clause(self, prefix: str, values: List[str]) -> Tuple[str, Dict[str, str]]:
        """Crea una clausola SQL sicura per IN (:id0, :id1, ...)"""
        placeholders = ", ".join([f":{prefix}{i}" for i in range(len(values))])
        params = {f"{prefix}{i}": v for i, v in enumerate(values)}
        return placeholders, params

    # ===========================================================
    # FUTURES
    # ===========================================================

    def get_futures_data(self, ticker_root: Optional[str] = None, isin: Optional[str] = None) -> List[Dict[str, Any]]:
        query = """
            SELECT fi.isin,
                   fr.ticker || 'A' AS active_isin,
                   fi.ticker AS contract,
                   fr.ticker || 'A ' || fr.bbg_type AS active_contract,
                   fr.*
            FROM AF_DATAMART_DBA.FUTURES_ROOTS fr
            INNER JOIN AF_DATAMART_DBA.FUTURES_INSTRUMENTS fi
                ON fi.ticker_root = fr.ticker
            WHERE 1=1
        """
        params = {}
        if ticker_root:
            query += " AND fr.ticker = :ticker_root"
            params["ticker_root"] = ticker_root
        if isin:
            query += " AND fi.isin = :isin"
            params["isin"] = isin
        query += " ORDER BY fi.ticker_root"

        data, cols = self.conn.execute_query(query, params)
        return [dict(zip(cols, row)) for row in data]

    def get_futures_identifiers(self) -> List[Dict[str, Any]]:
        query = """
            SELECT DISTINCT fi.isin, fi.ticker AS contract, fr.ticker AS root_ticker
            FROM AF_DATAMART_DBA.FUTURES_INSTRUMENTS fi
            JOIN AF_DATAMART_DBA.FUTURES_ROOTS fr
                ON fi.ticker_root = fr.ticker
            ORDER BY fr.ticker, fi.ticker
        """
        data, cols = self.conn.execute_query(query)
        return [dict(zip(cols, row)) for row in data]

    def get_future_field_by_roots(
            self,
            field: str,
            root_tickers: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Ritorna TICKER + il field richiesto dalla tabella FUTURES_ROOTS,
        filtrando opzionalmente per una lista di root_tickers (TICKER).
        """

        valid_fields = {
            "TICKER",
            "BBG_TYPE",
            "EXCH_SYMBOL",
            "DESCRIPTION",
            "UNDERLYING_TYPE",
            "UNDERLYING",
            "COUNTRY",
            "GEOGRAPHICAL_AREA",
            "CURRENCY",
            "CONTRACT_SIZE",
            "UNDERLYING_PRICE_MULTIPLIER",
            "ECONOMY",
            "CFI_CODE",
            "DELIVERY_TYPE",
            "REFERENCE_MARKET",
            "CALENDAR",
            "VALID_FROM",
        }

        field = field.upper()
        if field not in valid_fields:
            raise ValueError(f"Invalid field '{field}'. Must be one of: {', '.join(sorted(valid_fields))}")

        # includo sempre TICKER per chiave → se chiedi TICKER ti arriva solo TICKER
        select_fields = "TICKER" if field == "TICKER" else f"TICKER, {field}"

        query = f"""
            SELECT {select_fields}
            FROM AF_DATAMART_DBA.FUTURES_ROOTS
            WHERE 1=1
        """

        params: Dict[str, Any] = {}
        if root_tickers:
            placeholders, params = self._in_clause("TICKER", root_tickers)
            query += f" AND TICKER IN ({placeholders})"

        query += " ORDER BY TICKER"

        data, cols = self.conn.execute_query(query, params)
        return [dict(zip(cols, row)) for row in data]

    def get_equity_field(
            self,
            isin: Optional[List[str]],
            ticker: Optional[List[str]],
            fields: Optional[List[str]],
            market: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Union[str, float]]]:
        """
        Ottiene field specifici per equity.

        Args:
            isin: Lista di ISIN
            ticker: Lista di ticker
            fields: Lista di field da recuperare
            market: Lista di market codes (opzionale)

        Returns:
            {identifier: {field: value_or_None, ...}}
            Garantisce presenza di tutti gli identifier richiesti.
        """
        if not fields:
            return {}

        # Normalizzazione input
        market = list(set([m for m in market if m] if market else []))
        isin = [i for i in isin if i] if isin else []
        ticker = [t for t in ticker if t] if ticker else []

        params = {}

        # Determina se usare ISIN o TICKER come chiave
        if not isin and not ticker:
            # Nessun filtro: scarica tutto (caso raro)
            instrument_filter = "1 = 1"
            use_isin = True
            identifiers = []
        else:
            use_isin = bool(isin)
            if use_isin:
                identifiers = isin
                placeholders = ", ".join([f":isin_{i}" for i in range(len(isin))])
                instrument_filter = f"ei.ISIN IN ({placeholders})"
                for i, val in enumerate(isin):
                    params[f"isin_{i}"] = val
            else:
                identifiers = ticker
                placeholders = ", ".join([f":ticker_{i}" for i in range(len(ticker))])
                instrument_filter = f"ee.TICKER IN ({placeholders})"
                for i, val in enumerate(ticker):
                    params[f"ticker_{i}"] = val

        # Market filter
        market_filter = ""
        if market:
            placeholders = ", ".join([f":market_{i}" for i in range(len(market))])
            market_filter = f"AND ee.EXCHANGE_CODE IN ({placeholders})"
            for i, val in enumerate(market):
                params[f"market_{i}"] = val

        # Valid fields
        valid_fields = {
            "ISIN", "PRIMARY_TICKER", "DESCRIPTION",
            "PRIMARY_EXCHANGE_CODE", "EXCHANGE_CODE",
            "CURRENCY", "TICKER"
        }

        selected_fields = [f for f in fields if f in valid_fields]
        if not selected_fields:
            logger.warning(f"No valid fields selected from: {fields}")
            return {}

        # Query
        query = f"""
            SELECT *
            FROM EQUITIES_INSTRUMENTS ei
            JOIN EXCHANGE_INSTRUMENTS ee ON ee.INSTRUMENT_ID = ei.ID
            WHERE {instrument_filter}
            {market_filter}
        """

        data, cols = self.conn.execute_query(query, params)

        key = "ISIN" if use_isin else "TICKER"

        # 🆕 Inizializza con TUTTI gli identifier richiesti
        if identifiers:
            result = {
                identifier: {field.upper(): None for field in selected_fields}
                for identifier in identifiers
            }
        else:
            result = {}

        # 🆕 Popola solo gli identifier con dati
        for row in data:
            record = dict(zip(cols, row))
            identifier = record.get(key) or record.get(key.lower())

            if identifier:
                # Se non avevamo filtri, aggiungi dinamicamente
                if not identifiers:
                    result[identifier] = {field.upper(): None for field in selected_fields}

                if identifier in result:
                    for f in selected_fields:
                        val = (record.get(f) or
                               record.get(f.upper()) or
                               record.get(f.lower()))
                        if val is not None:
                            result[identifier][f.upper()] = val

        return result

    def get_equity_data(
            self) -> Tuple[list, list]:
        query = f"""
            SELECT *
            FROM EQUITIES_INSTRUMENTS ei
            JOIN EXCHANGE_INSTRUMENTS ee ON ee.INSTRUMENT_ID = ei.ID
        """
        return self.conn.execute_query(query)


    # ===========================================================
    # SWAPS
    # ===========================================================
    @cache_bsh_data
    def get_swap_data(self, swap_type: Optional[str] = None, tenor: Optional[str] = None) -> List[Dict[str, Any]]:
        query = """
            SELECT s.ticker, s.tenor, s.settlement_days, s.swap_type
            FROM AF_DATAMART_DBA.SWAPS s
            WHERE 1=1
        """
        params = {}
        if swap_type:
            query += " AND UPPER(s.swap_type) = UPPER(:swap_type)"
            params["swap_type"] = swap_type
        if tenor:
            query += " AND s.tenor = :tenor"
            params["tenor"] = tenor

        query += " ORDER BY s.ticker"
        data, cols = self.conn.execute_query(query, params)
        swaps = [dict(zip(cols, row)) for row in data]

        # Aggiunge manualmente gli swap USA
        us_swaps = [
            {"TICKER": f"USSWIT{t}", "TENOR": f"{t}Y", "SETTLEMENT_DAYS": 2, "SWAP_TYPE": "INTEREST RATE SWAP"}
            for t in [1, 2, 5, 10, 15, 20, 25, 30]
        ]
        return swaps + us_swaps

    @cache_bsh_data
    def get_cdx_fields(self, tickers, fields):
        """
        Restituisce più campi per più ticker CDXINDEX, in forma:
        {
            field1: { ticker1: value, ticker2: value },
            field2: { ticker1: value, ticker2: value },
            ...
        }
        """

        # Normalizzazione input
        if isinstance(tickers, str):
            tickers = [tickers]
        if isinstance(fields, str):
            fields = [fields]

        tickers = [t.upper() for t in tickers]
        fields = [f.upper() for f in fields]

        # Query bulk (molto più efficiente)
        query = f"""
            SELECT 
                ci.ticker_root,
                ci.index_name,
                ci.currency,
                ci.bbg_type,
                ci.series_start_date,
                ci.tenor
            FROM AF_DATAMART_DBA.CDS_INDEXES_ROOTS ci
            WHERE ci.ticker_root IN ({','.join([':t' + str(i) for i in range(len(tickers))])})
        """

        params = {f"t{i}": tickers[i] for i in range(len(tickers))}
        data, columns = self.conn.execute_query(query, params)

        # Organizzazione risultato
        # row_by_ticker[ticker] = {"ticker_root": ..., "currency": ..., ...}
        row_by_ticker = {
            row[0].upper(): dict(zip(columns, row))
            for row in data
        }

        # Output nella struttura richiesta
        out = {f: {} for f in fields}

        for f in fields:
            for t in tickers:
                value = row_by_ticker.get(t, {}).get(f.upper())
                out[f][t] = value

        return out

    @cache_bsh_data
    def get_cdx_data(self, ticker: Optional[str] = None, tenor: Optional[str] = None,
                     bbg_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Restituisce la lista dei CDXINDEX dalla tabella CDS_INDEXES_ROOTS,
        in forma flat come get_swap_data().

        Esempio output:
        [
            {
                "TICKER_ROOT": "CDX_NA_IG",
                "INDEX_NAME": "...",
                "CURRENCY": "USD",
                "BBG_TYPE": "...",
                "SERIES_START_DATE": date(...),
                "TENOR": "5Y"
            },
            ...
        ]
        """

        query = """
                SELECT ci.ticker_root, \
                       ci.index_name, \
                       ci.currency, \
                       ci.bbg_type, \
                       ci.series_start_date, \
                       ci.tenor
                FROM AF_DATAMART_DBA.CDS_INDEXES_ROOTS ci
                WHERE 1 = 1 \
                """

        params = {}

        if ticker:
            query += " AND UPPER(ci.ticker_root) = :ticker"
            params["ticker"] = ticker.upper()

        if tenor:
            query += " AND UPPER(ci.tenor) = :tenor"
            params["tenor"] = tenor.upper()

        if bbg_type:
            query += " AND UPPER(ci.bbg_type) = :bbg_type"
            params["bbg_type"] = bbg_type.upper()

        query += " ORDER BY ci.ticker_root"

        data, cols = self.conn.execute_query(query, params)

        # normalizza colonne in maiuscolo (come swap)
        cols = [c.upper() for c in cols]

        return [dict(zip(cols, row)) for row in data]

    @cache_bsh_data
    def get_swap_field(self, ticker, fields) -> Dict[str, Any]:

        raise NotImplementedError
        placeholders = ", ".join([f":ticker_{i}" for i in range(len(ticker))])
        instrument_filter = f"AND s.TICKER IN ({placeholders})"
        for i, val in enumerate(ticker):
                params[f"ticker_{i}"] = val

        query = f"""
            SELECT s.ticker, s.tenor, s.settlement_days, s.swap_type
            FROM AF_DATAMART_DBA.SWAPS s
            WHERE 1=1 {instrument_filter}
        """


        query += " ORDER BY s.ticker"
        data, cols = self.conn.execute_query(query, params)
        swaps = [dict(zip(cols, row)) for row in data]

        # Aggiunge manualmente gli swap USA
        us_swaps = [
            {"TICKER": f"USSWIT{t}", "TENOR": f"{t}Y", "SETTLEMENT_DAYS": 2, "SWAP_TYPE": "INTEREST RATE SWAP"}
            for t in [1, 2, 5, 10, 15, 20, 25, 30]
        ]
        results = {}
        for f in fields:
            results[f.lower()] = (swaps + us_swaps).get(f.upper())
        return swaps + us_swaps
    # ===========================================================
    # ETF TER
    # ===========================================================

    @cache_bsh_data
    def get_rates_index_data(
            self,
            ticker: Optional[str] = None,
            family: Optional[str] = None,
            tenor: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Restituisce la lista degli Interest Rate Index (EURIBOR, ESTR, SOFR...) e
        delle loro declinazioni (1M, 3M, ON...), in formato flat.

        Output esemplificativo:
        [
            {
                "TICKER": "EUR003M",
                "FAMILY": "EURIBOR",
                "TENOR": "3M",
                "COMPOUNDING": "NO_COMPOUNDING",
                "CURRENCY": "EUR",
                "DAY_COUNT": "TE",
                "BUSINESS_DAY_CONVENTION": "MODIFIED_FOLLOWING",
                "EOM": "EOM"
            },
            ...
        ]
        """

        query = """
            SELECT
                idx.index_name AS ticker,
                idx.index_family AS family,
                idx.tenor AS tenor,
                fam.compounding AS compounding,
                fam.currency AS currency,
                fam.calendar_code AS day_count,
                fam.business_day_convention AS business_day_convention,
                fam.end_of_month_convention AS eom
            FROM AF_DATAMART_DBA.INTEREST_RATE_INDEXES idx
            INNER JOIN AF_DATAMART_DBA.INTEREST_RATE_INDEX_FAMILY fam
                ON fam.index_family = idx.index_family
            WHERE 1 = 1
        """

        params = {}

        if ticker:
            query += " AND UPPER(idx.index_name) = :ticker"
            params["ticker"] = ticker.upper()

        if family:
            query += " AND UPPER(idx.index_family) = :family"
            params["family"] = family.upper()

        if tenor:
            query += " AND UPPER(idx.tenor) = :tenor"
            params["tenor"] = tenor.upper()

        query += " ORDER BY idx.index_name"

        data, cols = self.conn.execute_query(query, params)

        cols = [c.upper() for c in cols]

        return [dict(zip(cols, row)) for row in data]

    @cache_bsh_data
    def get_etf_ter(
            self,
            isin_list: List[str],
            day: Optional[Union[str, dt.date]] = None
    ) -> Dict[str, Dict[str, Optional[float]]]:
        """
        Ottiene il TER (Total Expense Ratio) per una lista di ISIN.

        Args:
            isin_list: Lista di ISIN da interrogare
            day: Data di riferimento (default: oggi)

        Returns:
            Dict con formato: {isin: {"TER": value_or_None}}
            Garantisce sempre la presenza di ogni ISIN richiesto.
            Se un ISIN non ha TER disponibile, il valore sarà None.

        Example:
            >>> query.get_etf_ter(["IE00B4L5Y983", "INVALID_ISIN"])
            {
                "IE00B4L5Y983": {"TER": 0.005},
                "INVALID_ISIN": {"TER": None}
            }
        """
        if not isin_list:
            return {}

        placeholders, params = self._in_clause("id", isin_list)
        day = day or today()
        params["ref_date"] = day if isinstance(day, str) else day.strftime("%d-%m-%Y")

        query = f"""
            SELECT BSH_ID, EXPENSE_RATIO AS TER
            FROM PCF_DAILY_INFO
            WHERE BSH_ID IN ({placeholders})
              AND REF_DATE = (
                  SELECT MAX(REF_DATE)
                  FROM PCF_DAILY_INFO
                  WHERE BSH_ID IN ({placeholders})
                    AND REF_DATE <= TO_DATE(:ref_date, 'DD-MM-YYYY')
              )
        """
        data, _ = self.conn.execute_query(query, params)

        # Fallback su tabella storica se nessun dato trovato
        if not data:
            query_fb = f"""
                SELECT BSH_ID, EXPENSE_RATIO AS TER
                FROM PCF_DAILY_INFO
                WHERE BSH_ID IN ({placeholders})
                  AND REF_DATE = (
                      SELECT MAX(REF_DATE)
                      FROM PCF_DAILY_INFO
                      WHERE BSH_ID IN ({placeholders})
                  )
            """
            data, _ = self.conn.execute_query(query_fb, params)

        # 🆕 Inizializza con TUTTI gli ISIN richiesti
        result = {isin: {"TER": None} for isin in isin_list}

        # 🆕 Popola solo gli ISIN che hanno dati
        for bsh_id, ter_value in data:
            if bsh_id in result:
                result[bsh_id]["TER"] = ter_value

        return result
    # ===========================================================
    # ETF FX_COMPOSITION
    # ===========================================================
    @cache_bsh_data
    def get_etf_fx(
            self,
            isin_list: List[str],
            day: dt.date,
            currency: Optional[str] = None,
            **kwargs,
    ) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        Ottiene la composizione FX per una lista di ISIN.

        Args:
            isin_list: Lista di ISIN
            day: Data di riferimento
            currency: Filtro opzionale per valuta specifica
            **kwargs: Parametri aggiuntivi (fx_fxfwrd mode)

        Returns:
            {isin: {"FX_COMPOSITION": {currency: weight, ...}}}
            Garantisce presenza di tutti gli ISIN, anche se senza dati.
        """
        if not isin_list:
            return {}

        day = day or dt.date.today()
        placeholders, params = self._in_clause("id", isin_list)
        params["ref_date"] = day.strftime("%d-%m-%Y")
        table_name = "PCF_FX_COMPOSITION_ONLINE" if (today().date() - day).days <= 28 else "PCF_FX_COMPOSITION"

        query = f"""
            SELECT BSH_ID, CURRENCY, WEIGHT, WEIGHT_FX_FORWARD, REF_DATE
            FROM {table_name}
            WHERE BSH_ID IN ({placeholders})
              AND REF_DATE = (
                  SELECT MAX(REF_DATE)
                  FROM {table_name}
                  WHERE BSH_ID IN ({placeholders})
                    AND REF_DATE <= TO_DATE(:ref_date, 'DD-MM-YYYY')
              )
        """
        if currency:
            query += " AND CURRENCY = :currency"
            params["currency"] = currency

        data, cols = self.conn.execute_query(query, params)

        # 🆕 Inizializza con tutti gli ISIN richiesti
        fx_dict = {
            isin: {"FX_COMPOSITION": {}, "FX_FORWARD": {}}
            for isin in isin_list
        }

        # 🆕 Popola solo gli ISIN con dati
        for bsh_id, curr, w, wf, _ in data:
            if bsh_id in fx_dict:
                fx_dict[bsh_id]["FX_COMPOSITION"][curr] = w or 0
                fx_dict[bsh_id]["FX_FORWARD"][curr] = wf or 0

        # Gestione modalità output
        mode = kwargs.get("fx_fxfwrd", "both").lower()
        if mode == "fx":
            return {k: {"FX_COMPOSITION": v["FX_COMPOSITION"]} for k, v in fx_dict.items()}
        elif mode == "fxfwrd":
            return {k: {"FX_COMPOSITION": v["FX_FORWARD"]} for k, v in fx_dict.items()}
        elif mode == "both":
            merged = {}
            for isin, comp in fx_dict.items():
                merged_fx = {
                    c: comp["FX_COMPOSITION"].get(c, 0) + comp["FX_FORWARD"].get(c, 0)
                    for c in set(comp["FX_COMPOSITION"]) | set(comp["FX_FORWARD"])
                }
                merged[isin] = {"FX_COMPOSITION": merged_fx}
            return merged
        else:
            raise ValueError("Invalid fx_fxfwrd parameter (use 'fx', 'fxfwrd', or 'both').")

    # ===========================================================
    # ETF PCF_COMPOSITION
    # ===========================================================
    @cache_bsh_data
    def get_etf_pcf(
            self,
            isin_list: Optional[list] = None,
            reference_date: Optional[Union[dt.date, str]] = None,
            include_cash: bool = False,
            columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Ottiene la composizione PCF per una lista di ISIN.

        Args:
            isin_list: Lista di ISIN (opzionale, se None ritorna tutti)
            reference_date: Data di riferimento (default: ultima disponibile)
            include_cash: Se True, include componenti cash
            columns: Lista colonne da includere (default: tutte)

        Returns:
            {isin: {"pcf_composition": [list of dicts]}}
            Garantisce presenza di tutti gli ISIN richiesti, anche se senza dati.
        """
        params = {}
        isin_filter, date_filter = "", ""

        if isin_list:
            isin_filter = " AND BSH_ID_ETF IN ({})".format(
                ", ".join([f":isin_{i}" for i, _ in enumerate(isin_list)])
            )
            for i, isin in enumerate(isin_list):
                params[f"isin_{i}"] = isin

        if reference_date:
            date_filter = " AND REF_DATE = TO_DATE(:ref_date, 'YYYY-MM-DD')"
            params["ref_date"] = reference_date if isinstance(reference_date, str) else reference_date.strftime(
                "%Y-%m-%d")

        if reference_date:
            query = f"""
                SELECT c.*
                FROM AF_DATAMART_DBA.PCF_COMPOSITION_ONLINE c
                WHERE 1=1 {isin_filter} {date_filter}
                ORDER BY c.REF_DATE, c.BSH_ID_ETF, c.BSH_ID_COMP
            """
        else:
            query = f"""
                SELECT c.*
                FROM AF_DATAMART_DBA.PCF_COMPOSITION_ONLINE c
                WHERE (c.BSH_ID_ETF, c.REF_DATE) IN (
                    SELECT BSH_ID_ETF, MAX(REF_DATE)
                    FROM AF_DATAMART_DBA.PCF_COMPOSITION_ONLINE
                    WHERE 1=1 {isin_filter}
                    GROUP BY BSH_ID_ETF
                )
                ORDER BY c.REF_DATE, c.BSH_ID_ETF, c.BSH_ID_COMP
            """

        data, cols = self.conn.execute_query(query, params)
        results = [dict(zip(cols, row)) for row in data]

        if include_cash:
            cash_query = f"""
                SELECT c.REF_DATE, i.isin AS ISIN_ETF, 'CASH' AS ISIN_COMP,
                       c.currency AS DESCRIPTION, c.weight_nav, c.weight_risk,
                       c.quantity AS N_INSTRUMENTS
                FROM af_pcf.etf_cash_components c
                JOIN af_pcf.instruments i ON c.instrument_id_etf = i.id
                WHERE 1=1 {isin_filter.replace('BSH_ID_ETF', 'i.isin')}
            """
            if reference_date:
                cash_query += " AND c.REF_DATE = TO_DATE(:ref_date, 'YYYY-MM-DD')"
            data_cash, cols_cash = self.conn.execute_query(cash_query, params)
            results += [dict(zip(cols_cash, row)) for row in data_cash]

        if columns:
            cols_lower = [c.lower() for c in columns]
            results = [{k: v for k, v in row.items() if k.lower() in cols_lower} for row in results]

        # 🆕 Inizializza con tutti gli ISIN richiesti
        if isin_list:
            grouped_results = {isin: {"pcf_composition": []} for isin in isin_list}
        else:
            grouped_results = defaultdict(lambda: {"pcf_composition": []})

        # 🆕 Popola solo gli ISIN con dati
        for row in results:
            key = row.get("BSH_ID_ETF") or row.get("ISIN_ETF")
            if key:
                if isin_list and key in grouped_results:
                    grouped_results[key]["pcf_composition"].append(row)
                elif not isin_list:
                    grouped_results[key]["pcf_composition"].append(row)

        return dict(grouped_results)
    # ===========================================================
    # ETF NAV
    # ===========================================================
    @cache_bsh_data
    def get_etf_nav(
            self,
            isins: List[str],
            start: dt.date,
            end: dt.date = None,
            corr_id_mapping: Dict[str, str] = {}
    ) -> Dict[str, Dict[str, Dict]]:
        """
        Ottiene i valori NAV storici per una lista di ISIN.

        Args:
            isins: Lista di ISIN
            start: Data inizio
            end: Data fine (default: oggi)
            corr_id_mapping: Mappatura ISIN → correlation_id alternativo

        Returns:
            {isin: {"NAV": {date: value, ...}}}
            Garantisce presenza di tutti gli ISIN, anche se senza dati.
        """
        if end is None:
            end = today()

        isin_values = "', '".join(isins)
        query = f"""
            SELECT REF_DATE, BSH_ID, NAV, NAV_CCY
            FROM AF_PCF.PCF_DAILY_INFO
            WHERE BSH_ID IN ('{isin_values}')
              AND REF_DATE >= TO_DATE('{start.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
              AND REF_DATE <= TO_DATE('{end.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
            ORDER BY BSH_ID, REF_DATE
        """
        db, cols = self.conn.execute_query(query)

        # Inizializza con tutti gli ISIN richiesti
        result = {
            corr_id_mapping.get(isin, isin): {"NAV": {}}
            for isin in isins
        }

        # Popola solo gli ISIN con dati
        if db:
            for ref_date, bsh_id, nav, nav_ccy in db:
                identifier = corr_id_mapping.get(bsh_id, bsh_id)
                if identifier in result:
                    result[identifier]["NAV"][ref_date] = nav

        return result
    # ===========================================================
    # ETF DIVIDENDS
    # ===========================================================
    @cache_bsh_data
    def get_etf_dividends(
            self,
            isins: List[str],
            start: dt.date,
            end: dt.date = None,
            corr_id_mapping: Dict[str, str] = {}
    ) -> Dict[str, Dict[str, Dict]]:
        """
        Ottiene i dividendi storici per una lista di ISIN.

        Args:
            isins: Lista di ISIN
            start: Data inizio
            end: Data fine (default: oggi)
            corr_id_mapping: Mappatura ISIN → correlation_id alternativo

        Returns:
            {isin: {"DIVIDEND_AMOUNT": {date: value, ...}}}
            Garantisce presenza di tutti gli ISIN, anche se senza dati.
        """
        if end is None:
            end = today()

        isin_values = "', '".join(isins)
        query = f"""
            SELECT REF_DATE, BSH_ID, DIVIDEND_AMOUNT
            FROM AF_PCF.PCF_DAILY_INFO
            WHERE BSH_ID IN ('{isin_values}')
              AND REF_DATE >= TO_DATE('{start.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
              AND REF_DATE <= TO_DATE('{end.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
              AND DIVIDEND_AMOUNT IS NOT NULL
            ORDER BY BSH_ID, REF_DATE
        """
        db, cols = self.conn.execute_query(query)

        # 🆕 Inizializza con tutti gli ISIN richiesti
        result = {
            corr_id_mapping.get(isin, isin): {"DIVIDEND_AMOUNT": {}}
            for isin in isins
        }

        # 🆕 Popola solo gli ISIN con dati
        if db:
            for ref_date, bsh_id, amount in db:
                identifier = corr_id_mapping.get(bsh_id, bsh_id)
                if identifier in result:
                    result[identifier]["DIVIDEND_AMOUNT"][ref_date] = amount

        return result
    # ===========================================================
    # ETF MARKETS
    # ===========================================================
    @cache_bsh_data
    def get_etf_markets(self, isin_list: List[str]) -> List[Dict[str, Any]]:
        if not isin_list:
            return []
        placeholders, params = self._in_clause("id", isin_list)
        query = f"""
            SELECT DISTINCT i.isin, ei.exchange_code, ei.currency
            FROM AF_DATAMART_DBA.INSTRUMENTS i
            JOIN AF_DATAMART_DBA.EXCHANGE_INSTRUMENTS ei
                ON i.id = ei.instrument_id
            WHERE i.isin IN ({placeholders})
              AND ei.status = 'ACTV'
            ORDER BY i.isin
        """
        data, cols = self.conn.execute_query(query, params)
        return [dict(zip(cols, row)) for row in data]

    # ===========================================================
    # LOOKUPS
    # ===========================================================

    def _prepare_placeholders_and_params(self, keys: List[str], prefix: str = "t") -> Tuple[str, Dict[str, str]]:
        placeholders = ", ".join(f":{prefix}{i}" for i in range(len(keys)))
        params = {f"{prefix}{i}": k for i, k in enumerate(keys)}
        return placeholders, params

    @cache_bsh_data
    def _map_ticker_isin(
            self,
            keys: Union[str, List[str]],
            type: Optional[str],
            direction: str,
            market_code: Optional[str] = None
    ) -> Dict[str, Optional[str]]:
        keys = [keys] if isinstance(keys, str) else keys
        placeholders, params = self._prepare_placeholders_and_params(keys)

        market_filter = ""
        if market_code:
            params["market_code"] = market_code
            market_filter = "AND e.exchange_code = :market_code"

        if type == "ETP":
            query = f"""
                SELECT ticker, isin 
                FROM af_datamart_dba.etps_instruments 
                WHERE {"ticker" if direction == "ticker_to_isin" else "isin"} IN ({placeholders})
            """
        elif type == "STOCK":
            query = f"""
                SELECT e.ticker, i.isin
                FROM af_datamart_dba.EXCHANGE_INSTRUMENTS e
                JOIN af_datamart_dba.INSTRUMENTS i ON e.instrument_id = i.id
                WHERE {"e.ticker" if direction == "ticker_to_isin" else "i.isin"} IN ({placeholders})
                {market_filter}
            """
        else:
            return {k: None for k in keys}

        data, _ = self.conn.execute_query(query, params)

        if direction == "ticker_to_isin":
            return {k: next((i for t, i in data if t == k), None) for k in keys}
        else:
            return {k: next((t for t, i in data if i == k), None) for k in keys}

    # Wrapper: ticker → ISIN
    @cache_bsh_data
    def get_isin_by_ticker(self, ticker: Union[str, List[str]], type: Optional[str]) -> Dict[str, Optional[str]]:
        return self._map_ticker_isin(ticker, type, direction="ticker_to_isin")

    # Wrapper: ISIN → ticker
    @cache_bsh_data
    def get_ticker_by_isin(self, isin: Union[str, List[str]], type: Optional[str], market: Optional[None]= None)\
            -> Dict[str, Optional[str]]:
        return self._map_ticker_isin(isin, type, market_code=market)

    @cache_bsh_data
    def get_instrument_type(self, isin: Union[str, List[str]]) -> Dict[str, Optional[str]]:
        isins = [isin] if isinstance(isin, str) else isin
        placeholders = ", ".join(f":i{i}" for i in range(len(isins)))
        params = {f"i{i}": v for i, v in enumerate(isins)}
        data, _ = self.conn.execute_query(
            f"SELECT isin, instrument_type FROM af_datamart_dba.instruments WHERE isin IN ({placeholders})",params)
        return {i: next((t.strip().upper() for s, t in data if s == i and t), None) for i in isins}

    @cache_bsh_data
    def get_etf_static_field(
            self,
            isin_list: List[str],
            subset: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Restituisce metadati statici degli ETF (da ETPS_INSTRUMENTS).

        Args:
            isin_list: Lista di ISIN da interrogare
            subset: Lista di colonne da recuperare (default: tutte)

        Returns:
            Dict con formato: {isin: {field: value_or_None, ...}}
            Garantisce sempre la presenza di ogni ISIN richiesto.
            Se un field non è disponibile per un ISIN, il valore sarà None.

        Example:
            >>> query.get_etf_static_field(["IE00B4L5Y983", "INVALID"], subset=["DESCRIPTION", "TER"])
            {
                "IE00B4L5Y983": {"DESCRIPTION": "iShares...", "TER": None},
                "INVALID": {"DESCRIPTION": None, "TER": None}
            }
        """
        if not isin_list:
            return {}

        all_columns = [
            "TICKER",
            "DESCRIPTION",
            "INSTRUMENT_TYPE",
            "UNDERLYING_TYPE",
            "UNDERLYING_CATEGORY",
            "ETP_TYPE",
            "LEVERAGE",
            "CURRENCY_HEDGING",
            "FUND_CURRENCY",
            "PAYMENT_POLICY",
            "ISSUE_DATE",
            "PRIMARY_EXCHANGE_CODE",
        ]

        # Selezione colonne (case-insensitive)
        if subset:
            subset_upper = [c.upper() for c in subset]
            invalid = [c for c in subset_upper if c not in all_columns]
            if invalid:
                logger.warning("Invalid columns in ETF static field request: %s", ", ".join(invalid))
            selected_cols = [c for c in all_columns if c in subset_upper]
        else:
            selected_cols = all_columns

        if not selected_cols:
            logger.warning("No valid columns selected for ETF static field request")
            return {}

        # Costruzione SELECT e filtro ISIN
        cols_str = ", ".join(["ISIN"] + selected_cols)
        placeholders, params = self._in_clause("isin", isin_list)
        query = f"""
            SELECT {cols_str}
            FROM AF_DATAMART_DBA.ETPS_INSTRUMENTS
            WHERE ISIN IN ({placeholders})
            ORDER BY ISIN
        """

        rows, cols = self.conn.execute_query(query, params)

        # 🆕 Inizializza con TUTTI gli ISIN richiesti
        result = {
            isin: {field: None for field in selected_cols}
            for isin in isin_list
        }

        # 🆕 Popola solo gli ISIN che hanno dati
        if rows:
            for row in rows:
                record = dict(zip(cols, row))
                isin = record.pop("ISIN", None)
                if isin and isin in result:
                    for field, value in record.items():
                        if field in result[isin]:
                            result[isin][field] = value
        else:
            logger.debug(f"No ETF static data found for ISINs: {isin_list}")

        return result

    @cache_bsh_data
    def get_etp_isins(
        self,
        segments: Optional[List[str]] = None,
        currency: Optional[str] = None,
        underlying: Optional[str] = None,
    ) -> List[str]:
        conditions = ["i.instrument_type = 'ETP'", "ei.status = 'ACTV'"]
        params = {}
        if segments:
            placeholders, seg_params = self._in_clause("seg", segments)
            conditions.append(f"ei.exchange_code IN ({placeholders})")
            params.update(seg_params)
        if currency:
            conditions.append("ei.currency = :currency")
            params["currency"] = currency
        if underlying:
            conditions.append("UPPER(e.underlying_type) = UPPER(:underlying)")
            params["underlying"] = underlying
        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT DISTINCT i.isin
            FROM AF_DATAMART_DBA.INSTRUMENTS i
            JOIN AF_DATAMART_DBA.EXCHANGE_INSTRUMENTS ei ON i.id = ei.instrument_id
            JOIN AF_DATAMART_DBA.ETPS e ON e.instrument_id = i.id
            WHERE {where_clause}
        """
        data, _ = self.conn.execute_query(query, params)
        return {"GENERAL": {"etp_isins" : [row[0] for row in data if row[0]]}}

    @cache_bsh_data
    def get_etps_data(self) -> list[dict]:
        """
        Carica tutti gli ETP da af_datamart_dba.etps_instruments.
        """
        data, cols = self.conn.execute_query("""
                     SELECT
                        e.ID,
                        e.H2O_ID,
                        e.CBONDS_ID,
                        e.ISIN,
                        e.DESCRIPTION,
                        e.CFI_CODE,
                        e.INSTRUMENT_TYPE,
                        i.SHORT_NAME,
                        e.INSTRUMENT_ID,
                        e.TICKER,
                        e.UNDERLYING_TYPE,
                        e.UNDERLYING_CATEGORY,
                        e.UNDERLYING_ID,
                        e.LEVERAGE,
                        e.ETP_TYPE,
                        e.CURRENCY_HEDGING,
                        e.STATUS,
                        e.MERGED_ETF_ID,
                        e.FUND_CURRENCY,
                        e.PAYMENT_POLICY,
                        e.ISSUE_DATE,
                        e.PRIMARY_EXCHANGE_CODE,
                        e.PRIMARY_EXCHANGE_ID,
                        e.UNDERLYING,
                        e.ISSUER_ID
                    FROM af_datamart_dba.etps_instruments e
                    JOIN ISSUERS i
                        ON e.ISSUER_ID = i.ISSUER_ID
                    WHERE e.STATUS = 'ACTIVE'

           """)
        return [dict(zip(cols, row)) for row in data]

    @cache_bsh_data
    def get_currency_data(
            self,
            currency_codes: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:

        if isinstance(currency_codes, str):
            currency_codes = [currency_codes]

        query = """
                SELECT c.CURRENCY_CODE, \
                       c.NUMERIC_CODE, \
                       c.CURRENCY_NAME, \
                       c.CURRENCY_TYPE, \
                       c.NEW_CURRENCY_CODE, \
                       m.CURRENCY_CODE_PRINCIPAL, \
                       m.CURRENCY_MULTIPLIER
                FROM AF_DATAMART_DBA.CURRENCIES c
                         LEFT JOIN AF_DATAMART_DBA.CURRENCIES_MULTIPLIER m
                                   ON c.CURRENCY_CODE = m.CURRENCY_CODE_SUBUNIT
                WHERE 1 = 1 \
                """

        params = {}

        if currency_codes:
            # Filter by currency codes (supports both principal and subunit)
            placeholders = ', '.join([f':code{i}' for i in range(len(currency_codes))])
            query += f"""
                AND (c.CURRENCY_CODE IN ({placeholders})
                     OR m.CURRENCY_CODE_PRINCIPAL IN ({placeholders}))
            """
            # Add parameters for both IN clauses
            for i, code in enumerate(currency_codes):
                params[f'code{i}'] = code.upper()

        query += " ORDER BY c.CURRENCY_CODE"

        data, cols = self.conn.execute_query(query, params)
        return [dict(zip(cols, row)) for row in data]

    # ===========================================================
    # GENERAL
    # ===========================================================
    @cache_bsh_data
    def get_all_markets(self) -> List[Dict[str, Any]]:
        query = """
            SELECT DISTINCT EXCHANGE_NAME
            FROM AF_DATAMART_DBA.EXCHANGES
            ORDER BY exchange_code
        """
        data, cols = self.conn.execute_query(query)
        return [dict(zip(cols, row)) for row in data]

    @cache_bsh_data
    def get_currencies_codes(self) -> List[Dict[str, Any]]:
        query = """
            SELECT DISTINCT currency_code AS CODE, description
            FROM AF_DATAMART_DBA.CURRENCIES
            ORDER BY currency_code
        """
        data, cols = self.conn.execute_query(query)
        return [dict(zip(cols, row)) for row in data]

    @cache_bsh_data
    def get_instrument_types(self) -> List[Dict[str, Any]]:
        query = """
            SELECT DISTINCT instrument_type
            FROM AF_DATAMART_DBA.INSTRUMENTS
            ORDER BY instrument_type
        """
        data, cols = self.conn.execute_query(query)
        return [dict(zip(cols, row)) for row in data]

    # ===========================================================
    # CONNECTION TEST
    # ===========================================================
    def test_connection(self) -> bool:
        try:
            self.conn.execute_query("SELECT 1 FROM DUAL")
            logger.info("Oracle connection test succeeded.")
            return True
        except Exception as e:
            logger.error(f"Oracle connection test failed: {e}")
            return False

    def get_stock_markets_info(self, isins: List[str]):
        if not isins:
            return []

        # Costruzione dei parametri dinamici
        placeholders = ", ".join([f":isin_{i}" for i in range(len(isins))])
        params = {f"isin_{i}": val for i, val in enumerate(isins)}

        query = f"""
            SELECT i.ISIN, ei.TICKER, ei.EXCHANGE_CODE, ei.CURRENCY
            FROM EXCHANGE_INSTRUMENTS ei
            JOIN INSTRUMENTS i ON ei.instrument_id = i.id
            WHERE i.ISIN IN ({placeholders})
        """
        grouped_results = defaultdict(lambda: {"stock_markets_info": []})
        data, cols = self.conn.execute_query(query, params)
        for row in data:
            record = dict(zip(cols, row))
            isin = record.get("ISIN") or record.get("isin")
            if isin in isins:
                grouped_results[isin]["stock_markets_info"].append(record)
        return grouped_results