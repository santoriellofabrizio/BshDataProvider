"""
info_data_api.py — Unified API for static and semi-static data.

This module defines the :class:`InfoDataAPI`, a high-level interface for querying
reference or semi-static information (TER, FX composition, PCF, NAV, etc.) from multiple
data providers (Oracle, Bloomberg, Timescale). It complements the
:class:`MarketDataAPI` for dynamic data by focusing on descriptive, fundamental, and
slowly changing fields.

Responsibilities:
    - Manage Reference, Bulk, and Historical static requests
    - Normalize identifiers (ISIN, ticker, id) and parameters across sources
    - Handle provider-specific data aggregation and field mapping
    - Integrate with caching and autocomplete mechanisms
    - Expose convenience wrappers (e.g., get_ter, get_fx_composition, get_pcf_composition)

Typical workflow:
    >>> from client import BSHDataClient
    >>> client = BSHDataClient()
    >>> api = InfoDataAPI(client)
    >>> ter = api.get_ter(isin="IE00B4L5Y983")
    >>> fx = api.get_fx_composition(isin="IE00B4L5Y983", source="oracle")
    >>> pcf = api.get_pcf_composition(["IE00B4L5Y983", "IE00B1FZS350"], reference_date="yesterday")
"""

import logging
import uuid
from datetime import timedelta
from typing import Optional, Union, List, Dict, Any, Literal

import pandas as pd
from dateutil.utils import today

from core.decorators.respect_cache_status import respect_cache_kwarg
from core.enums.fields import StaticField
from core.holidays.holiday_manager import HolidayManager
from core.requests.request_builder.request_builder import RequestBuilder
from core.utils.common import normalize_list, normalize_param
from interface.api.base_api import BaseAPI
from interface.api.type_hints import (
    InfoDataGetParams,
    FXCompositionParams,
    PCFCompositionParams,
    DividendsParams,
    TERParams,
    NAVParams,
    PricesParams
)

logger = logging.getLogger(__name__)


class InfoDataAPI(BaseAPI):
    """
    High-level API for static and semi-static data (TER, PCF, FX, anagraphic, ecc...).

    This class manages static-type requests such as Reference, Bulk, and Historical queries.
    It created Instrument and Requests (historical, bulk and reference) to be sent to central client.
    """

    # ============================================================
    # INTERNAL DISPATCH WITH RETRY SUPPORT
    # ============================================================

    def _dispatch(
            self,
            instruments: list,
            fields: Union[str, List[str]],
            source: Union[str, List[str], Dict[str, str]],
            type: Union[str, List[str], Dict[str, str]],
            subscriptions: Optional[Union[str, List[str], Dict[str, str]]] = None,
            market: Optional[Union[str, List[str], Dict[str, str]]] = None,
            fallbacks: Optional[List[Dict[str, Any]]] = None,
            **kwargs,
    ):
        """
        Generic dispatcher with automatic retry via fallbacks.

        Responsibilities:
            - Normalize parameters per instrument
            - Build request objects using the RequestBuilder
            - Send requests to the unified data client
            - On partial/incomplete results, retry with fallback configs
            - Aggregate and return combined results

        Args:
            instruments (list): List of instrument objects to query.
            fields (str | list[str]): Data fields to request.
            source (str | list[str] | dict[str, str]): Data source(s) (e.g., 'oracle', 'bloomberg').
            type (str | list[str] | dict[str, str]): Request type(s) ('reference', 'bulk', 'historical').
            subscriptions (str | list[str] | dict[str, str], optional): Optional subscription identifiers.
            market (str | list[str] | dict[str, str], optional): Market codes (e.g., 'ETFP', 'EUREX').
            fallbacks (list[dict], optional): Alternative configs to retry on incomplete results.
            **kwargs: Additional parameters forwarded to RequestBuilder.

        Returns:
            dict | pd.DataFrame | None: Aggregated results from all attempts.
        """
        if not instruments:
            return None

        fields = [fields] if isinstance(fields, str) else fields
        # Use normalize_param to support dict mode
        market = normalize_param(market, instruments, default=None)
        source = normalize_param(source, instruments, default=None)
        subscriptions = normalize_param(subscriptions, instruments, default=None)
        type = normalize_param(type, instruments, default=None)

        requests = []

        # Build requests
        for i, inst in enumerate(instruments):
            req = RequestBuilder.build_static_request(
                instrument=inst,
                fields=fields,
                market=market[i],
                source=source[i],
                subscriptions=subscriptions[i],
                type=type[i],
                **kwargs,
            )
            requests.append(req)

        batch_id = str(uuid.uuid4())
        self.log_request(f"[dispatch] batch={batch_id} InfoRequest n={len(requests)}")

        # Send initial requests
        result = self.client.send(requests)

        # Check if we need fallbacks
        if fallbacks:
            incomplete = self.client.tracker.get_failed() + self.client.tracker.get_incomplete()
            if incomplete:
                self.log_request(
                    f"[fallback] {len(incomplete)} incomplete requests, trying {len(fallbacks)} fallback(s)"
                )
                try:
                    result = self._retry_with_fallbacks(
                        incomplete_statuses=incomplete,
                        fallbacks=fallbacks,
                        current_result=result,
                        fields=fields,
                        **kwargs
                    )
                except Exception as e:
                    logger.error(f"[fallback] cannot retry: {e}")

        return result

    def _retry_with_fallbacks(
            self,
            incomplete_statuses: List[Any],
            fallbacks: List[Dict[str, Any]],
            current_result: Dict[str, Any],
            fields: List[str],
            **kwargs,
    ) -> Dict[str, Any]:
        """
        Retry incomplete requests with fallback configurations.

        Args:
            incomplete_statuses: List of RequestStatus objects that are incomplete/failed.
            fallbacks: List of alternative configs to try.
            current_result: Aggregated result from first attempt.
            fields: Fields that were requested.
            type: Request type(s).
            **kwargs: Additional parameters.

        Returns:
            dict: Merged results from all attempts.
        """
        merged_result = dict(current_result) if current_result else {}

        for fallback_idx, fallback_config in enumerate(fallbacks):
            self.log_request(
                f"[fallback {fallback_idx + 1}/{len(fallbacks)}] Retrying with config: {fallback_config}"
            )

            # Extract original instruments from incomplete statuses
            retry_instruments = [s.request.instrument for s in incomplete_statuses]

            # Override parameters from fallback config (support dict mode)
            retry_source = normalize_param(fallback_config.get("source"), retry_instruments, default=None)
            retry_market = normalize_param(fallback_config.get("market"), retry_instruments, default=None)
            retry_subscriptions = normalize_param(fallback_config.get("subscriptions"), retry_instruments, default=None)


            # Merge kwargs with fallback config (fallback overrides)
            retry_kwargs = {**kwargs}
            for key, value in fallback_config.items():
                if key not in ["source", "market", "subscriptions", "type"]:
                    retry_kwargs[key] = value

            # Build retry requests
            retry_requests = []
            for i, inst in enumerate(retry_instruments):
                req = RequestBuilder.build_static_request(
                    instrument=inst,
                    fields=fields,
                    market=retry_market[i],
                    source=retry_source[i],
                    subscriptions=retry_subscriptions[i],
                    **retry_kwargs,
                )
                retry_requests.append(req)

            # Send retry requests
            retry_result = self.client.send(retry_requests)

            # Merge results (retry results override original)
            if retry_result:
                merged_result.update(retry_result)

            # Check if all incomplete are now complete
            still_incomplete = self.client.tracker.get_incomplete()
            if not still_incomplete:
                self.log_request("[fallback] All requests now complete")
                break

        return merged_result

    # ============================================================
    # MAIN ENTRY POINT
    # ============================================================
    # ============================================================
    # InfoDataAPI - get() con overload
    # ============================================================

    @respect_cache_kwarg
    def get(
            self,
            type: str = None,
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            instruments: Optional[List] = None,  # ← NEW
            market: Optional[Union[str, List[str], Dict[str, str]]] = None,
            currency: Union[str, List[str], Dict[str, str]] = "EUR",
            autocomplete: Optional[bool] = None,
            **kwargs,
    ):
        """
        Retrieve static/semi-static data. Two modes:

        Mode 1 - Build instruments from identifiers:
            get(type='ETP', ticker='IUSA', fields='TER', source='bloomberg')

        Mode 2 - Use pre-built instruments:
            get(instruments=[etf1, etf2], fields='TER', source='bloomberg')

        Dict mode support (NEW):
            Parameters like currency, market, source can be passed as:
            - Single value: "USD" (replicated to all instruments)
            - List: ["USD", "EUR", "GBP"] (aligned with instruments)
            - Dict: {"AAPL": "USD", "MSFT": "EUR"} (mapped by instrument ID, default for others)

        Type hints support:
            For IDE autocomplete when using **kwargs pattern:

            from interface.api.type_hints import InfoDataGetParams

            def wrapper(**kwargs):
                params: InfoDataGetParams = kwargs  # IDE autocomplete enabled
                return api.get(**params)
        """
        # Mode 2: pre-built instruments
        if instruments is not None:
            return self.get_with_instruments(instruments=instruments, **kwargs)

        # Mode 1: build instruments
        auto = self.autocomplete if autocomplete is None else autocomplete
        ids, isins, tickers = self._resolve_identifiers(id, isin, ticker, autocomplete=auto)
        n = len(ids)

        currency = normalize_list(currency, n)
        market = normalize_list(market, n)
        type = normalize_list(type, n)

        # Separate instrument-building params from request params
        instrument_build_params = {
            k: v for k, v in kwargs.items()
            if k not in ['fields', 'source', 'subscriptions', 'request_type', 'fallbacks']
        }

        instruments = [
            self._build_instrument(
                id=ids[i], type=type[i], ticker=tickers[i], isin=isins[i],
                currency=currency[i], market=market[i], autocomplete=auto,
                **instrument_build_params
            )
            for i in range(n)
        ]

        return self.get_with_instruments(instruments=instruments, **kwargs)

    def get_with_instruments(
            self,
            instruments: list,
            fields: Union[str, List[str]] = "MID",
            source: Optional[Union[str, List[str], Dict[str, str]]] = None,
            subscriptions: Optional[Union[str, List[str], Dict[str, str]]] = None,
            market: Optional[Union[str, List[str], Dict[str, str]]] = None,
            request_type: Optional[Union[str, List[str], Dict[str, str]]] = None,
            fallbacks: Optional[List[Dict[str, Any]]] = None,
            **extra_params,
    ):
        """
        Execute request with pre-built instruments.

        Supports dict mode for parameters (mapped by instrument ID).
        """
        # Use normalize_param to support dict mode
        source = normalize_param(source, instruments, default=None)
        subscriptions = normalize_param(subscriptions, instruments, default=None)
        market = normalize_param(market, instruments, default=None)
        request_type = normalize_param(request_type, instruments, default=None)

        result = self._dispatch(
            instruments=instruments,
            fields=fields,
            source=source,
            subscriptions=subscriptions,
            market=market,
            type=request_type,
            fallbacks=fallbacks,
            **extra_params,
        )

        result = self._aggregate(result)
        return self._rename_fields(result, fields if isinstance(fields, list) else [fields], source)

    def _rename_fields(self, result, fields, source):
        """
        Rinomina i campi in base alla mappatura StaticField.
        Supporta sia:
          - dict[str, dict[str, Any]]  → ritorna dict
          - pd.DataFrame con MultiIndex (instrument, field) → ritorna DataFrame
        """
        try:
            # Crea mappatura Bloomberg → interno
            mapping = {
                StaticField.from_str(f, source).upper(): f for f in fields
            }

            # ------------------------------------------------------------
            # Caso: DataFrame
            # ------------------------------------------------------------
            if isinstance(result, pd.DataFrame):
                if isinstance(result.columns, pd.MultiIndex) and "field" in result.columns.names:
                    # livello "field" esplicito nel MultiIndex
                    new_fields = [
                        mapping.get(field.upper(), field)
                        for _, field in result.columns
                    ]
                    result.columns = pd.MultiIndex.from_tuples(
                        [(instr, new_field) for (instr, _), new_field in zip(result.columns, new_fields)],
                        names=result.columns.names,
                    )
                else:
                    # singolo livello colonne → rinomina direttamente
                    result.columns = [
                        mapping.get(c.upper(), c) for c in result.columns
                    ]
                return result

            # ------------------------------------------------------------
            # Caso: dict
            # ------------------------------------------------------------
            renamed = {}
            if isinstance(result, pd.Series):
                return result.rename(mapping)

            for instr, fields_dict in result.items():
                if not isinstance(fields_dict, dict):
                    renamed[instr] = fields_dict
                    continue
                renamed[instr] = {
                    mapping.get(field.upper(), field): value
                    for field, value in fields_dict.items()
                }

            return renamed

        except Exception as e:
            logger.warning("Failed to rename fields: %s", e)
            return result

    # ============================================================
    # CONVENIENCE WRAPPERS
    # ============================================================

    def get_ter(
            self,
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            source: Union[str, List[str], Dict[str, str]] = "bloomberg",
            **kwargs
    ):
        """
        Restituisce il TER (Total Expense Ratio) degli ETF.

        Supports dict mode for source parameter (mapped by instrument ID).
        """
        return self.get(
            type="ETP",
            id=id,
            isin=isin,
            ticker=ticker,
            source=source,
            fields="TER",
            request_type="reference",
            **kwargs
        )

    def get_dividends(
            self,
            isin: Optional[Union[str, List[str]]] = None,
            id: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            start=today() - timedelta(days=360),
            end=today(),
            source: Union[str, List[str], Dict[str, str]] = "bloomberg",
    ):
        """
        Restituisce i dividendi storici.

        Supports dict mode for source parameter (mapped by instrument ID).
        """
        return self.get(
            type="ETP",
            id=id,
            isin=isin,
            ticker=ticker,
            source=source,
            fields="DIVIDEND",
            request_type="bulk",
            start=start,
            end=end
        )

    def get_fx_composition(
            self,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            id: Optional[Union[str, List[str]]] = None,
            reference_date=None,
            fx_fxfwrd: Literal["fx", "fxfwrd", "both"] = "both",
            source: Union[str, List[str], Dict[str, str]] = "oracle",
            **kwargs
    ):
        """
        Restituisce la composizione valutaria (FX composition).

        Supports dict mode for source parameter (mapped by instrument ID).

        """
        # Type hint for kwargs (IDE support)
        _params: FXCompositionParams = kwargs

        return self.get(
            type="ETP",
            id=id,
            isin=isin,
            ticker=ticker,
            source=source,
            fields="FX_COMPOSITION",
            request_type="bulk",
            reference_date=reference_date,
            fx_fxfwrd=fx_fxfwrd,
            **kwargs
        ).T.fillna(0)

    def get_pcf_composition(
            self,
            id: Optional[Union[str, List[str]]] = None,
            isins: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            reference_date=None,
            source: Union[str, List[str], Dict[str, str]] = "oracle",
            include_cash=False,
            comp_field: Literal["WEIGHT_NAV", "N_INSTRUMENTS", "WEIGHT_RISK", "RAW"] = "WEIGHT_NAV"
    ):
        """
        Restituisce la composizione PCF.

        Supports dict mode for source parameter (mapped by instrument ID).
        """
        if isinstance(reference_date, str):
            ref = reference_date.lower()
            if ref == "yesterday":
                reference_date = HolidayManager().previous_business_day(today())
            elif ref == "last":
                reference_date = None

        raw = self.get(
            "ETP",
            id=id,
            ticker=ticker,
            isin=isins,
            source=source,
            fields="PCF_COMPOSITION",
            request_type="bulk",
            reference_date=reference_date,
            include_cash=include_cash
        )

        if isinstance(isins, str):
            isins = [isins]

        if comp_field.upper() == "RAW":
            return raw
        else:
            return raw.pivot_table(
                index="BSH_ID_ETF",
                columns="BSH_ID_COMP",
                values=comp_field.upper(),
                aggfunc="first"
            )

    def get_etp_fields(
            self,
            fields: Union[str, List[str]],
            type: str = "ETP",
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            market: Optional[Union[str, List[str], Dict[str, str]]] = None,
            source: Optional[Union[str, List[str], Dict[str, str]]] = "oracle",
            currency: Union[str, List[str], Dict[str, str]] = "EUR",
            subscriptions: Optional[Union[str, List[str], Dict[str, str]]] = None,
            autocomplete: Optional[bool] = None,
            **extra_params,
    ):
        """
        Get ETP fields.

        Supports dict mode for market, source, currency, subscriptions parameters (mapped by instrument ID).
        """
        return self.get(
            type=type,
            id=id,
            isin=isin,
            ticker=ticker,
            market=market,
            source=source,
            fields=fields,
            currency=currency,
            subscriptions=subscriptions,
            autocomplete=autocomplete,
            **extra_params
        )

    def get_nav(
            self,
            start,
            id: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            subscriptions: Optional[Union[str, List[str], Dict[str, str]]] = None,
            source: Union[str, List[str], Dict[str, str]] = "bloomberg",
            end=today()
    ):
        """
        Restituisce i NAV storici.

        Supports dict mode for source and subscriptions parameters (mapped by instrument ID).
        """
        return self.get(
            type="ETP",
            id=id,
            isin=isin,
            ticker=ticker,
            source=source,
            fields="NAV",
            subscriptions=subscriptions,
            start=start,
            end=end,
            request_type="historical"
        )

    def get_future_fields(
            self,
            fields: Union[str, List[str]],
            type: str = "FUTURE",
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            market: Optional[Union[str, List[str]]] = None,
            source: Optional[Union[str, List[str]]] = "oracle",
            currency: Union[str, List[str]] = "EUR",
            subscriptions: Optional[Union[str, List[str], Dict[str, str]]] = None,
            autocomplete: Optional[bool] = None,
            **extra_params,
    ):
        """
        Get future fields.

        kwargs: root, is_active_form, future_currency, future_underlying,
                suffix, timescale_root
        """
        return self.get(
            type,
            id,
            isin,
            ticker,
            market,
            source,
            fields,
            currency,
            subscriptions,
            autocomplete,
            **extra_params
        )

    def get_stock_fields(
            self,
            fields: Union[str, List[str]],
            ticker: Optional[Union[str, List[str]]] = None,
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            market: Optional[Union[str, List[str]]] = None,
            source: Optional[Union[str, List[str]]] = "oracle",
            currency: Union[str, List[str]] = None,
            subscriptions: Optional[Union[str, List[str]]] = None,
            **kwargs
    ):
        """Get stock fields."""
        return self.get(
            "STOCK",
            id,
            isin,
            ticker,
            market,
            source,
            fields,
            currency,
            subscriptions,
            **kwargs
        )

    def get_stock_markets(
            self,
            ticker: Optional[Union[str, List[str]]] = None,
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            source: Optional[Union[str, List[str]]] = "oracle",
            currency: Union[str, List[str]] = None,
            **kwargs
    ):
        """Get stock markets info."""
        results = self.get(
            "STOCK",
            id,
            isin,
            ticker,
            None,
            source,
            "STOCK_MARKETS_INFO",
            currency,
            **kwargs
        )
        return results.drop("ISIN", axis=1, errors="ignore")