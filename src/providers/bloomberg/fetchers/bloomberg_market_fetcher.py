"""
bloomberg_market_market_fetcher.py — Bloomberg market data fetcher.

This module defines :class:`BloombergMarketMarketFetcher`, which manages Bloomberg
market data retrieval (daily, snapshot, and intraday) via the BLPAPI interface.

It supports flexible batching and correlation ID tracking, returning results as
pure Python dictionaries of the form:
    {instrument_id: {field: {datetime_or_date: value}}}

Responsibilities:
    - Build and send Historical, Intraday, and Snapshot Bloomberg requests
    - Collect and parse BLPAPI event-driven responses
    - Handle MID, BID/ASK, and SPREAD field calculations
    - Integrate with caching via ``@cache_bsh_data``
    - Provide time normalization and holiday-aware logic

Typical workflow:
    1. Build one or more Bloomberg requests via MarketDataAPI
    2. Each request is dispatched to the appropriate fetcher method:
         - Daily → ``fetch_daily()``
         - Intraday → ``fetch_intraday()``
         - Snapshot → ``fetch_snapshot()``
    3. Results are parsed and normalized into a consistent structure

Example:
    >>> fetcher = BloombergMarketMarketFetcher(session, service)
    >>> res = fetcher.fetch_daily(requests, ["PX_LAST"], start, end)
    >>> snap = fetcher.fetch_snapshot(request)
    >
    """
import logging
import blpapi
from datetime import datetime, timedelta
from tqdm import tqdm
from typing import List, Dict, Union, Callable

from core.base_classes.base_fetcher import BaseMarketFetcher
from core.enums.frequency import Frequency
from core.utils.memory_provider import cache_bsh_data

logger = logging.getLogger(__name__)


class BloombergMarketMarketFetcher(BaseMarketFetcher):
    """
    Bloomberg market data fetcher (daily, snapshot, and intraday).

    This class is responsible for all Bloomberg market data operations. It
    constructs requests via the BLPAPI service, manages their asynchronous
    responses, and normalizes output into consistent Python data structures.
    It supports daily (EOD), intraday, and snapshot fetching, including
    derived fields like MID, SPREAD, and SPREAD_PCT.

    Responsibilities:
        - Execute Bloomberg HistoricalDataRequest, IntradayBarRequest, and custom snapshots
        - Parse event-driven responses and extract field-level values
        - Compute derived metrics (MID, SPREAD, SPREAD_PCT)
        - Apply holiday-aware scheduling and market calendars
        - Support caching through ``@cache_bsh_data`` for performance

    Args:
        session (blpapi.Session): Active Bloomberg session object.
        service (blpapi.Service): Reference data service opened via ``//blp/refdata``.
        show_progress (bool): Whether to show progress bars during batch fetches.

    Example:
        >>> fetcher = BloombergMarketMarketFetcher(session, service)
        >>> data = fetcher.fetch_daily(requests, ["PX_LAST"], start, end)
        >>> intraday = fetcher.fetch_intraday(request)
        >>> snapshot = fetcher.fetch_snapshot(request)
    """

    def __init__(self, session, service, show_progress: bool = True):
        super().__init__()
        self.session = session
        self.service = service
        self.show_progress = show_progress

    # ============================================================
    # DAILY / HISTORICAL
    # ============================================================

    @cache_bsh_data
    def fetch_daily(self, requests: List, fields: List[str], start: datetime, end: datetime) -> dict:
        logger.info("Starting Bloomberg daily fetch for %d instrument (%s)", len(requests), requests[0].market)
        market = requests[0].market
        results: Dict[str, List[Dict]] = {}

        # La subscription serve per il nome Bloomberg, non per l'id
        subs = [
            r.subscription(end) if callable(r.subscription) else (r.subscription or r.instrument.id)
            for r in requests
        ]
        corr_ids = [r.instrument.id for r in requests]

        start_date = start.date() if hasattr(start, "date") else start
        end_date = end.date() if hasattr(end, "date") else end
        business_days = self.holidays.get_business_days(start_date, end_date, market)
        if not len(business_days):
            logger.info("No open market days between %s and %s for %s", start_date, end_date, market)
            return {}

        with tqdm(total=len(business_days),
                  desc=f"Fetching Bloomberg daily ({market})",
                  disable=not self.show_progress) as pbar:
            try:
                req = self._make_historical_request(subs, fields, start_date, end_date)
                self.session.sendRequest(req)
                data = self._collect_historical_responses(results, fields)
            except Exception as e:
                logger.warning("Historical fetch failed: %s", e)
                return {}
            finally:
                pbar.close()

        # Converte in {corr_id: {field: {date: value}}}
        formatted = {}

        for sub, corr_id in zip(subs, corr_ids):

            # lista dict tipo:
            # [{'date': ..., 'mid': ...}, ...]
            entries = data.get(sub, [])

            # reindicizzo per data
            entries_by_date = {e["date"]: e for e in entries}

            inst_result = {}
            for field in fields:
                field_upper = field.upper()
                ts = {}

                # garantiamo TUTTI i business days in ordine
                for bd in business_days:
                    bd = bd.date()
                    e = entries_by_date.get(bd)
                    if e and field in e:
                        ts[bd] = e[field]
                    else:
                        ts[bd] = None

                inst_result[field] = ts

            formatted[corr_id] = inst_result

        return formatted

    # ============================================================
    # SNAPSHOT
    # ============================================================

    @cache_bsh_data
    def fetch_snapshot(self, request) -> dict:
        corr_id = request.instrument.id
        interval_window = request.extra_params.get("interval_window_snapshot", 15)
        results = {}

        logger.info("Fetching Bloomberg snapshot for %s (%s)", corr_id, request.fields)

        for field in request.fields:
            try:
                if field.upper() == "MID":
                    bid = self._fetch_snapshot_single(request, "BID", interval_window)
                    ask = self._fetch_snapshot_single(request, "ASK", interval_window)
                    common_days = bid.keys() & ask.keys()
                    results[field] = {d: (bid[d] + ask[d]) / 2 for d in common_days}
                else:
                    results[field] = self._fetch_snapshot_single(request, field, interval_window)
            except Exception as e:
                logger.warning("Snapshot fetch failed for %s (%s): %s", corr_id, field, e)

        return {corr_id: results}

    def _fetch_snapshot_single(self, request, field: str, interval_window: int) -> Dict[datetime.date, float]:
        """
        Fetch snapshot data for a single field across multiple days using batched requests.

        Instead of sending requests sequentially (one per day), this method:
        1. Sends ALL requests for all business days at once
        2. Collects responses asynchronously using correlation IDs
        3. Matches responses back to their respective days

        This approach reduces execution time from O(n*latency) to O(latency) where n = number of days.

        Args:
            request: Market data request object containing instrument, dates, and parameters
            field: Bloomberg field name (e.g., "BID", "ASK", "LAST")
            interval_window: Time window in minutes around snapshot_time to search for data

        Returns:
            Dictionary mapping dates to field values: {date: value}
        """
        values: Dict[datetime.date, float] = {}
        bin_interval = _parse_interval(request.extra_params.get("bin_interval", 5))
        ohlc_field = request.extra_params.get("event", "close")
        corr_id_base = request.instrument.id

        # Map correlation IDs to their corresponding days
        # 🔹 CORRETTO: Dict con chiavi tuple, non stringhe
        correlation_map: Dict[tuple, tuple] = {}

        # Get all business days in range
        business_days = self.holidays.get_business_days(
            request.start,
            request.end,
            request.market
        )

        if business_days.empty:
            logger.debug("No business days found for %s between %s and %s",
                         corr_id_base, request.start, request.end)
            return values

        # ============================================================
        # PHASE 1: SEND ALL REQUESTS (Non-blocking batch submission)
        # ============================================================
        for day in business_days:
            if self.holidays.is_holiday(day, request.market):
                continue

            # Build subscription for this specific day
            sub = (request.subscription(day)
                   if callable(request.subscription)
                   else request.subscription)

            # Calculate time window around snapshot time
            snap_dt = datetime.combine(day, request.snapshot_time)
            start_dt = snap_dt - timedelta(minutes=interval_window)
            end_dt = snap_dt + timedelta(minutes=interval_window)

            # Create unique correlation ID as tuple
            corr_id = (corr_id_base, day.strftime('%Y%m%d'), field)

            try:
                # Create and send request
                req = self._make_intraday_bar_request(
                    security=sub,
                    event_type=field,
                    interval=bin_interval,
                    start=start_dt,
                    end=end_dt
                )
                self.session.sendRequest(req, correlationId=blpapi.CorrelationId(corr_id))

                # Store mapping for response matching
                correlation_map[corr_id] = (day, snap_dt)

            except Exception as e:
                logger.warning("Failed to send request for %s %s on %s: %s",
                               corr_id_base, field, day, e)

        if not correlation_map:
            logger.debug("No valid requests sent for %s %s", corr_id_base, field)
            return values

        # ============================================================
        # PHASE 2: COLLECT ALL RESPONSES (Event-driven collection)
        # ============================================================
        max_timeout_count = 3  # Max consecutive timeouts
        timeout_count = 0
        max_total_wait = 30000  # 30 seconds total
        total_wait = 0

        while correlation_map and timeout_count < max_timeout_count:
            try:
                ev = self.session.nextEvent(timeout=5000)

                # Handle timeout events
                if ev.eventType() == blpapi.Event.TIMEOUT:
                    timeout_count += 1
                    total_wait += 5000
                    if total_wait >= max_total_wait:
                        logger.warning("Max total wait time reached for %s %s",
                                       corr_id_base, field)
                        break
                    continue

                timeout_count = 0  # Reset on valid event

                for msg in ev:
                    # Check if this is an IntradayBarResponse
                    if msg.messageType() != blpapi.Name("IntradayBarResponse"):
                        continue

                    # 🔹 CORRETTO: Usa .value() per recuperare la tupla originale
                    msg_corr_id = msg.correlationId().value()

                    if msg_corr_id not in correlation_map:
                        logger.debug("Received response for unknown correlation ID: %s", msg_corr_id)
                        continue

                    # Get the day and target time for this response
                    day, snap_dt = correlation_map[msg_corr_id]

                    try:
                        # Parse intraday bars from message
                        bars = self._parse_intraday_bars_from_message(msg, ohlc_field)

                        if bars:
                            # Find bar closest to snapshot time
                            nearest_time = min(bars.keys(), key=lambda t: abs(t - snap_dt))
                            values[day.date()] = bars[nearest_time]
                        else:
                            logger.debug("No bars returned for %s on %s", msg_corr_id, day)

                    except Exception as e:
                        logger.warning("Failed to parse response for %s on %s: %s",
                                       msg_corr_id, day, e)

                    # 🔹 Remove from pending map
                    del correlation_map[msg_corr_id]

                # Check if event is final response
                if ev.eventType() == blpapi.Event.RESPONSE:
                    logger.debug("Received final RESPONSE event, exiting collection loop")


            except Exception as e:
                logger.error("Error while collecting responses for %s %s: %s",
                             corr_id_base, field, e)
                break

        if correlation_map:
            logger.warning("Exited response loop with %d unfulfilled requests for %s %s",
                           len(correlation_map), corr_id_base, field)

        return values
    @staticmethod
    def _parse_intraday_bars_from_message(msg, ohlc_field: str = "close") -> Dict[datetime, float]:
        """
        Parse intraday bars from a Bloomberg IntradayBarResponse message.

        Args:
            msg: Bloomberg message object
            ohlc_field: Which OHLC field to extract ("open", "high", "low", "close")

        Returns:
            Dictionary mapping bar timestamps to values: {datetime: value}
        """
        bars = {}

        try:
            if not msg.hasElement("barData"):
                return bars

            bar_data = msg.getElement("barData")

            if not bar_data.hasElement("barTickData"):
                return bars

            bar_tick_data = bar_data.getElement("barTickData")

            for i in range(bar_tick_data.numValues()):
                bar_element = bar_tick_data.getValueAsElement(i)

                if bar_element.hasElement("time") and bar_element.hasElement(ohlc_field):
                    bar_time = bar_element.getElementAsDatetime("time")
                    bar_value = bar_element.getElementAsFloat(ohlc_field)
                    bars[bar_time] = bar_value

        except Exception as e:
            logger.warning("Error parsing intraday bars from message: %s", e)

        return bars
    # ============================================================
    # INTRADAY
    # ============================================================

    @cache_bsh_data
    def fetch_intraday(self, request) -> dict:
        corr_id = request.instrument.id
        interval = _parse_interval(request.frequency)
        ohlc_field = request.extra_params.get("ohlc_field")
        result_fields: Dict[str, Dict[datetime, float]] = {}

        if self.holidays.is_holiday(request.start.date(), request.market):
            logger.info("Skipping intraday fetch: %s is a holiday for %s", request.start.date(), request.market)
            return {}

        sub = request.subscription(current_date=request.start) if isinstance(request.subscription, Callable) else request.subscription

        for field in request.fields:
            field_upper = field.upper()
            try:
                if field_upper in ["MID","SPREAD","SPREAD_PCT"]:
                    bid_bars = self._parse_intraday_response(
                        self._make_intraday_bar_request(sub, "BID", interval, request.start, request.end),
                        ohlc_field,
                    )
                    ask_bars = self._parse_intraday_response(
                        self._make_intraday_bar_request(sub, "ASK", interval, request.start, request.end),
                        ohlc_field,
                    )
                    common = bid_bars.keys() & ask_bars.keys()
                    match field_upper:
                        case "MID":
                            result_fields[field] = {t: (bid_bars[t] + ask_bars[t]) / 2 for t in common}
                        case "SPREAD":
                            result_fields[field] = {t: (ask_bars[t] - bid_bars[t]) / 2 for t in common}
                        case "SPREAD_PCT":
                            result_fields[field] = {
                                t: (ask_bars[t] - bid_bars[t]) / ((ask_bars[t] + bid_bars[t]) / 2)
                                for t in common
                                if (ask_bars[t] + bid_bars[t]) != 0
                            }
                else:
                    req = self._make_intraday_bar_request(sub, field_upper, interval, request.start, request.end)
                    result_fields[field_upper] = self._parse_intraday_response(req, ohlc_field)
            except Exception as e:
                logger.warning("Intraday fetch failed for %s (%s): %s", corr_id, field_upper, e)

        return result_fields

    # ============================================================
    # BLOOMBERG API HELPERS
    # ============================================================

    def _make_historical_request(self, securities: List[str], fields: List[str], start, end):
        req = self.service.createRequest("HistoricalDataRequest")
        _append_values(req, "securities", securities)
        _append_values(req, "fields", fields)
        req.set("startDate", start.strftime("%Y%m%d"))
        req.set("endDate", end.strftime("%Y%m%d"))
        return req

    def _make_intraday_bar_request(self, security, event_type, interval, start, end):
        req = self.service.createRequest("IntradayBarRequest")
        req.set("security", security)
        req.set("eventType", event_type.upper())
        req.set("interval", interval)
        req.set("gapFillInitialBar", "true")
        req.set("startDateTime", start.strftime("%Y-%m-%dT%H:%M:%S"))
        req.set("endDateTime", end.strftime("%Y-%m-%dT%H:%M:%S"))
        return req

    def _collect_historical_responses(self, results: Dict[str, List[Dict]], fields: List[str]):
        """Parses HistoricalDataResponse messages and fills results."""
        while True:
            ev = self.session.nextEvent()
            for msg in ev:
                if msg.messageType() == blpapi.Name("HistoricalDataResponse"):
                    sec_data = msg.getElement("securityData")
                    sid = sec_data.getElementAsString("security")
                    field_data = sec_data.getElement("fieldData")
                    rows = []
                    for i in range(field_data.numValues()):
                        el = field_data.getValueAsElement(i)
                        row = {"date": el.getElementAsDatetime("date")}
                        for f in fields:
                            if el.hasElement(f):
                                row[f] = el.getElementAsFloat(f)
                        rows.append(row)
                    results[sid] = rows
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        return results

    def _parse_intraday_response(self, req, ohlc_field="close") -> Dict[datetime, float]:
        """Executes IntradayBarRequest and returns {time: value}."""
        self.session.sendRequest(req)
        values = {}
        while True:
            ev = self.session.nextEvent()
            for msg in ev:
                if msg.messageType() == blpapi.Name("IntradayBarResponse") and msg.hasElement("barData"):
                    bar_data = msg.getElement("barData").getElement("barTickData")
                    for i in range(bar_data.numValues()):
                        el = bar_data.getValueAsElement(i)
                        t = el.getElementAsDatetime("time")
                        v = el.getElementAsFloat(ohlc_field)
                        values[t] = v
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        return values


# ============================================================
# UTILS
# ============================================================

def _append_values(req, element, values):
    el = req.getElement(element)
    for v in values:
        el.appendValue(v)


def _parse_interval(interval: Union[str, int, Frequency]) -> int:
    if isinstance(interval, Frequency):
        interval = interval.value
    if isinstance(interval, int):
        return interval
    s = str(interval).strip().lower()
    if s.endswith("m"):
        return int(s[:-1])
    if s.endswith("h"):
        return int(s[:-1]) * 60
    raise ValueError(f"Unsupported interval: {interval}")
