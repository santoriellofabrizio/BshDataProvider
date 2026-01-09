import logging
from datetime import datetime
from typing import List, Dict, Any

import blpapi
from tqdm import tqdm

from core.base_classes.base_fetcher import BaseFetcher
from core.requests.requests import BaseRequest
from core.utils.memory_provider import cache_bsh_data
from providers.bloomberg.handlers.base_handlers import DailyPriceHandler

logger = logging.getLogger(__name__)


class BloombergDailyPriceHandler(DailyPriceHandler, BaseFetcher):
    """
    Handler for Bloomberg daily (historical) price data.

    Uses HistoricalDataRequest to fetch end-of-day market data.
    Returns format: {instrument_id: {field: {date: value}}}
    """

    def __init__(self, show_progress: bool = True):
        super().__init__()
        self.show_progress = show_progress

    def can_handle(self, req: BaseRequest) -> bool:
        """
        This handler handles daily market data requests.
        """
        # Check if it's a daily frequency request
        freq = str(getattr(req, "frequency", "")).lower()
        return "d" in freq

    @cache_bsh_data
    def process(
            self,
            requests: List[BaseRequest],
            session: blpapi.Session,
            service: blpapi.Service
    ) -> Dict[str, Dict[str, Dict[datetime.date, Any]]]:
        """
        Process Bloomberg daily price requests.

        Args:
            requests: List of daily market requests
            session: Bloomberg session
            service: Bloomberg refdata service

        Returns:
            Dict[instrument_id, Dict[field, Dict[date, value]]]
        """
        if not requests:
            return {}

        logger.info("Starting Bloomberg daily fetch for %d instruments", len(requests))

        # Extract market from first request (should be same for all)
        sample = requests[0]
        market = sample.market
        fields = sample.fields

        # Build subscriptions and correlation IDs
        subs = []
        corr_ids = []
        for r in requests:
            sub = (r.subscription(r.end) if callable(r.subscription)
                   else (r.subscription or r.instrument.id))
            subs.append(sub)
            corr_ids.append(r.instrument.id)

        # Get date range
        start_date = min(r.start for r in requests)
        end_date = max(r.end for r in requests)

        if hasattr(start_date, "date"):
            start_date = start_date.date()
        if hasattr(end_date, "date"):
            end_date = end_date.date()

        # Get business days for the market
        business_days = self.holidays.get_business_days(start_date, end_date, market)
        if not len(business_days):
            logger.info("No open market days between %s and %s for %s", start_date, end_date, market)
            return {}

        # Make request and collect responses
        results = {}
        with self.progress(f"Fetching daily data (BLOOMBERG) {start_date:%Y-%m-%d} -> {end_date:%Y-%m-%d}",
                           total=1) as pbar:
            try:
                req = self._make_historical_request(service, subs, fields, start_date, end_date)
                session.sendRequest(req)
                data = self._collect_historical_responses(session, results, fields)
                pbar.update(1)
            except Exception as e:
                logger.warning("Historical fetch failed: %s", e)
                return {}
            finally:
                pbar.close()

        # Convert to {corr_id: {field: {date: value}}}
        formatted = {}

        for sub, corr_id in zip(subs, corr_ids):
            # lista dict tipo: [{'date': ..., 'field': ...}, ...]
            entries = data.get(sub, [])

            # reindex by date
            entries_by_date = {e["date"]: e for e in entries}

            inst_result = {}
            for field in fields:
                ts = {}

                # guarantee ALL business days in order
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

    def _make_historical_request(
            self,
            service: blpapi.Service,
            securities: List[str],
            fields: List[str],
            start: datetime.date,
            end: datetime.date
    ):
        """Create Bloomberg HistoricalDataRequest."""
        req = service.createRequest("HistoricalDataRequest")
        self._append_values(req, "securities", securities)
        self._append_values(req, "fields", fields)
        req.set("startDate", start.strftime("%Y%m%d"))
        req.set("endDate", end.strftime("%Y%m%d"))
        return req

    def _collect_historical_responses(
            self,
            session: blpapi.Session,
            results: Dict[str, List[Dict]],
            fields: List[str]
    ) -> Dict[str, List[Dict]]:
        """Parse HistoricalDataResponse messages and fill results."""
        while True:
            ev = session.nextEvent()
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

    @staticmethod
    def _append_values(req, element: str, values: List[str]):
        """Append multiple values to a Bloomberg request element."""
        el = req.getElement(element)
        for v in values:
            el.appendValue(v)
