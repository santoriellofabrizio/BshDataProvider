import datetime
import logging
from datetime import date
from typing import List, Dict, Any, Set

import blpapi

from core.requests.requests import BaseRequest
from providers.bloomberg.handlers.base_handlers import BulkFieldHandler

logger = logging.getLogger(__name__)


class BloombergBulkHandler(BulkFieldHandler):
    """
    Handler for Bloomberg bulk data requests.

    Processes bulk fields (like DVD_HIST_ALL) using Bloomberg's ReferenceDataRequest API.
    Returns format: {subscription: {field: {date: value}}} or {subscription: {field: list}}
    """

    def can_handle(self, req: BaseRequest) -> bool:
        """
        This handler can handle any bulk request.
        """
        return req.request_type == "bulk"

    def process(
            self,
            requests: List[BaseRequest],
            session: blpapi.Session,
            service: blpapi.Service
    ) -> Dict[str, Dict[str, Any]]:
        """
        Process Bloomberg bulk data request.

        Args:
            requests: List of bulk requests
            session: Bloomberg session
            service: Bloomberg refdata service

        Returns:
            Dict[subscription, Dict[field, value]]
        """
        if not requests:
            return {}

        # Extract subscriptions and correlation IDs
        subscriptions = []
        corr_ids = []

        for req in requests:
            sub = req.subscription or req.instrument.isin or req.instrument.ticker
            subscriptions.append(sub)
            corr_ids.append(req.instrument.id)

        # Collect all unique fields
        fields = list({f.upper() for r in requests for f in r.fields})

        # Get date range from first request (if available)
        first_req = requests[0]
        start = getattr(first_req, "start", None)
        end = getattr(first_req, "end", None)

        logger.info("Processing Bloomberg bulk request: %d instruments, fields=%s",
                   len(subscriptions), fields)

        # Send all requests (bulk uses ReferenceDataRequest)
        self._send_reference_requests(service, session, subscriptions, fields, corr_ids)

        # Collect all responses
        raw_data = self._collect_batch_responses(
            session=session,
            response_type="ReferenceDataResponse",
            expected_corr_ids=set(corr_ids)
        )

        # Convert from {corr_id: data} to {subscription: data}
        result = {}
        for sub, corr_id in zip(subscriptions, corr_ids):
            if corr_id in raw_data:
                result[sub] = raw_data[corr_id]

        # Normalize bulk data based on field type
        return self.parse_bulk_raw_data(result, fields, start, end)

    def _send_reference_requests(
            self,
            service: blpapi.Service,
            session: blpapi.Session,
            subscriptions: List[str],
            fields: List[str],
            corr_ids: List[str]
    ):
        """Send Bloomberg ReferenceDataRequest for bulk fields."""
        for sub, cid in zip(subscriptions, corr_ids):
            request = service.createRequest("ReferenceDataRequest")

            # Handle ISIN format
            if sub.upper().endswith(" ISIN"):
                isin = sub.split()[0]
                bb_code = f"/isin/{isin}"
            else:
                bb_code = sub

            request.append("securities", bb_code)
            for f in fields:
                request.append("fields", f)

            corr_id_obj = blpapi.CorrelationId(cid)
            session.sendRequest(request, correlationId=corr_id_obj)
            logger.debug("Sent ReferenceDataRequest (bulk): %s (corr_id=%s)", bb_code, cid)

    def _collect_batch_responses(
            self,
            session: blpapi.Session,
            response_type: str,
            expected_corr_ids: Set[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Collect all Bloomberg responses for bulk requests.

        Args:
            session: Bloomberg session
            response_type: Type of Bloomberg response
            expected_corr_ids: Set of correlation IDs we're expecting

        Returns:
            Dictionary: {corr_id: {field: value}}
        """
        all_data: Dict[str, Dict[str, Any]] = {}
        errors: Dict[str, str] = {}
        pending_corr_ids = expected_corr_ids.copy()

        timeout_count = 0
        max_timeouts = 20

        while pending_corr_ids and timeout_count < max_timeouts:
            try:
                ev = session.nextEvent(timeout=2000)
                ev_type = ev.eventType()

                # Handle timeout
                if ev_type == blpapi.Event.TIMEOUT:
                    timeout_count += 1
                    logger.debug("Timeout %d/%d waiting for %d responses",
                               timeout_count, max_timeouts, len(pending_corr_ids))
                    continue

                timeout_count = 0  # Reset on valid event

                # Process messages
                if ev_type in (blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE):
                    for msg in ev:
                        if msg.messageType() != blpapi.Name(response_type):
                            continue

                        # Get correlation ID
                        msg_corr_ids = msg.correlationIds()
                        if not msg_corr_ids:
                            logger.warning("Message without correlation ID")
                            continue

                        corr_id = str(msg_corr_ids[0].value())

                        if corr_id not in pending_corr_ids:
                            logger.debug("Received response for unexpected corr_id: %s", corr_id)
                            continue

                        # Remove from pending list
                        pending_corr_ids.discard(corr_id)
                        logger.debug("Received response for %s (%d pending)",
                                   corr_id, len(pending_corr_ids))

                        # Parse bulk data
                        try:
                            sec_data_array = msg.getElement("securityData")

                            for i in range(sec_data_array.numValues()):
                                sec_data = sec_data_array.getValueAsElement(i)
                                sec = sec_data.getElementAsString("security")

                                # Handle security errors
                                if sec_data.hasElement("securityError"):
                                    err = sec_data.getElement("securityError").getElementAsString("message")
                                    errors[corr_id] = err
                                    logger.warning("Bloomberg security error for %s: %s", corr_id, err)
                                    continue

                                # Handle field exceptions
                                if sec_data.hasElement("fieldExceptions"):
                                    self._process_field_exceptions(sec_data, corr_id, errors)

                                # Parse field data (can be bulk arrays)
                                if sec_data.hasElement("fieldData"):
                                    field_data = sec_data.getElement("fieldData")
                                    record = {}

                                    for field in field_data.elements():
                                        name = str(field.name())
                                        if field.isArray():
                                            records = [
                                                self._parse_element(field.getValueAsElement(k))
                                                for k in range(field.numValues())
                                            ]
                                            record[name] = records
                                        else:
                                            record[name] = field.getValue()

                                    all_data[corr_id] = record

                        except Exception as e:
                            logger.error("Error processing message for %s: %s", corr_id, e, exc_info=True)

                # Exit when received RESPONSE finale
                if ev_type == blpapi.Event.RESPONSE:
                    if not pending_corr_ids:
                        logger.debug("All responses collected, exiting")
                        break
                    else:
                        logger.debug("Received RESPONSE event but still waiting for %d responses",
                                   len(pending_corr_ids))

            except Exception as e:
                logger.error("Error in response collection loop: %s", e, exc_info=True)
                break

        if pending_corr_ids:
            logger.warning("Exited with %d unfulfilled requests: %s",
                         len(pending_corr_ids), sorted(list(pending_corr_ids))[:10])

        logger.debug("%s completed: %d instruments received, %d errors, %d missing",
                    response_type, len(all_data), len(errors), len(pending_corr_ids))

        return all_data

    def _parse_element(self, element) -> Dict[str, Any]:
        """Recursively parse a complex element."""
        record = {}
        for sub in element.elements():
            name = str(sub.name())
            if sub.isArray():
                record[name] = [
                    self._parse_element(sub.getValueAsElement(k))
                    for k in range(sub.numValues())
                ]
            elif sub.isComplexType():
                record[name] = self._parse_element(sub)
            else:
                try:
                    record[name] = sub.getValue()
                except Exception as e:
                    logger.info("Error getting value for %s: %s", name, e)
        return record

    def _process_field_exceptions(self, sec_data, corr_id: str, errors: Dict[str, str]):
        """Process field exceptions from Bloomberg response."""
        field_excs = sec_data.getElement("fieldExceptions")
        for j in range(field_excs.numValues()):
            exc = field_excs.getValueAsElement(j)
            field_id = exc.getElementAsString("fieldId")
            err_msg = exc.getElement("errorInfo").getElementAsString("message")
            errors[f"{corr_id}:{field_id}"] = err_msg
            logger.info("Bloomberg field error for %s.%s: %s", corr_id, field_id, err_msg)

    def parse_bulk_raw_data(self, raw_data: Dict[str, Any], fields: List[str], start: date, end: date):
        """Parse and normalize bulk data based on field type."""
        if not fields:
            return raw_data

        field_upper = fields[0].upper()

        # Handle DVD_HIST_ALL (dividend history)
        if field_upper == "DVD_HIST_ALL":
            return self.parse_dividends_data(raw_data, start, end)

        # Add more bulk field parsers here as needed
        # elif field_upper == "ANOTHER_BULK_FIELD":
        #     return self.parse_another_field(raw_data, start, end)

        # Default: return as-is
        return raw_data

    def parse_dividends_data(
            self,
            raw_data: Dict[str, Any],
            start: date = None,
            end: date = None,
    ) -> Dict[str, Dict[str, Dict[date, float]]]:
        """
        Parse dividend data from DVD_HIST_ALL and filter by date range [start, end].
        Returns: {subscription: {"DIVIDEND_AMOUNT": {ex_date: amount}}}
        """
        parsed: Dict[str, Dict[str, Dict[date, float]]] = {}

        if isinstance(start, datetime.datetime):
            start = start.date()
        if isinstance(end, datetime.datetime):
            end = end.date()

        for subscription, fields_data in raw_data.items():
            dvd_data = fields_data.get("DVD_HIST_ALL")
            if not dvd_data or not isinstance(dvd_data, list):
                parsed[subscription] = {"DIVIDEND_AMOUNT": {}}
                continue

            divs: Dict[date, float] = {}
            for entry in dvd_data:
                ex_date = entry.get("Ex-Date") or entry.get("Ex Date")
                amount = entry.get("Dividend Amount") or entry.get("Amount")

                if not ex_date or amount is None:
                    continue

                if isinstance(ex_date, str):
                    try:
                        ex_date = date.fromisoformat(ex_date)
                    except ValueError:
                        continue

                if (start and ex_date < start) or (end and ex_date > end):
                    continue

                try:
                    divs[ex_date] = float(amount)
                except (TypeError, ValueError):
                    continue

            if divs:
                parsed[subscription] = {
                    "DIVIDEND_AMOUNT": dict(sorted(divs.items(), key=lambda x: x[0], reverse=True))
                }
            else:
                parsed[subscription] = {"DIVIDEND_AMOUNT": {}}

        return parsed
