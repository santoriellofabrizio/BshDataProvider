import logging
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any

from core.holidays.holiday_manager import HolidayManager
from core.requests.requests import BaseRequest
from providers.timescale.query_timescale import QueryTimeScale

logger = logging.getLogger(__name__)


class Handler(ABC):
    """
    Base handler for Chain of Responsibility pattern.
    
    Handles batches of requests, processes fields it can handle,
    and forwards remaining fields to the next handler in the chain.
    
    Flow:
        1. Split requests into can_handle / can't_handle
        2. Process compatible requests in batch
        3. Normalize output to format A: {FIELD: {subscription: value}}
        4. Convert to final format: {instrument_id: {FIELD: value}}
        5. Ensure completeness (all requested fields present, even if None)
        6. Forward remaining requests to next handler
    """

    def __init__(self):
        self._next: Optional['Handler'] = None
        self.holiday_manager = HolidayManager()

    def set_next(self, handler: 'Handler') -> 'Handler':
        """Link the next handler in the chain."""
        self._next = handler
        return handler

    def handle(self, requests: List[Any], query: QueryTimeScale) -> Dict[str, Any]:
        can_process = []
        remaining = []

        for req in requests:
            if self.can_handle(req):
                can_process.append(req)
            else:
                remaining.append(req)

        results = {}
        downstream_requests = []

        if can_process:
            # Process in batch
            raw_out = self.process(can_process, query) or {}

            # Normalize DIRECTLY to final format: {id: {field: value}}
            normalized = self._normalize_output(raw_out, can_process)

            # ============================================================
            # STEP 3: Ensure completeness and handle missing fields
            # ============================================================
            for req in can_process:
                req_id = req.instrument.id
                req_fields = {f.upper() for f in req.fields}

                # Get data for this instrument (might not exist)
                instrument_data = normalized.get(req_id, {})

                results.setdefault(req_id, {})
                missing = []

                # Check which fields are present and which are missing
                for field in req_fields:
                    if field in instrument_data and instrument_data[field] is not None:
                        results[req_id][field] = instrument_data[field]
                    else:
                        results[req_id][field] = None
                        missing.append(field)
                        logger.debug(f"Field '{field}' not found for {req_id}, set to None")

                # If some fields are missing, forward to next handler
                if missing:
                    req_copy = self._clone_request_with_fields(req, missing)
                    downstream_requests.append(req_copy)

        # Add requests this handler can't process at all
        if remaining:
            downstream_requests.extend(remaining)

        # Forward to next handler
        if downstream_requests and self._next:
            down = self._next.handle(downstream_requests, query)
            for req_id, data in down.items():
                results.setdefault(req_id, {}).update(data)

        return results

    def _normalize_output(
            self,
            out: Dict[str, Any],
            requests: List[BaseRequest]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Normalize output to FINAL format: {instrument_id: {FIELD: value}}

        Supports two input formats from process():
            Format A (field-centric): {FIELD: {subscription: value}}
            Format B (subscription-centric): {subscription: {FIELD: value}}

        Returns:
            Dict in FINAL format: {instrument_id: {FIELD: value}}
        """
        if not out:
            return {}

        # Build subscription -> instrument_id mapping
        sub_to_id = {}
        for req in requests:
            sub = req.subscription or req.instrument.id
            sub_to_id[sub] = req.instrument.id

        # Collect requested fields from all requests
        requested_fields = {
            f.upper()
            for req in requests
            for f in req.fields
        }

        keys = list(out.keys())

        # ============================================================
        # DETECT FORMAT
        # ============================================================
        # Check if any top-level key matches a requested field → Format A
        is_format_a = any(k.upper() in requested_fields for k in keys)

        # ============================================================
        # NORMALIZE TO FINAL FORMAT: {id: {field: value}}
        # ============================================================
        results = {}

        if is_format_a:
            # Format A: {FIELD: {subscription: value}} → {id: {field: value}}
            for field, submap in out.items():
                if not isinstance(submap, dict):
                    logger.warning(f"Expected dict for field '{field}', got {type(submap)}")
                    continue

                for sub, value in submap.items():
                    instrument_id = sub_to_id.get(sub, sub)
                    results.setdefault(instrument_id, {})[field.upper()] = value

        else:
            # Format B: {subscription: {FIELD: value}} → {id: {field: value}}
            for sub, fieldmap in out.items():
                if not isinstance(fieldmap, dict):
                    logger.warning(f"Expected dict for subscription '{sub}', got {type(fieldmap)}")
                    continue

                instrument_id = sub_to_id.get(sub, sub)
                results[instrument_id] = {
                    field.upper(): value
                    for field, value in fieldmap.items()
                }

        return results

    def _clone_request_with_fields(self, req: BaseRequest, fields: List[str]) -> BaseRequest:
        """
        Clone a request with a different set of fields.
        
        This is needed when some fields are handled and others need to go downstream.
        """
        import copy
        req_copy = copy.copy(req)
        req_copy.fields = fields
        return req_copy

    @abstractmethod
    def can_handle(self, req: BaseRequest) -> bool:
        """
        Check if this handler can process the given request.
        
        Args:
            req: Request to check
            
        Returns:
            True if this handler can process at least some fields from this request
        """
        pass

    @abstractmethod
    def process(
            self,
            requests: List[BaseRequest],
            query: QueryTimeScale
    ) -> Dict[str, Any]:
        """
        Process a batch of compatible requests.
        
        This method should query the data source and return results.
        It does NOT need to guarantee completeness - the handle() method
        will ensure all requested fields are present (even if None).
        
        Args:
            requests: List of requests this handler can process
            query: QueryTimescale instance for database access
            
        Returns:
            Data in either format A or B:
            
            Format A (field-centric):
                {FIELD: {subscription: value}}
                Example: {"MID": {"IHYG": {...}, "VUSA": {...}}}
            
            Format B (subscription-centric):
                {subscription: {FIELD: value}}
                Example: {"IHYG": {"MID": {...}, "VOLUME": {...}}}
            
        Note:
            - Return only the data you have; missing fields will be set to None
            - Either format is acceptable; normalization happens automatically
            - Do NOT add None for missing fields - handle() will do this
        """
        pass


class MarketDataHandler(Handler, ABC):
    """
    Abstract base class for market data handlers.

    Market data handlers process time-series market data (prices, volumes, etc).
    """
    pass
