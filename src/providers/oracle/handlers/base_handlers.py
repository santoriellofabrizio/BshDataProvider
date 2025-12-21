import logging
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Set

from core.requests.requests import BaseRequest
from providers.oracle.query_oracle import QueryOracle

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

    def set_next(self, handler: 'Handler') -> 'Handler':
        """Link the next handler in the chain."""
        self._next = handler
        return handler

    def handle(self, requests: List[Any], query: QueryOracle) -> Dict[str, Any]:
        """
        Handle a BATCH of requests.
        Each handler processes ONLY the fields it can handle, rest goes to next.

        Args:
            requests: List of requests to handle
            query: QueryOracle instance for database access

        Returns:
            Dict[instrument_id, Dict[field, value]]
            
        Example output:
            {
                "IHYG": {"TER": 0.55, "ISIN": "IE00B4L5Y983"},
                "VUSA": {"TER": 0.07, "ISIN": "IE00B3XXRP09"}
            }
        """
        can_process = []
        remaining = []

        # ============================================================
        # STEP 1: Split requests by capability
        # ============================================================
        for req in requests:
            if self.can_handle(req):
                can_process.append(req)
            else:
                remaining.append(req)

        results = {}
        downstream_requests = []

        # ============================================================
        # STEP 2: Process compatible requests in batch
        # ============================================================
        if can_process:
            # Collect all requested fields
            requested_fields = {
                f.upper()
                for req in can_process
                for f in req.fields
            }

            # Process in batch
            raw_out = self.process(can_process, query) or {}
            
            # Normalize to format A: {FIELD: {subscription: value}}
            normalized = self._normalize_output(raw_out, requested_fields)

            # ============================================================
            # STEP 3: Convert to final format and ensure completeness
            # ============================================================
            for req in can_process:
                req_id = req.instrument.id
                sub = req.subscription or req.instrument.isin or req.instrument.ticker
                req_fields = {f.upper() for f in req.fields}

                results.setdefault(req_id, {})
                handled_fields = set()

                # Extract values from normalized format
                for field, submap in normalized.items():
                    ufield = field.upper()
                    if ufield in req_fields:
                        val = submap.get(sub)
                        results[req_id][ufield] = val
                        handled_fields.add(ufield)

                # Ensure all requested fields are present (even if None)
                for field in req_fields:
                    if field not in results[req_id]:
                        results[req_id][field] = None
                        logger.debug(f"Field '{field}' not found for {req_id}, set to None")

                # If some fields are still missing/None, forward to next handler
                missing = {f for f in req_fields if results[req_id].get(f) is None}
                if missing:
                    req_copy = self._clone_request_with_fields(req, list(missing))
                    downstream_requests.append(req_copy)

        # ============================================================
        # STEP 4: Add requests this handler can't process at all
        # ============================================================
        if remaining:
            downstream_requests.extend(remaining)

        # ============================================================
        # STEP 5: Forward to next handler
        # ============================================================
        if downstream_requests and self._next:
            down = self._next.handle(downstream_requests, query)
            for req_id, data in down.items():
                results.setdefault(req_id, {}).update(data)

        return results

    def _normalize_output(
            self, 
            out: Dict[str, Any], 
            requested_fields: Set[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Normalize output to format A: {FIELD: {subscription: value}}
        
        Supports two input formats:
            Format A: {FIELD: {subscription: value}}  → Already normalized
            Format B: {subscription: {FIELD: value}}  → Needs inversion
        
        Detection strategy:
            If any top-level key matches a requested field → Format A
            Otherwise → Format B
        
        Args:
            out: Raw output from process()
            requested_fields: Set of uppercase field names that were requested
            
        Returns:
            Dict in format A: {FIELD: {subscription: value}}
            
        Examples:
            Input (Format A):
                {"TER": {"IHYG": 0.55, "VUSA": 0.07}}
            Output:
                {"TER": {"IHYG": 0.55, "VUSA": 0.07}}
            
            Input (Format B):
                {"IHYG": {"TER": 0.55, "ISIN": "IE00..."}}
            Output:
                {"TER": {"IHYG": 0.55}, "ISIN": {"IHYG": "IE00..."}}
        """
        if not out:
            return {}

        keys = list(out.keys())

        # Check if any top-level key is a requested field → Format A
        if any(k.upper() in requested_fields for k in keys):
            return out  # Already in format A

        # Otherwise → Format B, needs inversion
        normalized = {}
        for sub, fieldmap in out.items():
            if isinstance(fieldmap, dict):
                for field, value in fieldmap.items():
                    normalized.setdefault(field.upper(), {})[sub] = value
            else:
                logger.warning(f"Expected dict for subscription '{sub}', got {type(fieldmap)}")
        
        return normalized

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
            query: QueryOracle
    ) -> Dict[str, Any]:
        """
        Process a batch of compatible requests.
        
        This method should query the data source and return results.
        It does NOT need to guarantee completeness - the handle() method
        will ensure all requested fields are present (even if None).
        
        Args:
            requests: List of requests this handler can process
            query: QueryOracle instance for database access
            
        Returns:
            Data in either format A or B:
            
            Format A (field-centric):
                {FIELD: {subscription: value}}
                Example: {"TER": {"IHYG": 0.55, "VUSA": 0.07}}
            
            Format B (subscription-centric):
                {subscription: {FIELD: value}}
                Example: {"IHYG": {"TER": 0.55, "ISIN": "IE00..."}}
            
        Note:
            - Return only the data you have; missing fields will be set to None
            - Either format is acceptable; normalization happens automatically
            - Do NOT add None for missing fields - handle() will do this
        """
        pass


class ReferenceFieldHandler(Handler, ABC):
    """
    Abstract base class for reference field handlers.
    
    Reference handlers process static/semi-static fields like TER, ISIN, etc.
    """
    pass


class HistoricalFieldHandler(Handler, ABC):
    """
    Abstract base class for historical field handlers.
    
    Historical handlers process time-series data (NAV history, dividends, etc).
    """

    def set_next(self, handler: 'HistoricalFieldHandler') -> 'HistoricalFieldHandler':
        """Link the next handler in the chain."""
        self._next = handler
        return handler


class BulkFieldHandler(Handler, ABC):
    """
    Abstract base class for bulk field handlers.
    
    Bulk handlers process large datasets (PCF composition, FX composition, etc).
    """

    def set_next(self, handler: 'BulkFieldHandler') -> 'BulkFieldHandler':
        """Link the next handler in the chain."""
        self._next = handler
        return handler


class GeneralHandler(Handler, ABC):
    """
    Abstract base class for general (global) field handlers.

    General handlers process fields not tied to specific instruments,
    such as lookup tables, reference data, and global configurations.
    """

    def set_next(self, handler: 'GeneralHandler') -> 'GeneralHandler':
        """Link the next handler in the chain."""
        self._next = handler
        return handler
