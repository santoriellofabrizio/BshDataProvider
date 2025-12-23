import logging
from typing import Union, List, Dict, Any, Optional
from collections import defaultdict

from core.base_classes.base_provider import BaseProvider
from core.enums.datasources import DataSource
from core.requests.requests import BaseRequest, BaseMarketRequest, BaseStaticRequest
from core.response_tracking.request_tracker import RequestTracker
from core.utils.common import load_yaml
from core.utils.config_manager import ConfigManager
from core.utils.singleton import Singleton

from providers.bloomberg.bloomberg import BloombergProvider
from providers.oracle.provider import OracleProvider
from providers.timescale.provider import TimescaleProvider


class BSHDataClient(Singleton):
    """Client unificato per recupero dati da multiple sorgenti."""

    def __init__(self, config_manager: Optional[ConfigManager] = None, config_path=None):
        """
        Initialize BSHDataClient.
        
        Args:
            config_manager: ConfigManager instance (preferred, uses cached config)
            config_path: Path to config file (backward compatibility, creates ConfigManager)
        """
        
        # Support both ConfigManager (new) and config_path (backward compatibility)
        if config_manager is None:
            # Backward compatibility: create ConfigManager from path
            config_manager = ConfigManager.load(config_path)
        
        self._config_manager = config_manager
        
        # Get client config
        client_config = config_manager.get_client_config()

        provider_specs = {
            DataSource.TIMESCALE.value: (
                "timescale",
                lambda: TimescaleProvider(config_manager=config_manager)
            ),
            DataSource.BLOOMBERG.value: (
                "bloomberg",
                lambda: BloombergProvider()
            ),
            DataSource.ORACLE.value: (
                "oracle",
                lambda: OracleProvider(config_manager=config_manager)
            ),
        }

        self.providers = {
            name: self._init_lazy_provider(key, factory, client_config)
            for name, (key, factory) in provider_specs.items()
        }
        self._tracker = RequestTracker()

    # @staticmethod
    # def _init_lazy_provider(key: str, factory, cfg: dict):
    #     """Inizializzazione lazy: torna None se disattivato."""
    #     if not cfg.get(f"activate_{key}", True):
    #         return None
    #     return LazyProviderProxy(factory)

    @staticmethod
    def _init_lazy_provider(key: str, factory, client_config):
        """Inizializzazione lazy: torna None se disattivato."""
        # Support both dict (backward compat) and ClientConfig object
        if isinstance(client_config, dict):
            activate_key = f"activate_{key}"
            if not client_config.get(activate_key, True):
                return None
        else:
            # ClientConfig dataclass
            activate_key = f"activate_{key}"
            if not getattr(client_config, activate_key, True):
                return None
        return factory()


    @property
    def tracker(self) -> RequestTracker:
        return self._tracker

    def reset_tracker(self) -> None:
        self._tracker.reset()

    def send(self, requests: Union[BaseRequest, List[BaseRequest]]) -> Dict[str, Dict[str, Any]]:
        """Invia richieste e ritorna risultati aggregati."""
        if isinstance(requests, BaseRequest):
            requests = [requests]
        if not requests:
            return {}

        # Raggruppa per provider
        batches: Dict[str, List[BaseRequest]] = defaultdict(list)
        for req in requests:
            provider_name = str(req.source.value)
            batches[provider_name].append(req)
            self._tracker.track(req, provider=provider_name)

        results: Dict[str, Dict[str, Any]] = {}

        for src, batch in batches.items():
            provider = self._get_provider(src)
            try:
                batch_result = self._dispatch(provider, batch)
                if isinstance(batch_result, dict):
                    results.update(batch_result)
                elif hasattr(batch_result, "instrument"):
                    results[batch_result.instrument.id] = batch_result
            except Exception as e:
                logging.exception(f"Error from {provider.__class__.__name__}: {e}")
                for req in batch:
                    self._tracker.mark_failed(req.request_id, error=e)

        self._update_tracking(results)
        return results

    def _dispatch(self, provider: BaseProvider, batch: List[BaseRequest]):
        """Dispatch al metodo corretto del provider."""
        if isinstance(batch[0], BaseMarketRequest):
            return provider.fetch_market_data(batch)
        elif isinstance(batch[0], BaseStaticRequest):
            return provider.fetch_info_data(batch)
        raise TypeError(f"Unsupported request type {type(batch[0]).__name__}")

    def _update_tracking(self, results: Dict[str, Dict[str, Any]]) -> None:
        """Aggiorna tracker con i risultati."""
        for status in self._tracker.get_all():
            if status.state.is_terminal:
                continue
            req = status.request
            instr_id = self._get_instrument_id(req)
            instr_results = results.get(instr_id, {})

            fields = req.fields if isinstance(req.fields, list) else [req.fields]
            result_data = {f.upper(): instr_results.get(f.upper()) or instr_results.get(f.lower())
                           for f in fields if instr_results.get(f.upper()) or instr_results.get(f.lower())}

            self._tracker.update_with_result(req.request_id, result_data)

    def _get_instrument_id(self, req: BaseRequest) -> str:
        """Estrae instrument ID dalla request."""
        if isinstance(req, BaseMarketRequest):
            return req.instrument.id
        elif isinstance(req, BaseStaticRequest):
            return req.instrument.id if getattr(req, "instrument", None) else "GLOBAL"
        return "UNKNOWN"

    def _get_provider(self, source: Union[str, DataSource]) -> BaseProvider:
        if isinstance(source, DataSource):
            source = source.value
        provider = self.providers.get(source)
        if not provider:
            raise ValueError(f"Provider '{source}' non disponibile")
        return provider


def safe_init(provider_cls, *args, **kwargs):
    try:
        return provider_cls(*args, **kwargs)
    except Exception as e:
        logging.error(f"Failed to initialize {provider_cls.__name__}: {e}", exc_info=True)
        return None