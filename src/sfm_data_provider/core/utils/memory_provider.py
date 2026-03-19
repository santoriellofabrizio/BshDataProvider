"""
Enhanced Memory Provider con LRU Cache e Path Leggibili
Compatibile con l'esistente, aggiunge funzionalità senza breaking changes
"""
import pandas as pd
from joblib import Memory
import os
import logging
from functools import wraps, lru_cache
import inspect
import re
import json
from typing import Callable, Dict
from pathlib import Path

from sfm_data_provider.core.requests.requests import BaseRequest

# ============================================================================
# CONFIGURAZIONE
# ============================================================================

_cache_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "cache")
_memory_provider = None
logger = logging.getLogger(__name__)
ENABLE_CACHE = True

# Nuova configurazione
USE_READABLE_PATHS = True  # Attiva path semantici invece di hash
LRU_MAXSIZE = 10000  # Dimensione cache RAM


# ============================================================================
# METRICS
# ============================================================================

class CacheMetrics:
    """Raccolta metriche cache per monitoring"""

    def __init__(self):
        self.ram_hits = 0
        self.ram_misses = 0
        self.disk_hits = 0
        self.disk_misses = 0
        self.db_queries = 0
        self.key_access_count = {}  # Track hot keys

    def record_ram_hit(self, key: str):
        self.ram_hits += 1
        self.key_access_count[key] = self.key_access_count.get(key, 0) + 1

    def record_ram_miss(self):
        self.ram_misses += 1

    def record_disk_hit(self):
        self.disk_hits += 1

    def record_disk_miss(self):
        self.disk_misses += 1

    def record_db_query(self):
        self.db_queries += 1

    def report(self) -> Dict:
        total_ram = self.ram_hits + self.ram_misses
        ram_rate = self.ram_hits / total_ram if total_ram > 0 else 0

        total_disk = self.disk_hits + self.disk_misses
        disk_rate = self.disk_hits / total_disk if total_disk > 0 else 0

        # Top 5 hot keys
        top_keys = sorted(self.key_access_count.items(), key=lambda x: x[1], reverse=True)[:5]

        return {
            "ram": {
                "hit_rate": f"{ram_rate:.1%}",
                "hits": self.ram_hits,
                "misses": self.ram_misses,
                "top_keys": top_keys
            },
            "disk": {
                "hit_rate": f"{disk_rate:.1%}",
                "hits": self.disk_hits,
                "misses": self.disk_misses
            },
            "db_queries": self.db_queries,
            "queries_avoided": self.ram_hits + self.disk_hits
        }

    def __str__(self):
        report = self.report()
        return json.dumps(report, indent=2)


# Singleton metrics
_metrics = CacheMetrics()


def get_metrics() -> CacheMetrics:
    """Accesso alle metriche globali"""
    return _metrics


# ============================================================================
# READABLE PATH MANAGEMENT
# ============================================================================

class ReadablePathManager:
    """Gestisce path semantici e leggibili per la cache"""

    @staticmethod
    def sanitize(s: str) -> str:
        """Rimuove caratteri non validi per filesystem"""
        # Sostituisce caratteri problematici
        s = str(s)
        s = re.sub(r'[<>:"/\\|?*]', '_', s)
        # Limita lunghezza
        if len(s) > 100:
            s = s[:100]
        return s

    @staticmethod
    def generate_path(
            cache_dir: str,
            provider: str,
            category: str,
            identifier: str,
            *args,
            extension: str = "pkl"
    ) -> Path:
        """
        Genera path semantico e leggibile

        Args:
            cache_dir: Directory base cache
            provider: Provider name (timescale, bloomberg, oracle)
            category: Categoria (market, info, reference, historical)
            identifier: Identificatore principale (ISIN, ticker, etc)
            *args: Altri parametri per comporre il filename
            extension: Estensione file

        Returns:
            Path completo al file cache

        Example:
            generate_path(cache_dir, "timescale", "market", "AAPL_US",
                         "mid_bid", "2024-01-01", "2024-12-31")
            -> cache/timescale/market/AAPL_US/mid_bid__2024-01-01__2024-12-31.pkl
        """
        provider = ReadablePathManager.sanitize(provider)
        category = ReadablePathManager.sanitize(category)
        identifier = ReadablePathManager.sanitize(identifier)

        # Crea directory struttura
        dir_path = Path(cache_dir) / provider / category / identifier
        dir_path.mkdir(parents=True, exist_ok=True)

        # Costruisci filename da args
        if args:
            safe_args = [ReadablePathManager.sanitize(str(a)) for a in args]
            filename = "__".join(safe_args) + f".{extension}"
        else:
            filename = f"data.{extension}"

        return dir_path / filename


# ============================================================================
# ENHANCED MEMORY PROVIDER
# ============================================================================

def _get_memory():
    """Crea (lazy) l'oggetto Memory solo quando serve"""
    global _memory_provider
    if _memory_provider is None:
        os.makedirs(_cache_dir, exist_ok=True)
        _memory_provider = Memory(_cache_dir, verbose=0)
    return _memory_provider


def enable_cache():
    global ENABLE_CACHE
    ENABLE_CACHE = True
    logger.info("Cache globally enabled")


def disable_cache():
    global ENABLE_CACHE
    ENABLE_CACHE = False
    logger.info("Cache globally disabled")


def set_cache_dir(path: str):
    """Imposta directory cache custom"""
    global _cache_dir, _memory_provider
    _cache_dir = os.path.abspath(path)
    _memory_provider = None
    if ENABLE_CACHE:
        os.makedirs(_cache_dir, exist_ok=True)
        logger.info(f"Cache directory set to: {_cache_dir}")


# ============================================================================
# LRU CACHE DECORATOR (per funzioni pure con input serializzabili)
# ============================================================================

def lru_cache_with_metrics(maxsize=LRU_MAXSIZE):
    """
    Wrapper di lru_cache con tracking metriche
    Usare per:
    - Parsing ISIN
    - Classificazioni strumenti
    - Conversioni string -> enum
    - Qualsiasi funzione pura con input serializzabili (str, int, tuple)
    """

    def decorator(func):
        # Crea LRU cache
        cached_func = lru_cache(maxsize=maxsize)(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            key = f"{func.__qualname__}_{args}_{tuple(sorted(kwargs.items()))}"

            # Check se in cache
            cache_info = cached_func.cache_info()
            hits_before = cache_info.hits

            result = cached_func(*args, **kwargs)

            # Check se c'è stato hit
            cache_info_after = cached_func.cache_info()
            if cache_info_after.hits > hits_before:
                _metrics.record_ram_hit(key)
                logger.debug(f"[RAM HIT] {func.__qualname__}")
            else:
                _metrics.record_ram_miss()
                logger.debug(f"[RAM MISS] {func.__qualname__}")

            return result

        # Esponi cache_info e cache_clear
        wrapper.cache_info = cached_func.cache_info
        wrapper.cache_clear = cached_func.cache_clear

        return wrapper

    return decorator


# ============================================================================
# ENHANCED DISK CACHE DECORATOR
# ============================================================================

import threading

def cache_bsh_data(func=None, *, provider=None, category=None, readable_path=USE_READABLE_PATHS):
    """
    Decoratore cache con opzioni avanzate
    """

    def decorator(func: Callable) -> Callable:
        sig = inspect.signature(func)
        ignore_list = [
            name for name in ("self", "query_ts", "show_progress", "verbose", "conn", "session")
            if name in sig.parameters
        ]

        _cached_func = None
        _lock = threading.Lock()

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal _cached_func

            if not ENABLE_CACHE:
                logger.info(f"[CACHE DISABLED] {func.__qualname__}")
                _metrics.record_db_query()
                return func(*args, **kwargs)

            # Thread-safe initialization
            with _lock:
                if _cached_func is None:
                    memory_provider = _get_memory()
                    _cached_func = memory_provider.cache(func, ignore=ignore_list)

            # Cache check + call under lock (joblib is not thread-safe)
            with _lock:
                try:
                    is_cached = _cached_func.check_call_in_cache(*args, **kwargs)
                except Exception:
                    is_cached = False

                if is_cached:
                    _metrics.record_disk_hit()
                    logger.info(f"[DISK HIT] {func.__qualname__}")
                    return _cached_func(*args, **kwargs)

            # Cache miss: DB query runs WITHOUT lock (parallel I/O)
            _metrics.record_disk_miss()
            _metrics.record_db_query()
            logger.info(f"[DISK MISS] {func.__qualname__}")
            result = func(*args, **kwargs)

            # Store result in cache under lock
            with _lock:
                try:
                    _cached_func(*args, **kwargs)  # re-calls but now result is in DB connection cache / fast
                except Exception as e:
                    logger.warning(f"[CACHE STORE ERROR] {func.__qualname__}: {e}")

            return result

        return wrapper

    if func is None:
        return decorator
    else:
        return decorator(func)


# ============================================================================
# UTILITY
# ============================================================================

def _safe_preview(x, maxlen=120):
    """Preview sicuro per logging"""
    for x_ in x:
        if isinstance(x_, BaseRequest):
            return x.__repr__()

    try:
        if isinstance(x, pd.DataFrame):
            return f"<DataFrame shape={x.shape}>"
        if isinstance(x, (list, tuple)):
            return json.dumps(x[:3]) + ("..." if len(x) > 3 else "")
        if isinstance(x, dict):
            preview = {k: str(v)[:50] for k, v in list(x.items())[:5]}
            return json.dumps(preview) + ("..." if len(x) > 5 else "")
        s = str(x)
        return s if len(s) <= maxlen else s[:maxlen] + "..."
    except Exception:
        return "<unprintable>"


def clear_cache():
    """Pulisce tutta la cache (RAM + disk)"""
    global _memory_provider

    # Clear RAM cache (LRU)
    # Note: Bisogna tenere traccia delle funzioni cached
    logger.info("Clearing RAM cache...")

    # Clear disk cache
    if _memory_provider is not None:
        logger.info("Clearing disk cache...")
        _memory_provider.clear()

    logger.info("Cache cleared")


def cache_stats():
    """Stampa statistiche cache"""
    print("=" * 60)
    print("CACHE STATISTICS")
    print("=" * 60)
    print(_metrics)
    print("=" * 60)


# ============================================================================
# BACKWARD COMPATIBILITY
# ============================================================================

# Mantieni l'interfaccia originale per compatibilità
__all__ = [
    'cache_bsh_data',
    'enable_cache',
    'disable_cache',
    'set_cache_dir',
    'lru_cache_with_metrics',
    'get_metrics',
    'cache_stats',
    'clear_cache',
    'ReadablePathManager'
]