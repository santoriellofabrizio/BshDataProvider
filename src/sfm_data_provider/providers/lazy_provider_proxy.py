import threading


class LazyProviderProxy:
    def __init__(self, factory):
        self._factory = factory
        self._provider = None
        self._lock = threading.Lock()

    def _get(self):
        if self._provider is None:
            with self._lock:
                if self._provider is None:
                    self._provider = self._factory()
        return self._provider

    def __getattr__(self, item):
        return getattr(self._get(), item)
