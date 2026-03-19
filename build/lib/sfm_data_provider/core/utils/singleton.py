from threading import Lock


class Singleton:
    """Thread-safe Singleton metaclass."""

    _instances = {}
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with cls._lock:
                # Double-check locking
                if cls not in cls._instances:
                    instance = super().__new__(cls)
                    cls._instances[cls] = instance
        return cls._instances[cls]