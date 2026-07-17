"""依赖容器：按需懒加载核心组件，避免模块级循环引用。"""

from typing import Optional
from core.log_manager import get_writer, LogModule


class DependencyContainer:
    def __init__(self):
        self._cache_manager = None
        self._cache_cleaner = None
        self._auth_manager = None
        self._log_manager = None
        self._hit_tracker = None

    @property
    def cache_manager(self):
        if self._cache_manager is None:
            from cache import get_global_cache
            self._cache_manager = get_global_cache()
        return self._cache_manager

    @property
    def cache_cleaner(self):
        if self._cache_cleaner is None:
            from cache import get_cache_cleaner
            self._cache_cleaner = get_cache_cleaner()
        return self._cache_cleaner

    @property
    def hit_tracker(self):
        if self._hit_tracker is None:
            from cache import get_hit_rate_tracker
            self._hit_tracker = get_hit_rate_tracker()
        return self._hit_tracker

    @property
    def log_manager(self):
        if self._log_manager is None:
            self._log_manager = get_writer(LogModule.SYSTEM)
        return self._log_manager

    def get_logger(self, module: LogModule):
        return get_writer(module)

    @property
    def auth_manager(self):
        if self._auth_manager is None:
            from core.auth_manager import auth_scheduler
            self._auth_manager = auth_scheduler
        return self._auth_manager


container = DependencyContainer()


def get_cache_manager():
    return container.cache_manager


def get_cache_cleaner():
    return container.cache_cleaner


def get_hit_tracker():
    return container.hit_tracker


def get_logger(module: LogModule):
    return container.get_logger(module)
