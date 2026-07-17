"""驱动基类：统一初始化、请求节流；缓存由装饰器处理。"""

import asyncio
import contextvars
import time
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from core.base import FileItem, OperationResult, DriverInfo
from core.log_manager import get_writer, LogModule

# 协程级额外 API 延迟补偿，供缓存保持/STRM 等任务叠加在 operation_delay 之上，不影响其他协程
_extra_api_delay: contextvars.ContextVar[int] = contextvars.ContextVar('extra_api_delay', default=0)


class BaseDriver(ABC):
    def __init__(self, config):
        self.config = config
        self.account_id = getattr(config, 'account_id', 'default')
        self._cache_manager = None
        self._log = get_writer(LogModule.DRIVER)
        self._request_interval_lock = asyncio.Lock()
        self._last_request_started_at: Optional[float] = None

    def set_cache_manager(self, cache_manager):
        self._cache_manager = cache_manager

    def is_connectivity_test(self) -> bool:
        """account_id='temp_test' 代表只是新增/编辑账号时做一次连通性验证，不写入任何持久状态。"""
        account_id = getattr(self, "_account_id", None) or getattr(self, "account_id", None)
        return str(account_id) == "temp_test"

    async def wait_for_request_interval(self) -> None:
        delay_ms = int(getattr(self.config, "operation_delay", 0) or 0) + _extra_api_delay.get()
        if delay_ms <= 0:
            return

        async with self._request_interval_lock:
            now = time.monotonic()
            if self._last_request_started_at is not None:
                elapsed_ms = (now - self._last_request_started_at) * 1000
                remaining_ms = delay_ms - elapsed_ms
                if remaining_ms > 0:
                    await asyncio.sleep(remaining_ms / 1000.0)
            self._last_request_started_at = time.monotonic()

    @classmethod
    @abstractmethod
    def get_info(cls) -> DriverInfo:
        raise NotImplementedError

    @abstractmethod
    async def init(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def test_connection(self) -> OperationResult:
        raise NotImplementedError

    def supports_parallel_range_download(self) -> bool:
        """跨盘兜底下载是否允许多 Range 并发。CDN 拒绝并发 Range 的驱动覆写为 False。"""
        return True

    @staticmethod
    def normalize_transfer_hash(method: str, value: str) -> str:
        text = str(value or "").strip().lower()
        if str(method or "").lower() == "md5":
            if len(text) != 32 or any(ch not in "0123456789abcdef" for ch in text):
                return ""
            return text
        if str(method or "").lower() == "sha1":
            if len(text) != 40 or any(ch not in "0123456789abcdef" for ch in text):
                return ""
            return text
        return ""

    async def resolve_transfer_hash(self, item: FileItem, method: str, *, allow_stream: bool = False) -> str:
        extra = item.extra or {}
        hashes = extra.get("hashes") or {}
        value = hashes.get(method)
        if not value and method == "sha1":
            value = extra.get("hash_info")
        if not value and method == "md5":
            value = extra.get("md5") or extra.get("etag") or extra.get("content_md5")
        return self.normalize_transfer_hash(method, str(value or ""))

    async def rapid_upload_by_hash(
        self,
        parent_id: str,
        filename: str,
        hash_type: str,
        hash_value: str,
        size: int,
        duplicate: int = 1,
    ) -> OperationResult:
        """按文件指纹秒传（跨盘秒传通用能力）。

        目标盘若支持某种指纹的秒传，应覆写本方法并在 DRIVER_INFO.capabilities
        声明对应能力（如 rapid_sha1）。命中返回 data={"reuse": True, "file_id": ...}，
        未命中返回 data={"reuse": False}。默认不支持。
        """
        raise NotImplementedError(f"{self.__class__.__name__} 不支持指纹秒传")

    async def purge_file(self, file_id: str) -> OperationResult:
        """彻底删除（不进回收站）。默认等同 delete_file；具备彻底删除能力的驱动覆写。

        供跨盘探测临时目录、未命中空目录等内部清理场景使用，避免污染目标盘回收站。
        """
        return await self.delete_file(file_id)

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} account_id={self.account_id}>"
