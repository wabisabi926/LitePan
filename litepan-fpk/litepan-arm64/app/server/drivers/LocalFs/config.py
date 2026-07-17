"""本地存储驱动配置。"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from core.base import DriverConfig


@dataclass
class LocalFsConfig(DriverConfig):
    base_path: str = ""
    follow_symlinks: bool = False
    download_mode: str = "proxy"
    cache_ttl: Optional[int] = None
    operation_delay: int = 0
    download_url_ttl: int = 300

    def __post_init__(self) -> None:
        if isinstance(self.follow_symlinks, str):
            self.follow_symlinks = self.follow_symlinks.strip().lower() in ("true", "1", "yes", "on")

        raw = (self.base_path or "").strip()
        if not raw:
            raise ValueError("必须填写本地路径 base_path")
        if not raw.startswith("/"):
            raise ValueError("本地路径必须是绝对路径（以 / 开头），例如 /app/strm")

        try:
            self._resolved_base = Path(raw).resolve()
        except Exception as e:
            raise ValueError(f"无法解析本地路径: {e}")

        if self.download_mode not in ("proxy", "proxy_server"):
            self.download_mode = "proxy"
        if self.cache_ttl is not None and (self.cache_ttl < 0 or self.cache_ttl > 1440):
            raise ValueError("缓存时间必须在 0～1440 分钟之间，None 表示使用全局默认")
        if self.download_url_ttl < 30 or self.download_url_ttl > 3600:
            raise ValueError("下载 URL 有效期建议在 30～3600 秒之间")

    def resolved_base(self) -> Path:
        return self._resolved_base

    @classmethod
    def get_form_schema(cls) -> Dict[str, Any]:
        return {
            "base_path": {
                "type": "local_dir",
                "required": True,
                "label": "本地路径",
                "description": (
                    "容器内的绝对路径。点击「浏览」可直接选择，"
                    "若目录不存在会尝试自动创建；要暴露容器外目录请先在 docker-compose 里 volume mount 进来。"
                ),
                "placeholder": "/app/strm",
            },
        }

    def get_cache_config(self) -> Dict[str, Any]:
        return {
            "cache_ttl": self.cache_ttl * 60 if self.cache_ttl is not None else None,
        }
