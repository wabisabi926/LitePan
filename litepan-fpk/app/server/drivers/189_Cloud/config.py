"""天翼云盘驱动配置。"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.base import DriverConfig


@dataclass
class Cloud189Config(DriverConfig):
    refresh_token: str = ""
    access_token: str = ""
    expires_at: int = 0
    token_expires_at: int = 0
    last_refresh_time: int = 0
    auth_status: str = "active"
    refresh_attempts: int = 0
    root_folder_id: str = "-11"
    delete_mode: str = "trash"
    download_mode: str = "redirect"
    proxy_server: str = ""
    cache_ttl: Optional[int] = None
    operation_delay: int = 500

    auth_type: str = "token"
    token_expires_seconds: int = 604800
    refresh_advance_seconds: int = 86400
    retry_cooldown_seconds: int = 30
    supports_refresh: bool = True

    def __post_init__(self):
        if not self.refresh_token:
            raise ValueError("请先扫码登录获取天翼云盘授权")
        if not self.root_folder_id:
            self.root_folder_id = "-11"
        self.root_folder_id = str(self.root_folder_id).strip() or "-11"
        if self.root_folder_id in {"/", "0"}:
            self.root_folder_id = "-11"
        if self.delete_mode not in ["trash", "delete"]:
            raise ValueError("删除模式只能是 trash 或 delete")
        if self.download_mode not in ["redirect", "proxy", "proxy_server"]:
            raise ValueError("下载模式只能是 redirect, proxy 或 proxy_server")
        if self.cache_ttl is not None and (self.cache_ttl < 0 or self.cache_ttl > 1440):
            raise ValueError("缓存时间必须在 0-1440 分钟之间，None 表示使用全局默认值")

    @classmethod
    def get_form_schema(cls) -> Dict[str, Any]:
        return {
            "refresh_token": {
                "type": "password",
                "required": True,
                "label": "Token",
                "description": "扫码登录后自动写入，用于刷新天翼云盘会话；通常无需手动修改",
                "placeholder": "请点击扫码登录自动获取",
            },
            "root_folder_id": {
                "type": "text",
                "required": False,
                "label": "根目录ID",
                "description": "-11 表示个人云根目录",
                "default": "-11",
                "placeholder": "例如: -11",
            },
            "delete_mode": {
                "type": "select",
                "required": False,
                "label": "删除模式",
                "description": "trash=移到回收站，delete=先移入回收站后再清理",
                "default": "trash",
                "options": [
                    {"value": "trash", "label": "移动到回收站"},
                    {"value": "delete", "label": "永久删除"},
                ],
            },
            "download_mode": {
                "type": "select",
                "required": False,
                "label": "下载模式",
                "description": "302重定向更快；若客户端不兼容可切换为本地代理",
                "default": "redirect",
                "options": [
                    {"value": "redirect", "label": "302重定向"},
                    {"value": "proxy", "label": "本地代理"},
                    {"value": "proxy_server", "label": "代理服务器"},
                ],
            },
            "proxy_server": {
                "type": "text",
                "required": False,
                "label": "代理服务器地址",
                "description": "仅在下载模式为'代理服务器'时生效",
                "placeholder": "预留接口，暂无用处",
            },
            "cache_ttl": {
                "type": "number",
                "required": False,
                "label": "缓存时间(分钟)",
                "description": "该账号的全局缓存有效期。留空表示使用全局默认值，0 表示禁用缓存",
                "default": None,
                "placeholder": "留空使用全局默认缓存时间",
                "min": 0,
                "max": 1440,
            },
        }

    def get_auth_method(self) -> str:
        return "qrcode"

    def get_cache_config(self) -> Dict[str, Any]:
        return {
            "ttl": self.cache_ttl * 60 if self.cache_ttl is not None else None,
            "key_prefix": f"189cloud_{hash(self.refresh_token)}",
        }
