"""OneDrive 驱动配置。"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.base import DriverConfig


@dataclass
class OneDriveConfig(DriverConfig):
    access_token: str = ""
    refresh_token: str = ""
    root_item_id: str = "/"
    delete_mode: str = "trash"
    download_mode: str = "redirect"
    proxy_server: str = ""
    cache_ttl: Optional[int] = None
    operation_delay: int = 150

    auth_type: str = "token"
    token_expires_seconds: int = 3600
    refresh_advance_seconds: int = 600
    retry_cooldown_seconds: int = 30
    supports_refresh: bool = True

    def __post_init__(self):
        if not self.access_token:
            raise ValueError("访问令牌不能为空")
        if not self.refresh_token:
            raise ValueError("刷新令牌不能为空")

        self.root_item_id = str(self.root_item_id or "/").strip() or "/"
        if self.root_item_id in ("0", "root"):
            self.root_item_id = "/"
        elif self.root_item_id.startswith("/"):
            self.root_item_id = "/" + self.root_item_id.strip("/")

        if self.delete_mode not in ["trash", "delete"]:
            raise ValueError("删除模式只能是 trash 或 delete")
        if self.download_mode not in ["redirect", "proxy", "proxy_server"]:
            raise ValueError("下载模式只能是 redirect、proxy 或 proxy_server")
        if self.cache_ttl is not None and (self.cache_ttl < 0 or self.cache_ttl > 1440):
            raise ValueError("缓存时间必须在0-1440分钟之间，None表示使用全局默认值，0表示禁用缓存")

    def get_auth_method(self) -> str:
        return "oauth"

    def get_cache_config(self) -> Dict[str, Any]:
        token_key = self.refresh_token or self.access_token
        return {
            "ttl": self.cache_ttl * 60 if self.cache_ttl is not None else None,
            "key_prefix": f"onedrive_{hash(token_key)}",
        }

    @classmethod
    def get_form_schema(cls) -> Dict[str, Any]:
        return {
            "access_token": {
                "type": "password",
                "required": True,
                "label": "访问令牌",
                "description": "Microsoft Graph access_token",
                "placeholder": "请输入 access_token",
            },
            "refresh_token": {
                "type": "password",
                "required": True,
                "label": "刷新令牌",
                "description": "Microsoft Graph refresh_token，用于自动续期访问令牌",
                "placeholder": "请输入 refresh_token",
            },
            "root_item_id": {
                "type": "text",
                "required": False,
                "label": "根文件夹路径",
                "description": "/ 表示 OneDrive 根目录；也可填写 /test 这类从根目录开始的路径；高级用户也可填写 item id",
                "default": "/",
                "placeholder": "例如: / 或 /test",
            },
            "delete_mode": {
                "type": "select",
                "required": False,
                "label": "删除模式",
                "description": "OneDrive 支持移动到回收站，也支持直接永久删除",
                "default": "trash",
                "options": [
                    {"value": "trash", "label": "移到回收站"},
                    {"value": "delete", "label": "永久删除"},
                ],
            },
            "download_mode": {
                "type": "select",
                "required": False,
                "label": "下载模式",
                "description": "OneDrive 返回临时下载直链，默认使用302重定向",
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
                "description": "该账号的全局缓存有效期，空值表示使用全局默认值，0表示禁用缓存",
                "default": None,
                "placeholder": "请输入缓存时间，留空采用全局缓存时间",
                "min": 0,
                "max": 1440,
            },
        }
