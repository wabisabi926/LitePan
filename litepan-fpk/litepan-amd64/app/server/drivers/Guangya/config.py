from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.base import DriverConfig


@dataclass
class GuangYaConfig(DriverConfig):
    access_token: Optional[str] = None
    refresh_token: str = ""
    client_id: str = "aMe-8VSlkrbQXpUR"
    device_id: str = ""
    root_folder_id: str = ""
    delete_mode: str = "trash"
    download_mode: str = "redirect"
    proxy_server: str = ""
    cache_ttl: Optional[int] = None
    operation_delay: int = 300

    auth_type: str = "token"
    token_expires_seconds: int = 7200
    refresh_advance_seconds: int = 1800
    retry_cooldown_seconds: int = 30
    supports_refresh: bool = True

    def __post_init__(self):
        if not self.refresh_token:
            raise ValueError("刷新令牌不能为空")
        if self.delete_mode not in ["trash", "delete"]:
            raise ValueError("删除模式只能是 trash 或 delete")
        if self.download_mode not in ["redirect", "proxy", "proxy_server"]:
            raise ValueError("下载模式只能是 redirect、proxy 或 proxy_server")
        if self.cache_ttl is not None and (self.cache_ttl < 0 or self.cache_ttl > 1440):
            raise ValueError("缓存时间必须在0-1440分钟之间，None表示使用全局默认值，0表示禁用缓存")

        self.client_id = (self.client_id or "aMe-8VSlkrbQXpUR").strip() or "aMe-8VSlkrbQXpUR"
        self.device_id = (self.device_id or "").strip().lower()
        self.root_folder_id = str(self.root_folder_id or "").strip()
        if self.root_folder_id in {"0", "/"}:
            self.root_folder_id = ""

    @classmethod
    def get_form_schema(cls) -> Dict[str, Any]:
        return {
            "access_token": {
                "type": "password",
                "required": True,
                "label": "访问令牌",
                "description": "从光鸭认证页获取的 Access Token；失效后会自动回退到 refresh_token 刷新",
                "placeholder": "请输入 access_token"
            },
            "refresh_token": {
                "type": "password",
                "required": True,
                "label": "刷新令牌",
                "description": "从光鸭认证页获取的 Refresh Token，用于自动刷新访问令牌",
                "placeholder": "请输入 refresh_token"
            },
            "root_folder_id": {
                "type": "text",
                "required": False,
                "label": "根目录ID",
                "description": "留空表示网盘根目录，也可填写某个文件夹 fileId 作为挂载起点",
                "default": "",
                "placeholder": "例如: 1234567890"
            },
            "delete_mode": {
                "type": "select",
                "required": False,
                "label": "删除模式",
                "description": "先预留删除模式选项，是否支持永久删除后续再按实际接口能力确认",
                "default": "trash",
                "options": [
                    {"value": "trash", "label": "移到回收站"},
                    {"value": "delete", "label": "永久删除"}
                ]
            },
            "download_mode": {
                "type": "select",
                "required": False,
                "label": "下载模式",
                "description": "先预留下载模式选项，302 是否可用后续再根据实际接口验证",
                "default": "redirect",
                "options": [
                    {"value": "redirect", "label": "302重定向"},
                    {"value": "proxy", "label": "本地代理"},
                    {"value": "proxy_server", "label": "代理服务器"}
                ]
            },
            "proxy_server": {
                "type": "text",
                "required": False,
                "label": "代理服务器地址",
                "description": "仅在下载模式为“代理服务器”时生效",
                "placeholder": "预留接口，暂无用处"
            },
            "cache_ttl": {
                "type": "number",
                "required": False,
                "label": "缓存时间(分钟)",
                "description": "该账号的全局缓存有效期。空值表示使用全局默认值，0表示禁用缓存",
                "default": None,
                "placeholder": "留空使用全局默认缓存时间",
                "min": 0,
                "max": 1440
            },
        }

    def get_auth_method(self) -> str:
        return "oauth"

    def get_cache_config(self) -> Dict[str, Any]:
        return {
            "ttl": self.cache_ttl * 60 if self.cache_ttl is not None else None,
            "key_prefix": f"guangya_{hash(self.refresh_token)}"
        }
