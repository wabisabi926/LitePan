"""百度网盘 Open 驱动配置：以 refresh_token 驱动的 OAuth，路径式 root_folder_id。"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.base import DriverConfig


@dataclass
class BaiduOpenConfig(DriverConfig):
    access_token: Optional[str] = None
    refresh_token: str = ""
    root_folder_id: str = "/"
    delete_mode: str = "trash"
    download_mode: str = "proxy"
    proxy_server: str = ""
    cache_ttl: Optional[int] = None
    operation_delay: int = 300

    # 百度 token 有效期长（30 天），提前 24h 刷新足够
    auth_type: str = "token"
    token_expires_seconds: int = 2592000
    refresh_advance_seconds: int = 86400
    retry_cooldown_seconds: int = 30
    supports_refresh: bool = True

    def __post_init__(self):
        if not self.refresh_token:
            raise ValueError("刷新令牌不能为空")
        if not self.root_folder_id:
            self.root_folder_id = "/"

        self.root_folder_id = str(self.root_folder_id).strip() or "/"
        # 兼容早期把 "0" 当根目录的配置，统一改成 "/"
        if self.root_folder_id == "0":
            self.root_folder_id = "/"

        # 百度开放平台目前只有“移入回收站”，写死避免用户误选永久删除
        self.delete_mode = "trash"

        if self.download_mode not in ["redirect", "proxy", "proxy_server"]:
            raise ValueError("下载模式只能是 redirect、proxy 或 proxy_server")
        if self.cache_ttl is not None and (self.cache_ttl < 0 or self.cache_ttl > 1440):
            raise ValueError("缓存时间必须在 0-1440 分钟之间，None 表示使用全局默认值")

    @classmethod
    def get_form_schema(cls) -> Dict[str, Any]:
        return {
            "refresh_token": {
                "type": "password",
                "required": True,
                "label": "刷新令牌",
                "description": "百度网盘开放平台 Refresh Token，驱动会通过它自动换取访问令牌",
                "placeholder": "请输入刷新令牌",
            },
            "root_folder_id": {
                "type": "text",
                "required": False,
                "label": "根目录ID",
                "description": "/ 表示网盘根目录；也可填写 /apps/xxx 形式的目录路径作为挂载起点",
                "default": "/",
                "placeholder": "例如: /",
            },
            "delete_mode": {
                "type": "select",
                "required": False,
                "label": "删除模式",
                "description": "百度开放平台当前仅支持移动到回收站",
                "default": "trash",
                "options": [
                    {"value": "trash", "label": "移动到回收站"},
                ],
            },
            "download_mode": {
                "type": "select",
                "required": False,
                "label": "下载模式",
                "description": "百度网盘大文件下载依赖特定请求头，建议默认使用本地代理",
                "default": "proxy",
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
                "description": "仅在下载模式为代理服务器时生效",
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
        return "oauth"

    def get_cache_config(self) -> Dict[str, Any]:
        return {
            "ttl": self.cache_ttl * 60 if self.cache_ttl is not None else None,
            "key_prefix": f"baidu_open_{hash(self.refresh_token)}",
        }
