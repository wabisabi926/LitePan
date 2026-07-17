from dataclasses import dataclass
from typing import Dict, Any, Optional
from core.base import DriverConfig

@dataclass
class OneOneFiveOpenConfig(DriverConfig):
    access_token: str = ""
    refresh_token: str = ""
    root_folder_id: str = "0"
    delete_mode: str = "trash"      # trash=回收站，delete=永久删除
    download_mode: str = "redirect"  # redirect / proxy / proxy_server
    proxy_server: str = ""
    cache_ttl: Optional[int] = None  # 分钟；None 走全局默认，0 禁用该账号缓存

    # 115 接口频控较严，默认用较大的最小请求间隔
    operation_delay: int = 800

    # OAuth token 参数：115 token 2h 过期，内部统一提前 15min 刷新
    auth_type: str = "token"
    token_expires_seconds: int = 7200
    refresh_advance_seconds: int = 900
    retry_cooldown_seconds: int = 30
    supports_refresh: bool = True

    def __post_init__(self):
        if not self.access_token:
            raise ValueError("访问令牌不能为空")
        if not self.refresh_token:
            raise ValueError("刷新令牌不能为空")
        if self.delete_mode not in ["trash", "delete"]:
            raise ValueError("删除模式只能是 trash 或 delete")
        if self.download_mode not in ["redirect", "proxy", "proxy_server"]:
            raise ValueError("下载模式只能是 redirect, proxy 或 proxy_server")
        if self.cache_ttl is not None and (self.cache_ttl < 0 or self.cache_ttl > 1440):
            raise ValueError("缓存时间必须在0-1440分钟之间，None表示使用全局默认值，0表示禁用缓存")
    
    @classmethod
    def get_form_schema(cls) -> Dict[str, Any]:
        return {
            "access_token": {
                "type": "password", "required": True, "label": "访问令牌",
                "description": "115网盘开放API的访问令牌", "placeholder": "请输入访问令牌"
            },
            "refresh_token": {
                "type": "password", "required": True, "label": "刷新令牌",
                "description": "115网盘开放API的刷新令牌", "placeholder": "请输入刷新令牌"
            },
            "root_folder_id": {
                "type": "text", "required": False, "label": "根目录ID",
                "description": "指定一个文件夹ID作为根目录，默认为0", "default": "0", "placeholder": "例如: 0"
            },
            "delete_mode": {
                "type": "select", "required": False, "label": "删除模式",
                "description": "115网盘的删除操作：trash=移到回收站，delete=永久删除回收站中的文件",
                "default": "trash",
                "options": [{"value": "trash", "label": "移到回收站"}, {"value": "delete", "label": "永久删除"}]
            },
            "download_mode": {
                "type": "select", "required": False, "label": "下载模式",
                "description": "302重定向更快，但部分客户端不兼容",
                "default": "redirect",
                "options": [
                    {"value": "redirect", "label": "302重定向"},
                    {"value": "proxy", "label": "本地代理"},
                    {"value": "proxy_server", "label": "代理服务器"}
                ]
            },
            "proxy_server": {
                "type": "text", "required": False, "label": "代理服务器地址",
                "description": "仅在下载模式为'代理服务器'时生效", "placeholder": "预留接口，暂无用处"
            },
            "cache_ttl": {
                "type": "number", "required": False, "label": "缓存时间(分钟)",
                "description": "该账号的全局缓存有效期，适用于文件列表、详情、WebDAV等所有缓存。空值表示使用全局默认值，0表示禁用缓存",
                "default": None,
                "placeholder": "请输入缓存时间，留空采用全局缓存时间",
                "min": 0,
                "max": 1440
            },
            # operation_delay 属于内部调优项，不对外暴露
        }

    def get_auth_method(self) -> str:
        return "oauth"

    def get_cache_config(self) -> Dict[str, Any]:
        return {
            "ttl": self.cache_ttl * 60 if self.cache_ttl is not None else None,
            "key_prefix": f"115_open_{hash(self.access_token)}"
        }
