"""123 云盘驱动配置：走旧站的 username+password 登录，token 由系统在后台维护。"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from core.base import DriverConfig


@dataclass
class Pan123ReverseConfig(DriverConfig):
    username: str = ""
    password: str = ""

    # access_token 内部字段：由登录接口写入，用户不直接填
    access_token: Optional[str] = None
    api_base_url: Optional[str] = None
    login_uuid: Optional[str] = None

    root_folder_id: str = "0"

    delete_mode: str = "trash"      # trash=回收站，delete=永久删除
    download_mode: str = "redirect"  # redirect / proxy / proxy_server
    proxy_server: str = ""

    cache_ttl: Optional[int] = None
    # 123 的直链带签名且极易失效，不能复用
    download_url_ttl: int = 0

    operation_delay: int = 200

    # 123 旧接口 token 过期很久（约 30 天），提前 10 小时刷新即可
    auth_type: str = "token"
    token_expires_seconds: int = 2592000
    refresh_advance_seconds: int = 36000
    retry_cooldown_seconds: int = 30
    supports_refresh: bool = True

    def __post_init__(self):
        if not self.access_token and not (self.username and self.password):
            raise ValueError("必须提供 username + password 或 access_token")

        if self.cache_ttl is not None and (self.cache_ttl < 0 or self.cache_ttl > 1440):
            raise ValueError("缓存时间必须在0-1440分钟之间，None表示使用全局默认值，0表示禁用缓存")

    def get_auth_method(self) -> str:
        if self.access_token:
            return "access_token"
        elif self.username and self.password:
            return "username_password"
        else:
            return "unknown"

    def is_username_auth(self) -> bool:
        return bool(self.username and self.password)

    def is_token_auth(self) -> bool:
        return bool(self.access_token)

    def get_cache_config(self) -> Dict[str, Any]:
        return {
            "cache_ttl": self.cache_ttl * 60 if self.cache_ttl is not None else None
        }

    @classmethod
    def get_form_schema(cls) -> Dict[str, Any]:
        return {
            "username": {
                "type": "text",
                "required": True,
                "label": "用户名",
                "description": "123云盘用户名",
                "placeholder": "请输入用户名"
            },
            "password": {
                "type": "password",
                "required": True,
                "label": "密码",
                "description": "123云盘密码",
                "placeholder": "请输入密码"
            },
            "root_folder_id": {
                "type": "text",
                "required": False,
                "label": "根目录ID",
                "description": "指定一个文件夹ID作为根目录，默认为0",
                "default": "0",
                "placeholder": "例如: 0"
            },
            "delete_mode": {
                "type": "select",
                "required": False,
                "label": "删除模式",
                "description": "123云盘的删除操作：trash=移到回收站，delete=永久删除回收站中的文件",
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
                "description": "302重定向更快，但部分客户端不兼容",
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
                "description": "仅在下载模式为'代理服务器'时生效",
                "placeholder": "预留接口，暂无用处"
            },
            "cache_ttl": {
                "type": "number",
                "required": False,
                "label": "缓存时间(分钟)",
                "description": "该账号的缓存有效期。空值表示使用全局默认值，0表示禁用缓存，>0表示使用账号设置的时间",
                "default": None,
                "placeholder": "请输入缓存时间，留空采用全局缓存时间",
                "min": 0,
                "max": 1440
            },
        }
    
    @classmethod
    def get_driver_info(cls) -> Dict[str, Any]:
        return {
            "name": "123云盘",
            "display_name": "123云盘",
            "version": "3.1.0",
            "description": "123云盘旧接口，随时可能移除",
            "author": "LitePan",
            "capabilities": ["list", "info", "download", "create_folder", "delete", "batch_delete", "rename", "move", "upload"],
            "auth_type": "token",
            "icon": "123pan",
            "color": "#1890ff"
        } 
