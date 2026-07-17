from dataclasses import dataclass
from typing import Dict, Any, Optional
from core.base import DriverConfig

@dataclass
class QuarkReverseConfig(DriverConfig):
    cookie: str = ""
    root_folder_id: str = "0"
    delete_mode: str = "trash"      # trash=回收站，delete=永久删除
    download_mode: str = "proxy"    # 夸克不支持 302，默认走本地代理
    proxy_server: str = ""
    cache_ttl: Optional[int] = None

    operation_delay: int = 500
    # 夸克下载链接带签名短时有效，缓存 120s 用来合并并发取直链
    download_url_ttl: int = 120

    # 本地代理分片：夸克 CDN 单连接限速，每片 10MB 一次 Range GET 拉满；
    # 与 OpenList 的 quark/UC 取值一致。串行（默认）即可吃满 NAS 出口带宽。
    proxy_part_size: int = 10 * 1024 * 1024

    # 夸克走 Cookie，70min 做一次健康检查
    auth_type: str = "cookie"
    health_check_interval: int = 4200
    retry_cooldown_seconds: int = 30
    supports_refresh: bool = True

    def __post_init__(self):
        if not self.cookie:
            raise ValueError("Cookie不能为空")
        if self.delete_mode not in ["trash", "delete"]:
            raise ValueError("删除模式只能是 trash 或 delete")
        if self.download_mode not in ["redirect", "proxy", "proxy_server"]:
            raise ValueError("下载模式只能是 redirect, proxy 或 proxy_server")
        if self.cache_ttl is not None and (self.cache_ttl < 0 or self.cache_ttl > 1440):
            raise ValueError("缓存时间必须在0-1440分钟之间，None表示使用全局默认值，0表示禁用缓存")
    
    @classmethod
    def get_form_schema(cls) -> Dict[str, Any]:
        return {
            "cookie": {
                "type": "textarea", "required": True, "label": "Cookie",
                "description": "从浏览器开发者工具中获取的完整Cookie字符串", 
                "placeholder": "请输入完整的Cookie字符串，包含所有认证信息"
            },
            "root_folder_id": {
                "type": "text", "required": False, "label": "根目录ID",
                "description": "指定一个文件夹ID作为根目录，默认为0", "default": "0", "placeholder": "例如: 0"
            },
            "delete_mode": {
                "type": "select", "required": False, "label": "删除模式",
                "description": "夸克网盘的删除操作：trash=移到回收站，delete=永久删除回收站中的文件",
                "default": "trash",
                "options": [{"value": "trash", "label": "移到回收站"}, {"value": "delete", "label": "永久删除"}]
            },
            "download_mode": {
                "type": "select", "required": False, "label": "下载模式",
                "description": "夸克网盘不支持302重定向",
                "default": "proxy",
                "options": [
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
            # operation_delay 内部调优项，不暴露
        }

    def get_auth_method(self) -> str:
        return "cookie"

    def get_cache_config(self) -> Dict[str, Any]:
        return {
            "ttl": self.cache_ttl * 60 if self.cache_ttl is not None else None,
            "key_prefix": f"quark_reverse_{hash(self.cookie)}"
        }
