"""WebDAV 远端存储驱动配置。"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.base import DriverConfig


@dataclass
class WebdavConfig(DriverConfig):
    base_url: str = ""
    username: str = ""
    password: str = ""
    root_path: str = ""
    verify_ssl: bool = True
    timeout_seconds: int = 60
    download_mode: str = "proxy"
    proxy_server: str = ""
    cache_ttl: Optional[int] = None
    operation_delay: int = 0
    download_url_ttl: int = 0


    max_retries: int = 2
    max_concurrency: int = 4

    def __post_init__(self) -> None:
        if isinstance(self.verify_ssl, str):
            self.verify_ssl = self.verify_ssl.strip().lower() in ("true", "1", "yes", "on")
        base = (self.base_url or "").strip()
        if not base:
            raise ValueError("必须填写 WebDAV 根地址 base_url")
        if not (base.startswith("http://") or base.startswith("https://")):
            raise ValueError("base_url 必须以 http:// 或 https:// 开头")
        if self.timeout_seconds < 5 or self.timeout_seconds > 600:
            raise ValueError("timeout_seconds 建议在 5～600 之间")
        if self.download_mode not in ("redirect", "proxy", "proxy_server"):
            raise ValueError("download_mode 只能是 redirect、proxy 或 proxy_server")
        if self.cache_ttl is not None and (self.cache_ttl < 0 or self.cache_ttl > 1440):
            raise ValueError("缓存时间必须在 0～1440 分钟之间，None 表示使用全局默认")
        # 隐藏字段的兜底（老账号配置里可能没有；普通用户也不需要在表单里看到）
        try:
            self.max_retries = int(self.max_retries if self.max_retries is not None else 2)
        except (TypeError, ValueError):
            self.max_retries = 2
        if self.max_retries < 0:
            self.max_retries = 0
        if self.max_retries > 5:
            self.max_retries = 5
        try:
            self.max_concurrency = int(self.max_concurrency or 4)
        except (TypeError, ValueError):
            self.max_concurrency = 4
        if self.max_concurrency < 1:
            self.max_concurrency = 1
        if self.max_concurrency > 32:
            self.max_concurrency = 32

    @classmethod
    def get_form_schema(cls) -> Dict[str, Any]:
        # pairRow 让管理端把同号字段塞进「两列网格」的同一行
        return {
            "base_url": {
                "type": "text",
                "required": True,
                "label": "WebDAV 根 URL",
                "description": "服务端 WebDAV 根地址，如 Nextcloud：https://example.com/remote.php/webdav/",
                "placeholder": "https://",
                "pairRow": 1,
            },
            "root_path": {
                "type": "text",
                "required": False,
                "label": "子目录路径",
                "description": "相对根 URL 的子路径，无首尾斜杠，如 Documents/Work；留空即整库为根",
                "default": "",
                "placeholder": "可选子目录",
                "pairRow": 1,
            },
            "username": {
                "type": "text",
                "required": False,
                "label": "用户名",
                "description": "留空则不发送 Basic 认证（仅匿名可读时）",
                "placeholder": "可选",
            },
            "password": {
                "type": "password",
                "required": False,
                "label": "密码",
                "description": "与用户名一起用于 HTTP Basic 认证",
                "placeholder": "可选",
            },
            "verify_ssl": {
                "type": "select",
                "required": False,
                "label": "TLS 证书校验",
                "description": "自签名证书可选不校验（有中间人风险，仅建议在可信网络）",
                "default": True,
                "pairRow": 2,
                "options": [
                    {"value": True, "label": "校验证书"},
                    {"value": False, "label": "不校验（自签名）"},
                ],
            },
            "timeout_seconds": {
                "type": "number",
                "required": False,
                "label": "请求超时（秒）",
                "description": (
                    "PROPFIND 等元数据请求的超时；PUT 上传会自动放宽，不受此值约束。"
                    "上游响应慢可以调大一点（建议 15～120）。"
                ),
                "default": 60,
                "min": 5,
                "max": 600,
                "pairRow": 3,
            },
            "download_mode": {
                "type": "select",
                "required": False,
                "label": "下载策略",
                "description": (
                    "proxy=本机流式转发（带 Basic 鉴权的 WebDAV 推荐，最稳定）；"
                    "302=仅当服务端会把 GET 转发到可匿名访问的公网直链（如 OpenList 挂载阿里/夸克）才可用；"
                    "若服务端要求 Basic 鉴权才能访问直链，会自动降级回 proxy。"
                ),
                "default": "proxy",
                "pairRow": 3,
                "options": [
                    {"value": "proxy", "label": "本地代理（推荐）"},
                    {"value": "redirect", "label": "302 重定向（直链可匿名访问时）"},
                ],
            },
            "cache_ttl": {
                "type": "number",
                "required": False,
                "label": "缓存时间（分钟）",
                "description": "空=全局默认；0=禁用",
                "default": None,
                "placeholder": "留空",
                "min": 0,
                "max": 1440,
                "pairRow": 2,
            },
            # 高级字段：max_retries / max_concurrency 不暴露在表单里，使用合理默认值。
            # 出问题需要调整时可以直接改数据库里的 config JSON。
        }

    def get_cache_config(self) -> Dict[str, Any]:
        return {
            "cache_ttl": self.cache_ttl * 60 if self.cache_ttl is not None else None,
        }
