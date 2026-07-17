from .config import OneDriveConfig
from .driver import OneDriveDriver


DRIVER_INFO = {
    "name": "onedrive",
    "display_name": "OneDrive",
    "version": "0.1.0",
    "description": "Microsoft Graph / OneDrive 官方 API 接入，支持 OAuth 认证、文件浏览、下载与基础文件管理",
    "author": "LitePan",
    "capabilities": ["list", "info", "download", "create_folder", "delete", "batch_delete", "rename", "move", "copy", "upload"],
    "driver_class": OneDriveDriver,
    "config_class": OneDriveConfig,
    "card_color": "#0078D4",
    "card_name": "One",
    "card_logo": "/logos/onedrive.png",
    "icon": "fa-cloud",
    "sort_order": 11,
    "auto_oauth": 1,
}

__all__ = [
    "OneDriveConfig",
    "OneDriveDriver",
    "DRIVER_INFO",
]
