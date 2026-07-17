from .config import Pan123OpenConfig
from .driver import Pan123OpenDriver

DRIVER_INFO = {
    "name": "123_open",
    "display_name": "123云盘Open",
    "version": "0.1.0",
    "description": "123云盘官方开放 API 接入",
    "author": "LitePan",
    "config_class": Pan123OpenConfig,
    "driver_class": Pan123OpenDriver,
    "capabilities": ["list", "info", "download", "create_folder", "delete", "batch_delete", "rename", "move", "copy"],
    "provide_hashes": ["md5"],
    "rapid_upload": ["sha1", "md5"],
    "card_color": "#1890ff",
    "card_name": "123 Open",
    "card_logo": "/logos/123.png",
    "icon": "fa-cloud",
    "sort_order": 2,
    "auto_oauth": 1,
}

__all__ = [
    "Pan123OpenConfig",
    "Pan123OpenDriver",
    "DRIVER_INFO",
]
