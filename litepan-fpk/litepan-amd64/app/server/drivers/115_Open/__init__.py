from .driver import OneOneFiveOpenDriver
from .config import OneOneFiveOpenConfig

DRIVER_INFO = {
    "name": "115_open",
    "display_name": "115网盘Open",
    "version": "3.1.0",
    "description": "115网盘官方API接入，支持文件管理、上传下载等功能",
    "author": "LitePan",
    "capabilities": ["list", "info", "download", "create_folder", "delete", "batch_delete", "rename", "move"],
    "provide_hashes": ["sha1"],
    "driver_class": OneOneFiveOpenDriver,
    "config_class": OneOneFiveOpenConfig,
    "card_color": "#22A7F0",
    "card_name": "115",
    "card_logo": "/logos/115.png",
    "icon": "fa-folder",
    "sort_order": 1,
    "auto_oauth": 1
}
