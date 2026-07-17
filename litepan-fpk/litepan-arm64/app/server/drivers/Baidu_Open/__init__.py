from .driver import BaiduOpenDriver
from .config import BaiduOpenConfig


DRIVER_INFO = {
    "name": "baidu_open",
    "display_name": "百度网盘Open",
    "version": "3.1.0",
    "description": "百度网盘官方开放API接入，当前支持OAuth认证与文件浏览",
    "author": "LitePan",
    "capabilities": ["list", "info", "download", "create_folder", "delete", "batch_delete", "rename", "move", "upload"],
    "provide_hashes": ["md5"],
    "driver_class": BaiduOpenDriver,
    "config_class": BaiduOpenConfig,
    "card_color": "#FF4C94",
    "card_name": "百度",
    "card_logo": "/logos/baidu.png",
    "icon": "fa-cloud",
    "sort_order": 4,
    "auto_oauth": 1
}
