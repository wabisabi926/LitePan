from .driver import Cloud139Driver
from .config import Cloud139Config

DRIVER_INFO = {
    "name": "139_cloud",
    "display_name": "移动云盘",
    "version": "0.2.1",
    "description": "移动云盘 (139 Cloud) ",
    "author": "LitePan",
    "capabilities": [
        "list",
        "info",
        "download",
        "create_folder",
        "delete",
        "batch_delete",
        "rename",
        "move",
        "copy",
    ],
    "driver_class": Cloud139Driver,
    "config_class": Cloud139Config,
    "card_color": "#0391FF",
    "card_name": "移动",
    "card_logo": "/logos/yidong.png",
    "icon": "fa-mobile-alt",
    "sort_order": 6,
    "auto_oauth": 0,
}
