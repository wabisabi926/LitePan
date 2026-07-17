from .driver import QuarkReverseDriver
from .config import QuarkReverseConfig

DRIVER_INFO = {
    "name": "quark_reverse",
    "display_name": "夸克网盘",
    "version": "3.1.0",
    "description": "夸克网盘接入，支持Cookie认证和文件管理功能",
    "author": "LitePan",
    "capabilities": ["list", "info", "download", "create_folder", "delete", "batch_delete", "rename", "move", "upload"],
    "driver_class": QuarkReverseDriver,
    "config_class": QuarkReverseConfig,
    "card_color": "#7B68EE",
    "card_name": "夸克",
    "card_logo": "/logos/quark.png",
    "icon": "fa-star",
    "sort_order": 5,
    "supports_qr_login": 1
}
