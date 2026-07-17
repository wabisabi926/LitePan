from .driver import Cloud189Driver
from .config import Cloud189Config

DRIVER_INFO = {
    "name": "189_cloud",
    "display_name": "天翼云盘",
    "version": "0.1.0",
    "description": "天翼云盘，支持扫码登录、个人云文件列表和常用文件操作",
    "author": "LitePan",
    "capabilities": [
        "list", "info", "download",
        "create_folder", "delete", "batch_delete", "rename",
        "move", "batch_move", "copy", "batch_copy",
    ],
    "driver_class": Cloud189Driver,
    "config_class": Cloud189Config,
    "card_color": "#FEC52C",
    "card_name": "天翼",
    "card_logo": "/logos/tianyi.png",
    "icon": "fa-cloud",
    "sort_order": 7,
    "supports_qr_login": 1,
}
