from .driver import Pan123ReverseDriver
from .config import Pan123ReverseConfig

DRIVER_INFO = {
    "name": "123_reverse",
    "display_name": "123云盘",
    "version": "3.1.0",
    "description": "123云盘旧接口，随时可能移除",
    "author": "LitePan",
    "config_class": Pan123ReverseConfig,
    "driver_class": Pan123ReverseDriver,
    "capabilities": ["list", "info", "download", "create_folder", "delete", "batch_delete", "rename", "move"],
    "card_color": "#1890ff",
    "card_name": "123",
    "card_logo": "/logos/123.png",
    "icon": "fa-cloud",
    "sort_order": 3
}

__all__ = [
    'Pan123ReverseDriver',
    'Pan123ReverseConfig',
    'DRIVER_INFO'
]
