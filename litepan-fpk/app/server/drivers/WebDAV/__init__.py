from .config import WebdavConfig
from .driver import WebdavDriver

DRIVER_INFO = {
    "name": "webdav",
    "display_name": "WebDAV",
    "version": "0.1.0",
    "description": "挂载标准 WebDAV",
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
        "upload",
    ],
    "driver_class": WebdavDriver,
    "config_class": WebdavConfig,
    "card_color": "#0d9488",
    "card_name": "DAV",
    "card_logo": "/logos/webdav.png",
    "icon": "fa-server",
    "sort_order": 10,
}

__all__ = ["WebdavDriver", "WebdavConfig", "DRIVER_INFO"]
