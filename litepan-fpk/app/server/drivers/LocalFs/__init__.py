from .config import LocalFsConfig
from .driver import LocalFsDriver

DRIVER_INFO = {
    "name": "local_fs",
    "display_name": "本地存储",
    "version": "0.1.0",
    "description": "把容器内的本地目录作为存储账号挂载，适合将 STRM 目录通过 WebDAV 暴露给爆米花等播放器",
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
        "upload",
    ],
    "driver_class": LocalFsDriver,
    "config_class": LocalFsConfig,
    "card_color": "#04C7C9",
    "card_name": "本地",
    "card_logo": "/logos/local.png",
    "icon": "fa-folder-open",
    "sort_order": 100,
}

__all__ = ["LocalFsDriver", "LocalFsConfig", "DRIVER_INFO"]
