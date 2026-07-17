"""百度网盘 Open 驱动数据模型。"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from core.base import FileItem


def normalize_content_md5(value: str) -> str:
    text = str(value or "").strip().lower()
    if len(text) != 32:
        return ""
    if any(ch not in "0123456789abcdef" for ch in text):
        return ""
    return text


@dataclass
class BaiduOpenFile:
    fs_id: str
    path: str
    server_filename: str
    isdir: int
    size: int = 0
    server_mtime: int = 0
    server_ctime: int = 0
    md5: str = ""
    category: int = 0
    dlink: str = ""
    thumbs: Dict[str, str] = field(default_factory=dict)
    dir_empty: int = -1       # -1=未知, 0=有子目录, 1=空目录

    @classmethod
    def from_list_api(cls, data: Dict[str, Any], fallback_path: str = "") -> "BaiduOpenFile":
        file_path = str(data.get("path") or fallback_path or "")
        filename = str(
            data.get("server_filename")
            or data.get("filename")
            or file_path.rsplit("/", 1)[-1]
            or "unknown"
        )
        return cls(
            fs_id=str(data.get("fs_id") or file_path or filename),
            path=file_path,
            server_filename=filename,
            isdir=int(data.get("isdir", 0) or 0),
            size=int(data.get("size", 0) or 0),
            server_mtime=int(data.get("server_mtime", 0) or data.get("local_mtime", 0) or 0),
            server_ctime=int(data.get("server_ctime", 0) or data.get("local_ctime", 0) or 0),
            md5=str(data.get("md5", "") or ""),
            category=int(data.get("category", 0) or 0),
            dlink="",
            thumbs=data.get("thumbs") or {},
            dir_empty=int(data.get("dir_empty", -1)),
        )

    @classmethod
    def from_metas_api(cls, data: Dict[str, Any]) -> "BaiduOpenFile":
        file_path = str(data.get("path") or "")
        filename = str(
            data.get("filename")
            or data.get("server_filename")
            or file_path.rsplit("/", 1)[-1]
            or "unknown"
        )
        return cls(
            fs_id=str(data.get("fs_id") or file_path or filename),
            path=file_path,
            server_filename=filename,
            isdir=int(data.get("isdir", 0) or 0),
            size=int(data.get("size", 0) or 0),
            server_mtime=int(data.get("server_mtime", 0) or 0),
            server_ctime=int(data.get("server_ctime", 0) or 0),
            md5=str(data.get("md5", "") or ""),
            category=int(data.get("category", 0) or 0),
            dlink=str(data.get("dlink", "") or ""),
            thumbs=data.get("thumbs") or {},
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any], fallback_path: str = "") -> "BaiduOpenFile":
        return cls.from_list_api(data, fallback_path)

    def to_file_item(self) -> FileItem:
        modified = datetime.fromtimestamp(self.server_mtime, tz=timezone.utc) if self.server_mtime else None
        created = datetime.fromtimestamp(self.server_ctime, tz=timezone.utc) if self.server_ctime else None

        return FileItem(
            id=self.path or self.fs_id,
            name=self.server_filename,
            path=self.path,
            size=self.size,
            is_dir=self.isdir == 1,
            modified=modified,
            created=created,
            extra={
                "fs_id": self.fs_id,
                "md5": self.md5,
                "category": self.category,
                "dlink": self.dlink,
                "thumbs": self.thumbs,
                "dir_empty": self.dir_empty,
            },
        )
