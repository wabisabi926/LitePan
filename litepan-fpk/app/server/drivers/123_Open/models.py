"""123 云盘 Open 响应模型转换。"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from core.base import FileItem


class Pan123OpenFile:
    def __init__(self, data: Dict[str, Any]):
        self.data = data or {}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Pan123OpenFile":
        return cls(data)

    def _first_value(self, *keys: str) -> Any:
        for key in keys:
            value = self.data.get(key)
            if value is not None and value != "":
                return value
        return ""

    def get_id(self) -> str:
        return str(self._first_value("fileId", "fileID", "file_id", "id"))

    def get_name(self) -> str:
        return (
            self.data.get("filename")
            or self.data.get("fileName")
            or self.data.get("file_name")
            or self.data.get("name")
            or ""
        )

    def is_directory(self) -> bool:
        value = self.data.get("type", self.data.get("fileType"))
        return str(value) == "1"

    def get_size(self) -> int:
        raw = self.data.get("size", 0)
        try:
            return int(raw or 0)
        except (TypeError, ValueError):
            return 0

    def get_parent_id(self) -> str:
        return str(self._first_value("parentFileId", "parentFileID", "parent_id"))

    def is_trashed(self) -> bool:
        raw = self.data.get("trashed", self.data.get("isTrashed", self.data.get("is_trashed", 0)))
        return str(raw).lower() in ("1", "true", "yes")

    def get_etag(self) -> str:
        return str(self.data.get("etag") or self.data.get("etagName") or self.data.get("hash") or "")

    def _parse_time(self, value: Any) -> Optional[datetime]:
        if value in (None, "", 0, "0"):
            return None
        try:
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(value, tz=timezone.utc)
            text = str(value).strip()
            if text.isdigit():
                return datetime.fromtimestamp(int(text), tz=timezone.utc)
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError, OSError):
            return None

    def get_created_time(self) -> Optional[datetime]:
        for key in ("createAt", "createdAt", "createTime", "created_at"):
            parsed = self._parse_time(self.data.get(key))
            if parsed:
                return parsed
        return None

    def get_modified_time(self) -> Optional[datetime]:
        for key in ("updateAt", "updatedAt", "updateTime", "updated_at"):
            parsed = self._parse_time(self.data.get(key))
            if parsed:
                return parsed
        return None

    def to_file_item(self) -> FileItem:
        file_id = self.get_id()
        return FileItem(
            id=file_id,
            name=self.get_name(),
            path=f"/{file_id}" if file_id else "",
            size=self.get_size(),
            is_dir=self.is_directory(),
            modified=self.get_modified_time(),
            created=self.get_created_time(),
            download_url=None,
            thumbnail_url=self.data.get("thumbnail") or self.data.get("cover") or None,
            mime_type=None,
            extra={
                "parent_id": self.get_parent_id(),
                "etag": self.get_etag(),
                "status": self.data.get("status"),
                "trashed": self.is_trashed(),
                "raw": self.data,
            },
        )
