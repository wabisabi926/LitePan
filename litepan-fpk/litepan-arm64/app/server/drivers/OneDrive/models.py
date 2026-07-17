"""OneDrive / Microsoft Graph 响应模型转换。"""

from datetime import datetime
from typing import Any, Dict, Optional

from core.base import FileItem


class OneDriveFile:
    def __init__(self, data: Dict[str, Any]):
        self.data = data or {}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OneDriveFile":
        return cls(data)

    def get_id(self) -> str:
        return str(self.data.get("id") or "")

    def get_name(self) -> str:
        return str(self.data.get("name") or "")

    def is_directory(self) -> bool:
        return isinstance(self.data.get("folder"), dict)

    def get_size(self) -> int:
        if self.is_directory():
            return 0
        try:
            return int(self.data.get("size") or 0)
        except (TypeError, ValueError):
            return 0

    def get_parent_id(self) -> str:
        parent_ref = self.data.get("parentReference") or {}
        return str(parent_ref.get("id") or "")

    def get_drive_id(self) -> str:
        parent_ref = self.data.get("parentReference") or {}
        return str(parent_ref.get("driveId") or "")

    def get_web_url(self) -> str:
        return str(self.data.get("webUrl") or "")

    def get_download_url(self) -> str:
        return str(self.data.get("@microsoft.graph.downloadUrl") or "")

    def get_mime_type(self) -> Optional[str]:
        file_info = self.data.get("file") or {}
        mime_type = file_info.get("mimeType")
        return str(mime_type) if mime_type else None

    def _parse_time(self, value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None

    def get_created_time(self) -> Optional[datetime]:
        return self._parse_time(self.data.get("createdDateTime"))

    def get_modified_time(self) -> Optional[datetime]:
        return self._parse_time(self.data.get("lastModifiedDateTime"))

    def to_file_item(self) -> FileItem:
        item_id = self.get_id()
        return FileItem(
            id=item_id,
            name=self.get_name(),
            path=f"/{item_id}" if item_id else "",
            size=self.get_size(),
            is_dir=self.is_directory(),
            modified=self.get_modified_time(),
            created=self.get_created_time(),
            download_url=None,
            thumbnail_url=None,
            mime_type=self.get_mime_type(),
            extra={
                "parent_id": self.get_parent_id(),
                "drive_id": self.get_drive_id(),
                "web_url": self.get_web_url(),
                "download_url": self.get_download_url(),
                "etag": self.data.get("eTag") or self.data.get("cTag") or "",
                "raw": self.data,
            },
        )
