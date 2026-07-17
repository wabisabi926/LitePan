"""移动云盘数据模型：将 API 响应转成统一 FileItem。"""

from typing import Dict, Any, Optional
from datetime import datetime, timezone
from core.base import FileItem
from core.log_manager import get_writer, LogModule


class Cloud139File:
    def __init__(self, data: Dict[str, Any]):
        self.data = data
        self._log = get_writer(LogModule.DRIVER)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Cloud139File":
        return cls(data)

    def get_id(self) -> str:
        return str(self.data.get("fileId") or self.data.get("catalogId") or "")

    def get_name(self) -> str:
        return str(self.data.get("name") or self.data.get("catalogName") or "")

    def is_directory(self) -> bool:
        file_type = self.data.get("type", "")
        return file_type == "folder"

    def get_size(self) -> int:
        return int(self.data.get("size", 0) or 0)

    def get_parent_id(self) -> str:
        return str(self.data.get("parentFileId") or self.data.get("parentCatalogId") or "")

    def get_modified_time(self) -> Optional[datetime]:
        time_str = (
            self.data.get("updatedAt")
            or self.data.get("updateDate")
            or self.data.get("lastModified")
        )
        if not time_str:
            return None
        try:
            if isinstance(time_str, (int, float)):
                return datetime.fromtimestamp(time_str / 1000, tz=timezone.utc)
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f%z"]:
                try:
                    return datetime.strptime(str(time_str), fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            return datetime.fromtimestamp(float(str(time_str)) / 1000, tz=timezone.utc)
        except (ValueError, TypeError):
            self._log.warning(f"时间格式解析失败: {time_str}", driver_name="139_cloud")
        return None

    def get_created_time(self) -> Optional[datetime]:
        time_str = self.data.get("createdAt") or self.data.get("createDate") or self.data.get("createTime")
        if not time_str:
            return None
        try:
            if isinstance(time_str, (int, float)):
                return datetime.fromtimestamp(time_str / 1000, tz=timezone.utc)
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f%z"]:
                try:
                    return datetime.strptime(str(time_str), fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        except (ValueError, TypeError):
            pass
        return None

    def get_thumbnail(self) -> str:
        thumbnails = self.data.get("thumbnailUrls")
        if isinstance(thumbnails, list):
            for thumb in thumbnails:
                if isinstance(thumb, dict):
                    url = thumb.get("url", "")
                    if url:
                        return url
        return self.data.get("thumbnailURL") or ""

    def get_content_hash(self) -> str:
        return self.data.get("contentHash") or ""

    def to_file_item(self) -> FileItem:
        return FileItem(
            id=self.get_id(),
            name=self.get_name(),
            path=f"/{self.get_id()}",
            size=self.get_size(),
            is_dir=self.is_directory(),
            modified=self.get_modified_time(),
            created=self.get_created_time(),
            download_url=None,
            thumbnail_url=self.get_thumbnail() or None,
            mime_type=None,
            extra={
                "parent_id": self.get_parent_id(),
                "content_hash": self.get_content_hash(),
                "thumbnail": self.get_thumbnail(),
            },
        )
