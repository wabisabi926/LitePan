"""天翼云盘响应模型转换。"""

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from core.base import FileItem


def _parse_time(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 1e12:
                ts = ts / 1000
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        text = str(value).strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%b %d, %Y %H:%M:%S %p"):
            try:
                # 天翼返回的时间按北京时间理解，统一转 UTC 存储。
                dt = datetime.strptime(text, fmt)
                return dt.replace(tzinfo=timezone(timedelta(hours=8))).astimezone(timezone.utc)
            except ValueError:
                continue
    except Exception:
        return None
    return None


class Cloud189Item:
    def __init__(self, data: Dict[str, Any], is_dir: bool):
        self.data = data or {}
        self.is_dir = is_dir

    @classmethod
    def from_file(cls, data: Dict[str, Any]) -> "Cloud189Item":
        return cls(data, False)

    @classmethod
    def from_folder(cls, data: Dict[str, Any]) -> "Cloud189Item":
        return cls(data, True)

    def get_id(self) -> str:
        return str(self.data.get("id") or self.data.get("fileId") or self.data.get("folderId") or "")

    def get_name(self) -> str:
        return str(self.data.get("name") or self.data.get("fileName") or self.data.get("folderName") or "")

    def get_size(self) -> int:
        if self.is_dir:
            return 0
        try:
            return int(self.data.get("size") or 0)
        except (TypeError, ValueError):
            return 0

    def get_parent_id(self) -> str:
        return str(self.data.get("parentId") or self.data.get("parentFolderId") or "")

    def get_modified(self) -> Optional[datetime]:
        return _parse_time(self.data.get("lastOpTime") or self.data.get("lastOpDate") or self.data.get("modifyDate"))

    def get_created(self) -> Optional[datetime]:
        return _parse_time(self.data.get("createDate") or self.data.get("createTime"))

    def get_thumbnail(self) -> Optional[str]:
        icon = self.data.get("icon") or {}
        if isinstance(icon, dict):
            return icon.get("smallUrl") or icon.get("mediumUrl") or icon.get("max600")
        return None

    def to_file_item(self) -> FileItem:
        return FileItem(
            id=self.get_id(),
            name=self.get_name(),
            path=f"/{self.get_id()}",
            size=self.get_size(),
            is_dir=self.is_dir,
            modified=self.get_modified(),
            created=self.get_created(),
            thumbnail_url=self.get_thumbnail(),
            extra={
                "parent_id": self.get_parent_id(),
                "md5": self.data.get("md5") or "",
            },
        )
