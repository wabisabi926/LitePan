from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict

from core.base import FileItem


@dataclass
class GuangYaFile:
    file_id: str
    parent_id: str
    file_name: str
    file_size: int = 0
    res_type: int = 1
    ctime: int = 0
    utime: int = 0
    depth: int = 0
    dir_type: int = 0
    audit_status: int = 0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GuangYaFile":
        return cls(
            file_id=str(data.get("fileId") or ""),
            parent_id=str(data.get("parentId") or ""),
            file_name=str(data.get("fileName") or ""),
            file_size=int(data.get("fileSize", 0) or 0),
            res_type=int(data.get("resType", 1) or 1),
            ctime=int(data.get("ctime", 0) or 0),
            utime=int(data.get("utime", 0) or 0),
            depth=int(data.get("depth", 0) or 0),
            dir_type=int(data.get("dirType", 0) or 0),
            audit_status=int(data.get("auditStatus", 0) or 0),
        )

    def to_file_item(self) -> FileItem:
        modified = datetime.fromtimestamp(self.utime, tz=timezone.utc) if self.utime else None
        created = datetime.fromtimestamp(self.ctime, tz=timezone.utc) if self.ctime else None
        return FileItem(
            id=self.file_id,
            name=self.file_name or self.file_id,
            path=self.parent_id,
            size=self.file_size,
            is_dir=self.res_type == 2,
            modified=modified,
            created=created,
            extra={
                "parent_id": self.parent_id,
                "res_type": self.res_type,
                "depth": self.depth,
                "dir_type": self.dir_type,
                "audit_status": self.audit_status,
            },
        )
