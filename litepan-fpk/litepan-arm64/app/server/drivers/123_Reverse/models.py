"""123 云盘（逆向）驱动数据模型。"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from core.base import FileItem


@dataclass
class Pan123ReverseFile:
    file_id: int
    file_name: str
    size: int
    update_at: datetime
    type: int  # 1=文件夹，0=文件
    etag: str
    s3_key_flag: str
    download_url: Optional[str] = None

    def to_file_item(self, parent_id: str = "0") -> FileItem:
        return FileItem(
            id=str(self.file_id),
            name=self.file_name,
            path=parent_id,
            size=self.size,
            is_dir=self.type == 1,
            modified=self.update_at,
            created=self.update_at,  # 123 没有独立的 created
            download_url=self.download_url,
            thumbnail_url=None,
            mime_type=None,
            extra={
                "etag": self.etag,
                "s3_key_flag": self.s3_key_flag,
                "type": self.type,
                "parent_id": parent_id
            }
        )

    @classmethod
    def _parse_size(cls, data: Dict[str, Any]) -> int:
        for key in ("Size", "size", "TotalSize", "totalSize"):
            raw = data.get(key)
            if raw is None or raw == "":
                continue
            try:
                return int(raw)
            except (TypeError, ValueError):
                continue
        return 0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Pan123ReverseFile':
        update_at_str = data.get("UpdateAt", "")
        if update_at_str:
            try:
                if "T" in update_at_str:
                    update_at = datetime.fromisoformat(update_at_str.replace("Z", "+00:00"))
                else:
                    update_at = datetime.strptime(update_at_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except:
                update_at = datetime.now(timezone.utc)
        else:
            update_at = datetime.now(timezone.utc)

        return cls(
            file_id=data.get("FileId", 0),
            file_name=data.get("FileName", ""),
            size=cls._parse_size(data),
            update_at=update_at,
            type=data.get("Type", 0),
            etag=data.get("Etag", ""),
            s3_key_flag=data.get("S3KeyFlag", ""),
            download_url=data.get("DownloadUrl")
        )


@dataclass
class Pan123ReverseFileList:
    next: str
    total: int
    info_list: list[Pan123ReverseFile]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Pan123ReverseFileList':
        data_dict = data.get("data", {})

        info_list = []
        for file_data in data_dict.get("InfoList", []):
            info_list.append(Pan123ReverseFile.from_dict(file_data))

        return cls(
            next=data_dict.get("Next", "0"),
            total=data_dict.get("Total", 0),
            info_list=info_list
        )


@dataclass
class Pan123ReverseLoginResponse:
    code: int
    message: str
    token: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Pan123ReverseLoginResponse':
        data_field = data.get("data")
        token = None
        if data_field and isinstance(data_field, dict):
            token = data_field.get("token")

        return cls(
            code=data.get("code", 0),
            message=data.get("message", ""),
            token=token
        )


@dataclass
class Pan123ReverseDownloadInfo:
    download_url: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Pan123ReverseDownloadInfo':
        return cls(
            download_url=data.get("data", {}).get("DownloadUrl", "")
        )
