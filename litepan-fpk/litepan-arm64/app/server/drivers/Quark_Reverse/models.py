"""夸克网盘数据模型：把夸克响应转换成统一 FileItem。"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any
from core.base import FileItem


class QuarkFile:
    def __init__(self, data: Dict[str, Any]):
        self.fid: str = data.get('fid', '')
        self.file_name: str = data.get('file_name', '')
        self.size: int = data.get('size', 0)
        self.file_type: int = data.get('file_type', 1)  # 0=文件夹，1=文件
        self.created_at: Optional[int] = data.get('created_at')
        self.updated_at: Optional[int] = data.get('updated_at', data.get('l_updated_at'))
        self.pdir_fid: str = data.get('pdir_fid', '0')
        self.is_file: bool = data.get('file', True)

    def is_folder(self) -> bool:
        return self.file_type == 0 or not self.is_file

    def is_trashed(self) -> bool:
        return False

    def to_file_item(self) -> FileItem:
        return FileItem(
            id=self.fid,
            name=self.file_name,
            path=f"/{self.fid}",
            is_dir=self.is_folder(),
            size=self.size,
            modified=self._parse_time(self.updated_at),
            created=self._parse_time(self.created_at),
            extra={
                'parent_id': self.pdir_fid
            }
        )

    def _parse_time(self, timestamp) -> Optional[datetime]:
        if not timestamp:
            return None

        try:
            # 夸克返回的时间一般是毫秒，但历史数据有秒级，按阈值兼容
            if isinstance(timestamp, (int, float)):
                if timestamp > 1e12:
                    return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
                else:
                    return datetime.fromtimestamp(timestamp, tz=timezone.utc)
            return None
        except (ValueError, OSError):
            return None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QuarkFile':
        return cls(data)

    def __str__(self) -> str:
        return f"<QuarkFile {self.file_name} ({'folder' if self.is_folder() else 'file'})>"

    def __repr__(self) -> str:
        return self.__str__()
