"""115 驱动数据模型：把 115 响应 dict 转成统一 FileItem。"""

from typing import Dict, Any, Optional
from datetime import datetime, timezone
from core.base import FileItem
from core.log_manager import get_writer, LogModule


class OneOneFiveFile:
    def __init__(self, data: Dict[str, Any]):
        self.data = data
        self._log = get_writer(LogModule.DRIVER)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OneOneFiveFile':
        return cls(data)

    def get_id(self) -> str:
        return str(self.data.get('fid', self.data.get('file_id', '')))

    def get_name(self) -> str:
        return self.data.get('n', self.data.get('fn', self.data.get('file_name', '')))

    def is_directory(self) -> bool:
        # 列表接口: fc='0' 目录；详情接口: file_category=0 目录
        if 'file_category' in self.data:
            return str(self.data.get('file_category')) == '0'
        return str(self.data.get('fc', '1')) == '0'

    def get_size(self) -> int:
        for key in ('size_byte', 's', 'fs'):
            raw = self.data.get(key)
            if raw is None or raw == '':
                continue
            try:
                return int(raw)
            except (TypeError, ValueError):
                continue
        return 0

    def get_parent_id(self) -> str:
        parent_id = (
            self.data.get('pid')
            or self.data.get('cid')
            or self.data.get('parent_id')
            or ''
        )
        return str(parent_id)

    def _parse_time_value(self, value: Any) -> Optional[datetime]:
        if value is None or value == '' or value == 0:
            return None
        try:
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(value, tz=timezone.utc)
            if isinstance(value, str):
                text = value.strip()
                if not text or text == '0':
                    return None
                if text.isdigit() or text.replace('.', '', 1).isdigit():
                    return datetime.fromtimestamp(float(text), tz=timezone.utc)
                for fmt in ('%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S'):
                    try:
                        return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
                    except ValueError:
                        continue
                if 'T' in text:
                    return datetime.fromisoformat(text.replace('Z', '+00:00'))
        except (ValueError, TypeError, OSError) as e:
            self._log.warning(f"时间格式解析失败: {value}, 错误: {e}", driver_name="115_open")
        return None

    def get_modified_time(self) -> Optional[datetime]:
        # 列表: t/upt；详情 get_info: utime/ptime
        for key in ('t', 'upt', 'utime', 'ptime'):
            parsed = self._parse_time_value(self.data.get(key))
            if parsed:
                return parsed
        return None

    def get_pick_code(self) -> str:
        # 115 提取码字段在不同接口下名字不一致，逐个兜底
        pc_value = self.data.get('pc', '')
        pick_code_value = self.data.get('pick_code', '')
        pickcode_value = self.data.get('pickcode', '')
        code_value = self.data.get('code', '')

        pick_code = pc_value or pick_code_value or pickcode_value or code_value

        if pick_code:
            self._log.debug(f"文件'{self.get_name()}'的提取码: {pick_code}", driver_name="115_open")

        return pick_code

    def get_hash_info(self) -> str:
        return self.data.get('sha1', '')

    def get_thumbnail(self) -> str:
        return self.data.get('thumb', '')

    def is_trashed(self) -> bool:
        return False

    def to_file_item(self) -> FileItem:
        modified_time = self.get_modified_time()

        return FileItem(
            id=self.get_id(),
            name=self.get_name(),
            path=f"/{self.get_id()}",  # 115 没有真正的 path，这里用 id 占位
            size=self.get_size(),
            is_dir=self.is_directory(),
            modified=modified_time,
            created=None,
            download_url=None,
            thumbnail_url=self.get_thumbnail() or None,
            mime_type=None,
            extra={
                'parent_id': self.get_parent_id(),
                'pick_code': self.get_pick_code(),
                'hash_info': self.get_hash_info(),
                'hashes': {'sha1': self.get_hash_info()} if self.get_hash_info() else {},
                'thumbnail': self.get_thumbnail()
            }
        )


class OneOneFiveRecycleFile:
    def __init__(self, data: Dict[str, Any]):
        self.data = data
        self._log = get_writer(LogModule.DRIVER)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OneOneFiveRecycleFile':
        return cls(data)

    def get_recycle_id(self) -> str:
        return str(self.data.get('id', ''))

    def get_file_id(self) -> str:
        return str(self.data.get('file_id', ''))

    def get_name(self) -> str:
        return self.data.get('file_name', '')

    def get_delete_time(self) -> int:
        dtime = self.data.get('dtime', '0')
        try:
            return int(dtime)
        except (ValueError, TypeError):
            return 0

    def is_directory(self) -> bool:
        return self.data.get('file_category', 0) == 0

    def get_size(self) -> int:
        for key in ('size_byte', 's', 'fs', 'size'):
            raw = self.data.get(key)
            if raw is None or raw == '':
                continue
            try:
                return int(raw)
            except (TypeError, ValueError):
                continue
        return 0

    def to_file_item(self) -> FileItem:
        delete_time = self.get_delete_time()
        modified_time = None
        if delete_time > 0:
            try:
                modified_time = datetime.fromtimestamp(delete_time, tz=timezone.utc)
            except (ValueError, TypeError):
                pass
        
        return FileItem(
            id=self.get_recycle_id(),
            name=self.get_name(),
            path=f"/recycle/{self.get_recycle_id()}",
            size=self.get_size(),
            is_dir=self.is_directory(),
            modified=modified_time,
            created=None,
            download_url=None,
            thumbnail_url=None,
            mime_type=None,
            extra={
                'recycle_id': self.get_recycle_id(),
                'original_file_id': self.get_file_id(),
                'delete_time': delete_time,
                'is_recycled': True
            }
        )
