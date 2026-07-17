"""驱动层基础数据模型、能力检测与配置基类。"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Protocol, runtime_checkable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


@dataclass
class FileItem:
    id: str
    name: str
    path: str = ""
    size: int = 0
    is_dir: bool = False
    modified: Optional[datetime] = None
    created: Optional[datetime] = None
    download_url: Optional[str] = None
    thumbnail_url: Optional[str] = None
    mime_type: Optional[str] = None
    extra: Dict[str, Any] = None

    def __post_init__(self):
        if self.extra is None:
            self.extra = {}


@dataclass
class OperationResult:
    success: bool
    message: str = ""
    data: Any = None


@dataclass
class DriverInfo:
    name: str
    display_name: str
    version: str
    capabilities: List[str]
    description: str = ""
    author: str = ""


CANONICAL_DRIVER_CAPABILITIES = [
    'list',
    'info',
    'download',
    'create_folder',
    'delete',
    'batch_delete',
    'rename',
    'move',
    'upload',
    'copy',
    'chunk_download',
    'resume_download',
    'share',
    'batch_share',
]

CAPABILITY_ALIAS_MAP = {
    'list_files': 'list',
    'file_info': 'info',
    'get_download_url': 'download',
    'delete_file': 'delete',
    'batch_delete_file': 'batch_delete',
    'rename_file': 'rename',
    'move_file': 'move',
    'upload_file': 'upload',
    'copy_file': 'copy',
    'share_file': 'share',
}


def normalize_driver_capabilities(capabilities: List[str]) -> List[str]:
    """归一化到 canonical 能力名并按固定顺序返回，保证前端 capabilities 顺序一致。"""
    seen = set()

    for capability in capabilities or []:
        canonical = CAPABILITY_ALIAS_MAP.get(capability, capability)
        if canonical in CANONICAL_DRIVER_CAPABILITIES:
            seen.add(canonical)

    return [capability for capability in CANONICAL_DRIVER_CAPABILITIES if capability in seen]

def get_driver_capabilities(driver) -> List[str]:
    """鸭子类型：按方法名探测驱动真正支持的能力集合。"""
    capabilities = []

    if hasattr(driver, 'list_files') and callable(getattr(driver, 'list_files')):
        capabilities.append('list')
    if hasattr(driver, 'file_info') and callable(getattr(driver, 'file_info')):
        capabilities.append('info')
    if hasattr(driver, 'create_folder') and callable(getattr(driver, 'create_folder')):
        capabilities.append('create_folder')
    if hasattr(driver, 'delete_file') and callable(getattr(driver, 'delete_file')):
        capabilities.append('delete')
    if hasattr(driver, 'batch_delete_file') and callable(getattr(driver, 'batch_delete_file')):
        capabilities.append('batch_delete')
    if hasattr(driver, 'rename_file') and callable(getattr(driver, 'rename_file')):
        capabilities.append('rename')
    if hasattr(driver, 'move_file') and callable(getattr(driver, 'move_file')):
        capabilities.append('move')
    if hasattr(driver, 'get_download_url') and callable(getattr(driver, 'get_download_url')):
        capabilities.append('download')

    if hasattr(driver, 'upload_file') and callable(getattr(driver, 'upload_file')):
        capabilities.append('upload')
    if hasattr(driver, 'copy_file') and callable(getattr(driver, 'copy_file')):
        capabilities.append('copy')
    if hasattr(driver, 'chunk_download') and callable(getattr(driver, 'chunk_download')):
        capabilities.append('chunk_download')
    if hasattr(driver, 'resume_download') and callable(getattr(driver, 'resume_download')):
        capabilities.append('resume_download')
    if hasattr(driver, 'share_file') and callable(getattr(driver, 'share_file')):
        capabilities.append('share')
    if hasattr(driver, 'batch_share') and callable(getattr(driver, 'batch_share')):
        capabilities.append('batch_share')

    return normalize_driver_capabilities(capabilities)


def driver_supports(driver, capability: str) -> bool:
    capability = CAPABILITY_ALIAS_MAP.get(capability, capability)
    method_map = {
        'list': 'list_files',
        'info': 'file_info',
        'create_folder': 'create_folder',
        'delete': 'delete_file',
        'batch_delete': 'batch_delete_file',
        'rename': 'rename_file',
        'move': 'move_file',
        'download': 'get_download_url',
        'upload': 'upload_file',
        'copy': 'copy_file',
        'chunk_download': 'chunk_download',
        'resume_download': 'resume_download',
        'share': 'share_file',
        'batch_share': 'batch_share'
    }
    
    method_name = method_map.get(capability)
    if not method_name:
        return False
    
    return hasattr(driver, method_name) and callable(getattr(driver, method_name))


def setup_driver_cache(driver, cache_manager):
    if hasattr(driver, 'set_cache_manager'):
        driver.set_cache_manager(cache_manager)


class DriverConfig:
    @classmethod
    def get_form_schema(cls) -> Dict[str, Any]:
        """子类实现，返回前端动态表单 schema。"""
        raise NotImplementedError("子类必须实现 get_form_schema 方法")

# 异常定义在 core/exceptions.py
