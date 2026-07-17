"""本地存储驱动：把容器内的目录当作一个"网盘账号"暴露。
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import shutil
import time
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Awaitable, Callable, Dict, List, Optional
from urllib.parse import quote

import asyncio
from fastapi import UploadFile

from core.base import DriverInfo, FileItem, OperationResult
from core.driver_base import BaseDriver
from core.log_manager import LogModule, get_writer
from core.operation_wrapper import auto_cleanup_cache

from .config import LocalFsConfig


# 进程内单例：内部下载签名密钥（重启后失效，签名 URL 自然过期）
_DOWNLOAD_SIGNING_KEY = secrets.token_bytes(32)


def get_local_fs_signing_key() -> bytes:
    return _DOWNLOAD_SIGNING_KEY


def sign_local_fs_path(account_id: str, rel_path: str, expires_at: int) -> str:
    payload = f"{account_id}|{rel_path}|{expires_at}".encode("utf-8")
    return hmac.new(_DOWNLOAD_SIGNING_KEY, payload, hashlib.sha256).hexdigest()


def verify_local_fs_signature(account_id: str, rel_path: str, expires_at: int, signature: str) -> bool:
    if expires_at < int(time.time()):
        return False
    expected = sign_local_fs_path(account_id, rel_path, expires_at)
    return hmac.compare_digest(expected, signature or "")


class LocalFsDriver(BaseDriver):
    _UPLOAD_CHUNK_SIZE: int = 4 * 1024 * 1024

    def __init__(self, config: LocalFsConfig):
        super().__init__(config)
        self.config: LocalFsConfig = config
        self._log = get_writer(LogModule.DRIVER)
        self._base: Path = config.resolved_base()

    @classmethod
    def get_info(cls) -> DriverInfo:
        return DriverInfo(
            name="local_fs",
            display_name="本地存储",
            version="0.1.0",
            capabilities=[
                "list", "info", "download", "create_folder",
                "delete", "batch_delete", "rename", "move", "copy", "upload",
            ],
            description="把容器内的本地目录作为存储账号挂载（适合暴露 STRM 目录给爆米花等播放器）",
            author="LitePan",
        )

    async def init(self) -> None:
        if not self._base.exists():
            try:
                self._base.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                self._log.warning(
                    f"本地存储 base_path 不存在且无法创建: {self._base} ({e})",
                    driver_name="local_fs",
                )
        self._log.info(f"本地存储初始化: base={self._base}", driver_name="local_fs")

    async def close(self) -> None:
        return

    async def test_connection(self) -> OperationResult:
        try:
            if not self._base.exists():
                return OperationResult(success=False, message=f"路径不存在: {self._base}")
            if not self._base.is_dir():
                return OperationResult(success=False, message=f"路径不是目录: {self._base}")
            if not os.access(self._base, os.R_OK):
                return OperationResult(success=False, message=f"路径不可读: {self._base}")
            return OperationResult(success=True, message=f"本地存储就绪: {self._base}")
        except Exception as e:
            return OperationResult(success=False, message=f"连接测试失败: {e}")

    # ---------------- 路径工具 ----------------

    def _is_root_id(self, file_id: Optional[str]) -> bool:
        p = (file_id or "").strip().strip("/")
        return p in ("", "0")

    def _normalize_rel(self, file_id: str) -> str:
        if self._is_root_id(file_id):
            return ""
        raw = str(file_id or "").strip().strip("/")
        parts = []
        for seg in PurePosixPath(raw).parts:
            if seg in (".", ".."):
                raise ValueError(f"非法路径片段: {seg}")
            parts.append(seg)
        return str(PurePosixPath(*parts)) if parts else ""

    def _safe_resolve(self, rel: str) -> Path:
        rel_clean = self._normalize_rel(rel)
        target = (self._base / rel_clean) if rel_clean else self._base
        try:
            resolved = target.resolve(strict=False)
        except Exception as e:
            raise ValueError(f"解析路径失败: {e}")

        if not self.config.follow_symlinks:
            try:
                if target.is_symlink() or any(p.is_symlink() for p in target.parents if str(p).startswith(str(self._base))):
                    raise ValueError("不允许通过符号链接访问（请在驱动配置开启）")
            except (OSError, PermissionError):
                pass

        base_resolved = self._base.resolve(strict=False)
        try:
            resolved.relative_to(base_resolved)
        except ValueError:
            raise ValueError(f"路径越界（必须位于 {base_resolved} 之内）")
        return resolved

    def _rel_of(self, abs_path: Path) -> str:
        try:
            return str(PurePosixPath(abs_path.resolve().relative_to(self._base.resolve())))
        except Exception:
            return ""

    def _parent_id_of(self, rel: str) -> str:
        if not rel or rel == ".":
            return "0"
        parent = PurePosixPath(rel).parent
        s = str(parent)
        return "0" if s in ("", ".") else s

    def _to_file_item(self, abs_path: Path) -> FileItem:
        try:
            stat = abs_path.stat()
        except FileNotFoundError:
            raise
        is_dir = abs_path.is_dir()
        rel = self._rel_of(abs_path)
        modified = datetime.fromtimestamp(stat.st_mtime) if stat else None
        created = datetime.fromtimestamp(stat.st_ctime) if stat else None
        return FileItem(
            id=rel if rel else "0",
            name=abs_path.name,
            path=rel if rel else "",
            size=0 if is_dir else int(stat.st_size),
            is_dir=is_dir,
            modified=modified,
            created=created,
            extra={"parent_id": self._parent_id_of(rel)} if rel else {},
        )

    # ---------------- 读取接口 ----------------

    async def list_files(self, parent_id: str = "0") -> List[FileItem]:
        target = self._safe_resolve(parent_id)
        if not target.exists():
            return []
        if not target.is_dir():
            return []
        items: List[FileItem] = []

        def _scan():
            results: List[FileItem] = []
            try:
                with os.scandir(target) as it:
                    for entry in it:
                        try:
                            results.append(self._to_file_item(Path(entry.path)))
                        except Exception:
                            continue
            except (PermissionError, OSError) as e:
                self._log.warning(f"读取目录失败 {target}: {e}", driver_name="local_fs")
            results.sort(key=lambda x: (not x.is_dir, x.name.lower()))
            return results

        items = await asyncio.to_thread(_scan)
        return items

    async def file_info(self, file_id: str) -> Optional[FileItem]:
        try:
            target = self._safe_resolve(file_id)
        except ValueError:
            return None
        if not target.exists():
            return None
        return self._to_file_item(target)

    async def get_download_url(self, file_id: str, user_agent: str = "") -> str:
        try:
            target = self._safe_resolve(file_id)
        except ValueError as e:
            raise ValueError(str(e))
        if not target.exists() or not target.is_file():
            raise ValueError(f"文件不存在或不可下载: {file_id}")

        rel = self._normalize_rel(file_id)
        account_id = str(getattr(self, "account_id", ""))
        ttl = int(getattr(self.config, "download_url_ttl", 300) or 300)
        expires_at = int(time.time()) + ttl
        signature = sign_local_fs_path(account_id, rel, expires_at)

        port = int(os.getenv("LITEPAN_PORT", "5211"))
        path_encoded = quote(rel, safe="/")
        return (
            f"http://127.0.0.1:{port}/api/local-fs/raw/{account_id}/{path_encoded}"
            f"?expires={expires_at}&sig={signature}"
        )

    # ---------------- 写入接口 ----------------

    @staticmethod
    def _validate_entry_name(name: str) -> Optional[str]:
        n = (name or "").strip()
        if not n:
            return None
        if "/" in n or "\\" in n or n in (".", ".."):
            return None
        return n

    @auto_cleanup_cache("create_folder")
    async def create_folder(self, parent_id: str, name: str) -> OperationResult:
        try:
            folder_name = self._validate_entry_name(name)
            if not folder_name:
                return OperationResult(success=False, message="文件夹名称无效")
            parent_abs = self._safe_resolve(parent_id)
            target = parent_abs / folder_name
            if target.exists():
                return OperationResult(success=False, message=f"已存在同名条目: {folder_name}")
            await asyncio.to_thread(target.mkdir, parents=False, exist_ok=False)
            rel = self._rel_of(target)
            return OperationResult(
                success=True,
                message=f"文件夹 '{folder_name}' 创建成功",
                data={
                    "folder_id": rel,
                    "parent_path": self._normalize_rel(parent_id),
                    "folder_name": folder_name,
                },
            )
        except Exception as e:
            self._log.error(f"本地存储新建文件夹失败: {e}", driver_name="local_fs")
            return OperationResult(success=False, message=f"新建文件夹失败: {e}")

    @auto_cleanup_cache("delete_file")
    async def delete_file(self, file_id: str) -> OperationResult:
        return await self._delete_files([file_id])

    @auto_cleanup_cache("batch_delete_file")
    async def batch_delete_file(self, file_ids: List[str]) -> OperationResult:
        if not file_ids:
            return OperationResult(success=True, message="没有文件需要删除")
        return await self._delete_files(file_ids)

    async def _delete_files(self, file_ids: List[str]) -> OperationResult:
        parent_ids = set()
        deleted = 0
        try:
            for fid in file_ids:
                if self._is_root_id(fid):
                    return OperationResult(success=False, message="不能删除存储根目录")
                target = self._safe_resolve(fid)
                if not target.exists():
                    continue
                rel = self._rel_of(target)
                parent_ids.add(self._parent_id_of(rel))
                if target.is_dir():
                    await asyncio.to_thread(shutil.rmtree, target)
                else:
                    await asyncio.to_thread(target.unlink)
                deleted += 1
            return OperationResult(
                success=True,
                message=f"已删除 {deleted} 项",
                data={
                    "deleted_count": deleted,
                    "file_ids": file_ids,
                    "parent_ids": list(parent_ids),
                },
            )
        except Exception as e:
            self._log.error(f"本地存储删除失败: {e}", driver_name="local_fs")
            return OperationResult(success=False, message=f"删除失败: {e}")

    @auto_cleanup_cache("rename_file")
    async def rename_file(self, file_id: str, new_name: str) -> OperationResult:
        try:
            if self._is_root_id(file_id):
                return OperationResult(success=False, message="不能重命名根目录")
            new_name = self._validate_entry_name(new_name)
            if not new_name:
                return OperationResult(success=False, message="新名称无效")
            src = self._safe_resolve(file_id)
            if not src.exists():
                return OperationResult(success=False, message="源文件不存在")
            dst = src.parent / new_name
            if dst.exists():
                return OperationResult(success=False, message=f"目标已存在: {new_name}")
            await asyncio.to_thread(os.rename, str(src), str(dst))
            new_rel = self._rel_of(dst)
            return OperationResult(
                success=True,
                message="重命名成功",
                data={
                    "file_id": new_rel,
                    "parent_id": self._parent_id_of(new_rel),
                    "new_name": new_name,
                },
            )
        except Exception as e:
            self._log.error(f"本地存储重命名失败: {e}", driver_name="local_fs")
            return OperationResult(success=False, message=f"重命名失败: {e}")

    @auto_cleanup_cache("move_file")
    async def move_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        return await self._transfer_files(file_ids, target_parent_id, copy_mode=False)

    @auto_cleanup_cache("copy_file")
    async def copy_file(
        self,
        file_ids: List[str],
        target_parent_id: str,
        source_parent_id: str = None,
    ) -> OperationResult:
        return await self._transfer_files(file_ids, target_parent_id, copy_mode=True)

    async def _transfer_files(
        self,
        file_ids: List[str],
        target_parent_id: str,
        copy_mode: bool,
    ) -> OperationResult:
        action_label = "复制" if copy_mode else "移动"
        try:
            target_dir = self._safe_resolve(target_parent_id)
            if not target_dir.exists() or not target_dir.is_dir():
                return OperationResult(success=False, message=f"目标目录不存在: {target_parent_id}")

            moved_parent_ids = set()
            done = 0
            for fid in file_ids:
                if self._is_root_id(fid):
                    return OperationResult(success=False, message=f"不能{action_label}根目录")
                src = self._safe_resolve(fid)
                if not src.exists():
                    return OperationResult(success=False, message=f"源不存在: {fid}")
                dst = target_dir / src.name
                if dst.resolve() == src.resolve():
                    continue
                if dst.exists():
                    return OperationResult(success=False, message=f"目标已存在同名条目: {src.name}")
                moved_parent_ids.add(self._parent_id_of(self._rel_of(src)))
                if copy_mode:
                    if src.is_dir():
                        await asyncio.to_thread(shutil.copytree, str(src), str(dst))
                    else:
                        await asyncio.to_thread(shutil.copy2, str(src), str(dst))
                else:
                    await asyncio.to_thread(shutil.move, str(src), str(dst))
                done += 1

            moved_parent_ids.add(self._normalize_rel(target_parent_id) or "0")
            return OperationResult(
                success=True,
                message=f"已{action_label} {done} 项",
                data={
                    "moved_count": done,
                    "parent_ids": list(moved_parent_ids),
                    "target_parent_id": self._normalize_rel(target_parent_id) or "0",
                },
            )
        except Exception as e:
            self._log.error(f"本地存储{action_label}失败: {e}", driver_name="local_fs")
            return OperationResult(success=False, message=f"{action_label}失败: {e}")

    @auto_cleanup_cache("upload_file")
    async def upload_file(
        self,
        upload_file: UploadFile,
        parent_path: str = "0",
        conflict_policy: str = "overwrite",
    ) -> OperationResult:
        try:
            parent_dir = self._safe_resolve(parent_path)
            if not parent_dir.exists() or not parent_dir.is_dir():
                return OperationResult(success=False, message="目标目录不存在")
            file_name = self._validate_entry_name(upload_file.filename or "")
            if not file_name:
                return OperationResult(success=False, message="上传文件名无效")

            dst = parent_dir / file_name
            if dst.exists():
                policy = (conflict_policy or "overwrite").lower()
                if policy == "skip":
                    return OperationResult(success=True, message=f"已跳过同名文件: {file_name}")
                if policy == "rename":
                    dst = self._suffix_for_unique(dst)

            tmp = dst.with_name(dst.name + ".part")
            try:
                def _open_write(path):
                    return open(path, "wb")
                fout = await asyncio.to_thread(_open_write, str(tmp))
                try:
                    while True:
                        chunk = await upload_file.read(self._UPLOAD_CHUNK_SIZE)
                        if not chunk:
                            break
                        await asyncio.to_thread(fout.write, chunk)
                finally:
                    await asyncio.to_thread(fout.close)
                await asyncio.to_thread(os.replace, str(tmp), str(dst))
            finally:
                if tmp.exists():
                    try:
                        tmp.unlink()
                    except OSError:
                        pass

            rel = self._rel_of(dst)
            return OperationResult(
                success=True,
                message="上传成功",
                data={
                    "file_id": rel,
                    "parent_id": self._normalize_rel(parent_path) or "0",
                    "file_name": dst.name,
                    "size": dst.stat().st_size,
                },
            )
        except Exception as e:
            self._log.error(f"本地存储上传失败: {e}", driver_name="local_fs")
            return OperationResult(success=False, message=f"上传失败: {e}")
        finally:
            try:
                await upload_file.close()
            except Exception:
                pass

    async def upload_local_file(
        self,
        local_path: str,
        file_name: str,
        parent_path: str = "0",
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        conflict_policy: str = "overwrite",
        resume_state: Optional[Dict[str, Any]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    ) -> OperationResult:
        try:
            parent_dir = self._safe_resolve(parent_path)
            if not parent_dir.exists() or not parent_dir.is_dir():
                return OperationResult(success=False, message="目标目录不存在")
            name = self._validate_entry_name(file_name)
            if not name:
                return OperationResult(success=False, message="文件名无效")

            src_path = Path(local_path)
            if not src_path.is_file():
                return OperationResult(success=False, message="本地源文件不存在")

            dst = parent_dir / name
            if dst.exists():
                policy = (conflict_policy or "overwrite").lower()
                if policy == "skip":
                    return OperationResult(success=True, message="已跳过同名文件")
                if policy == "rename":
                    dst = self._suffix_for_unique(dst)

            total = src_path.stat().st_size
            tmp = dst.with_name(dst.name + ".part")
            uploaded = 0
            try:
                fin = await asyncio.to_thread(open, str(src_path), "rb")
                fout = await asyncio.to_thread(open, str(tmp), "wb")
                try:
                    while True:
                        chunk = await asyncio.to_thread(fin.read, self._UPLOAD_CHUNK_SIZE)
                        if not chunk:
                            break
                        await asyncio.to_thread(fout.write, chunk)
                        uploaded += len(chunk)
                        if progress_callback:
                            try:
                                await progress_callback(uploaded, total, "正在写入本地存储")
                            except Exception:
                                pass
                finally:
                    await asyncio.to_thread(fin.close)
                    await asyncio.to_thread(fout.close)
                await asyncio.to_thread(os.replace, str(tmp), str(dst))
            finally:
                if tmp.exists():
                    try:
                        tmp.unlink()
                    except OSError:
                        pass

            rel = self._rel_of(dst)
            return OperationResult(
                success=True,
                message="上传成功",
                data={
                    "file_id": rel,
                    "parent_id": self._normalize_rel(parent_path) or "0",
                    "file_name": dst.name,
                    "size": dst.stat().st_size,
                },
            )
        except Exception as e:
            self._log.error(f"本地存储上传失败: {e}", driver_name="local_fs")
            return OperationResult(success=False, message=f"上传失败: {e}")

    async def upload_local_file_with_resume(
        self,
        local_path: str,
        file_name: str,
        parent_path: str = "0",
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        conflict_policy: str = "overwrite",
        resume_state: Optional[Dict[str, Any]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    ) -> OperationResult:
        return await self.upload_local_file(
            local_path, file_name, parent_path,
            progress_callback=progress_callback,
            conflict_policy=conflict_policy,
            resume_state=resume_state,
            state_callback=state_callback,
        )

    def _suffix_for_unique(self, target: Path) -> Path:
        stem, suffix = target.stem, target.suffix
        counter = 1
        while True:
            candidate = target.with_name(f"{stem} ({counter}){suffix}")
            if not candidate.exists():
                return candidate
            counter += 1
            if counter > 9999:
                raise RuntimeError("无法生成唯一文件名")
