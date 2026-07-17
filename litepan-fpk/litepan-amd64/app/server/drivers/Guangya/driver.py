import asyncio
import base64
import hashlib
import hmac
import mimetypes
import os
import secrets
import tempfile
import xml.etree.ElementTree as ET
from email.utils import formatdate
from typing import Any, Awaitable, Callable, Dict, List, Optional
from urllib.parse import quote, urlencode, urlparse, urlunparse

import aiohttp
from fastapi import UploadFile

from core.base import DriverInfo, FileItem, OperationResult
from core.driver_base import BaseDriver
from core.operation_wrapper import auto_cleanup_cache, with_file_info_cache, with_file_list_cache

from .api import GuangYaAPI, GuangYaApiHelper
from .config import GuangYaConfig
from .models import GuangYaFile


class GuangYaDriver(BaseDriver):
    def __init__(self, config: GuangYaConfig):
        super().__init__(config)
        self.access_token = config.access_token
        self.refresh_token = config.refresh_token
        self.client_id = config.client_id
        self.device_id = self._normalize_device_id(config.device_id)
        self._session: Optional[aiohttp.ClientSession] = None
        self._file_index: Dict[str, FileItem] = {}
        self._refresh_lock = asyncio.Lock()

    @classmethod
    def get_info(cls) -> DriverInfo:
        return DriverInfo(
            name="guangya",
            display_name="光鸭云盘",
            version="0.1.0",
            capabilities=["list", "info", "download", "create_folder", "delete", "batch_delete", "rename", "move", "copy", "upload"],
            description="光鸭云盘接入，当前支持认证、文件列表、新建文件夹、重命名、删除、下载、移动与上传",
            author="LitePan",
        )

    async def init(self) -> None:
        await self._ensure_session()
        if self.access_token:
            if not await self._validate_token():
                self.access_token = None
                self.config.access_token = None
        if not self.access_token:
            await self._refresh_access_token_locked(force=False)
        self._log.debug("光鸭云盘驱动初始化完成", driver_name="guangya")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._log.debug("光鸭云盘驱动已关闭", driver_name="guangya")

    async def sync_runtime_auth_state(self, config: Optional[Dict[str, Any]] = None) -> None:
        if isinstance(config, dict):
            if "access_token" in config:
                self.access_token = config.get("access_token") or None
            if "refresh_token" in config:
                self.refresh_token = config.get("refresh_token") or ""
            if "client_id" in config:
                self.client_id = (config.get("client_id") or self.client_id).strip() or self.client_id
            if "device_id" in config:
                self.device_id = self._normalize_device_id(config.get("device_id") or self.device_id)
        if self._session and not self._session.closed:
            self._session.headers.update(GuangYaApiHelper.build_api_headers(self.device_id, self.access_token or ""))

    async def test_connection(self) -> OperationResult:
        try:
            if not self.access_token:
                await self._refresh_access_token_locked(force=False)

            if not await self._validate_token():
                await self._refresh_access_token_locked(force=True)

            files = await self.list_files("0")
            return OperationResult(success=True, message=f"连接成功，根目录当前可见 {len(files)} 个项目")
        except Exception as e:
            return OperationResult(success=False, message=f"连接测试失败: {str(e)}")

    async def refresh_auth(self) -> bool:
        try:
            await self._refresh_access_token_locked(force=True, notify_success=False)
            return True
        except Exception as e:
            self._log.error(f"光鸭云盘认证刷新失败: {e}", driver_name="guangya")
            return False

    async def _ensure_session(self) -> None:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers=GuangYaApiHelper.build_api_headers(self.device_id, self.access_token or ""),
                cookie_jar=aiohttp.DummyCookieJar(),
            )

    async def _apply_operation_delay(self) -> None:
        await self.wait_for_request_interval()

    def _normalize_device_id(self, device_id: str = "") -> str:
        candidate = (device_id or "").strip().lower()
        if len(candidate) == 32 and all(ch in "0123456789abcdef" for ch in candidate):
            return candidate
        return secrets.token_hex(16)

    def _get_root_parent_id(self) -> str:
        return str(self.config.root_folder_id or "").strip()

    def _resolve_parent_id(self, parent_id: str) -> str:
        normalized = str(parent_id or "").strip()
        if normalized in ("", "0", "/"):
            return self._get_root_parent_id()
        return normalized

    async def _validate_token(self) -> bool:
        if not self.access_token:
            return False
        await self._ensure_session()
        headers = GuangYaApiHelper.build_account_headers(self.client_id, self.device_id)
        headers["Authorization"] = f"Bearer {self.access_token}"

        await self._apply_operation_delay()
        async with self._session.get(
            GuangYaAPI.ACCOUNT_BASE_URL + GuangYaAPI.ENDPOINTS["user_me"],
            headers=headers,
        ) as response:
            if response.status != 200:
                return False
            data = await response.json()
            return bool(str(data.get("sub") or "").strip())

    async def _refresh_access_token(self) -> bool:
        if not self.refresh_token:
            raise Exception("缺少刷新令牌，无法获取访问令牌")

        headers = GuangYaApiHelper.build_account_headers(self.client_id, self.device_id)
        async with aiohttp.ClientSession(
            headers=headers,
            cookie_jar=aiohttp.DummyCookieJar(),
        ) as session:
            async with session.post(
                GuangYaAPI.ACCOUNT_BASE_URL + "/v1/auth/token",
                json={
                    "client_id": self.client_id,
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                },
                timeout=aiohttp.ClientTimeout(total=20.0),
            ) as response:
                try:
                    token_data = await response.json()
                except Exception:
                    body_text = await response.text()
                    raise Exception(f"刷新访问令牌失败: {body_text[:300]}")

                if response.status != 200:
                    raise Exception(
                        GuangYaApiHelper.get_error_message(
                            token_data,
                            f"刷新访问令牌失败（HTTP {response.status}）",
                        )
                    )

        access_token = (token_data.get("access_token") or "").strip()
        refresh_token = (token_data.get("refresh_token") or "").strip()
        if not access_token:
            raise Exception(GuangYaApiHelper.get_error_message(token_data, "刷新访问令牌失败：缺少 access_token"))

        self.access_token = access_token
        self.config.access_token = access_token
        if refresh_token:
            self.refresh_token = refresh_token
            self.config.refresh_token = refresh_token

        await self._ensure_session()
        self._session.headers.update(GuangYaApiHelper.build_api_headers(self.device_id, self.access_token))
        await self._persist_tokens()
        self._log.info("✅ 光鸭云盘认证刷新成功", driver_name="guangya")
        return True

    async def _persist_tokens(self) -> None:
        if not getattr(self, "account_id", None) or str(self.account_id) == "temp_test":
            return
        try:
            from database.db import db

            account_id = int(self.account_id)
            account = await db.get_account(account_id)
            if not account:
                return

            current_config = dict(account["config"])
            current_config["access_token"] = self.access_token
            current_config["refresh_token"] = self.refresh_token
            current_config["client_id"] = self.client_id
            current_config["device_id"] = self.device_id
            await db.update_account(account_id, config=current_config)
        except Exception as e:
            self._log.warning(f"光鸭云盘 Token 持久化失败: {e}", driver_name="guangya")

    async def _notify_direct_refresh_success(self) -> None:
        account_id = getattr(self, "_account_id", None) or getattr(self, "account_id", None)
        if not account_id or str(account_id) == "temp_test":
            return
        try:
            from core.auth_manager import sync_driver_refresh_success

            await sync_driver_refresh_success(int(account_id), self)
        except Exception as e:
            self._log.warning(f"光鸭云盘刷新成功后同步认证状态失败: {e}", driver_name="guangya")

    async def _api_request(
        self,
        path: str,
        body: Dict[str, Any],
        *,
        retried: bool = False,
        allowed_codes: Optional[set[int]] = None,
    ) -> Dict[str, Any]:
        await self._ensure_session()
        if not self.access_token:
            await self._refresh_access_token_locked(force=False)

        token_used = self.access_token
        headers = GuangYaApiHelper.build_api_headers(self.device_id, token_used)
        await self._apply_operation_delay()
        async with self._session.post(
            GuangYaAPI.API_BASE_URL + path,
            json=body,
            headers=headers,
        ) as response:
            if response.status in (401, 403) and not retried:
                await self._refresh_access_token_locked(force=False, expected_access_token=token_used)
                return await self._api_request(path, body, retried=True, allowed_codes=allowed_codes)

            text = await response.text()
            if response.status != 200:
                raise Exception(f"光鸭云盘API错误 ({response.status}): {text[:500]}")

            data = await response.json()
            code = int(data.get("code", 0) or 0)
            if code != 0 and code not in (allowed_codes or set()):
                raise Exception(GuangYaApiHelper.get_error_message(data, "光鸭云盘业务请求失败"))
            return data

    async def _refresh_access_token_locked(
        self,
        *,
        force: bool = True,
        expected_access_token: Optional[str] = None,
        notify_success: bool = True,
    ) -> bool:
        async with self._refresh_lock:
            if not force and self.access_token:
                if expected_access_token is None or self.access_token != expected_access_token:
                    return True

            await self._refresh_access_token()
            if notify_success:
                await self._notify_direct_refresh_success()
            return True

    async def _wait_task_done(self, task_id: str) -> None:
        normalized_task_id = str(task_id or "").strip()
        if not normalized_task_id:
            return

        for attempt in range(30):
            data = await self._api_request(
                GuangYaAPI.ENDPOINTS["task_status"],
                {"taskId": normalized_task_id},
            )
            status = int(data.get("data", {}).get("status", 0) or 0)
            if status == 2:
                return
            if status in (-1, 3):
                raise Exception(f"任务执行失败，状态码: {status}")
            if attempt < 29:
                await asyncio.sleep(0.3)

    async def _list_recycle_items(self) -> List[Dict[str, Any]]:
        page = 0
        page_size = 50
        result: List[Dict[str, Any]] = []

        while True:
            data = await self._api_request(
                GuangYaAPI.ENDPOINTS["file_list"],
                {
                    "parentId": "",
                    "pageSize": page_size,
                    "dirType": 4,
                    "orderBy": 10,
                    "sortType": 0,
                    **({"page": page} if page > 0 else {}),
                },
            )
            data_block = data.get("data") or {}
            if not isinstance(data_block, dict):
                break

            items = data_block.get("list") or []
            total = int(data_block.get("total", 0) or 0)
            if not items:
                break

            result.extend(items)
            if total > 0 and len(result) >= total:
                break
            if len(items) < page_size:
                break
            page += 1

        return result

    async def _delete_via_task(self, file_ids: List[str]) -> None:
        data = await self._api_request(
            GuangYaAPI.ENDPOINTS["delete_file"],
            {"fileIds": file_ids},
        )
        task_id = str(data.get("data", {}).get("taskId") or "").strip()
        if task_id:
            await self._wait_task_done(task_id)

    async def _move_via_task(self, file_ids: List[str], target_parent_id: str) -> None:
        data = await self._api_request(
            GuangYaAPI.ENDPOINTS["move_file"],
            {
                "fileIds": file_ids,
                "parentId": target_parent_id,
            },
        )
        task_id = str(data.get("data", {}).get("taskId") or "").strip()
        if task_id:
            await self._wait_task_done(task_id)

    async def _wait_upload_task_info(self, task_id: str) -> str:
        normalized_task_id = str(task_id or "").strip()
        if not normalized_task_id:
            return ""

        for attempt in range(300):
            data = await self._api_request(
                GuangYaAPI.ENDPOINTS["upload_task_info"],
                {"taskId": normalized_task_id},
                allowed_codes={145, 146, 147, 155, 163},
            )
            data_block = data.get("data") or {}
            if isinstance(data_block, dict):
                file_id = str(data_block.get("fileId") or "").strip()
                if file_id:
                    return file_id
            code = int(data.get("code", 0) or 0)
            if code not in (0, 145, 146, 147, 155, 163):
                raise Exception(GuangYaApiHelper.get_error_message(data, f"上传任务失败，状态码: {code}"))
            if attempt < 299:
                await asyncio.sleep(1)
        raise Exception(f"上传任务超时: {normalized_task_id}")

    @with_file_list_cache
    async def list_files(self, parent_id: str = "0") -> List[FileItem]:
        resolved_parent_id = self._resolve_parent_id(parent_id)
        page = 0
        page_size = 50
        result: List[FileItem] = []

        while True:
            data = await self._api_request(
                GuangYaAPI.ENDPOINTS["file_list"],
                {
                    "parentId": resolved_parent_id,
                    "page": page,
                    "pageSize": page_size,
                    "dirType": 0,
                    "orderBy": 3,
                    "sortType": 1,
                    "fileTypes": [],
                },
            )
            items = data.get("data", {}).get("list", []) or []
            total = int(data.get("data", {}).get("total", 0) or 0)

            for item in items:
                file_item = GuangYaFile.from_dict(item).to_file_item()
                result.append(file_item)
                self._file_index[file_item.id] = file_item

            if not items:
                break
            if total > 0 and len(result) >= total:
                break
            if len(items) < page_size:
                break
            page += 1

        return result

    @with_file_info_cache
    async def file_info(self, file_id: str) -> Optional[FileItem]:
        cached = self._file_index.get(str(file_id))
        if cached:
            return cached
        return FileItem(
            id=str(file_id),
            name=f"file_{file_id}",
            path="",
            size=0,
            is_dir=False,
            extra={},
        )

    async def get_download_url(self, file_id: str, user_agent: str = None) -> str:
        normalized_file_id = str(file_id or "").strip()
        if not normalized_file_id:
            raise Exception("文件ID不能为空")

        cached = self._file_index.get(normalized_file_id)
        if cached and cached.is_dir:
            raise Exception("文件夹不支持下载")

        data = await self._api_request(
            GuangYaAPI.ENDPOINTS["download_url"],
            {"fileId": normalized_file_id},
        )
        data_block = data.get("data") or {}
        if not isinstance(data_block, dict):
            raise Exception("下载接口返回格式错误")

        download_url = str(data_block.get("signedURL") or "").strip()
        if not download_url:
            download_url = str(data_block.get("downloadUrl") or "").strip()
        if not download_url:
            raise Exception("下载地址为空")

        return download_url

    async def get_download_info(self, file_id: str, user_agent: str = None) -> Dict[str, Any]:
        """返回 download_url + size + file_name，给 STRM/WebDAV 这类会进入本地代理
        路径（api/strm.py、webdav/server.py）的入口用。

        实现思路：
        光鸭的 /get_res_download_url 只回 OSS 签名直链，不带 size / file_name；
        而 /userres/v1/file/get_file_detail 是按 fileId 直接取单文件元数据的官方
        网页端接口，response.data.fileInfo 里直接有 fileSize、fileName、resType
        等字段。STRM 冷启动场景（飞牛影视 / Emby 直接访问 /play/{account_id}/{file_id}）
        和热路径都走同一条 file_detail 接口，行为稳定，避免分支差异。

        相比直接对 OSS 直链做 HEAD / Range bytes=0-0 GET 探测，这条接口走我们已
        持有的 access_token 鉴权链路，受 OSS 后端是否走 chunked transfer 等不
        确定行为影响小，且能顺手取到 mimeType、parentId、md5 等附加元信息。
        """
        normalized_file_id = str(file_id or "").strip()
        if not normalized_file_id:
            raise Exception("文件ID不能为空")

        file_size = 0
        file_name = ""
        is_dir = False

        try:
            detail = await self._api_request(
                GuangYaAPI.ENDPOINTS["file_detail"],
                {"fileId": normalized_file_id},
            )
            info = (detail.get("data") or {}).get("fileInfo") or {}
            file_size = int(info.get("fileSize") or 0)
            file_name = str(info.get("fileName") or "").strip()
            # res_type=2 代表文件夹，与 list_files 的 GuangYaFile 模型保持一致
            is_dir = int(info.get("resType", 1) or 1) == 2
        except Exception as e:
            self._log.debug(
                f"光鸭单文件详情接口失败，回退到 _file_index 缓存兜底: {e}",
                driver_name="guangya",
            )

        if is_dir:
            raise Exception("文件夹不支持下载")

        # 兜底：详情接口异常（接口暂时抖动 / 服务端临时故障）时复用 list_files
        # 写进 _file_index 的元数据，避免单点故障让播放彻底挂掉
        cached = self._file_index.get(normalized_file_id)
        if cached:
            if cached.is_dir:
                raise Exception("文件夹不支持下载")
            if file_size <= 0:
                file_size = int(cached.size or 0)
            if not file_name:
                file_name = cached.name or ""

        download_url = await self.get_download_url(normalized_file_id, user_agent)

        return {
            "download_url": download_url,
            "file_name": file_name or f"file_{normalized_file_id}",
            "size": int(file_size or 0),
        }

    @auto_cleanup_cache("create_folder")
    async def create_folder(self, parent_id: str, name: str) -> OperationResult:
        folder_name = (name or "").strip()
        if not folder_name:
            return OperationResult(success=False, message="文件夹名称不能为空")

        resolved_parent_id = self._resolve_parent_id(parent_id)
        try:
            data = await self._api_request(
                GuangYaAPI.ENDPOINTS["create_dir"],
                {
                    "parentId": resolved_parent_id,
                    "dirName": folder_name,
                },
            )
            folder_data = data.get("data", {}) or {}
            folder_id = str(folder_data.get("fileId") or "").strip()

            if folder_id:
                folder_item = GuangYaFile.from_dict(
                    {
                        "fileId": folder_id,
                        "parentId": resolved_parent_id,
                        "fileName": folder_data.get("fileName") or folder_name,
                        "resType": folder_data.get("resType", 2),
                        "ctime": folder_data.get("ctime", 0),
                        "utime": folder_data.get("utime", 0),
                    }
                ).to_file_item()
                self._file_index[folder_id] = folder_item

            return OperationResult(
                success=True,
                message=f"文件夹 '{folder_name}' 创建成功",
                data={
                    "folder_id": folder_id,
                    "parent_path": parent_id,
                    "folder_name": folder_name,
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"创建文件夹失败: {str(e)}")

    @auto_cleanup_cache("rename_file")
    async def rename_file(self, file_id: str, new_name: str) -> OperationResult:
        normalized_file_id = str(file_id or "").strip()
        target_name = (new_name or "").strip()

        if not normalized_file_id:
            return OperationResult(success=False, message="文件ID不能为空")
        if not target_name:
            return OperationResult(success=False, message="新名称不能为空")

        cached = self._file_index.get(normalized_file_id)
        parent_id = str((cached.extra or {}).get("parent_id") or "") if cached else ""
        old_name = cached.name if cached else ""

        try:
            await self._api_request(
                GuangYaAPI.ENDPOINTS["rename"],
                {
                    "fileId": normalized_file_id,
                    "newName": target_name,
                },
            )

            if cached:
                cached.name = target_name
                self._file_index[normalized_file_id] = cached

            return OperationResult(
                success=True,
                message=f"文件重命名为 '{target_name}' 成功",
                data={
                    "file_id": normalized_file_id,
                    "parent_id": parent_id,
                    "old_name": old_name,
                    "new_name": target_name,
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"重命名失败: {str(e)}")

    @auto_cleanup_cache("delete_file")
    async def delete_file(self, file_id: str) -> OperationResult:
        return await self._delete_files([file_id])

    @auto_cleanup_cache("batch_delete_file")
    async def batch_delete_file(self, file_ids: List[str]) -> OperationResult:
        if not file_ids:
            return OperationResult(success=True, message="没有文件需要删除")
        return await self._delete_files(file_ids)

    async def purge_file(self, file_id: str) -> OperationResult:
        """彻底删除（强制清空回收站），用于跨盘探测临时目录等内部清理，不受账号 delete_mode 影响。"""
        return await self._delete_files([str(file_id or "").strip()], force_permanent=True)

    async def _delete_files(self, file_ids: List[str], force_permanent: bool = False) -> OperationResult:
        normalized_file_ids = [str(file_id or "").strip() for file_id in file_ids if str(file_id or "").strip()]
        if not normalized_file_ids:
            return OperationResult(success=True, message="没有文件需要删除")

        parent_ids = set()
        for file_id in normalized_file_ids:
            cached = self._file_index.get(file_id)
            if cached and cached.extra:
                parent_id = str(cached.extra.get("parent_id") or "").strip()
                if parent_id:
                    parent_ids.add(parent_id)

        try:
            await self._delete_via_task(normalized_file_ids)

            message = f"已将 {len(normalized_file_ids)} 个文件移到回收站" if len(normalized_file_ids) > 1 else "已将文件移到回收站"

            if self.config.delete_mode == "delete" or force_permanent:
                recycle_map: Dict[str, Dict[str, Any]] = {}
                for _ in range(6):
                    recycle_items = await self._list_recycle_items()
                    recycle_map = {
                        str(item.get("fileId") or "").strip(): item
                        for item in recycle_items
                        if str(item.get("fileId") or "").strip()
                    }
                    if all(file_id in recycle_map for file_id in normalized_file_ids):
                        break
                    await asyncio.sleep(0.4)

                recycle_ids = [file_id for file_id in normalized_file_ids if file_id in recycle_map]
                if len(recycle_ids) != len(normalized_file_ids):
                    missing = [file_id for file_id in normalized_file_ids if file_id not in recycle_map]
                    raise Exception(f"已移入回收站，但未找到回收站记录: {', '.join(missing)}")

                await self._delete_via_task(recycle_ids)
                message = f"已永久删除 {len(normalized_file_ids)} 个文件" if len(normalized_file_ids) > 1 else "已永久删除文件"

            for file_id in normalized_file_ids:
                self._file_index.pop(file_id, None)

            count = len(normalized_file_ids)
            return OperationResult(
                success=True,
                message=message,
                data={
                    "deleted_count": count,
                    "file_ids": normalized_file_ids,
                    "parent_ids": list(parent_ids),
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"删除失败: {str(e)}")

    @auto_cleanup_cache("move_file")
    async def move_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        normalized_file_ids = [str(file_id or "").strip() for file_id in file_ids if str(file_id or "").strip()]
        if not normalized_file_ids:
            return OperationResult(success=True, message="没有文件需要移动")

        resolved_target_parent_id = self._resolve_parent_id(target_parent_id)
        source_parent_ids = set()
        for file_id in normalized_file_ids:
            cached = self._file_index.get(file_id)
            if cached and cached.extra:
                parent_id = str(cached.extra.get("parent_id") or "").strip()
                if parent_id:
                    source_parent_ids.add(parent_id)

        try:
            await self._move_via_task(normalized_file_ids, resolved_target_parent_id)
            for file_id in normalized_file_ids:
                cached = self._file_index.get(file_id)
                if cached:
                    cached.path = resolved_target_parent_id
                    cached.extra["parent_id"] = resolved_target_parent_id
                    self._file_index[file_id] = cached

            return OperationResult(
                success=True,
                message=f"已移动 {len(normalized_file_ids)} 个文件到目标目录",
                data={
                    "moved_count": len(normalized_file_ids),
                    "file_ids": normalized_file_ids,
                    "target_parent_id": target_parent_id,
                    "source_parent_ids": list(source_parent_ids),
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"移动失败: {str(e)}")

    @auto_cleanup_cache("copy_file")
    async def copy_file(self, file_ids: List[str], target_parent_id: str, source_parent_id: str = None) -> OperationResult:
        normalized_file_ids = [str(fid or "").strip() for fid in file_ids if str(fid or "").strip()]
        if not normalized_file_ids:
            return OperationResult(success=True, message="没有文件需要复制")

        resolved_target_parent_id = self._resolve_parent_id(target_parent_id)
        try:
            data = await self._api_request(
                GuangYaAPI.ENDPOINTS["copy_file"],
                {
                    "fileIds": normalized_file_ids,
                    "parentId": resolved_target_parent_id,
                },
            )
            task_id = str(data.get("data", {}).get("taskId") or "").strip()
            if task_id:
                await self._wait_task_done(task_id)

            return OperationResult(
                success=True,
                message=f"已复制 {len(normalized_file_ids)} 个文件到目标目录",
                data={
                    "copied_count": len(normalized_file_ids),
                    "file_ids": normalized_file_ids,
                    "target_parent_id": target_parent_id,
                    "source_parent_ids": [source_parent_id] if source_parent_id else [],
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"复制失败: {str(e)}")

    async def batch_copy_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        return await self.copy_file(file_ids, target_parent_id)

    async def upload_file(
        self,
        upload_file: UploadFile,
        parent_path: str = "0",
        conflict_policy: str = "overwrite",
    ) -> OperationResult:
        temp_path = ""
        try:
            temp_path = await self._save_upload_to_tempfile(upload_file)
            return await self.upload_local_file(
                temp_path,
                upload_file.filename or "",
                parent_path,
                conflict_policy=conflict_policy,
            )
        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            try:
                await upload_file.close()
            except Exception:
                pass

    async def _fetch_file_md5(self, file_id: str) -> str:
        """按 fileId 走详情接口取真实整文件 md5（光鸭列表不带 md5）。"""
        detail = await self._api_request(
            GuangYaAPI.ENDPOINTS["file_detail"],
            {"fileId": str(file_id or "").strip()},
        )
        info = (detail.get("data") or {}).get("fileInfo") or {}
        return str(info.get("md5") or "")

    async def resolve_transfer_hash(self, item: FileItem, method: str, *, allow_stream: bool = False) -> str:
        if str(method or "").lower() != "md5":
            return ""
        extra = item.extra or {}
        value = extra.get("md5")
        if not value:
            # 详情接口轻量，但仍仅在确需指纹时（探测/执行阶段）调用，避免扫描期逐文件请求
            if not allow_stream:
                return ""
            try:
                value = await self._fetch_file_md5(item.id)
            except Exception as exc:
                self._log.warning(f"光鸭获取 md5 失败 {item.name}: {exc}", driver_name="guangya")
                return ""
        return self.normalize_transfer_hash("md5", str(value or ""))

    async def rapid_upload_by_hash(
        self,
        parent_id: str,
        filename: str,
        hash_type: str,
        hash_value: str,
        size: int,
        duplicate: int = 1,
    ) -> OperationResult:
        if str(hash_type or "").lower() != "md5":
            raise NotImplementedError("光鸭云盘仅支持 md5 秒传")
        md5 = self.normalize_transfer_hash("md5", hash_value)
        if not md5:
            return OperationResult(success=False, message="无效的 MD5 指纹")

        resolved_parent_id = self._resolve_parent_id(parent_id)
        token_data, code = await self._get_upload_token(resolved_parent_id, filename, int(size or 0))
        task_id = str(token_data.get("taskId") or "").strip()
        if not task_id:
            return OperationResult(success=False, message="未获取到上传任务ID")

        # 服务端偶发直接判定秒传（无需提交指纹）
        if code == 156:
            file_id = await self._wait_upload_task_info(task_id)
            return OperationResult(success=True, message="秒传命中", data={
                "reuse": True, "file_id": file_id, "parent_id": resolved_parent_id,
            })

        check = await self._api_request(
            GuangYaAPI.ENDPOINTS["check_can_flash_upload"],
            {"taskId": task_id, "md5": md5},
        )
        if not bool((check.get("data") or {}).get("canFlashUpload")):
            return OperationResult(success=True, message="未命中秒传", data={"reuse": False})

        file_id = await self._wait_upload_task_info(task_id)
        return OperationResult(success=True, message="秒传命中", data={
            "reuse": True, "file_id": file_id, "parent_id": resolved_parent_id,
        })

    @auto_cleanup_cache("upload_file")
    async def upload_local_file(
        self,
        local_path: str,
        file_name: str,
        parent_path: str = "0",
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        conflict_policy: str = "overwrite",
    ) -> OperationResult:
        try:
            target_name = os.path.basename((file_name or "").strip())
            if not target_name:
                return OperationResult(success=False, message="上传文件名不能为空")
            if not local_path or not os.path.exists(local_path):
                return OperationResult(success=False, message="待上传文件不存在")

            resolved_parent_id = self._resolve_parent_id(parent_path)
            file_size = os.path.getsize(local_path)
            token_data, code = await self._get_upload_token(resolved_parent_id, target_name, file_size)
            task_id = str(token_data.get("taskId") or "").strip()

            if code == 156:
                file_id = await self._wait_upload_task_info(task_id)
                await self._notify_upload_progress(progress_callback, file_size, file_size, "秒传成功")
                return self._build_upload_success_result(resolved_parent_id, target_name, file_size, file_id, rapid_upload=True)

            await self._upload_to_oss(local_path, file_size, token_data, progress_callback)
            file_id = await self._wait_upload_task_info(task_id)
            await self._notify_upload_progress(progress_callback, file_size, file_size, "上传成功")
            return self._build_upload_success_result(resolved_parent_id, target_name, file_size, file_id)
        except Exception as e:
            return OperationResult(success=False, message=f"上传文件失败: {str(e)}")

    async def _save_upload_to_tempfile(self, upload_file: UploadFile) -> str:
        suffix = os.path.splitext(upload_file.filename or "")[1]
        fd, temp_path = tempfile.mkstemp(prefix="litepan_guangya_", suffix=suffix)
        os.close(fd)
        try:
            with open(temp_path, "wb") as temp_fp:
                while True:
                    chunk = await upload_file.read(1024 * 1024)
                    if not chunk:
                        break
                    temp_fp.write(chunk)
            return temp_path
        except Exception:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            raise

    async def _notify_upload_progress(
        self,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        uploaded_bytes: int = 0,
        total_bytes: int = 0,
        message: str = "",
    ) -> None:
        if progress_callback:
            await progress_callback(uploaded_bytes, total_bytes, message)

    async def _get_upload_token(self, parent_id: str, name: str, size: int) -> tuple[Dict[str, Any], int]:
        data = await self._api_request(
            GuangYaAPI.ENDPOINTS["upload_token"],
            {
                "capacity": 2,
                "name": name,
                "parentId": parent_id,
                "res": {"fileSize": size},
            },
            allowed_codes={156},
        )
        code = int(data.get("code", 0) or 0)
        token_data = data.get("data") or {}
        if not isinstance(token_data, dict):
            raise Exception("上传凭证返回格式错误")

        creds = token_data.get("creds") if isinstance(token_data.get("creds"), dict) else {}
        token_data["accessKeyID"] = token_data.get("accessKeyID") or creds.get("accessKeyID")
        token_data["secretAccessKey"] = token_data.get("secretAccessKey") or creds.get("secretAccessKey")
        token_data["sessionToken"] = token_data.get("sessionToken") or creds.get("sessionToken")
        token_data["endPoint"] = token_data.get("endPoint") or token_data.get("fullEndPoint")

        if not str(token_data.get("taskId") or "").strip():
            raise Exception("上传凭证缺少 taskId")
        return token_data, code

    async def _upload_to_oss(
        self,
        local_path: str,
        file_size: int,
        token_data: Dict[str, Any],
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    ) -> None:
        object_path = str(token_data.get("objectPath") or "").strip()
        bucket = str(token_data.get("bucketName") or "").strip()
        endpoint = self._normalize_oss_endpoint(str(token_data.get("endPoint") or ""), bucket)
        access_key_id = str(token_data.get("accessKeyID") or "").strip()
        access_key_secret = str(token_data.get("secretAccessKey") or "").strip()
        security_token = str(token_data.get("sessionToken") or "").strip()

        if not object_path or not bucket or not endpoint or not access_key_id or not access_key_secret or not security_token:
            raise Exception("上传凭证不完整")

        if file_size <= 0:
            await self._oss_put_object(
                endpoint=endpoint,
                bucket=bucket,
                object_name=object_path,
                token_data=token_data,
                data=b"",
                content_length=0,
                progress_callback=progress_callback,
                total_size=0,
            )
            return

        part_size = self._calc_upload_part_size(file_size)
        if file_size <= part_size:
            async def payload():
                sent = 0
                with open(local_path, "rb") as fp:
                    while True:
                        chunk = fp.read(1024 * 1024)
                        if not chunk:
                            break
                        sent += len(chunk)
                        await self._notify_upload_progress(progress_callback, sent, file_size, "正在上传到光鸭云盘")
                        yield chunk

            await self._oss_put_object(
                endpoint=endpoint,
                bucket=bucket,
                object_name=object_path,
                token_data=token_data,
                data=payload(),
                content_length=file_size,
                progress_callback=progress_callback,
                total_size=file_size,
            )
            return

        upload_id = await self._oss_initiate_multipart_upload(endpoint, bucket, object_path, token_data)
        parts = await self._oss_upload_parts(
            endpoint=endpoint,
            bucket=bucket,
            object_name=object_path,
            upload_id=upload_id,
            token_data=token_data,
            local_path=local_path,
            file_size=file_size,
            part_size=part_size,
            progress_callback=progress_callback,
        )
        await self._oss_complete_multipart_upload(endpoint, bucket, object_path, upload_id, parts, token_data)

    async def _oss_put_object(
        self,
        *,
        endpoint: str,
        bucket: str,
        object_name: str,
        token_data: Dict[str, Any],
        data: Any,
        content_length: int,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        total_size: int = 0,
    ) -> None:
        mime_type = mimetypes.guess_type(object_name)[0] or "application/octet-stream"
        headers = self._build_oss_headers(
            method="PUT",
            bucket=bucket,
            object_name=object_name,
            token_data=token_data,
            content_length=content_length,
            content_type=mime_type,
        )
        url = self._build_oss_url(endpoint, bucket, object_name)
        await self._notify_upload_progress(progress_callback, 0, total_size, "正在上传到光鸭云盘")
        async with self._session.put(url, headers=headers, data=data) as response:
            if response.status != 200:
                raise Exception(f"OSS上传失败: HTTP {response.status}, {await response.text()}")

    async def _oss_initiate_multipart_upload(self, endpoint: str, bucket: str, object_name: str, token_data: Dict[str, Any]) -> str:
        query = {"uploads": None, "sequential": None}
        url = self._build_oss_url(endpoint, bucket, object_name, query)
        headers = self._build_oss_headers(
            method="POST",
            bucket=bucket,
            object_name=object_name,
            token_data=token_data,
            subresources=query,
            content_length=0,
            content_type="application/octet-stream",
        )
        async with self._session.post(url, headers=headers, data=b"") as response:
            if response.status != 200:
                raise Exception(f"初始化OSS分片上传失败: HTTP {response.status}, {await response.text()}")
            xml_text = await response.text()
        root = ET.fromstring(xml_text)
        upload_id = root.findtext(".//UploadId") or ""
        if not upload_id:
            raise Exception("OSS初始化未返回 UploadId")
        return upload_id

    async def _oss_upload_parts(
        self,
        *,
        endpoint: str,
        bucket: str,
        object_name: str,
        upload_id: str,
        token_data: Dict[str, Any],
        local_path: str,
        file_size: int,
        part_size: int,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    ) -> List[Dict[str, Any]]:
        total_parts = max(1, (file_size + part_size - 1) // part_size)
        parts: List[Dict[str, Any]] = []
        with open(local_path, "rb") as fp:
            for part_number in range(1, total_parts + 1):
                offset = (part_number - 1) * part_size
                current_part_size = min(part_size, file_size - offset)
                fp.seek(offset)
                etag = await self._oss_upload_part(
                    endpoint=endpoint,
                    bucket=bucket,
                    object_name=object_name,
                    upload_id=upload_id,
                    part_number=part_number,
                    file_obj=fp,
                    part_size=current_part_size,
                    token_data=token_data,
                    uploaded_offset=offset,
                    total_size=file_size,
                    total_parts=total_parts,
                    progress_callback=progress_callback,
                )
                parts.append({"part_number": part_number, "etag": etag})
        return parts

    async def _oss_upload_part(
        self,
        *,
        endpoint: str,
        bucket: str,
        object_name: str,
        upload_id: str,
        part_number: int,
        file_obj,
        part_size: int,
        token_data: Dict[str, Any],
        uploaded_offset: int,
        total_size: int,
        total_parts: int,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    ) -> str:
        query = {"partNumber": str(part_number), "uploadId": upload_id}
        url = self._build_oss_url(endpoint, bucket, object_name, query)
        headers = self._build_oss_headers(
            method="PUT",
            bucket=bucket,
            object_name=object_name,
            token_data=token_data,
            subresources=query,
            content_length=part_size,
            content_type="application/octet-stream",
        )

        async def payload():
            sent = 0
            remaining = part_size
            while remaining > 0:
                chunk = file_obj.read(min(1024 * 1024, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                sent += len(chunk)
                await self._notify_upload_progress(
                    progress_callback,
                    uploaded_offset + sent,
                    total_size,
                    f"正在上传到光鸭云盘，分片（{part_number}/{total_parts}）",
                )
                yield chunk

        async with self._session.put(url, headers=headers, data=payload()) as response:
            if response.status != 200:
                raise Exception(f"上传OSS分片失败(part {part_number}): HTTP {response.status}, {await response.text()}")
            etag = response.headers.get("ETag", "").strip().strip('"')
        if not etag:
            raise Exception(f"上传OSS分片失败(part {part_number})，未返回ETag")
        return etag

    async def _oss_complete_multipart_upload(
        self,
        endpoint: str,
        bucket: str,
        object_name: str,
        upload_id: str,
        parts: List[Dict[str, Any]],
        token_data: Dict[str, Any],
    ) -> None:
        root = ET.Element("CompleteMultipartUpload")
        for part in parts:
            part_node = ET.SubElement(root, "Part")
            ET.SubElement(part_node, "PartNumber").text = str(part["part_number"])
            ET.SubElement(part_node, "ETag").text = f"\"{part['etag']}\""
        body = ET.tostring(root, encoding="utf-8", xml_declaration=True)

        query = {"uploadId": upload_id}
        url = self._build_oss_url(endpoint, bucket, object_name, query)
        headers = self._build_oss_headers(
            method="POST",
            bucket=bucket,
            object_name=object_name,
            token_data=token_data,
            subresources=query,
            content_length=len(body),
            content_type="application/xml",
        )
        async with self._session.post(url, headers=headers, data=body) as response:
            if response.status != 200:
                raise Exception(f"完成OSS分片上传失败: HTTP {response.status}, {await response.text()}")

    def _build_upload_success_result(
        self,
        parent_id: str,
        target_name: str,
        file_size: int,
        file_id: str,
        rapid_upload: bool = False,
    ) -> OperationResult:
        if file_id:
            self._file_index[file_id] = FileItem(
                id=file_id,
                name=target_name,
                path=parent_id,
                size=file_size,
                is_dir=False,
                extra={"parent_id": parent_id, "rapid_upload": rapid_upload},
            )
        return OperationResult(
            success=True,
            message=f"文件 '{target_name}' 上传成功",
            data={
                "file_id": file_id,
                "file_name": target_name,
                "file_size": file_size,
                "parent_id": parent_id,
                "rapid_upload": rapid_upload,
            },
        )

    def _normalize_oss_endpoint(self, endpoint: str, bucket: str) -> str:
        ep = str(endpoint or "").strip()
        if not ep:
            return ep
        if not ep.startswith(("http://", "https://")):
            ep = "https://" + ep
        parsed = urlparse(ep)
        if not parsed.netloc:
            return ep
        host = parsed.netloc
        prefix = str(bucket or "").strip() + "."
        if prefix != "." and host.startswith(prefix):
            host = host[len(prefix):]
        return urlunparse((parsed.scheme, host, parsed.path.rstrip("/"), "", "", ""))

    def _build_oss_url(
        self,
        endpoint: str,
        bucket: str,
        object_name: str,
        query: Optional[Dict[str, Optional[str]]] = None,
    ) -> str:
        parsed = urlparse(endpoint)
        scheme = parsed.scheme or "https"
        host = parsed.netloc or parsed.path
        object_key = quote(str(object_name or "").lstrip("/"), safe="/")
        url = f"{scheme}://{bucket}.{host}/{object_key}"
        if query:
            query_string = urlencode({key: value for key, value in query.items() if value is not None})
            empty_keys = [key for key, value in query.items() if value is None]
            pieces = empty_keys + ([query_string] if query_string else [])
            url = f"{url}?{'&'.join(pieces)}"
        return url

    def _build_oss_headers(
        self,
        *,
        method: str,
        bucket: str,
        object_name: str,
        token_data: Dict[str, Any],
        subresources: Optional[Dict[str, Optional[str]]] = None,
        content_length: Optional[int] = None,
        content_type: str = "",
    ) -> Dict[str, str]:
        access_key_id = str(token_data.get("accessKeyID") or "").strip()
        access_key_secret = str(token_data.get("secretAccessKey") or "").strip()
        security_token = str(token_data.get("sessionToken") or "").strip()
        if not access_key_id or not access_key_secret or not security_token:
            raise Exception("OSS上传凭证缺少AccessKey或SecurityToken")

        headers: Dict[str, str] = {
            "Date": formatdate(usegmt=True),
            "x-oss-security-token": security_token,
        }
        if content_type:
            headers["Content-Type"] = content_type
        if content_length is not None:
            headers["Content-Length"] = str(content_length)
        headers["Authorization"] = self._build_oss_authorization(
            method=method,
            bucket=bucket,
            object_name=object_name,
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
            headers=headers,
            subresources=subresources,
        )
        return headers

    def _build_oss_authorization(
        self,
        *,
        method: str,
        bucket: str,
        object_name: str,
        access_key_id: str,
        access_key_secret: str,
        headers: Dict[str, str],
        subresources: Optional[Dict[str, Optional[str]]] = None,
    ) -> str:
        canonical_resource = self._build_canonical_oss_resource(bucket, object_name, subresources)
        canonical_headers = self._build_canonical_oss_headers(headers)
        string_to_sign = "\n".join(
            [
                method.upper(),
                headers.get("Content-MD5", ""),
                headers.get("Content-Type", ""),
                headers.get("Date", ""),
                f"{canonical_headers}{canonical_resource}",
            ]
        )
        digest = hmac.new(access_key_secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha1).digest()
        signature = base64.b64encode(digest).decode("utf-8")
        return f"OSS {access_key_id}:{signature}"

    def _build_canonical_oss_headers(self, headers: Dict[str, str]) -> str:
        oss_headers = []
        for key, value in headers.items():
            lowered = key.lower().strip()
            if lowered.startswith("x-oss-"):
                oss_headers.append((lowered, " ".join(str(value).strip().split())))
        oss_headers.sort(key=lambda item: item[0])
        return "".join(f"{key}:{value}\n" for key, value in oss_headers)

    def _build_canonical_oss_resource(
        self,
        bucket: str,
        object_name: str,
        subresources: Optional[Dict[str, Optional[str]]] = None,
    ) -> str:
        resource = f"/{bucket}/{str(object_name or '').lstrip('/')}"
        if not subresources:
            return resource
        allowed = {"uploads", "uploadId", "partNumber", "sequential"}
        items = []
        for key in sorted(subresources.keys()):
            if key not in allowed:
                continue
            value = subresources[key]
            items.append(str(key) if value is None or value == "" else f"{key}={value}")
        if items:
            resource = f"{resource}?{'&'.join(items)}"
        return resource

    def _calc_upload_part_size(self, size: int) -> int:
        # 分片串行上传，片越小请求次数越多、签名/往返开销越大。OSS 限制：单片 100KB~5GB、
        # 最多 10000 片，故在限制内尽量用大分片以减少请求数（128MB 上限可覆盖到 ~1.25TB）。
        mb = 1024 * 1024
        gb = 1024 * 1024 * 1024
        if size <= 16 * mb:
            return 16 * mb
        if size <= 4 * gb:
            return 16 * mb
        if size <= 32 * gb:
            return 32 * mb
        if size <= 128 * gb:
            return 64 * mb
        return 128 * mb
