"""123 云盘 Open 驱动：官方开放 API 接入。"""

import asyncio
import hashlib
import os
import tempfile
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aiohttp
from fastapi import UploadFile

from config import get_oauth_server_url
from core.base import DriverInfo, FileItem, OperationResult
from core.driver_base import BaseDriver
from core.operation_wrapper import auto_cleanup_cache, with_file_info_cache, with_file_list_cache

from .api import Pan123OpenAPI, Pan123OpenApiHelper
from .config import Pan123OpenConfig
from .models import Pan123OpenFile


class Pan123OpenDriver(BaseDriver):
    def __init__(self, config: Pan123OpenConfig):
        super().__init__(config)
        self.access_token = config.access_token
        self.refresh_token = config.refresh_token
        self._session: Optional[aiohttp.ClientSession] = None
        self._oauth_server_url = get_oauth_server_url()
        self._refresh_lock = asyncio.Lock()

    @classmethod
    def get_info(cls) -> DriverInfo:
        return DriverInfo(
            name="123_open",
            display_name="123云盘Open",
            version="0.1.0",
            capabilities=[
                "list",
                "info",
                "download",
                "create_folder",
                "delete",
                "batch_delete",
                "rename",
                "move",
                "copy",
                "upload",
            ],
            description="123云盘官方开放 API 接入",
            author="LitePan",
        )

    async def init(self) -> None:
        await self._ensure_session()
        if not self.access_token and self.refresh_token:
            await self._refresh_access_token_locked(force=False)
        self._log.debug("123云盘Open驱动初始化完成", driver_name="123_open")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._log.debug("123云盘Open驱动已关闭", driver_name="123_open")

    async def _ensure_session(self) -> None:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(cookie_jar=aiohttp.DummyCookieJar())

    async def _apply_operation_delay(self) -> None:
        await self.wait_for_request_interval()

    async def test_connection(self) -> OperationResult:
        try:
            response = await self._api_request("user_info", "GET")
            data = Pan123OpenApiHelper.extract_data(response) or {}
            nickname = (
                data.get("nickname")
                or data.get("username")
                or data.get("phone")
                or data.get("uid")
                or "123云盘用户"
            )
            return OperationResult(success=True, message=f"连接成功：{nickname}")
        except Exception as e:
            return OperationResult(success=False, message=f"连接测试失败: {str(e)}")

    async def _api_request(
        self,
        operation: str,
        method: str,
        params: Dict[str, Any] = None,
        json_data: Dict[str, Any] = None,
        _retried: bool = False,
    ) -> Dict[str, Any]:
        await self._ensure_session()
        if not self.access_token:
            await self._refresh_access_token_locked(force=False)

        token_used = self.access_token
        url = Pan123OpenAPI.BASE_URL + Pan123OpenAPI.ENDPOINTS[operation]
        kwargs: Dict[str, Any] = {
            "headers": Pan123OpenApiHelper.build_headers(token_used),
            "params": params or {},
        }
        if json_data is not None:
            kwargs["json"] = json_data

        await self._apply_operation_delay()
        async with self._session.request(method, url, **kwargs) as response:
            if response.status == 401 and not _retried:
                await self._refresh_access_token_locked(force=False, expected_access_token=token_used)
                return await self._api_request(operation, method, params=params, json_data=json_data, _retried=True)

            if response.status != 200:
                error_text = await response.text()
                raise Exception(f"123云盘Open API HTTP错误 ({response.status}): {error_text[:500]}")

            resp_data = await response.json()
            success, error_msg, code = Pan123OpenApiHelper.check_success(resp_data)
            if not success:
                if not _retried and Pan123OpenApiHelper.is_token_expired(code, error_msg):
                    self._log.info("123云盘Open访问令牌失效，尝试自动刷新", driver_name="123_open")
                    await self._refresh_access_token_locked(force=False, expected_access_token=token_used)
                    return await self._api_request(operation, method, params=params, json_data=json_data, _retried=True)
                raise Exception(error_msg)

            return resp_data

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

    async def _refresh_access_token(self) -> bool:
        if not self.refresh_token:
            raise Exception("缺少刷新令牌，无法获取访问令牌")

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self._oauth_server_url}/api/oauth/refresh",
                json={"driver_type": "123云盘Open", "refresh_token": self.refresh_token},
                timeout=aiohttp.ClientTimeout(total=15.0),
            ) as response:
                resp_data = await response.json()
                if response.status != 200 or not resp_data.get("success"):
                    raise Exception(resp_data.get("message") or "刷新访问令牌失败")

        token_data = resp_data.get("data", {})
        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        if not access_token:
            raise Exception("刷新访问令牌失败：缺少 access_token")

        self.access_token = access_token
        self.config.access_token = access_token
        if refresh_token:
            self.refresh_token = refresh_token
            self.config.refresh_token = refresh_token

        await self._persist_tokens()
        self._log.debug("123云盘Open访问令牌刷新成功", driver_name="123_open")
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
            await db.update_account(account_id, config=current_config)
        except Exception as e:
            self._log.warning(f"123云盘Open Token 持久化失败: {e}", driver_name="123_open")

    async def _notify_direct_refresh_success(self) -> None:
        account_id = getattr(self, "_account_id", None) or getattr(self, "account_id", None)
        if not account_id or str(account_id) == "temp_test":
            return
        try:
            from core.auth_manager import sync_driver_refresh_success

            await sync_driver_refresh_success(int(account_id), self)
        except Exception as e:
            self._log.warning(f"123云盘Open刷新成功后同步认证状态失败: {e}", driver_name="123_open")

    async def refresh_auth(self) -> bool:
        try:
            await self._refresh_access_token_locked(force=True, notify_success=False)
            self._log.info("123云盘Open认证刷新成功", driver_name="123_open")
            return True
        except Exception as e:
            self._log.error(f"123云盘Open认证刷新失败: {e}", driver_name="123_open")
            return False

    def _normalize_parent_id(self, parent_id: str) -> str:
        configured_root = str(self.config.root_folder_id or "0").strip() or "0"
        if parent_id in (None, "", "/", "root"):
            parent_id = configured_root
        elif str(parent_id) == "0" and configured_root != "0":
            parent_id = configured_root
        normalized = str(parent_id or "0").strip()
        return normalized or "0"

    @with_file_list_cache
    async def list_files(self, parent_id: str = "0") -> List[FileItem]:
        normalized_parent_id = self._normalize_parent_id(parent_id)
        all_files: List[FileItem] = []
        last_file_id: Optional[str] = None

        while True:
            params: Dict[str, Any] = {
                "parentFileId": normalized_parent_id,
                "limit": 100,
            }
            if last_file_id not in (None, "", "-1"):
                params["lastFileId"] = last_file_id

            response = await self._api_request("file_list", "GET", params=params)
            data = Pan123OpenApiHelper.extract_data(response) or {}
            file_list = data.get("fileList") or data.get("file_list") or data.get("list") or []

            for item in file_list:
                file_model = Pan123OpenFile.from_dict(item)
                if not file_model.is_trashed():
                    all_files.append(file_model.to_file_item())

            next_last_file_id = str(data.get("lastFileId", data.get("last_file_id", "-1")))
            if next_last_file_id in ("-1", "", last_file_id):
                break
            last_file_id = next_last_file_id

        return all_files

    @with_file_info_cache
    async def file_info(self, file_id: str) -> FileItem:
        normalized_file_id = self._normalize_parent_id(file_id)
        if normalized_file_id == "0":
            return FileItem(
                id="0",
                name="根目录",
                path="/0",
                is_dir=True,
                extra={"parent_id": "", "driver": "123_open"},
            )

        response = await self._api_request(
            "file_detail",
            "GET",
            params={"fileID": normalized_file_id},
        )
        data = Pan123OpenApiHelper.extract_data(response) or {}
        file_data = data.get("fileInfo") or data.get("file") or data
        return Pan123OpenFile.from_dict(file_data).to_file_item()

    async def get_download_url(self, file_id: str, user_agent: str = None) -> str:
        response = await self._api_request(
            "download",
            "GET",
            params={"fileId": str(file_id)},
        )
        data = Pan123OpenApiHelper.extract_data(response) or {}
        download_url = data.get("downloadUrl") or data.get("download_url") or data.get("url")
        if not download_url:
            raise Exception("获取下载链接失败：响应中缺少 downloadUrl")
        return download_url

    @auto_cleanup_cache("create_folder")
    async def create_folder(self, parent_id: str, name: str) -> OperationResult:
        folder_name = (name or "").strip()
        if not folder_name:
            return OperationResult(success=False, message="文件夹名称不能为空")

        normalized_parent_id = self._normalize_parent_id(parent_id)

        try:
            response = await self._api_request(
                "create_folder",
                "POST",
                json_data={
                    "name": folder_name,
                    "parentID": normalized_parent_id,
                },
            )
            data = Pan123OpenApiHelper.extract_data(response) or {}
            folder_id = str(data.get("dirID") or data.get("dirId") or data.get("fileId") or "")
            folder_item = FileItem(
                id=folder_id,
                name=folder_name,
                path=f"/{folder_id}" if folder_id else "",
                is_dir=True,
                extra={
                    "parent_id": normalized_parent_id,
                    "driver": "123_open",
                },
            )
            return OperationResult(
                success=True,
                message=f"文件夹 '{folder_name}' 创建成功",
                data={
                    "folder_id": folder_id,
                    "folder_name": folder_name,
                    "parent_id": normalized_parent_id,
                    "file_item": folder_item,
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

        parent_id = ""
        old_name = ""
        try:
            old_info = await self.file_info(normalized_file_id)
            parent_id = old_info.extra.get("parent_id", "") if old_info and old_info.extra else ""
            old_name = old_info.name if old_info else ""
        except Exception as e:
            self._log.warning(f"123云盘Open重命名前获取文件详情失败: {e}", driver_name="123_open")

        try:
            await self._api_request(
                "rename",
                "PUT",
                json_data={
                    "fileId": normalized_file_id,
                    "fileName": target_name,
                },
            )
            return OperationResult(
                success=True,
                message=f"重命名成功: {old_name or normalized_file_id} -> {target_name}",
                data={
                    "file_id": normalized_file_id,
                    "parent_id": parent_id,
                    "old_name": old_name,
                    "new_name": target_name,
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"重命名失败: {str(e)}")

    def _chunk_file_ids(self, file_ids: List[str], chunk_size: int = 100) -> List[List[str]]:
        return [file_ids[index:index + chunk_size] for index in range(0, len(file_ids), chunk_size)]

    async def _collect_parent_ids(self, file_ids: List[str]) -> List[str]:
        parent_ids: List[str] = []
        for file_id in file_ids:
            try:
                info = await self.file_info(file_id)
                parent_id = info.extra.get("parent_id", "") if info and info.extra else ""
                if parent_id not in (None, ""):
                    parent_ids.append(str(parent_id))
            except Exception as e:
                self._log.warning(f"123云盘Open获取文件父目录失败: file_id={file_id}, 错误={e}", driver_name="123_open")
        return sorted(set(parent_ids))

    @auto_cleanup_cache("move_file")
    async def move_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        normalized_ids = [str(file_id).strip() for file_id in file_ids if str(file_id).strip()]
        if not normalized_ids:
            return OperationResult(success=False, message="没有指定要移动的文件")

        normalized_target_parent_id = self._normalize_parent_id(target_parent_id)
        source_parent_ids = await self._collect_parent_ids(normalized_ids)

        try:
            for chunk in self._chunk_file_ids(normalized_ids):
                await self._api_request(
                    "move",
                    "POST",
                    json_data={
                        "fileIDs": chunk,
                        "toParentFileID": normalized_target_parent_id,
                    },
                )

            return OperationResult(
                success=True,
                message=f"已移动 {len(normalized_ids)} 个文件到目标目录",
                data={
                    "moved_count": len(normalized_ids),
                    "file_ids": normalized_ids,
                    "target_parent_id": normalized_target_parent_id,
                    "source_parent_ids": source_parent_ids,
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"移动失败: {str(e)}")

    async def _copy_single_file(self, file_id: str, target_parent_id: str) -> str:
        try:
            response = await self._api_request(
                "copy",
                "POST",
                json_data={
                    "fileId": file_id,
                    "targetDirId": target_parent_id,
                },
            )
        except Exception:
            response = await self._api_request(
                "copy",
                "POST",
                json_data={
                    "fileId": file_id,
                    "targetDirID": target_parent_id,
                },
            )
        data = Pan123OpenApiHelper.extract_data(response) or {}
        return str(data.get("targetFileId") or data.get("targetFileID") or "")

    async def _wait_async_copy_done(self, task_id: str) -> None:
        if not task_id:
            raise Exception("批量复制任务缺少 taskId")

        max_attempts = 30
        for attempt in range(max_attempts):
            response = await self._api_request(
                "async_copy_process",
                "GET",
                params={"taskId": task_id},
            )
            data = Pan123OpenApiHelper.extract_data(response) or {}
            status = str(data.get("status", data.get("taskStatus", data.get("process", ""))))
            if status == "2":
                return
            if status == "3":
                raise Exception(data.get("message") or data.get("failReason") or "批量复制任务失败")

            await asyncio.sleep(0.5 if attempt < 10 else 1.0)

        raise Exception("批量复制任务超时，请稍后刷新目标目录查看结果")

    async def _copy_multiple_files(self, file_ids: List[str], target_parent_id: str) -> str:
        try:
            response = await self._api_request(
                "async_copy",
                "POST",
                json_data={
                    "fileIds": file_ids,
                    "targetDirId": target_parent_id,
                },
            )
        except Exception:
            response = await self._api_request(
                "async_copy",
                "POST",
                json_data={
                    "fileIds": file_ids,
                    "targetDirID": target_parent_id,
                },
            )
        data = Pan123OpenApiHelper.extract_data(response) or {}
        task_id = str(data.get("taskId") or data.get("taskID") or "")
        await self._wait_async_copy_done(task_id)
        return task_id

    @auto_cleanup_cache("copy_file")
    async def copy_file(self, file_ids: List[str], target_parent_id: str, source_parent_id: str = None) -> OperationResult:
        normalized_ids = [str(file_id).strip() for file_id in file_ids if str(file_id).strip()]
        if not normalized_ids:
            return OperationResult(success=False, message="没有指定要复制的文件")

        normalized_target_parent_id = self._normalize_parent_id(target_parent_id)
        source_parent_ids = [str(source_parent_id)] if source_parent_id not in (None, "") else await self._collect_parent_ids(normalized_ids)
        if source_parent_ids and all(str(parent_id) == normalized_target_parent_id for parent_id in source_parent_ids):
            return OperationResult(
                success=False,
                message="123云盘Open不支持复制到同一目录",
                data={"warning": True},
            )

        try:
            copied_file_ids: List[str] = []
            task_ids: List[str] = []

            if len(normalized_ids) == 1:
                copied_file_id = await self._copy_single_file(normalized_ids[0], normalized_target_parent_id)
                if copied_file_id:
                    copied_file_ids.append(copied_file_id)
            else:
                for chunk in self._chunk_file_ids(normalized_ids):
                    task_id = await self._copy_multiple_files(chunk, normalized_target_parent_id)
                    if task_id:
                        task_ids.append(task_id)

            return OperationResult(
                success=True,
                message=f"已复制 {len(normalized_ids)} 个文件到目标目录",
                data={
                    "copied_count": len(normalized_ids),
                    "file_ids": normalized_ids,
                    "copied_file_ids": copied_file_ids,
                    "task_ids": task_ids,
                    "target_parent_id": normalized_target_parent_id,
                    "source_parent_ids": source_parent_ids,
                },
            )
        except Exception as e:
            if "不能复制目录" in str(e):
                return OperationResult(
                    success=False,
                    message="123云盘官方Open接口暂不支持复制文件夹",
                    data={"warning": True},
                )
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

    @auto_cleanup_cache("upload_file")
    async def upload_local_file(
        self,
        local_path: str,
        file_name: str,
        parent_path: str = "0",
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        conflict_policy: str = "overwrite",
    ) -> OperationResult:
        return await self._upload_local_file_impl(
            local_path=local_path,
            file_name=file_name,
            parent_path=parent_path,
            progress_callback=progress_callback,
            conflict_policy=conflict_policy,
        )

    @auto_cleanup_cache("upload_file")
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
        return await self._upload_local_file_impl(
            local_path=local_path,
            file_name=file_name,
            parent_path=parent_path,
            progress_callback=progress_callback,
            conflict_policy=conflict_policy,
        )

    async def _upload_local_file_impl(
        self,
        *,
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

            self._validate_upload_file_name(target_name)
            normalized_parent_id = self._normalize_parent_id(parent_path)
            file_size = os.path.getsize(local_path)
            if file_size <= 0:
                return OperationResult(success=False, message="暂不支持上传空文件")

            if conflict_policy == "skip":
                existing = await self._find_existing_file_in_parent(normalized_parent_id, target_name)
                if existing:
                    return OperationResult(
                        success=True,
                        message=f"文件 '{target_name}' 已存在，已跳过",
                        data={
                            "skipped": True,
                            "file_name": target_name,
                            "parent_id": normalized_parent_id,
                        },
                    )

            await self._notify_upload_progress(progress_callback, 0, file_size, "正在计算文件校验值")
            file_md5 = await asyncio.to_thread(self._calculate_file_md5, local_path)

            await self._notify_upload_progress(progress_callback, 0, file_size, "正在准备上传")
            create_data = await self._create_upload_file(
                parent_id=normalized_parent_id,
                target_name=target_name,
                file_size=file_size,
                file_md5=file_md5,
                conflict_policy=conflict_policy,
            )

            file_id = str(create_data.get("fileID") or create_data.get("fileId") or "")
            if bool(create_data.get("reuse")):
                await self._notify_upload_progress(progress_callback, file_size, file_size, "秒传成功")
                resolved_item = await self._resolve_uploaded_file_item(
                    parent_id=normalized_parent_id,
                    target_name=target_name,
                    file_size=file_size,
                    preferred_file_id=file_id,
                )
                if resolved_item:
                    file_id = str(resolved_item.id)
                    target_name = resolved_item.name
                return self._build_upload_success_result(
                    parent_id=normalized_parent_id,
                    target_name=target_name,
                    file_size=file_size,
                    file_id=file_id,
                    rapid_upload=True,
                )

            preupload_id = str(create_data.get("preuploadID") or create_data.get("preuploadId") or "")
            slice_size = int(create_data.get("sliceSize") or 0)
            servers = self._normalize_upload_servers(create_data.get("servers") or [])
            if not preupload_id:
                raise Exception("上传初始化失败：响应中缺少 preuploadID")
            if slice_size <= 0:
                raise Exception("上传初始化失败：响应中缺少有效 sliceSize")
            if not servers:
                raise Exception("上传初始化失败：响应中缺少上传域名")

            await self._upload_file_slices(
                local_path=local_path,
                file_size=file_size,
                preupload_id=preupload_id,
                slice_size=slice_size,
                servers=servers,
                progress_callback=progress_callback,
            )

            file_id = await self._complete_upload(
                preupload_id,
                progress_callback=progress_callback,
                file_size=file_size,
            )
            resolved_item = await self._resolve_uploaded_file_item(
                parent_id=normalized_parent_id,
                target_name=target_name,
                file_size=file_size,
                preferred_file_id=file_id,
            )
            if resolved_item:
                file_id = str(resolved_item.id)
                target_name = resolved_item.name

            await self._notify_upload_progress(progress_callback, file_size, file_size, "上传成功")
            return self._build_upload_success_result(
                parent_id=normalized_parent_id,
                target_name=target_name,
                file_size=file_size,
                file_id=file_id,
            )
        except Exception as e:
            return OperationResult(success=False, message=f"上传文件失败: {str(e)}")

    async def _save_upload_to_tempfile(self, upload_file: UploadFile) -> str:
        suffix = os.path.splitext(upload_file.filename or "")[1]
        fd, temp_path = tempfile.mkstemp(prefix="litepan_123open_", suffix=suffix)
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

    def _validate_upload_file_name(self, file_name: str) -> None:
        if len(file_name) >= 256:
            raise ValueError("文件名长度不能超过255个字符")
        if not file_name.strip():
            raise ValueError("文件名不能为空")
        invalid_chars = set('"\\/:*?|><')
        if any(char in invalid_chars for char in file_name):
            raise ValueError('文件名不能包含以下字符："\\/:*?|><')

    def _map_conflict_policy_to_duplicate_mode(self, conflict_policy: str) -> int:
        policy = str(conflict_policy or "overwrite").strip().lower()
        if policy == "overwrite":
            return 2
        if policy in {"keep_both", "keep_both_new", "rename"}:
            return 1
        return 1

    def _to_api_file_id(self, file_id: str) -> Any:
        normalized = str(file_id or "0").strip() or "0"
        return int(normalized) if normalized.isdigit() else normalized

    @auto_cleanup_cache("upload_file")
    async def rapid_upload_by_hash(
        self,
        parent_id: str,
        filename: str,
        hash_type: str,
        hash_value: str,
        size: int,
        duplicate: int = 1,
    ) -> OperationResult:
        normalized_parent = self._normalize_parent_id(parent_id)
        hash_kind = str(hash_type or "").lower()

        if hash_kind == "sha1":
            sha1 = self.normalize_transfer_hash("sha1", hash_value)
            if not sha1:
                return OperationResult(success=False, message="无效的 SHA1 指纹")
            response = await self._api_request(
                "sha1_reuse",
                "POST",
                json_data={
                    "parentFileID": self._to_api_file_id(normalized_parent),
                    "filename": filename,
                    "sha1": sha1,
                    "size": int(size or 0),
                    "duplicate": int(duplicate or 1),
                },
            )
            data = Pan123OpenApiHelper.extract_data(response) or {}
            reuse = bool(data.get("reuse"))
            file_id = data.get("fileID") or data.get("fileId")
            return OperationResult(
                success=True,
                message="秒传命中" if reuse else "未命中秒传",
                data={
                    "reuse": reuse,
                    "file_id": str(file_id) if file_id else "",
                    "parent_id": normalized_parent,
                },
            )

        if hash_kind == "md5":
            file_md5 = self.normalize_transfer_hash("md5", hash_value)
            if not file_md5:
                return OperationResult(success=False, message="无效的 MD5 指纹")
            conflict_policy = "overwrite" if int(duplicate or 1) == 2 else "rename"
            create_data = await self._create_upload_file(
                parent_id=normalized_parent,
                target_name=filename,
                file_size=int(size or 0),
                file_md5=file_md5,
                conflict_policy=conflict_policy,
            )
            reuse = bool(create_data.get("reuse"))
            file_id = create_data.get("fileID") or create_data.get("fileId")
            return OperationResult(
                success=True,
                message="秒传命中" if reuse else "未命中秒传",
                data={
                    "reuse": reuse,
                    "file_id": str(file_id) if file_id else "",
                    "parent_id": normalized_parent,
                },
            )

        raise NotImplementedError(f"123云盘Open不支持 {hash_type} 秒传")

    async def _create_upload_file(
        self,
        *,
        parent_id: str,
        target_name: str,
        file_size: int,
        file_md5: str,
        conflict_policy: str,
    ) -> Dict[str, Any]:
        response = await self._api_request(
            "upload_create",
            "POST",
            json_data={
                "parentFileID": self._to_api_file_id(parent_id),
                "filename": target_name,
                "etag": file_md5.lower(),
                "size": file_size,
                "duplicate": self._map_conflict_policy_to_duplicate_mode(conflict_policy),
                "containDir": False,
            },
        )
        data = Pan123OpenApiHelper.extract_data(response) or {}
        if not isinstance(data, dict):
            raise Exception("上传初始化失败：响应 data 格式异常")
        return data

    def _normalize_upload_servers(self, servers: Any) -> List[str]:
        if isinstance(servers, str):
            servers = [servers]
        normalized_servers: List[str] = []
        for server in servers or []:
            value = str(server or "").strip().rstrip("/")
            if not value:
                continue
            if not value.startswith(("http://", "https://")):
                value = f"https://{value}"
            normalized_servers.append(value)
        return normalized_servers

    async def _upload_file_slices(
        self,
        *,
        local_path: str,
        file_size: int,
        preupload_id: str,
        slice_size: int,
        servers: List[str],
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    ) -> None:
        total_slices = max(1, (file_size + slice_size - 1) // slice_size)
        uploaded_bytes = 0
        with open(local_path, "rb") as fp:
            for slice_no in range(1, total_slices + 1):
                chunk = fp.read(slice_size)
                if not chunk:
                    break
                slice_md5 = hashlib.md5(chunk).hexdigest()
                await self._upload_single_slice(
                    preupload_id=preupload_id,
                    slice_no=slice_no,
                    slice_md5=slice_md5,
                    chunk=chunk,
                    servers=servers,
                    uploaded_base=uploaded_bytes,
                    file_size=file_size,
                    total_slices=total_slices,
                    progress_callback=progress_callback,
                )
                uploaded_bytes = min(file_size, uploaded_bytes + len(chunk))
                await self._notify_upload_progress(
                    progress_callback,
                    uploaded_bytes,
                    file_size,
                    f"正在上传到123云盘Open，分片（{slice_no}/{total_slices}）",
                )

    async def _upload_single_slice(
        self,
        *,
        preupload_id: str,
        slice_no: int,
        slice_md5: str,
        chunk: bytes,
        servers: List[str],
        uploaded_base: int,
        file_size: int,
        total_slices: int,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    ) -> None:
        last_error = ""
        for attempt in range(max(3, len(servers))):
            server = servers[(slice_no + attempt - 1) % len(servers)]
            body, content_type, content_length = self._build_slice_multipart_body(
                preupload_id=preupload_id,
                slice_no=slice_no,
                slice_md5=slice_md5,
                chunk=chunk,
                uploaded_base=uploaded_base,
                file_size=file_size,
                progress_callback=progress_callback,
                progress_message=f"正在上传到123云盘Open，分片（{slice_no}/{total_slices}）",
            )
            headers = Pan123OpenApiHelper.build_headers(self.access_token)
            headers["Content-Type"] = content_type
            headers["Content-Length"] = str(content_length)

            await self._ensure_session()
            await self._apply_operation_delay()
            async with self._session.post(
                f"{server}/upload/v2/file/slice",
                headers=headers,
                data=body,
                timeout=aiohttp.ClientTimeout(total=180.0),
            ) as response:
                if response.status == 401:
                    await self._refresh_access_token_locked(force=False, expected_access_token=self.access_token)
                    last_error = "访问令牌已刷新，请重试分片上传"
                    continue
                try:
                    resp_data = await response.json()
                except Exception:
                    body_text = await response.text()
                    resp_data = {"code": response.status, "message": body_text[:500]}

                if response.status == 200:
                    success, error_msg, code = Pan123OpenApiHelper.check_success(resp_data)
                    if success:
                        return
                    if Pan123OpenApiHelper.is_token_expired(code, error_msg):
                        await self._refresh_access_token_locked(force=False, expected_access_token=self.access_token)
                    last_error = error_msg
                else:
                    last_error = f"HTTP {response.status}: {resp_data.get('message') or resp_data}"

            await asyncio.sleep(0.5 * (attempt + 1))

        raise Exception(f"上传分片 {slice_no} 失败: {last_error or '未知错误'}")

    def _build_slice_multipart_body(
        self,
        *,
        preupload_id: str,
        slice_no: int,
        slice_md5: str,
        chunk: bytes,
        uploaded_base: int,
        file_size: int,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        progress_message: str = "",
    ):
        boundary = f"----LitePan123Open{os.urandom(8).hex()}"

        def field_part(name: str, value: str) -> bytes:
            return (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                f"{value}\r\n"
            ).encode("utf-8")

        prefix = b"".join(
            [
                field_part("preuploadID", preupload_id),
                field_part("sliceNo", str(slice_no)),
                field_part("sliceMD5", slice_md5),
                (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="slice"; filename="slice-{slice_no}"\r\n'
                    "Content-Type: application/octet-stream\r\n\r\n"
                ).encode("utf-8"),
            ]
        )
        suffix = f"\r\n--{boundary}--\r\n".encode("utf-8")
        content_length = len(prefix) + len(chunk) + len(suffix)

        async def body_generator():
            yield prefix
            sent = 0
            step = 1024 * 1024
            for offset in range(0, len(chunk), step):
                piece = chunk[offset:offset + step]
                if not piece:
                    continue
                sent += len(piece)
                yield piece
                await self._notify_upload_progress(
                    progress_callback,
                    min(file_size, uploaded_base + sent),
                    file_size,
                    progress_message,
                )
            yield suffix

        return body_generator(), f"multipart/form-data; boundary={boundary}", content_length

    async def _complete_upload(
        self,
        preupload_id: str,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        file_size: int = 0,
    ) -> str:
        last_error = ""
        for attempt in range(90):
            await self._notify_upload_progress(
                progress_callback,
                file_size,
                file_size,
                "正在校验文件",
            )
            try:
                response = await self._api_request(
                    "upload_complete",
                    "POST",
                    json_data={"preuploadID": preupload_id},
                )
            except Exception as e:
                if not self._is_upload_checking_error(e):
                    raise
                last_error = str(e)
                await asyncio.sleep(1.0)
                continue

            data = Pan123OpenApiHelper.extract_data(response) or {}
            completed = bool(data.get("completed"))
            file_id = str(data.get("fileID") or data.get("fileId") or "")
            if completed:
                return file_id
            await asyncio.sleep(1.0 if attempt >= 3 else 0.5)

        if last_error:
            raise Exception(f"上传完成确认超时，请稍后刷新目录查看结果: {last_error}")
        raise Exception("上传完成确认超时，请稍后刷新目录查看结果")

    def _is_upload_checking_error(self, error: Exception) -> bool:
        message = str(error or "")
        return (
            "文件正在校验" in message
            or "正在校验" in message
            or "请间隔1秒后再试" in message
            or "请间隔 1 秒后再试" in message
        )

    def _calculate_file_md5(self, local_path: str) -> str:
        md5 = hashlib.md5()
        with open(local_path, "rb") as fp:
            for chunk in iter(lambda: fp.read(1024 * 1024), b""):
                if not chunk:
                    break
                md5.update(chunk)
        return md5.hexdigest()

    async def _find_existing_file_in_parent(self, parent_id: str, target_name: str) -> Optional[FileItem]:
        files = await self.list_files(parent_id or "0")
        for item in files or []:
            if item.name == target_name:
                return item
        return None

    async def _resolve_uploaded_file_item(
        self,
        *,
        parent_id: str,
        target_name: str,
        file_size: int,
        preferred_file_id: str = "",
    ) -> Optional[FileItem]:
        if preferred_file_id:
            try:
                info = await self.file_info(preferred_file_id)
                if info and not info.is_dir:
                    return info
            except Exception as e:
                self._log.warning(f"123云盘Open上传后按文件ID获取详情失败: {e}", driver_name="123_open")

        try:
            files = await self.list_files(parent_id or "0")
        except Exception as e:
            self._log.warning(f"123云盘Open上传后刷新目录失败: {e}", driver_name="123_open")
            return None

        candidates = [
            item for item in files or []
            if not item.is_dir and int(item.size or 0) == int(file_size)
        ]
        for item in candidates:
            if item.name == target_name:
                return item
        return candidates[0] if len(candidates) == 1 else None

    def _build_upload_success_result(
        self,
        *,
        parent_id: str,
        target_name: str,
        file_size: int,
        file_id: str = "",
        rapid_upload: bool = False,
    ) -> OperationResult:
        data = {
            "file_id": file_id or None,
            "file_name": target_name,
            "parent_id": parent_id,
            "size": file_size,
        }
        if rapid_upload:
            data["rapid_upload"] = True
        message = f"文件 '{target_name}' 秒传成功" if rapid_upload else f"文件 '{target_name}' 上传成功"
        return OperationResult(success=True, message=message, data=data)

    def _build_delete_payload(self, file_ids: List[str]) -> Dict[str, Any]:
        normalized_ids = [str(file_id).strip() for file_id in file_ids if str(file_id).strip()]
        return {"fileIDs": normalized_ids}

    async def _request_delete_chunks(self, operation: str, file_ids: List[str]) -> None:
        for chunk in self._chunk_file_ids(file_ids):
            await self._api_request(
                operation,
                "POST",
                json_data=self._build_delete_payload(chunk),
            )

    async def _delete_files(self, file_ids: List[str]) -> OperationResult:
        normalized_ids = [str(file_id).strip() for file_id in file_ids if str(file_id).strip()]
        if not normalized_ids:
            return OperationResult(success=False, message="文件ID不能为空")

        action_label = "删除到回收站"
        parent_ids = await self._collect_parent_ids(normalized_ids)

        try:
            await self._request_delete_chunks("trash", normalized_ids)

            return OperationResult(
                success=True,
                message=f"已{action_label} {len(normalized_ids)} 个文件",
                data={
                    "file_ids": normalized_ids,
                    "delete_mode": "trash",
                    "parent_ids": parent_ids,
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"{action_label}失败: {str(e)}")

    @auto_cleanup_cache("delete_file")
    async def delete_file(self, file_id: str) -> OperationResult:
        return await self._delete_files([file_id])

    @auto_cleanup_cache("batch_delete_file")
    async def batch_delete_file(self, file_ids: List[str]) -> OperationResult:
        return await self._delete_files(file_ids)
