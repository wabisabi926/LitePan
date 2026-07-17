"""OneDrive 驱动：Microsoft Graph 官方 API 接入。"""

import asyncio
import os
import tempfile
from typing import Any, Awaitable, Callable, Dict, List, Optional
from urllib.parse import quote

import aiohttp
from fastapi import UploadFile

from config import get_oauth_server_url
from core.base import DriverInfo, FileItem, OperationResult
from core.driver_base import BaseDriver
from core.operation_wrapper import auto_cleanup_cache, with_file_info_cache, with_file_list_cache

from .api import OneDriveAPI, OneDriveApiHelper
from .config import OneDriveConfig
from .models import OneDriveFile


class OneDriveDriver(BaseDriver):
    def __init__(self, config: OneDriveConfig):
        super().__init__(config)
        self.access_token = config.access_token
        self.refresh_token = config.refresh_token
        self._session: Optional[aiohttp.ClientSession] = None
        self._oauth_server_url = get_oauth_server_url()
        self._refresh_lock = asyncio.Lock()

    @classmethod
    def get_info(cls) -> DriverInfo:
        return DriverInfo(
            name="onedrive",
            display_name="OneDrive",
            version="0.1.0",
            capabilities=["list", "info", "download", "create_folder", "delete", "batch_delete", "rename", "move", "copy", "upload"],
            description="Microsoft Graph / OneDrive 官方 API 接入",
            author="LitePan",
        )

    async def init(self) -> None:
        await self._ensure_session()
        if not self.access_token and self.refresh_token:
            await self._refresh_access_token_locked(force=False)
        self._log.debug("OneDrive驱动初始化完成", driver_name="onedrive")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._log.debug("OneDrive驱动已关闭", driver_name="onedrive")

    async def _ensure_session(self) -> None:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(cookie_jar=aiohttp.DummyCookieJar())

    async def _apply_operation_delay(self) -> None:
        await self.wait_for_request_interval()

    def _root_reference(self) -> str:
        root_ref = str(self.config.root_item_id or "/").strip() or "/"
        if root_ref in ("0", "root"):
            return "/"
        if root_ref.startswith("/"):
            return "/" + root_ref.strip("/")
        return root_ref

    def _normalize_item_id(self, item_id: str) -> str:
        normalized = str(item_id or "").strip()
        if normalized in ("", "/", "0", "root"):
            return self._root_reference()
        return normalized

    def _item_path(self, endpoint: str, item_id: str) -> str:
        escaped = quote(str(item_id), safe="")
        return OneDriveAPI.ENDPOINTS[endpoint].format(item_id=escaped)

    def _path_item_path(self, endpoint: str, item_path: str) -> str:
        normalized_path = "/" + str(item_path or "").strip("/")
        if normalized_path == "/":
            return OneDriveAPI.ENDPOINTS["root_children" if endpoint == "path_children" else "root"]
        escaped = quote(normalized_path.strip("/"), safe="/")
        return OneDriveAPI.ENDPOINTS[endpoint].format(item_path=escaped)

    def _upload_item_path(self, parent_id: str, file_name: str, suffix: str = "") -> str:
        normalized_parent_id = self._normalize_item_id(parent_id)
        if normalized_parent_id == "/":
            item_path = quote(file_name, safe="")
            return f"/me/drive/root:/{item_path}:{suffix}"
        if self._is_path_reference(normalized_parent_id):
            full_path = f"{normalized_parent_id.strip('/')}/{file_name}"
            item_path = quote(full_path, safe="/")
            return f"/me/drive/root:/{item_path}:{suffix}"
        parent_ref = quote(str(normalized_parent_id), safe="")
        item_name = quote(file_name, safe="")
        return f"/me/drive/items/{parent_ref}:/{item_name}:{suffix}"

    @staticmethod
    def _is_virtual_root_id(item_id: str) -> bool:
        return str(item_id or "").strip() in ("", "/", "0", "root")

    @staticmethod
    def _is_path_reference(item_id: str) -> bool:
        return str(item_id or "").strip().startswith("/")

    def _drive_item_path(self, drive_id: str, item_id: str, suffix: str = "") -> str:
        escaped_drive_id = quote(str(drive_id), safe="")
        escaped_item_id = quote(str(item_id), safe="")
        return f"/drives/{escaped_drive_id}/items/{escaped_item_id}{suffix}"

    def _parent_reference(self, parent_id: str) -> Dict[str, str]:
        normalized_parent_id = self._normalize_item_id(parent_id)
        if normalized_parent_id == "/":
            return {"path": "/drive/root:"}
        if self._is_path_reference(normalized_parent_id):
            return {"path": f"/drive/root:{normalized_parent_id}"}
        return {"id": normalized_parent_id}

    def _chunk_file_ids(self, file_ids: List[str], chunk_size: int = 20) -> List[List[str]]:
        return [file_ids[index:index + chunk_size] for index in range(0, len(file_ids), chunk_size)]

    @staticmethod
    def _upload_conflict_behavior(conflict_policy: str) -> str:
        if conflict_policy == "skip":
            return "fail"
        if conflict_policy == "rename":
            return "rename"
        return "replace"

    @staticmethod
    def _split_copy_name(name: str) -> tuple[str, str]:
        if not name:
            return "未命名文件", ""
        if name.startswith(".") and name.count(".") == 1:
            return name, ""
        dot_index = name.rfind(".")
        if dot_index <= 0:
            return name, ""
        return name[:dot_index], name[dot_index:]

    @classmethod
    def _next_available_copy_name(cls, original_name: str, occupied_names: set[str]) -> str:
        if original_name not in occupied_names:
            return original_name

        stem, suffix = cls._split_copy_name(original_name)
        index = 1
        while True:
            candidate = f"{stem} {index}{suffix}"
            if candidate not in occupied_names:
                return candidate
            index += 1

    async def _collect_directory_names(self, parent_id: str) -> set[str]:
        try:
            return {item.name for item in await self.list_files(parent_id) if item and item.name}
        except Exception as e:
            self._log.warning(f"OneDrive获取目标目录文件名失败: parent_id={parent_id}, 错误={e}", driver_name="onedrive")
            return set()

    async def _notify_upload_progress(
        self,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]],
        uploaded_bytes: int,
        total_bytes: int,
        message: str,
    ) -> None:
        if progress_callback:
            try:
                await progress_callback(uploaded_bytes, total_bytes, message)
            except Exception as e:
                self._log.debug(f"OneDrive上传进度回调异常（忽略）: {e}", driver_name="onedrive")

    async def _save_upload_to_tempfile(self, upload_file: UploadFile) -> str:
        suffix = os.path.splitext(upload_file.filename or "")[1]
        fd, temp_path = tempfile.mkstemp(prefix="litepan_onedrive_upload_", suffix=suffix)
        os.close(fd)
        with open(temp_path, "wb") as temp_file:
            while True:
                chunk = await upload_file.read(1024 * 1024)
                if not chunk:
                    break
                temp_file.write(chunk)
        return temp_path

    async def _iter_upload_chunk(
        self,
        chunk: bytes,
        *,
        uploaded_base: int,
        file_size: int,
        progress_message: str,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    ):
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

    async def test_connection(self) -> OperationResult:
        try:
            drive = await self._api_request("drive", "GET")
            owner = drive.get("owner") or {}
            user = owner.get("user") or {}
            name = user.get("displayName") or drive.get("name") or "OneDrive"
            return OperationResult(success=True, message=f"连接成功：{name}")
        except Exception as e:
            return OperationResult(success=False, message=f"连接测试失败: {str(e)}")

    async def _api_request(
        self,
        operation: str,
        method: str,
        *,
        path: Optional[str] = None,
        url: Optional[str] = None,
        params: Dict[str, Any] = None,
        json_data: Dict[str, Any] = None,
        _retried: bool = False,
    ) -> Dict[str, Any]:
        await self._ensure_session()
        if not self.access_token:
            await self._refresh_access_token_locked(force=False)

        token_used = self.access_token
        request_url = url or f"{OneDriveAPI.BASE_URL}{path or OneDriveAPI.ENDPOINTS[operation]}"

        await self._apply_operation_delay()
        async with self._session.request(
            method,
            request_url,
            headers=OneDriveApiHelper.build_headers(token_used),
            params=params or {},
            json=json_data,
        ) as response:
            if response.status == 204:
                return {}
            try:
                resp_data = await response.json()
            except Exception:
                resp_text = await response.text()
                if response.status < 200 or response.status >= 300 or resp_text.strip():
                    resp_data = {"error": {"message": resp_text[:500]}}
                else:
                    resp_data = {}

            if response.status in (401, 403) and not _retried:
                await self._refresh_access_token_locked(force=False, expected_access_token=token_used)
                return await self._api_request(
                    operation,
                    method,
                    path=path,
                    url=url,
                    params=params,
                    json_data=json_data,
                    _retried=True,
                )

            if response.status < 200 or response.status >= 300:
                message = OneDriveApiHelper.extract_error_message(resp_data)
                raise Exception(f"OneDrive API HTTP错误 ({response.status}): {message}")

            if isinstance(resp_data, dict) and resp_data.get("error"):
                if not _retried and OneDriveApiHelper.is_token_expired(response.status, resp_data):
                    await self._refresh_access_token_locked(force=False, expected_access_token=token_used)
                    return await self._api_request(
                        operation,
                        method,
                        path=path,
                        url=url,
                        params=params,
                        json_data=json_data,
                        _retried=True,
                    )
                raise Exception(OneDriveApiHelper.extract_error_message(resp_data))

            return resp_data

    async def _copy_request(
        self,
        file_id: str,
        parent_reference: Dict[str, str],
        target_name: str = "",
        _retried: bool = False,
    ) -> str:
        await self._ensure_session()
        if not self.access_token:
            await self._refresh_access_token_locked(force=False)

        token_used = self.access_token
        request_url = f"{OneDriveAPI.BASE_URL}{self._item_path('copy', file_id)}"
        payload = {
            "parentReference": parent_reference,
            "@microsoft.graph.conflictBehavior": "rename",
        }
        if target_name:
            payload["name"] = target_name

        await self._apply_operation_delay()
        async with self._session.post(
            request_url,
            headers=OneDriveApiHelper.build_headers(token_used),
            json=payload,
        ) as response:
            if response.status in (401, 403) and not _retried:
                await self._refresh_access_token_locked(force=False, expected_access_token=token_used)
                return await self._copy_request(file_id, parent_reference, target_name, _retried=True)

            if response.status < 200 or response.status >= 300:
                try:
                    resp_data = await response.json()
                except Exception:
                    resp_data = {"error": {"message": (await response.text())[:500]}}
                message = OneDriveApiHelper.extract_error_message(resp_data)
                raise Exception(f"OneDrive API HTTP错误 ({response.status}): {message}")

            return response.headers.get("Location", "")

    async def _upload_small_file(
        self,
        local_path: str,
        file_name: str,
        parent_path: str,
        file_size: int,
        conflict_behavior: str,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    ) -> Dict[str, Any]:
        path = self._upload_item_path(parent_path, file_name, "/content")
        request_url = f"{OneDriveAPI.BASE_URL}{path}?@microsoft.graph.conflictBehavior={conflict_behavior}"

        await self._ensure_session()
        if not self.access_token:
            await self._refresh_access_token_locked(force=False)

        progress_message = "正在上传到OneDrive，分片（1/1）"
        await self._notify_upload_progress(progress_callback, 0, file_size, progress_message)
        with open(local_path, "rb") as file_obj:
            payload = file_obj.read()

        resp_data: Dict[str, Any] = {}
        for attempt in range(2):
            await self._apply_operation_delay()
            async with self._session.put(
                request_url,
                headers={
                    **OneDriveApiHelper.build_headers(self.access_token),
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(file_size),
                },
                data=self._iter_upload_chunk(
                    payload,
                    uploaded_base=0,
                    file_size=file_size,
                    progress_message=progress_message,
                    progress_callback=progress_callback,
                ),
            ) as response:
                try:
                    parsed_data = await response.json()
                except Exception:
                    resp_text = await response.text()
                    parsed_data = {"error": {"message": resp_text[:500]}} if resp_text.strip() else {}

                if response.status in (401, 403) and attempt == 0:
                    await self._refresh_access_token_locked(force=False, expected_access_token=self.access_token)
                    continue

                if response.status < 200 or response.status >= 300:
                    message = OneDriveApiHelper.extract_error_message(parsed_data)
                    raise Exception(f"OneDrive API HTTP错误 ({response.status}): {message}")

                resp_data = parsed_data if isinstance(parsed_data, dict) else {}
                break

        await self._notify_upload_progress(progress_callback, file_size, file_size, "上传成功")
        return resp_data

    async def _create_upload_session(
        self,
        file_name: str,
        parent_path: str,
        conflict_behavior: str,
    ) -> str:
        response = await self._api_request(
            "create_upload_session",
            "POST",
            path=self._upload_item_path(parent_path, file_name, "/createUploadSession"),
            json_data={
                "item": {
                    "@microsoft.graph.conflictBehavior": conflict_behavior,
                },
            },
        )
        upload_url = str(response.get("uploadUrl") or "")
        if not upload_url:
            raise Exception("创建上传会话失败：响应中缺少 uploadUrl")
        return upload_url

    async def _upload_large_file(
        self,
        local_path: str,
        file_name: str,
        parent_path: str,
        file_size: int,
        conflict_behavior: str,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    ) -> Dict[str, Any]:
        await self._notify_upload_progress(progress_callback, 0, file_size, "正在创建上传会话")
        upload_url = await self._create_upload_session(file_name, parent_path, conflict_behavior)

        chunk_size = 10 * 1024 * 1024
        total_chunks = max(1, (file_size + chunk_size - 1) // chunk_size)
        uploaded = 0
        last_response: Dict[str, Any] = {}

        with open(local_path, "rb") as file_obj:
            chunk_index = 0
            while uploaded < file_size:
                chunk_index += 1
                chunk = file_obj.read(min(chunk_size, file_size - uploaded))
                if not chunk:
                    break

                start = uploaded
                end = uploaded + len(chunk) - 1
                progress_message = f"正在上传到OneDrive，分片（{chunk_index}/{total_chunks}）"
                await self._notify_upload_progress(
                    progress_callback,
                    uploaded,
                    file_size,
                    progress_message,
                )

                for attempt in range(3):
                    await self._apply_operation_delay()
                    async with self._session.put(
                        upload_url,
                        headers={
                            "Content-Length": str(len(chunk)),
                            "Content-Range": f"bytes {start}-{end}/{file_size}",
                        },
                        data=self._iter_upload_chunk(
                            chunk,
                            uploaded_base=start,
                            file_size=file_size,
                            progress_message=progress_message,
                            progress_callback=progress_callback,
                        ),
                    ) as response:
                        try:
                            resp_data = await response.json()
                        except Exception:
                            resp_text = await response.text()
                            resp_data = {"error": {"message": resp_text[:500]}} if resp_text.strip() else {}

                        if response.status in (200, 201, 202):
                            last_response = resp_data if isinstance(resp_data, dict) else {}
                            break

                        if response.status in (500, 502, 503, 504) and attempt < 2:
                            await asyncio.sleep(1 + attempt)
                            continue

                        message = OneDriveApiHelper.extract_error_message(resp_data)
                        raise Exception(f"OneDrive 上传分片失败 ({response.status}): {message}")
                else:
                    raise Exception("OneDrive 上传分片失败")

                uploaded = end + 1
                await self._notify_upload_progress(progress_callback, uploaded, file_size, progress_message)

        await self._notify_upload_progress(progress_callback, file_size, file_size, "上传成功")
        return last_response

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
                json={"driver_type": "OneDrive", "refresh_token": self.refresh_token},
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
        self._log.debug("OneDrive访问令牌刷新成功", driver_name="onedrive")
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
            self._log.warning(f"OneDrive Token 持久化失败: {e}", driver_name="onedrive")

    async def _notify_direct_refresh_success(self) -> None:
        account_id = getattr(self, "_account_id", None) or getattr(self, "account_id", None)
        if not account_id or str(account_id) == "temp_test":
            return
        try:
            from core.auth_manager import sync_driver_refresh_success

            await sync_driver_refresh_success(int(account_id), self)
        except Exception as e:
            self._log.warning(f"OneDrive刷新成功后同步认证状态失败: {e}", driver_name="onedrive")

    async def refresh_auth(self) -> bool:
        try:
            await self._refresh_access_token_locked(force=True, notify_success=False)
            self._log.info("OneDrive认证刷新成功", driver_name="onedrive")
            return True
        except Exception as e:
            self._log.error(f"OneDrive认证刷新失败: {e}", driver_name="onedrive")
            return False

    @with_file_list_cache
    async def list_files(self, parent_id: str = "root") -> List[FileItem]:
        normalized_parent_id = self._normalize_item_id(parent_id)
        if normalized_parent_id == "/":
            path = OneDriveAPI.ENDPOINTS["root_children"]
        elif self._is_path_reference(normalized_parent_id):
            path = self._path_item_path("path_children", normalized_parent_id)
        else:
            path = self._item_path("item_children", normalized_parent_id)

        all_files: List[FileItem] = []
        next_url: Optional[str] = None
        params = {
            "$top": "200",
            "$select": "id,name,size,folder,file,parentReference,createdDateTime,lastModifiedDateTime,webUrl,eTag,cTag",
        }

        while True:
            response = await self._api_request(
                "file_list",
                "GET",
                path=None if next_url else path,
                url=next_url,
                params=None if next_url else params,
            )
            for item_data in response.get("value") or []:
                if isinstance(item_data, dict) and not item_data.get("deleted"):
                    all_files.append(OneDriveFile.from_dict(item_data).to_file_item())

            next_url = response.get("@odata.nextLink")
            if not next_url:
                break

        all_files.sort(key=lambda item: (not item.is_dir, item.name.lower()))
        return all_files

    @with_file_info_cache
    async def file_info(self, file_id: str) -> FileItem:
        normalized_file_id = self._normalize_item_id(file_id)
        if normalized_file_id == "/":
            response = await self._api_request("root", "GET")
        elif self._is_path_reference(normalized_file_id):
            response = await self._api_request(
                "item",
                "GET",
                path=self._path_item_path("path_item", normalized_file_id),
                params={
                    "$select": "id,name,size,folder,file,parentReference,createdDateTime,lastModifiedDateTime,webUrl,eTag,cTag",
                },
            )
        else:
            response = await self._api_request(
                "item",
                "GET",
                path=self._item_path("item", normalized_file_id),
                params={
                    "$select": "id,name,size,folder,file,parentReference,createdDateTime,lastModifiedDateTime,webUrl,eTag,cTag",
                },
            )
        return OneDriveFile.from_dict(response).to_file_item()

    async def get_download_url(self, file_id: str, user_agent: str = None) -> str:
        file_item = await self.file_info(file_id)
        download_url = ""
        if file_item and file_item.extra:
            download_url = str(file_item.extra.get("download_url") or "")

        if not download_url:
            response = await self._api_request(
                "item",
                "GET",
                path=self._item_path("item", self._normalize_item_id(file_id)),
            )
            download_url = str(response.get("@microsoft.graph.downloadUrl") or "")

        if not download_url:
            raise Exception("获取下载链接失败：响应中缺少 downloadUrl")
        return download_url

    @auto_cleanup_cache("create_folder")
    async def create_folder(self, parent_id: str, name: str) -> OperationResult:
        folder_name = (name or "").strip()
        if not folder_name:
            return OperationResult(success=False, message="文件夹名称不能为空")

        normalized_parent_id = self._normalize_item_id(parent_id)
        if normalized_parent_id == "/":
            path = OneDriveAPI.ENDPOINTS["root_children"]
        elif self._is_path_reference(normalized_parent_id):
            path = self._path_item_path("path_children", normalized_parent_id)
        else:
            path = self._item_path("item_children", normalized_parent_id)

        try:
            response = await self._api_request(
                "create_folder",
                "POST",
                path=path,
                json_data={
                    "name": folder_name,
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "rename",
                },
            )
            folder_item = OneDriveFile.from_dict(response).to_file_item()
            folder_id = folder_item.id
            return OperationResult(
                success=True,
                message=f"文件夹 '{folder_name}' 创建成功",
                data={
                    "folder_id": folder_id,
                    "folder_name": folder_item.name or folder_name,
                    "parent_id": normalized_parent_id,
                    "file_size": folder_item.size if folder_item else 0,
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"创建文件夹失败: {str(e)}")

    async def _collect_parent_ids(self, file_ids: List[str]) -> List[str]:
        parent_ids: List[str] = []
        for file_id in file_ids:
            try:
                info = await self.file_info(file_id)
                parent_id = info.extra.get("parent_id", "") if info and info.extra else ""
                if parent_id not in (None, ""):
                    parent_ids.append(str(parent_id))
            except Exception as e:
                self._log.warning(f"OneDrive获取文件父目录失败: file_id={file_id}, 错误={e}", driver_name="onedrive")
        return sorted(set(parent_ids))

    @auto_cleanup_cache("move_file")
    async def move_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        normalized_ids = [str(file_id).strip() for file_id in file_ids if str(file_id).strip()]
        if not normalized_ids:
            return OperationResult(success=False, message="没有指定要移动的文件")
        if any(self._is_virtual_root_id(file_id) for file_id in normalized_ids):
            return OperationResult(success=False, message="根目录不支持移动")

        normalized_target_parent_id = self._normalize_item_id(target_parent_id)
        parent_reference = self._parent_reference(target_parent_id)
        source_parent_ids = await self._collect_parent_ids(normalized_ids)

        try:
            for chunk in self._chunk_file_ids(normalized_ids):
                for file_id in chunk:
                    await self._api_request(
                        "move",
                        "PATCH",
                        path=self._item_path("item", file_id),
                        json_data={
                            "parentReference": parent_reference,
                            "@microsoft.graph.conflictBehavior": "rename",
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

    async def batch_move_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        return await self.move_file(file_ids, target_parent_id)

    @auto_cleanup_cache("copy_file")
    async def copy_file(self, file_ids: List[str], target_parent_id: str, source_parent_id: str = None) -> OperationResult:
        normalized_ids = [str(file_id).strip() for file_id in file_ids if str(file_id).strip()]
        if not normalized_ids:
            return OperationResult(success=False, message="没有指定要复制的文件")
        if any(self._is_virtual_root_id(file_id) for file_id in normalized_ids):
            return OperationResult(success=False, message="根目录不支持复制")

        normalized_target_parent_id = self._normalize_item_id(target_parent_id)
        parent_reference = self._parent_reference(target_parent_id)
        source_parent_ids = [str(source_parent_id)] if source_parent_id not in (None, "") else await self._collect_parent_ids(normalized_ids)

        try:
            monitor_urls: List[str] = []
            copied_names: List[str] = []
            occupied_names = await self._collect_directory_names(target_parent_id)
            for chunk in self._chunk_file_ids(normalized_ids):
                for file_id in chunk:
                    source_info = await self.file_info(file_id)
                    target_name = self._next_available_copy_name(source_info.name, occupied_names)
                    occupied_names.add(target_name)
                    copied_names.append(target_name)

                    monitor_url = await self._copy_request(file_id, parent_reference, target_name)
                    if monitor_url:
                        monitor_urls.append(monitor_url)

            return OperationResult(
                success=True,
                message=f"已复制 {len(normalized_ids)} 个文件到目标目录",
                data={
                    "copied_count": len(normalized_ids),
                    "file_ids": normalized_ids,
                    "copied_names": copied_names,
                    "monitor_urls": monitor_urls,
                    "target_parent_id": normalized_target_parent_id,
                    "source_parent_ids": source_parent_ids,
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"复制失败: {str(e)}")

    async def batch_copy_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        return await self.copy_file(file_ids, target_parent_id)

    @staticmethod
    def _is_api_not_found_error(error: Exception) -> bool:
        message = str(error or "").lower()
        return "api not found" in message or "not found" in message and "api" in message

    async def _permanent_delete_file(self, file_id: str, drive_id: str = "") -> None:
        try:
            await self._api_request(
                "permanent_delete",
                "POST",
                path=f"{self._item_path('item', file_id)}/permanentDelete",
            )
            return
        except Exception as first_error:
            if not drive_id or not self._is_api_not_found_error(first_error):
                raise
            self._log.warning(
                f"OneDrive永久删除 /me 路径不可用，尝试 drive 路径: {first_error}",
                driver_name="onedrive",
            )

        await self._api_request(
            "permanent_delete",
            "POST",
            path=self._drive_item_path(drive_id, file_id, "/permanentDelete"),
        )

    @auto_cleanup_cache("rename_file")
    async def rename_file(self, file_id: str, new_name: str) -> OperationResult:
        if self._is_virtual_root_id(file_id):
            return OperationResult(success=False, message="根目录不支持重命名")
        normalized_file_id = self._normalize_item_id(file_id)
        target_name = (new_name or "").strip()
        if not target_name:
            return OperationResult(success=False, message="新名称不能为空")

        parent_id = ""
        old_name = ""
        try:
            old_info = await self.file_info(normalized_file_id)
            parent_id = old_info.extra.get("parent_id", "") if old_info and old_info.extra else ""
            old_name = old_info.name if old_info else ""
        except Exception as e:
            self._log.warning(f"OneDrive重命名前获取文件详情失败: {e}", driver_name="onedrive")

        try:
            response = await self._api_request(
                "rename",
                "PATCH",
                path=self._item_path("item", normalized_file_id),
                json_data={"name": target_name},
            )
            updated_item = OneDriveFile.from_dict(response).to_file_item()
            return OperationResult(
                success=True,
                message=f"重命名成功: {old_name or normalized_file_id} -> {updated_item.name or target_name}",
                data={
                    "file_id": normalized_file_id,
                    "parent_id": parent_id,
                    "old_name": old_name,
                    "new_name": updated_item.name or target_name,
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"重命名失败: {str(e)}")

    async def _delete_files(self, file_ids: List[str]) -> OperationResult:
        normalized_ids = [self._normalize_item_id(file_id) for file_id in file_ids if str(file_id).strip()]
        if not normalized_ids:
            return OperationResult(success=False, message="没有指定要删除的文件")
        if any(self._is_virtual_root_id(file_id) for file_id in file_ids):
            return OperationResult(success=False, message="根目录不支持删除")

        parent_ids: List[str] = []
        drive_ids: Dict[str, str] = {}
        for file_id in normalized_ids:
            try:
                file_info = await self.file_info(file_id)
                if file_info and file_info.extra:
                    parent_id = file_info.extra.get("parent_id", "")
                    if parent_id not in (None, ""):
                        parent_ids.append(str(parent_id))
                    drive_ids[file_id] = str(file_info.extra.get("drive_id", "") or "")
            except Exception as e:
                self._log.warning(f"OneDrive删除前获取文件详情失败: file_id={file_id}, 错误={e}", driver_name="onedrive")

        if self.config.delete_mode == "delete":
            missing_drive_ids = [file_id for file_id in normalized_ids if not drive_ids.get(file_id)]
            if missing_drive_ids:
                return OperationResult(success=False, message="永久删除失败：缺少 drive_id")

        action_label = "永久删除" if self.config.delete_mode == "delete" else "移到回收站"
        try:
            if self.config.delete_mode == "delete":
                for file_id in normalized_ids:
                    await self._permanent_delete_file(file_id, drive_ids[file_id])
            else:
                for file_id in normalized_ids:
                    await self._api_request(
                        "delete",
                        "DELETE",
                        path=self._item_path("item", file_id),
                    )

            return OperationResult(
                success=True,
                message=f"已{action_label} {len(normalized_ids)} 个文件",
                data={
                    "file_id": normalized_ids[0] if len(normalized_ids) == 1 else None,
                    "file_ids": normalized_ids,
                    "delete_mode": self.config.delete_mode,
                    "parent_ids": sorted(set(parent_ids)),
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

    async def get_download_info(self, file_id: str, user_agent: str = None) -> Dict[str, Any]:
        file_item = await self.file_info(file_id)
        download_url = await self.get_download_url(file_id, user_agent)
        return {
            "download_url": download_url,
            "file_name": file_item.name if file_item else f"file_{file_id}",
            "size": file_item.size if file_item else 0,
            "file_info": file_item,
            "effective_mode": self.config.download_mode,
        }

    async def upload_file(
        self,
        upload_file: UploadFile,
        parent_path: str = "root",
        conflict_policy: str = "overwrite",
    ) -> OperationResult:
        temp_path = ""
        try:
            temp_path = await self._save_upload_to_tempfile(upload_file)
            return await self.upload_local_file(
                temp_path,
                upload_file.filename or os.path.basename(temp_path),
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

    async def upload_local_file_with_resume(
        self,
        local_path: str,
        file_name: str,
        parent_path: str = "root",
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        conflict_policy: str = "overwrite",
        resume_state: Optional[Dict[str, Any]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    ) -> OperationResult:
        return await self.upload_local_file(
            local_path,
            file_name,
            parent_path,
            progress_callback=progress_callback,
            conflict_policy=conflict_policy,
            resume_state=resume_state,
            state_callback=state_callback,
        )

    @auto_cleanup_cache("upload_file")
    async def upload_local_file(
        self,
        local_path: str,
        file_name: str,
        parent_path: str = "root",
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        conflict_policy: str = "overwrite",
        resume_state: Optional[Dict[str, Any]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    ) -> OperationResult:
        _ = (resume_state, state_callback)

        target_name = os.path.basename((file_name or "").strip())
        if not target_name:
            return OperationResult(success=False, message="上传文件名不能为空")
        if not local_path or not os.path.exists(local_path):
            return OperationResult(success=False, message="待上传文件不存在")

        file_size = os.path.getsize(local_path)
        if file_size <= 0:
            return OperationResult(success=False, message="暂不支持上传空文件")

        parent_id = self._normalize_item_id(parent_path)
        occupied_names = await self._collect_directory_names(parent_path)
        if conflict_policy == "skip" and target_name in occupied_names:
            await self._notify_upload_progress(progress_callback, file_size, file_size, "已跳过")
            return OperationResult(
                success=True,
                message=f"文件 '{target_name}' 已存在，已跳过",
                data={
                    "skipped": True,
                    "file_name": target_name,
                    "parent_id": parent_id,
                },
            )
        if conflict_policy == "rename":
            target_name = self._next_available_copy_name(target_name, occupied_names)

        conflict_behavior = self._upload_conflict_behavior(conflict_policy)

        try:
            if file_size <= 4 * 1024 * 1024:
                response = await self._upload_small_file(
                    local_path,
                    target_name,
                    parent_path,
                    file_size,
                    conflict_behavior,
                    progress_callback,
                )
            else:
                response = await self._upload_large_file(
                    local_path,
                    target_name,
                    parent_path,
                    file_size,
                    conflict_behavior,
                    progress_callback,
                )

            uploaded_item = OneDriveFile.from_dict(response).to_file_item() if response else None
            return OperationResult(
                success=True,
                message=f"文件 '{target_name}' 上传成功",
                data={
                    "file_id": uploaded_item.id if uploaded_item else "",
                    "file_name": uploaded_item.name if uploaded_item and uploaded_item.name else target_name,
                    "parent_id": parent_id,
                    "file_size": uploaded_item.size if uploaded_item else file_size,
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"上传失败: {str(e)}")

    def __str__(self) -> str:
        masked = f"{self.access_token[:12]}..." if self.access_token else "empty"
        return f"<{self.__class__.__name__} access_token={masked}>"
