"""天翼云盘驱动：第一阶段支持扫码登录、刷新会话、个人云列表。"""

import asyncio
import base64
import hashlib
import json
import os
import re
import time
import uuid
from urllib.parse import quote, unquote
from typing import Any, Awaitable, Callable, Dict, List, Optional
import xml.etree.ElementTree as ET

import aiohttp
from yarl import URL
from fastapi import UploadFile

from core.base import DriverInfo, FileItem, OperationResult
from core.auth_manager import RefreshOutcome
from core.driver_base import BaseDriver
from core.operation_wrapper import auto_cleanup_cache, with_file_info_cache, with_file_list_cache

from .api import Cloud189API, Cloud189ApiHelper
from .config import Cloud189Config
from .models import Cloud189Item


class Cloud189Driver(BaseDriver):
    def __init__(self, config: Cloud189Config):
        super().__init__(config)
        self.refresh_token = config.refresh_token
        self._session: Optional[aiohttp.ClientSession] = None
        self._upload_session: Optional[aiohttp.ClientSession] = None
        self._token_info: Dict[str, Any] = {}
        self._known_items: Dict[str, FileItem] = {}

    @classmethod
    def get_info(cls) -> DriverInfo:
        return DriverInfo(
            name="189_cloud",
            display_name="天翼云盘",
            version="0.1.0",
            capabilities=[
                "list", "info", "download", "upload",
                "create_folder", "delete", "batch_delete", "rename",
                "move", "batch_move", "copy", "batch_copy",
            ],
            description="天翼云盘 PC 接口接入",
            author="LitePan",
        )

    @classmethod
    async def start_qr_login(cls) -> Dict[str, Any]:
        from .qr_login import qr_login_manager
        return await qr_login_manager.start()

    @classmethod
    async def poll_qr_login(cls, state_id: str) -> Dict[str, Any]:
        from .qr_login import qr_login_manager
        return await qr_login_manager.poll(state_id)

    async def init(self) -> None:
        await self._ensure_session()
        await self._refresh_token()
        self._log.debug("天翼云盘驱动初始化完成", driver_name="189_cloud")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        if self._upload_session and not self._upload_session.closed:
            await self._upload_session.close()
        self._log.debug("天翼云盘驱动已关闭", driver_name="189_cloud")

    async def sync_runtime_auth_state(self, config: Optional[Dict[str, Any]] = None) -> None:
        if isinstance(config, dict) and "refresh_token" in config:
            self.refresh_token = config.get("refresh_token") or ""
            self.config.refresh_token = self.refresh_token

    async def _ensure_session(self) -> None:
        if not self._session or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self._session = aiohttp.ClientSession(
                headers=Cloud189API.HEADERS,
                timeout=timeout,
                cookie_jar=aiohttp.DummyCookieJar(),
            )

    async def _ensure_upload_session(self) -> None:
        if not self._upload_session or self._upload_session.closed:
            timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=300)
            self._upload_session = aiohttp.ClientSession(
                timeout=timeout,
                cookie_jar=aiohttp.DummyCookieJar(),
            )

    async def _apply_operation_delay(self) -> None:
        await self.wait_for_request_interval()

    def _has_session(self) -> bool:
        return bool(self._token_info.get("sessionKey") and self._token_info.get("sessionSecret"))

    def _is_success_code(self, value: Any) -> bool:
        return value in (None, "", 0, "0")

    async def _refresh_token(self) -> bool:
        await self._ensure_session()
        if not self.refresh_token:
            raise Exception("缺少 refresh_token，请重新扫码登录")

        form = {
            "clientId": Cloud189API.APP_ID,
            "refreshToken": self.refresh_token,
            "grantType": "refresh_token",
            "format": "json",
        }
        await self._apply_operation_delay()
        async with self._session.post(
            f"{Cloud189API.AUTH_URL}/api/oauth2/refreshToken.do",
            data=form,
            headers={"Accept": "application/json;charset=UTF-8"},
        ) as response:
            data = await response.json(content_type=None)
            if response.status != 200:
                raise Exception(f"刷新令牌失败 HTTP {response.status}")

        if not self._is_success_code(data.get("res_code")):
            raise Exception(data.get("res_message") or "刷新令牌失败")

        new_refresh_token = data.get("refreshToken") or self.refresh_token
        access_token = data.get("accessToken") or data.get("access_token") or ""
        self.refresh_token = new_refresh_token
        self.config.refresh_token = new_refresh_token
        self.config.access_token = access_token
        self._token_info.update(data)

        now = int(time.time())
        self.config.last_refresh_time = now
        expires_in = self._extract_expires_in(data)
        if expires_in:
            self.config.token_expires_seconds = expires_in
            self.config.expires_at = now + expires_in
            self.config.token_expires_at = now + expires_in

        if access_token:
            await self._refresh_session(access_token)
        elif not self._has_session():
            raise Exception("刷新令牌成功但未获取到 accessToken")

        return True

    async def _refresh_session(self, access_token: str) -> None:
        params = Cloud189ApiHelper.client_suffix()
        params.update({
            "appId": Cloud189API.APP_ID,
            "accessToken": access_token,
        })
        await self._apply_operation_delay()
        async with self._session.get(
            f"{Cloud189API.API_URL}/getSessionForPC.action",
            params=params,
            headers={"X-Request-ID": str(uuid.uuid4())},
        ) as response:
            data = await response.json(content_type=None)
            if response.status != 200:
                raise Exception(f"刷新会话失败 HTTP {response.status}")

        if not self._is_success_code(data.get("res_code")):
            raise Exception(data.get("res_message") or "刷新会话失败")
        self._token_info.update(data)

    @staticmethod
    def _extract_expires_in(data: Dict[str, Any]) -> int:
        for key in ("expiresIn", "expires_in", "expires"):
            try:
                value = int(data.get(key) or 0)
            except (TypeError, ValueError):
                value = 0
            if value > 0:
                return value
        return 0

    async def refresh_auth(self) -> RefreshOutcome:
        try:
            await self._refresh_token()
            self.config.auth_status = "active"
            self.config.refresh_attempts = 0
            return RefreshOutcome.SUCCESS
        except Exception as e:
            self._log.warning(f"天翼云盘认证刷新失败: {e}", driver_name="189_cloud")
            message = str(e)
            if "缺少 refresh_token" in message or "重新扫码登录" in message:
                return RefreshOutcome.FATAL
            return RefreshOutcome.RETRYABLE

    def _signed_headers(self, method: str, url: str, params: str = "") -> Dict[str, str]:
        session_key = self._token_info.get("sessionKey") or ""
        session_secret = self._token_info.get("sessionSecret") or ""
        if not session_key or not session_secret:
            raise Exception("天翼云盘会话未初始化")
        return Cloud189ApiHelper.signature_headers(session_key, session_secret, method, url, params)

    async def _api_request(self, method: str, url: str, *, params: Optional[Dict[str, Any]] = None, retry: bool = True) -> Dict[str, Any]:
        await self._ensure_session()
        request_params = Cloud189ApiHelper.client_suffix()
        if params:
            request_params.update({k: str(v) for k, v in params.items() if v is not None})

        await self._apply_operation_delay()
        async with self._session.request(
            method,
            url,
            params=request_params,
            headers=self._signed_headers(method, url),
        ) as response:
            text = await response.text()

        if ("InvalidSessionKey" in text or "userSessionBO is null" in text) and retry:
            await self._refresh_token()
            return await self._api_request(method, url, params=params, retry=False)

        if response.status != 200:
            raise Exception(f"天翼云盘 API HTTP {response.status}: {text[:200]}")

        try:
            data = __import__("json").loads(text)
        except Exception:
            raise Exception(f"天翼云盘 API 返回非 JSON: {text[:200]}")

        if isinstance(data, dict):
            res_code = data.get("res_code")
            if res_code not in (None, 0, "0"):
                raise Exception(data.get("res_message") or data.get("message") or f"天翼云盘 API 错误: {res_code}")
            code = data.get("code")
            if code and code != "SUCCESS":
                raise Exception(data.get("message") or data.get("msg") or f"天翼云盘 API 错误: {code}")

        return data

    async def _api_form_request(self, method: str, url: str, *, data: Optional[Dict[str, Any]] = None, retry: bool = True) -> Dict[str, Any]:
        await self._ensure_session()
        request_params = Cloud189ApiHelper.client_suffix()

        await self._apply_operation_delay()
        async with self._session.request(
            method,
            url,
            params=request_params,
            data={k: str(v) for k, v in (data or {}).items() if v is not None},
            headers=self._signed_headers(method, url),
        ) as response:
            text = await response.text()

        if ("InvalidSessionKey" in text or "userSessionBO is null" in text) and retry:
            await self._refresh_token()
            return await self._api_form_request(method, url, data=data, retry=False)

        if response.status != 200:
            raise Exception(f"天翼云盘 API HTTP {response.status}: {text[:200]}")

        try:
            result = json.loads(text)
        except Exception:
            raise Exception(f"天翼云盘 API 返回非 JSON: {text[:200]}")

        if isinstance(result, dict):
            res_code = result.get("res_code")
            if res_code not in (None, "", 0, "0"):
                raise Exception(result.get("res_message") or result.get("message") or f"天翼云盘 API 错误: {res_code}")
            code = result.get("code")
            if code and code != "SUCCESS":
                raise Exception(result.get("message") or result.get("msg") or f"天翼云盘 API 错误: {code}")

        return result

    async def test_connection(self) -> OperationResult:
        try:
            await self.list_files("0")
            login_name = self._token_info.get("loginName") or "天翼云盘"
            return OperationResult(success=True, message=f"连接成功：{login_name}")
        except Exception as e:
            return OperationResult(success=False, message=f"连接测试失败: {str(e)}")

    def _api_parent_id(self, parent_id: Optional[str]) -> str:
        if parent_id is None or str(parent_id).strip() in {"", "0", "/"}:
            return self.config.root_folder_id or "-11"
        return str(parent_id).strip()

    @with_file_list_cache
    async def list_files(self, parent_id: str = "0") -> List[FileItem]:
        api_parent_id = self._api_parent_id(parent_id)
        url = f"{Cloud189API.API_URL}/listFiles.action"
        page_size = 1000
        result: List[FileItem] = []

        for page_num in range(1, 10000):
            data = await self._api_request("GET", url, params={
                "folderId": api_parent_id,
                "fileType": "0",
                "mediaAttr": "0",
                "iconOption": "5",
                "pageNum": page_num,
                "pageSize": page_size,
                "recursive": "0",
                "orderBy": "filename",
                "descending": "false",
            })
            list_ao = (data.get("fileListAO") or {}) if isinstance(data, dict) else {}
            folders = list_ao.get("folderList") or []
            files = list_ao.get("fileList") or []
            if not folders and not files:
                break

            for item in folders:
                file_item = Cloud189Item.from_folder(item).to_file_item()
                self._known_items[file_item.id] = file_item
                result.append(file_item)
            for item in files:
                file_item = Cloud189Item.from_file(item).to_file_item()
                self._known_items[file_item.id] = file_item
                result.append(file_item)

            if len(folders) + len(files) < page_size:
                break

        return result

    @with_file_info_cache
    async def file_info(self, file_id: str) -> Optional[FileItem]:
        known = self._known_items.get(str(file_id))
        if known:
            return known
        if str(file_id) in {"0", "/", self.config.root_folder_id}:
            return FileItem(
                id=self.config.root_folder_id,
                name="天翼云盘",
                path="/",
                is_dir=True,
                size=0,
            )
        return FileItem(
            id=str(file_id),
            name=f"file_{file_id}",
            path=f"/{file_id}",
            is_dir=False,
            size=0,
        )

    @auto_cleanup_cache("create_folder")
    async def create_folder(self, parent_id: str, name: str) -> OperationResult:
        folder_name = (name or "").strip()
        if not folder_name:
            return OperationResult(success=False, message="文件夹名称不能为空")
        try:
            api_parent_id = self._api_parent_id(parent_id)
            data = await self._api_request("POST", f"{Cloud189API.API_URL}/createFolder.action", params={
                "parentFolderId": api_parent_id,
                "folderName": folder_name,
                "relativePath": "",
            })
            folder = Cloud189Item.from_folder(data).to_file_item() if data else None
            folder_id = folder.id if folder else (data.get("id") if isinstance(data, dict) else "")
            if folder and folder.id:
                self._known_items[folder.id] = folder
            return OperationResult(
                success=True,
                message=f"文件夹 '{folder_name}' 创建成功",
                data={"folder_id": folder_id, "parent_id": parent_id, "folder_name": folder_name},
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
        if normalized_file_id in {"0", "/", self.config.root_folder_id}:
            return OperationResult(success=False, message="根目录不支持重命名")

        try:
            known = self._known_items.get(normalized_file_id)
            is_dir = bool(known and known.is_dir)
            parent_id = str((known.extra or {}).get("parent_id") or "") if known else ""
            old_name = known.name if known else ""
            if is_dir:
                endpoint = "renameFolder.action"
                params = {"folderId": normalized_file_id, "destFolderName": target_name}
            else:
                endpoint = "renameFile.action"
                params = {"fileId": normalized_file_id, "destFileName": target_name}

            try:
                await self._api_request("POST", f"{Cloud189API.API_URL}/{endpoint}", params=params)
            except Exception:
                if known is not None:
                    raise
                # 兜底：如果没有列表缓存，不知道是文件还是目录，就换另一种端点再试一次。
                fallback_endpoint = "renameFolder.action"
                fallback_params = (
                    {"folderId": normalized_file_id, "destFolderName": target_name}
                )
                await self._api_request("POST", f"{Cloud189API.API_URL}/{fallback_endpoint}", params=fallback_params)

            if known:
                known.name = target_name
            return OperationResult(
                success=True,
                message=f"重命名为 '{target_name}' 成功",
                data={
                    "file_id": normalized_file_id,
                    "parent_id": parent_id,
                    "old_name": old_name,
                    "new_name": target_name,
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"重命名失败: {str(e)}")

    def _build_batch_task_info(self, file_id: str) -> Dict[str, Any]:
        item = self._known_items.get(str(file_id))
        return {
            "fileId": str(file_id),
            "fileName": item.name if item else "",
            "isFolder": 1 if item and item.is_dir else 0,
        }

    async def _create_batch_task(
        self,
        task_type: str,
        task_infos: List[Dict[str, Any]],
        target_folder_id: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        form = {
            "type": task_type,
            "taskInfos": json.dumps(task_infos, ensure_ascii=False),
            "targetFolderId": target_folder_id or "",
        }
        if extra:
            form.update(extra)
        data = await self._api_form_request("POST", f"{Cloud189API.API_URL}/batch/createBatchTask.action", data=form)
        task_id = data.get("taskId") or data.get("task_id")
        if not task_id:
            raise Exception("天翼云盘未返回批量任务ID")
        return str(task_id)

    async def _wait_batch_task(self, task_type: str, task_id: str, interval: float = 0.3, timeout: float = 20.0) -> Dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + timeout
        last_state: Dict[str, Any] = {}
        while True:
            last_state = await self._api_form_request("POST", f"{Cloud189API.API_URL}/batch/checkBatchTask.action", data={
                "type": task_type,
                "taskId": task_id,
            })
            status = int(last_state.get("taskStatus") or 0)
            if status == 4:
                failed = int(last_state.get("failedCount") or 0)
                if failed > 0:
                    raise Exception(f"批量任务失败 {failed} 项")
                return last_state
            if status == 2:
                raise Exception("批量任务存在冲突")
            if asyncio.get_running_loop().time() >= deadline:
                raise Exception("等待批量任务完成超时")
            await asyncio.sleep(interval)

    async def _delete_files(self, file_ids: List[str]) -> OperationResult:
        normalized_file_ids = [str(file_id or "").strip() for file_id in file_ids if str(file_id or "").strip()]
        if not normalized_file_ids:
            return OperationResult(success=True, message="没有文件需要删除")
        if any(file_id in {"0", "/", self.config.root_folder_id} for file_id in normalized_file_ids):
            return OperationResult(success=False, message="根目录不支持删除")

        try:
            parent_ids = set()
            for file_id in normalized_file_ids:
                item = self._known_items.get(file_id)
                parent_id = str((item.extra or {}).get("parent_id") or "") if item else ""
                if parent_id:
                    parent_ids.add(parent_id)

            task_infos = [self._build_batch_task_info(file_id) for file_id in normalized_file_ids]
            task_id = await self._create_batch_task("DELETE", task_infos)
            await self._wait_batch_task("DELETE", task_id, interval=0.3)

            if self.config.delete_mode == "delete":
                clear_task_id = await self._create_batch_task("CLEAR_RECYCLE", task_infos)
                await self._wait_batch_task("CLEAR_RECYCLE", clear_task_id, interval=1.0)
                message = f"已永久删除 {len(normalized_file_ids)} 个文件"
            else:
                message = f"已将 {len(normalized_file_ids)} 个文件移到回收站"

            for file_id in normalized_file_ids:
                self._known_items.pop(file_id, None)

            return OperationResult(
                success=True,
                message=message,
                data={
                    "deleted_count": len(normalized_file_ids),
                    "file_ids": normalized_file_ids,
                    "parent_ids": list(parent_ids),
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"删除失败: {str(e)}")

    @auto_cleanup_cache("delete_file")
    async def delete_file(self, file_id: str) -> OperationResult:
        return await self._delete_files([file_id])

    @auto_cleanup_cache("batch_delete_file")
    async def batch_delete_file(self, file_ids: List[str]) -> OperationResult:
        return await self._delete_files(file_ids)

    def _source_parent_ids_for(self, file_ids: List[str]) -> List[str]:
        parent_ids = []
        seen = set()
        for file_id in file_ids:
            item = self._known_items.get(str(file_id))
            parent_id = str((item.extra or {}).get("parent_id") or "") if item else ""
            if parent_id and parent_id not in seen:
                seen.add(parent_id)
                parent_ids.append(parent_id)
        return parent_ids

    async def _transfer_files(self, task_type: str, file_ids: List[str], target_parent_id: str) -> OperationResult:
        normalized_file_ids = [str(file_id or "").strip() for file_id in file_ids if str(file_id or "").strip()]
        if not normalized_file_ids:
            action = "移动" if task_type == "MOVE" else "复制"
            return OperationResult(success=True, message=f"没有文件需要{action}")

        target_id = self._api_parent_id(target_parent_id)
        if any(file_id in {"0", "/", self.config.root_folder_id} for file_id in normalized_file_ids):
            action = "移动" if task_type == "MOVE" else "复制"
            return OperationResult(success=False, message=f"根目录不支持{action}")

        try:
            task_infos = [self._build_batch_task_info(file_id) for file_id in normalized_file_ids]
            target_item = self._known_items.get(target_id)
            extra = {}
            if target_item and target_item.name:
                extra["targetFileName"] = target_item.name

            task_id = await self._create_batch_task(task_type, task_infos, target_folder_id=target_id, extra=extra)
            await self._wait_batch_task(task_type, task_id, interval=0.4 if task_type == "MOVE" else 1.0)

            source_parent_ids = self._source_parent_ids_for(normalized_file_ids)
            if task_type == "MOVE":
                for file_id in normalized_file_ids:
                    item = self._known_items.get(file_id)
                    if item and item.extra is not None:
                        item.extra["parent_id"] = target_id
                message = f"已移动 {len(normalized_file_ids)} 个文件"
                data_key = "moved_count"
            else:
                message = f"已复制 {len(normalized_file_ids)} 个文件"
                data_key = "copied_count"

            return OperationResult(
                success=True,
                message=message,
                data={
                    data_key: len(normalized_file_ids),
                    "file_ids": normalized_file_ids,
                    "source_parent_ids": source_parent_ids,
                    "target_parent_id": target_id,
                },
            )
        except Exception as e:
            action = "移动" if task_type == "MOVE" else "复制"
            return OperationResult(success=False, message=f"{action}失败: {str(e)}")

    @auto_cleanup_cache("move_file")
    async def move_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        return await self._transfer_files("MOVE", file_ids, target_parent_id)

    async def batch_move_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        return await self.move_file(file_ids, target_parent_id)

    @auto_cleanup_cache("copy_file")
    async def copy_file(self, file_ids: List[str], target_parent_id: str, source_parent_id: str = None) -> OperationResult:
        target_id = self._api_parent_id(target_parent_id)
        source_ids = []
        if source_parent_id not in (None, ""):
            source_ids = [self._api_parent_id(source_parent_id)]
        else:
            source_ids = self._source_parent_ids_for([str(file_id or "").strip() for file_id in file_ids])

        if len(source_ids) == 1 and source_ids[0] == target_id:
            return OperationResult(
                success=False,
                message="天翼云盘不支持复制到同一目录",
                data={"warning": True},
            )
        return await self._transfer_files("COPY", file_ids, target_parent_id)

    async def batch_copy_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        return await self.copy_file(file_ids, target_parent_id)

    async def get_download_info(self, file_id: str, user_agent: str = None) -> Dict[str, Any]:
        file_item = self._known_items.get(str(file_id)) or await self.file_info(file_id)
        if file_item and file_item.is_dir:
            raise Exception("文件夹不支持下载")

        data = await self._api_request("GET", f"{Cloud189API.API_URL}/getFileDownloadUrl.action", params={
            "fileId": file_id,
            "dt": "3",
            "flag": "1",
        })
        download_url = str(data.get("fileDownloadUrl") or data.get("downloadUrl") or data.get("url") or "")
        if not download_url:
            raise Exception("天翼云盘未返回下载链接")
        download_url = download_url.replace("&amp;", "&")
        download_url = re.sub(r"^http://", "https://", download_url, flags=re.IGNORECASE)
        resolved_name = file_item.name if file_item and file_item.name else ""
        if resolved_name.startswith("file_"):
            resolved_name = ""

        await self._ensure_session()
        try:
            async with self._session.get(
                download_url,
                headers={"User-Agent": user_agent or Cloud189API.USER_AGENT},
                allow_redirects=False,
            ) as response:
                if not resolved_name:
                    resolved_name = self._filename_from_headers(response.headers)
                if response.status in (301, 302, 303, 307, 308):
                    location = response.headers.get("Location") or response.headers.get("location")
                    if location:
                        download_url = location.replace("&amp;", "&")
        except Exception as e:
            self._log.warning(f"天翼云盘解析真实下载链接失败，使用原始链接: {e}", driver_name="189_cloud")

        return {
            "download_url": download_url,
            "file_name": resolved_name or (file_item.name if file_item else f"file_{file_id}"),
            "size": int(file_item.size or 0) if file_item else 0,
        }

    async def get_download_url(self, file_id: str, user_agent: str = None) -> str:
        info = await self.get_download_info(file_id, user_agent)
        return info["download_url"]

    async def get_download_headers(self, file_id: str, user_agent: str = None) -> Dict[str, str]:
        return {
            "User-Agent": user_agent or Cloud189API.USER_AGENT,
            "Referer": Cloud189API.WEB_URL,
        }

    async def _notify_upload_progress(
        self,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]],
        uploaded: int,
        total: int,
        message: str,
    ) -> None:
        if progress_callback:
            await progress_callback(uploaded, total, message)

    async def _save_upload_to_tempfile(self, upload_file: UploadFile) -> str:
        import tempfile

        suffix = os.path.splitext(upload_file.filename or "")[1]
        fd, temp_path = tempfile.mkstemp(prefix="litepan_189_upload_", suffix=suffix)
        try:
            with os.fdopen(fd, "wb") as temp_file:
                while True:
                    chunk = await upload_file.read(1024 * 1024)
                    if not chunk:
                        break
                    temp_file.write(chunk)
            return temp_path
        except Exception:
            try:
                os.remove(temp_path)
            except OSError:
                pass
            raise

    @staticmethod
    def _upload_part_size(file_size: int) -> int:
        default = 10 * 1024 * 1024
        if file_size > default * 2 * 999:
            multiple = max((file_size + 1999 * default - 1) // (1999 * default), 5)
            return multiple * default
        if file_size > default * 999:
            return default * 2
        return default

    @staticmethod
    def _calculate_multipart_hashes(local_path: str, part_size: int) -> Dict[str, Any]:
        file_md5 = hashlib.md5()
        part_md5_hexes: List[str] = []
        part_infos: List[str] = []
        file_size = os.path.getsize(local_path)
        part_number = 1

        with open(local_path, "rb") as file:
            while True:
                data = file.read(part_size)
                if not data:
                    break
                file_md5.update(data)
                part_digest = hashlib.md5(data).digest()
                part_hex = part_digest.hex().upper()
                part_md5_hexes.append(part_hex)
                part_infos.append(f"{part_number}-{base64.b64encode(part_digest).decode()}")
                part_number += 1

        file_md5_hex = file_md5.hexdigest().upper()
        slice_md5 = file_md5_hex
        if file_size > part_size:
            slice_md5 = hashlib.md5("\n".join(part_md5_hexes).encode()).hexdigest().upper()

        return {
            "file_md5": file_md5_hex,
            "slice_md5": slice_md5,
            "part_infos": part_infos,
        }

    @staticmethod
    def _pkcs7_pad(data: bytes, block_size: int = 16) -> bytes:
        padding = block_size - len(data) % block_size
        return data + bytes([padding]) * padding

    def _encrypt_upload_params(self, params: Dict[str, Any]) -> str:
        try:
            from Crypto.Cipher import AES
        except ImportError as e:
            raise Exception("缺少 pycryptodome 依赖，无法使用天翼云盘分片上传；请重新安装 requirements.txt 后重启程序") from e

        session_secret = self._token_info.get("sessionSecret") or ""
        if len(session_secret) < 16:
            raise Exception("天翼云盘会话密钥无效，请重新授权")

        plain = "&".join(
            f"{key}={value}"
            for key, value in sorted(params.items())
            if value is not None
        )
        cipher = AES.new(session_secret[:16].encode(), AES.MODE_ECB)
        encrypted = cipher.encrypt(self._pkcs7_pad(plain.encode()))
        return encrypted.hex().upper()

    async def _upload_encrypted_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        await self._ensure_session()
        url = f"{Cloud189API.UPLOAD_URL}/person/{endpoint.lstrip('/')}"
        encrypted_params = self._encrypt_upload_params(params)
        request_params = Cloud189ApiHelper.client_suffix()
        request_params["params"] = encrypted_params

        await self._apply_operation_delay()
        async with self._session.get(
            url,
            params=request_params,
            headers=self._signed_headers("GET", url, encrypted_params),
        ) as response:
            text = await response.text()

        if ("InvalidSessionKey" in text or "userSessionBO is null" in text):
            await self._refresh_token()
            encrypted_params = self._encrypt_upload_params(params)
            request_params = Cloud189ApiHelper.client_suffix()
            request_params["params"] = encrypted_params
            await self._apply_operation_delay()
            async with self._session.get(
                url,
                params=request_params,
                headers=self._signed_headers("GET", url, encrypted_params),
            ) as response:
                text = await response.text()

        if response.status != 200:
            raise Exception(f"天翼云盘上传 API HTTP {response.status}: {text[:300]}")

        try:
            data = json.loads(text)
        except Exception:
            raise Exception(f"天翼云盘上传 API 返回非 JSON: {text[:300]}")

        if isinstance(data, dict):
            code = data.get("code")
            if code and code != "SUCCESS":
                raise Exception(data.get("msg") or data.get("message") or f"天翼云盘上传 API 错误: {code}")
            res_code = data.get("res_code")
            if res_code not in (None, "", 0, "0"):
                raise Exception(data.get("res_message") or data.get("message") or f"天翼云盘上传 API 错误: {res_code}")

        return data

    @staticmethod
    def _parse_upload_headers(raw_headers: str) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        for item in (raw_headers or "").split("&"):
            if not item or "=" not in item:
                continue
            key, _, value = item.partition("=")
            if key:
                headers[key] = value
        return headers

    @staticmethod
    def _select_upload_part_url(upload_urls: Any, expected_part_number: int) -> tuple[int, Dict[str, Any]]:
        """从天翼返回的分片上传地址中取出本次要上传的分片。"""
        candidates: List[tuple[int, Dict[str, Any]]] = []

        if isinstance(upload_urls, dict):
            items = upload_urls.items()
        elif isinstance(upload_urls, list):
            items = enumerate(upload_urls, start=1)
        else:
            items = []

        for key, value in items:
            if not isinstance(value, dict):
                continue
            part_number = value.get("partNumber") or value.get("part_number")
            if part_number is None and isinstance(key, str) and key.startswith("partNumber_"):
                part_number = key.removeprefix("partNumber_")
            if part_number is None:
                part_number = key
            try:
                part_number_int = int(part_number)
            except (TypeError, ValueError):
                part_number_int = expected_part_number
            candidates.append((part_number_int, value))

        if not candidates:
            return expected_part_number, {}

        candidates.sort(key=lambda item: item[0])
        for part_number, value in candidates:
            if part_number == expected_part_number:
                return part_number, value
        return candidates[0]

    @staticmethod
    def _count_uploaded_parts(parts_resp: Dict[str, Any]) -> Optional[int]:
        data = parts_resp.get("data") if isinstance(parts_resp, dict) else None
        if isinstance(data, dict):
            upl = data.get("uploadedPartList")
            if isinstance(upl, str):
                stripped = upl.strip()
                if stripped == "":
                    return 0
                return len([x for x in stripped.split(",") if x.strip()])
            if isinstance(upl, dict):
                try:
                    return int(upl.get("uploadedPartCount"))
                except (TypeError, ValueError):
                    ids = str(upl.get("uploadedPartIds") or "").strip()
                    return len([x for x in ids.split(",") if x.strip()]) if ids else 0
            if isinstance(upl, list):
                return len(upl)

            for key in ("parts", "partList", "uploadedParts", "uploadParts"):
                value = data.get(key)
                if isinstance(value, list):
                    return len(value)
                if isinstance(value, dict):
                    return len(value)
            for key in ("uploadedPartNumber", "uploadedPartCount", "partCount"):
                value = data.get(key)
                try:
                    return int(value)
                except (TypeError, ValueError):
                    continue
        if isinstance(data, list):
            return len(data)
        return None

    async def _put_multipart_part(
        self,
        request_url: str,
        request_headers: Dict[str, str],
        local_path: str,
        offset: int,
        size: int,
        part_number: int,
        total_parts: int,
        base_uploaded: int,
        total_size: int,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    ) -> None:
        await self._ensure_session()
        headers = dict(request_headers)
        headers["Content-Length"] = str(size)

        def read_part_data() -> bytes:
            with open(local_path, "rb") as file:
                file.seek(offset)
                return file.read(size)

        part_data = await asyncio.to_thread(read_part_data)
        if len(part_data) != size:
            raise Exception(f"分片 {part_number}/{total_parts} 读取不完整: {len(part_data)}/{size}")

        suffix_pairs = "&".join(
            f"{quote(str(k), safe='')}={quote(str(v), safe='')}"
            for k, v in Cloud189ApiHelper.client_suffix().items()
        )
        sep = "&" if "?" in request_url else "?"
        put_url = f"{request_url}{sep}{suffix_pairs}"

        await self._ensure_upload_session()

        async with self._upload_session.put(
            URL(put_url, encoded=True),
            headers=headers,
            data=part_data,
            timeout=aiohttp.ClientTimeout(total=None, connect=30, sock_read=300),
        ) as response:
            text = await response.text()
            if response.status != 200:
                raise Exception(f"上传分片 {part_number}/{total_parts} 失败 HTTP {response.status}: {text[:300]}")
            if "errorCode" in text or "Error" in text:
                try:
                    root = ET.fromstring(text)
                    code = root.findtext("code") or root.findtext("errorCode") or ""
                    message = root.findtext("message") or root.findtext("errorMessage") or text[:300]
                    if code:
                        raise Exception(f"上传分片 {part_number}/{total_parts} 失败: {code} {message}")
                except ET.ParseError:
                    pass

        await self._notify_upload_progress(
            progress_callback,
            min(total_size, base_uploaded + size),
            total_size,
            f"正在上传到天翼云盘，分片（{part_number}/{total_parts}）",
        )

    @auto_cleanup_cache("upload_file")
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
        resume_state: Optional[Dict[str, Any]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    ) -> OperationResult:
        target_name = os.path.basename((file_name or "").strip())
        if not target_name:
            return OperationResult(success=False, message="上传文件名不能为空")
        if not os.path.exists(local_path):
            return OperationResult(success=False, message="本地上传文件不存在")

        file_size = os.path.getsize(local_path)
        if file_size <= 0:
            return OperationResult(success=False, message="暂不支持上传空文件")

        try:
            parent_id = self._api_parent_id(parent_path)
            part_size = self._upload_part_size(file_size)
            await self._notify_upload_progress(progress_callback, 0, file_size, "正在计算分片MD5")
            hash_info = await asyncio.to_thread(self._calculate_multipart_hashes, local_path, part_size)

            await self._notify_upload_progress(progress_callback, 0, file_size, "正在创建上传会话")
            init_resp = await self._upload_encrypted_request("initMultiUpload", {
                "parentFolderId": parent_id,
                "fileName": quote(target_name, safe=""),
                "fileSize": str(file_size),
                "sliceSize": str(part_size),
                "lazyCheck": "1",
            })
            init_data = init_resp.get("data") or {}
            upload_file_id = str(init_data.get("uploadFileId") or "")
            file_exists = int(init_data.get("fileDataExists") or 0)
            part_infos = hash_info["part_infos"]
            if not upload_file_id:
                return OperationResult(success=False, message="天翼云盘未返回上传会话信息")

            if file_exists != 1:
                total_parts = len(part_infos)
                uploaded = 0
                for index, part_info in enumerate(part_infos, start=1):
                    upload_urls_resp = await self._upload_encrypted_request("getMultiUploadUrls", {
                        "uploadFileId": upload_file_id,
                        "partInfo": part_info,
                    })
                    upload_urls = upload_urls_resp.get("uploadUrls") or upload_urls_resp.get("data") or {}
                    part_number, upload_data = self._select_upload_part_url(upload_urls, index)
                    request_url = upload_data.get("requestURL") or upload_data.get("requestUrl") or ""
                    request_headers = self._parse_upload_headers(upload_data.get("requestHeader") or "")
                    if not request_url:
                        return OperationResult(success=False, message=f"第 {index} 个分片未返回上传地址")

                    offset = (part_number - 1) * part_size
                    current_part_size = min(part_size, file_size - offset)
                    if current_part_size <= 0:
                        return OperationResult(success=False, message=f"第 {index} 个分片返回了无效编号: {part_number}")
                    await self._put_multipart_part(
                        request_url,
                        request_headers,
                        local_path,
                        offset,
                        current_part_size,
                        part_number,
                        total_parts,
                        uploaded,
                        file_size,
                        progress_callback,
                    )
                    uploaded += current_part_size

                try:
                    parts_resp = await self._upload_encrypted_request("getUploadedPartsInfo", {
                        "uploadFileId": upload_file_id,
                    })
                    uploaded_parts = self._count_uploaded_parts(parts_resp)
                    if uploaded_parts is not None and uploaded_parts != total_parts:
                        self._log.warning(
                            f"天翼云盘上传分片登记数量异常: {uploaded_parts}/{total_parts}",
                            driver_name="189_cloud",
                        )
                    else:
                        if uploaded_parts is None:
                            self._log.warning(
                                f"天翼云盘上传分片登记检查返回无法识别: {str(parts_resp)[:500]}",
                                driver_name="189_cloud",
                            )
                        else:
                            self._log.debug(
                                f"天翼云盘上传分片登记检查完成: {uploaded_parts}/{total_parts}",
                                driver_name="189_cloud",
                            )
                except Exception as check_error:
                    self._log.warning(f"天翼云盘查询已上传分片失败: {check_error}", driver_name="189_cloud")

            await self._notify_upload_progress(progress_callback, file_size, file_size, "正在提交上传")
            commit_resp = await self._upload_encrypted_request("commitMultiUploadFile", {
                "uploadFileId": upload_file_id,
                "fileMd5": hash_info["file_md5"],
                "sliceMd5": hash_info["slice_md5"],
                "lazyCheck": "1",
                "isLog": "0",
                "opertype": "3" if conflict_policy == "overwrite" else "1",
            })
            committed = commit_resp.get("file") or {}
            uploaded_name = committed.get("fileName") or target_name
            file_id = committed.get("userFileId") or committed.get("fileId") or upload_file_id

            await self._notify_upload_progress(progress_callback, file_size, file_size, "上传成功")
            return OperationResult(
                success=True,
                message=f"文件 '{uploaded_name}' 上传成功",
                data={
                    "file_id": file_id,
                    "file_name": uploaded_name,
                    "size": file_size,
                    "parent_id": parent_id,
                    "rapid_upload": file_exists == 1,
                    "part_size": part_size,
                    "part_count": len(part_infos),
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"上传文件失败: {str(e)}")

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
            local_path,
            file_name,
            parent_path,
            progress_callback=progress_callback,
            conflict_policy=conflict_policy,
            resume_state=resume_state,
            state_callback=state_callback,
        )

    @staticmethod
    def _filename_from_headers(headers: Any) -> str:
        content_disposition = headers.get("Content-Disposition") or headers.get("content-disposition") or ""
        if not content_disposition:
            return ""
        match = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", content_disposition, re.IGNORECASE)
        if match:
            return unquote(match.group(1).strip().strip('"'))
        match = re.search(r'filename\s*=\s*"([^"]+)"', content_disposition, re.IGNORECASE)
        if match:
            return unquote(match.group(1).strip())
        match = re.search(r"filename\s*=\s*([^;]+)", content_disposition, re.IGNORECASE)
        if match:
            return unquote(match.group(1).strip().strip('"'))
        return ""
