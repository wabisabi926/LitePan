"""移动云盘 (139 Cloud) 驱动：新版个人云 API，token 认证。"""

import asyncio
import base64
import hashlib
import json
import os
import tempfile
from collections import deque
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aiohttp
from fastapi import UploadFile

from core.base import FileItem, OperationResult, DriverInfo
from core.driver_base import BaseDriver
from core.operation_wrapper import (
    auto_cleanup_cache,
    with_file_info_cache,
    with_file_list_cache,
)
from .config import Cloud139Config
from .models import Cloud139File
from .api import Cloud139API, Cloud139ApiHelper


class Cloud139Driver(BaseDriver):
    _FILE_INFO_MAX_DIRS = 30000

    def __init__(self, config: Cloud139Config):
        super().__init__(config)
        self.authorization = config.authorization
        self._session: Optional[aiohttp.ClientSession] = None
        self._personal_host: Optional[str] = None
        self._refresh_lock = asyncio.Lock()
        self._account: Optional[str] = None
        self._auth_manager = None

    @classmethod
    def get_info(cls) -> DriverInfo:
        return DriverInfo(
            name="139_cloud",
            display_name="移动云盘",
            version="0.3.0",
            capabilities=[
                "list", "info", "download", "upload", "create_folder",
                "delete", "batch_delete", "rename", "move", "copy",
            ],
            description="移动云盘 (139 Cloud) 新版个人云 API 接入",
            author="LitePan",
        )

    async def init(self) -> None:
        await self._ensure_session()
        try:
            self._account = Cloud139ApiHelper.get_account(self.authorization)
        except Exception as e:
            self._log.warning(f"无法解析 Authorization 中的账号: {e}", driver_name="139_cloud")
            self._account = None

        if Cloud139ApiHelper.is_token_expired(self.authorization) and not self.is_connectivity_test():
            self._log.info("启动时检测到 Token 即将过期，主动刷新", driver_name="139_cloud")
            try:
                if await self._refresh_token_locked():
                    await self._notify_direct_refresh_success()
            except Exception as e:
                self._log.warning(f"启动期主动刷新失败，将依赖被动刷新链路: {e}", driver_name="139_cloud")

        self._log.debug("移动云盘驱动初始化完成", driver_name="139_cloud")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._log.debug("移动云盘驱动已关闭", driver_name="139_cloud")

    async def _ensure_session(self) -> None:
        if not self._session or self._session.closed:
            headers = Cloud139API.HEADERS.copy()
            self._session = aiohttp.ClientSession(
                headers=headers,
                cookie_jar=aiohttp.DummyCookieJar(),
            )

    async def _apply_operation_delay(self) -> None:
        await self.wait_for_request_interval()

    async def test_connection(self) -> OperationResult:
        try:
            await self._api_request("file_list", body={
                "parentFileId": self._api_parent_id(),
                "pageInfo": {"pageCursor": "", "pageSize": 1},
                "orderBy": "updated_at",
                "orderDirection": "DESC",
            })
            account = self._account or "移动云盘用户"
            return OperationResult(success=True, message=f"连接成功：{account}")
        except Exception as e:
            return OperationResult(success=False, message=f"连接测试失败: {str(e)}")

    def _api_parent_id(self, parent_id: Optional[str] = None) -> str:
        p = parent_id if parent_id is not None else self.config.root_folder_id
        if not p or str(p).strip() in ("/", "0", ""):
            return "/"
        return str(p).strip()

    def _is_auth_error(self, status: int, code: str = "", message: str = "") -> bool:
        if status in (401, 403):
            return True
        if code and code in Cloud139API.AUTH_ERROR_CODES:
            return True
        keyword = (message or "").lower()
        if "token" in keyword and ("expire" in keyword or "invalid" in keyword):
            return True
        if "未授权" in message or "认证失败" in message or "登录" in message:
            return True
        return False

    async def _get_personal_host(self) -> str:
        if self._personal_host:
            return self._personal_host

        body = {
            "userInfo": {
                "userType": 1,
                "accountType": 1,
                "accountName": self._account or "",
            },
            "modAddrType": 1,
        }
        body_str = json.dumps(body, ensure_ascii=False)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rand_str = Cloud139ApiHelper._gen_rand_str(16)
        sign = Cloud139ApiHelper.calc_sign(body_str, ts, rand_str)
        headers = Cloud139ApiHelper.build_route_headers(self.authorization, ts, rand_str, sign)

        await self._ensure_session()
        await self._apply_operation_delay()
        async with self._session.post(
            Cloud139API.ROUTE_POLICY_URL, headers=headers, data=body_str
        ) as response:
            if response.status != 200:
                text = await response.text()
                if self._is_auth_error(response.status):
                    raise Exception(f"HTTP {response.status} 认证失败: {text[:200]}")
                raise Exception(f"获取路由策略失败 ({response.status}): {text[:300]}")
            data = await response.json()

        if not data.get("success"):
            code = str(data.get("code", ""))
            msg = data.get("message", "未知错误")
            if self._is_auth_error(0, code, msg):
                raise Exception(f"路由策略认证失败 (code={code}): {msg}")
            raise Exception(f"路由策略请求失败: {msg}")

        for policy in data.get("data", {}).get("routePolicyList", []):
            if policy.get("modName") == "personal":
                host = policy.get("httpsUrl") or policy.get("httpUrl")
                if host:
                    self._personal_host = host
                    self._log.debug(f"获取个人云主机: {host}", driver_name="139_cloud")
                    return host

        raise Exception("未找到个人云主机地址")

    async def _signed_request(
        self,
        url: str,
        body: Dict[str, Any],
        svc_type: str = "1",
        method: str = "POST",
    ) -> Dict[str, Any]:
        body_str = json.dumps(body, ensure_ascii=False)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rand_str = Cloud139ApiHelper._gen_rand_str(16)
        sign = Cloud139ApiHelper.calc_sign(body_str, ts, rand_str)
        headers = Cloud139ApiHelper.build_signed_headers(
            self.authorization, ts, rand_str, sign, svc_type
        )

        await self._ensure_session()
        await self._apply_operation_delay()
        async with self._session.request(method, url, headers=headers, data=body_str) as response:
            text_body = None
            if response.status != 200:
                text_body = await response.text()
                if self._is_auth_error(response.status):
                    raise Exception(f"HTTP {response.status} 认证失败: {text_body[:200]}")
                raise Exception(f"API请求失败 ({response.status}): {text_body[:300]}")
            try:
                data = await response.json()
            except Exception:
                raise Exception("API 响应解析失败：非 JSON 内容")

        if isinstance(data, dict) and not data.get("success", True):
            code = str(data.get("code", ""))
            msg = data.get("message") or "API 返回错误"
            if self._is_auth_error(0, code, msg):
                raise Exception(f"139 API 认证失败 (code={code}): {msg}")
            raise Exception(msg)

        return data

    async def _api_request(
        self,
        operation: str,
        body: Optional[Dict[str, Any]] = None,
        svc_type: str = "1",
        method: str = "POST",
    ) -> Dict[str, Any]:
        if not self._session:
            await self._ensure_session()

        endpoint = Cloud139API.ENDPOINTS.get(operation)
        if not endpoint:
            raise Exception(f"未知操作: {operation}")
        host = await self._get_personal_host()
        url = f"{host}{endpoint}"

        body = dict(body or {})

        try:
            return await self._signed_request(url, body, svc_type=svc_type, method=method)
        except Exception as e:
            error_msg = str(e)
            looks_auth = (
                "认证失败" in error_msg
                or "401" in error_msg
                or "403" in error_msg
            )
            if looks_auth and not self.is_connectivity_test():
                self._log.warning(f"检测到认证错误，尝试被动刷新: {error_msg}", driver_name="139_cloud")
                refreshed = await self._handle_auth_error(error_msg)
                if refreshed:
                    self._log.info("✅ 被动刷新成功，重新尝试请求", driver_name="139_cloud")
                    return await self._signed_request(url, body, svc_type=svc_type, method=method)
            raise

    async def _refresh_token_locked(self) -> bool:
        async with self._refresh_lock:
            return await self._refresh_token()

    async def _refresh_token(self) -> bool:
        if not self.authorization:
            return False

        try:
            _, token_info, _ = Cloud139ApiHelper.parse_token(self.authorization)
        except Exception:
            self._log.warning("Authorization 格式异常，无法刷新", driver_name="139_cloud")
            return False

        xml_body = (
            "<root>"
            f"<token>{token_info}</token>"
            f"<account>{self._account or ''}</account>"
            "<clienttype>656</clienttype>"
            "</root>"
        )

        await self._ensure_session()
        async with self._session.post(
            Cloud139API.TOKEN_REFRESH_URL,
            data=xml_body,
            headers={
                "Content-Type": "application/xml;charset=UTF-8",
                "Referer": "https://yun.139.com/",
                "User-Agent": Cloud139API.USER_AGENT,
            },
        ) as response:
            if response.status != 200:
                self._log.error(f"Token 刷新失败，HTTP {response.status}", driver_name="139_cloud")
                return False
            text = await response.text()

        import xml.etree.ElementTree as ET
        try:
            root = ET.fromstring(text)
        except ET.ParseError as e:
            self._log.error(f"Token 刷新响应解析失败: {e}", driver_name="139_cloud")
            return False

        return_code = root.findtext("return", default="")
        if return_code != "0":
            desc = root.findtext("desc", default="")
            self._log.error(f"Token 刷新失败，return={return_code} desc={desc}", driver_name="139_cloud")
            return False

        new_token = (
            root.findtext("token", default="")
            or root.findtext("accessToken", default="")
        )
        if not new_token:
            self._log.error("Token 刷新成功但响应未返回新 token", driver_name="139_cloud")
            return False

        expire_seconds_str = root.findtext("expiretime", default="").strip()
        if expire_seconds_str.isdigit():
            try:
                new_expires = int(expire_seconds_str)
                if 600 <= new_expires <= 365 * 86400:
                    self.config.token_expires_seconds = new_expires
            except Exception:
                pass

        new_auth = base64.b64encode(
            f"pc:{self._account or ''}:{new_token}".encode()
        ).decode()
        self.authorization = new_auth
        self.config.authorization = new_auth

        await self._persist_tokens()
        self._log.info(
            f"✅ 移动云盘 Token 刷新成功（新 token 有效期 {expire_seconds_str or '未知'} 秒）",
            driver_name="139_cloud",
        )
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

            current_config = dict(account["config"]) if isinstance(account["config"], dict) else {}
            current_config["authorization"] = self.authorization
            await db.update_account(account_id, config=current_config)
        except Exception as e:
            self._log.warning(f"139 云盘 Token 持久化失败: {e}", driver_name="139_cloud")

    async def _notify_direct_refresh_success(self) -> None:
        account_id = getattr(self, "_account_id", None) or getattr(self, "account_id", None)
        if not account_id or str(account_id) == "temp_test":
            return
        try:
            from core.auth_manager import sync_driver_refresh_success
            await sync_driver_refresh_success(int(account_id), self)
        except Exception as e:
            self._log.warning(f"刷新成功后同步认证状态失败: {e}", driver_name="139_cloud")

    async def refresh_auth(self):
        from core.auth_manager import RefreshOutcome
        try:
            success = await self._refresh_token_locked()
            if success:
                self._log.info("✅ 移动云盘认证刷新成功", driver_name="139_cloud")
                return RefreshOutcome.SUCCESS

            if Cloud139ApiHelper.is_token_expired(self.authorization):
                self._log.error(
                    "❌ 移动云盘 Token 已过期或即将过期且无法续期，需要重新抓取 Authorization",
                    driver_name="139_cloud",
                )
                return RefreshOutcome.FATAL
            self._log.warning("⚠️ 移动云盘刷新失败（暂时性），稍后重试", driver_name="139_cloud")
            return RefreshOutcome.RETRYABLE
        except Exception as e:
            self._log.error(f"❌ 移动云盘认证刷新异常: {e}", driver_name="139_cloud")
            return RefreshOutcome.RETRYABLE

    def set_auth_manager(self, auth_manager):
        self._auth_manager = auth_manager

    def _sync_auth_from_config(self, source_config) -> None:
        if not source_config:
            return
        new_auth = getattr(source_config, "authorization", None)
        if new_auth and new_auth != self.authorization:
            self.authorization = new_auth
            self.config.authorization = new_auth
            try:
                self._account = Cloud139ApiHelper.get_account(new_auth)
            except Exception:
                pass

    async def _handle_auth_error(self, error_reason: str):
        try:
            if self.is_connectivity_test():
                return False
            self._log.warning(f"触发被动刷新: {error_reason}", driver_name="139_cloud")

            if hasattr(self, "_account_id"):
                try:
                    from core.auth_manager import auth_scheduler, handle_auth_error
                    normalized_account_id = int(self._account_id)
                    if normalized_account_id in auth_scheduler.auth_managers:
                        success = await handle_auth_error(normalized_account_id)
                        if success:
                            auth_manager = auth_scheduler.auth_managers.get(normalized_account_id)
                            self._sync_auth_from_config(getattr(auth_manager, "config", None))
                            self._log.info("✅ 被动刷新成功 (通过认证管理器)", driver_name="139_cloud")
                            return True
                        self._log.warning("⚠️ 认证管理器刷新失败，本次不再追加直接刷新", driver_name="139_cloud")
                        return False
                except Exception as e:
                    self._log.warning(f"⚠️ 认证管理器刷新异常，尝试直接刷新: {e}", driver_name="139_cloud")

            from core.auth_manager import RefreshOutcome
            outcome = await self.refresh_auth()
            if outcome == RefreshOutcome.SUCCESS:
                await self._notify_direct_refresh_success()
                self._log.info("✅ 被动刷新成功（通过直接调用）", driver_name="139_cloud")
                return True
            self._log.error("❌ 被动刷新失败", driver_name="139_cloud")
            return False

        except Exception as e:
            self._log.error(f"❌ 被动刷新异常: {e}", driver_name="139_cloud")
            return False

    @with_file_list_cache
    async def list_files(self, parent_id: str = "/") -> List[FileItem]:
        parent_file_id = self._api_parent_id(parent_id)

        all_files: List[FileItem] = []
        next_cursor = ""
        while True:
            response = await self._api_request("file_list", body={
                "imageThumbnailStyleList": ["Small", "Large"],
                "orderBy": "updated_at",
                "orderDirection": "DESC",
                "pageInfo": {"pageCursor": next_cursor, "pageSize": 100},
                "parentFileId": parent_file_id,
            })

            data = response.get("data") or {}
            for item_data in data.get("items", []) or []:
                file_obj = Cloud139File.from_dict(item_data)
                all_files.append(file_obj.to_file_item())

            next_cursor = (data.get("nextPageCursor") or "").strip()
            if not next_cursor:
                break

        return all_files

    @with_file_info_cache
    async def file_info(self, file_id: str) -> Optional[FileItem]:
        if not file_id or file_id in ("/", "0"):
            return FileItem(
                id="/",
                name="根目录",
                path="/",
                size=0,
                is_dir=True,
                modified=None,
                created=None,
                download_url=None,
                thumbnail_url=None,
                mime_type=None,
                extra={"is_root": True},
            )

        target = str(file_id)
        queue: deque[str] = deque([self._api_parent_id()])
        visited: set[str] = set()
        while queue:
            if len(visited) >= self._FILE_INFO_MAX_DIRS:
                self._log.warning(
                    f"file_info 扫描目录数已达上限 {self._FILE_INFO_MAX_DIRS}，仍未找到 file_id={target}",
                    driver_name="139_cloud",
                )
                break
            parent = queue.popleft()
            if parent in visited:
                continue
            visited.add(parent)
            try:
                items = await self.list_files(parent)
            except Exception as e:
                self._log.warning(
                    f"file_info 列出目录失败 parent={parent} file_id={target}: {e}",
                    driver_name="139_cloud",
                )
                continue
            for f in items:
                if str(f.id) == target:
                    return f
                if f.is_dir:
                    queue.append(str(f.id))

        return FileItem(
            id=file_id,
            name=f"file_{file_id}",
            path="",
            size=0,
            is_dir=False,
            modified=datetime.now(timezone.utc),
            created=datetime.now(timezone.utc),
            download_url=None,
            thumbnail_url=None,
            mime_type=None,
        )

    @auto_cleanup_cache("create_folder")
    async def create_folder(self, parent_id: str, name: str) -> OperationResult:
        try:
            response = await self._api_request("create_folder", body={
                "parentFileId": self._api_parent_id(parent_id),
                "name": (name or "").strip(),
                "description": "",
                "type": "folder",
                "fileRenameMode": "force_rename",
            })
            folder_data = response.get("data", {}) or {}
            folder_id = folder_data.get("fileId", "")
            return OperationResult(
                success=True,
                message=f"文件夹 '{name}' 创建成功",
                data={"folder_id": folder_id, "parent_id": parent_id, "folder_name": name},
            )
        except Exception as e:
            return OperationResult(success=False, message=f"创建文件夹失败: {str(e)}")

    async def _delete_files_batch(self, file_ids: List[str]) -> OperationResult:
        try:
            body = {"fileIds": list(file_ids)}
            if self.config.delete_mode == "delete":
                operation = "permanent_delete"
                msg = f"已永久删除 {len(file_ids)} 个文件"
            else:
                operation = "delete_file"
                msg = f"已删除 {len(file_ids)} 个文件"
            await self._api_request(operation, body=body)
            return OperationResult(success=True, message=msg, data={"file_ids": list(file_ids)})
        except Exception as e:
            return OperationResult(success=False, message=f"删除失败: {str(e)}")

    @auto_cleanup_cache("delete_file")
    async def delete_file(self, file_id: str) -> OperationResult:
        return await self._delete_files_batch([file_id])

    @auto_cleanup_cache("batch_delete_file")
    async def batch_delete_file(self, file_ids: List[str]) -> OperationResult:
        if not file_ids:
            return OperationResult(success=True, message="没有文件需要删除")
        return await self._delete_files_batch(file_ids)

    @auto_cleanup_cache("rename_file")
    async def rename_file(self, file_id: str, new_name: str) -> OperationResult:
        try:
            await self._api_request("rename_file", body={
                "fileId": file_id,
                "name": (new_name or "").strip(),
                "description": "",
            })
            return OperationResult(
                success=True,
                message=f"重命名为 '{new_name}' 成功",
                data={"file_id": file_id, "new_name": new_name},
            )
        except Exception as e:
            return OperationResult(success=False, message=f"重命名失败: {str(e)}")

    @auto_cleanup_cache("move_file")
    async def move_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        try:
            if not file_ids:
                return OperationResult(success=False, message="没有指定要移动的文件")
            await self._api_request("move_file", body={
                "fileIds": list(file_ids),
                "toParentFileId": self._api_parent_id(target_parent_id),
            })
            return OperationResult(
                success=True,
                message=f"已移动 {len(file_ids)} 个文件",
                data={
                    "moved_count": len(file_ids),
                    "file_ids": list(file_ids),
                    "target_parent_id": target_parent_id,
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"移动失败: {str(e)}")

    @auto_cleanup_cache("copy_file")
    async def copy_file(self, file_ids: List[str], target_parent_id: str, source_parent_id: str = None) -> OperationResult:
        try:
            if not file_ids:
                return OperationResult(success=False, message="没有指定要复制的文件")
            await self._api_request("copy_file", body={
                "fileIds": list(file_ids),
                "toParentFileId": self._api_parent_id(target_parent_id),
            })
            return OperationResult(
                success=True,
                message=f"已复制 {len(file_ids)} 个文件",
                data={
                    "copied_count": len(file_ids),
                    "file_ids": list(file_ids),
                    "target_parent_id": target_parent_id,
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"复制失败: {str(e)}")

    async def get_download_info(self, file_id: str, user_agent: str = None) -> dict:
        response = await self._api_request("get_download_url", body={"fileId": file_id})
        data = response.get("data", {}) or {}
        url = data.get("cdnUrl") or data.get("url") or ""
        if not url:
            raise Exception("获取下载链接失败")
        return {
            "download_url": url,
            "file_name": data.get("fileName") or None,
            "size": int(data.get("size") or 0),
        }

    async def get_download_url(self, file_id: str, user_agent: str = None) -> str:
        info = await self.get_download_info(file_id, user_agent)
        return info["download_url"]

    async def get_download_headers(self, file_id: str, user_agent: str = None) -> Dict[str, str]:
        return {
            "User-Agent": user_agent or Cloud139API.USER_AGENT,
            "Referer": "https://yun.139.com/",
            "Origin": "https://yun.139.com",
        }

    # ── 上传相关 ───────────────────────────────────────────────

    @staticmethod
    def _calculate_file_sha256(local_path: str) -> str:
        hasher = hashlib.sha256()
        with open(local_path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()

    @staticmethod
    def _guess_mime_type(file_name: str) -> str:
        ext = os.path.splitext(file_name or "")[1].lower()
        mime_map = {
            ".pdf": "application/pdf",
            ".doc": "application/msword",
            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".xls": "application/vnd.ms-excel",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
            ".png": "image/png", ".gif": "image/gif", ".webp": "image/webp",
            ".mp4": "video/mp4", ".mkv": "video/x-matroska",
            ".mp3": "audio/mpeg", ".flac": "audio/flac",
            ".zip": "application/zip", ".rar": "application/x-rar-compressed",
            ".7z": "application/x-7z-compressed", ".tar": "application/x-tar",
            ".txt": "text/plain", ".json": "application/json",
        }
        return mime_map.get(ext, "application/octet-stream")

    async def _save_upload_to_tempfile(self, upload_file: UploadFile) -> str:
        suffix = os.path.splitext(upload_file.filename or "")[1]
        fd, temp_path = tempfile.mkstemp(prefix="litepan_139_", suffix=suffix)
        os.close(fd)
        try:
            with open(temp_path, "wb") as f:
                while True:
                    chunk = await upload_file.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
            return temp_path
        except Exception:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    @staticmethod
    async def _notify_progress(
        callback: Optional[Callable[[int, int, str], Awaitable[None]]],
        uploaded: int,
        total: int,
        status: str,
    ) -> None:
        if callback:
            try:
                await callback(uploaded, total, status)
            except Exception:
                pass

    async def _put_file_part(
        self,
        local_path: str,
        offset: int,
        part_size: int,
        upload_url: str,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        base_uploaded: int = 0,
        total: int = 0,
        part_label: str = "",
    ) -> None:
        await self._ensure_session()
        stream_chunk = 1024 * 1024
        uploaded = 0
        with open(local_path, "rb") as f:
            f.seek(offset)

            async def body_stream():
                nonlocal uploaded
                while uploaded < part_size:
                    chunk = f.read(min(stream_chunk, part_size - uploaded))
                    if not chunk:
                        break
                    uploaded += len(chunk)
                    await self._notify_progress(
                        progress_callback, base_uploaded + uploaded, total,
                        f"正在上传 {part_label}",
                    )
                    yield chunk

            headers = {
                "Content-Type": "application/octet-stream",
                "Content-Length": str(part_size),
            }
            async with self._session.put(
                upload_url,
                data=body_stream(),
                headers=headers,
            ) as response:
                if response.status not in (200, 201, 204):
                    text = await response.text()
                    raise Exception(f"上传分片失败 (HTTP {response.status}): {text[:300]}")

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
                os.remove(temp_path)
            await upload_file.close()

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

        file_size = os.path.getsize(local_path)
        if file_size == 0:
            return OperationResult(success=False, message="暂不支持上传空文件")

        try:
            await self._notify_progress(progress_callback, 0, file_size, "正在计算文件哈希")
            content_hash = await asyncio.to_thread(self._calculate_file_sha256, local_path)
            mime_type = self._guess_mime_type(target_name)

            chunk_size = 512 * 1024 * 1024 if file_size > 30 * 1024 * 1024 * 1024 else 100 * 1024 * 1024
            request_parts = []
            offset = 0
            part_num = 1
            while offset < file_size:
                ps = min(chunk_size, file_size - offset)
                request_parts.append({
                    "parallelHashCtx": {"partOffset": offset},
                    "partNumber": part_num,
                    "partSize": ps,
                })
                offset += ps
                part_num += 1

            await self._notify_progress(progress_callback, 0, file_size, "正在发起上传")
            create_body = {
                "contentHash": content_hash,
                "contentHashAlgorithm": "SHA256",
                "contentType": mime_type,
                "fileRenameMode": "auto_rename",
                "name": target_name,
                "parentFileId": self._api_parent_id(parent_path),
                "partInfos": request_parts,
                "size": file_size,
                "type": "file",
            }
            create_resp = await self._api_request("upload_create", body=create_body)
            create_data = create_resp.get("data", {}) or {}

            if create_data.get("rapidUpload"):
                return OperationResult(
                    success=True,
                    message=f"文件 '{target_name}' 秒传成功",
                    data={"file_id": create_data.get("fileId"), "file_name": target_name, "rapid_upload": True},
                )

            file_id = create_data.get("fileId")
            upload_id = create_data.get("uploadId")
            part_infos = create_data.get("partInfos", []) or []

            if not file_id or not upload_id or not part_infos:
                return OperationResult(success=False, message="服务器未返回上传地址")

            uploaded = 0
            total_parts = len(part_infos)
            for i, part in enumerate(part_infos):
                upload_url = part.get("uploadUrl")
                if not upload_url:
                    return OperationResult(success=False, message=f"第 {part.get('partNumber')} 分片缺少上传地址")
                part_size = part.get("partSize") or request_parts[i]["partSize"]

                await self._put_file_part(
                    local_path, request_parts[i]["parallelHashCtx"]["partOffset"],
                    part_size, upload_url,
                    progress_callback=progress_callback,
                    base_uploaded=uploaded,
                    total=file_size,
                    part_label=f"({i + 1}/{total_parts})",
                )
                uploaded += part_size

            await self._notify_progress(progress_callback, file_size, file_size, "正在完成上传")
            complete_body = {
                "contentHash": content_hash,
                "contentHashAlgorithm": "SHA256",
                "fileId": file_id,
                "uploadId": upload_id,
            }
            await self._api_request("upload_complete", body=complete_body)

            await self._notify_progress(progress_callback, file_size, file_size, "上传成功")
            return OperationResult(
                success=True,
                message=f"文件 '{target_name}' 上传成功",
                data={"file_id": file_id, "file_name": target_name, "size": file_size},
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
            local_path, file_name, parent_path,
            progress_callback=progress_callback,
            conflict_policy=conflict_policy,
            resume_state=resume_state,
            state_callback=state_callback,
        )

    def __str__(self) -> str:
        masked = f"{self.authorization[:12]}..." if self.authorization else "empty"
        return f"<{self.__class__.__name__} auth={masked}>"
