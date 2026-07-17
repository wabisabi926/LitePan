"""百度网盘 Open 驱动：走官方开放 API，token 由 OAuth 中转服务自动续期。"""

import asyncio
import hashlib
import json
import os
import tempfile
import uuid
from posixpath import dirname
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aiohttp
from fastapi import UploadFile

from config import get_oauth_server_url
from core.base import DriverInfo, FileItem, OperationResult
from core.driver_base import BaseDriver
from core.operation_wrapper import auto_cleanup_cache, with_file_info_cache, with_file_list_cache

from .api import BaiduOpenAPI, BaiduOpenApiHelper
from .config import BaiduOpenConfig
from .models import BaiduOpenFile, normalize_content_md5


class BaiduOpenDriver(BaseDriver):
    def __init__(self, config: BaiduOpenConfig):
        super().__init__(config)
        self.access_token = config.access_token
        self.refresh_token = config.refresh_token
        self._session: Optional[aiohttp.ClientSession] = None
        self._transfer_md5_cache: Dict[str, str] = {}
        self._oauth_server_url = get_oauth_server_url()
        self._refresh_lock = asyncio.Lock()

    def supports_parallel_range_download(self) -> bool:
        return False

    @classmethod
    def get_info(cls) -> DriverInfo:
        return DriverInfo(
            name="baidu_open",
            display_name="百度网盘Open",
            version="3.1.0",
            capabilities=["list", "info", "download", "create_folder", "delete", "batch_delete", "rename", "move", "copy", "upload"],
            description="百度网盘官方开放 API 接入",
            author="LitePan",
        )

    async def init(self) -> None:
        await self._ensure_session()
        if not self.access_token and self.refresh_token:
            await self._refresh_access_token_locked(force=False)
        self._log.debug("百度网盘Open驱动初始化完成", driver_name="baidu_open")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._log.debug("百度网盘Open驱动已关闭", driver_name="baidu_open")

    async def _ensure_session(self) -> None:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers=BaiduOpenApiHelper.build_headers(),
                cookie_jar=aiohttp.DummyCookieJar(),
            )

    async def _apply_operation_delay(self) -> None:
        await self.wait_for_request_interval()

    async def test_connection(self) -> OperationResult:
        try:
            if not self.access_token:
                await self._refresh_access_token_locked(force=False)

            response = await self._api_request("user_info", "GET")
            username = response.get("netdisk_name") or response.get("baidu_name") or "百度网盘用户"
            vip_type = response.get("vip_type", 0)
            vip_label = {0: "普通用户", 1: "VIP会员", 2: "SVIP超级会员"}.get(vip_type, f"VIP类型{vip_type}")
            return OperationResult(success=True, message=f"连接成功：{username}（{vip_label}）")
        except Exception as e:
            return OperationResult(success=False, message=f"连接测试失败: {str(e)}")

    async def _api_request(
        self,
        operation: str,
        method: str,
        params: Dict[str, Any] = None,
        data: Dict[str, Any] = None,
        _retried: bool = False,
    ) -> Dict[str, Any]:
        await self._ensure_session()
        if not self.access_token:
            await self._refresh_access_token_locked(force=False)

        token_used = self.access_token
        url = BaiduOpenAPI.BASE_URL + BaiduOpenAPI.ENDPOINTS[operation]
        request_params = BaiduOpenApiHelper.build_params(operation, token_used, params)

        kwargs: Dict[str, Any] = {"params": request_params}
        if data is not None:
            kwargs["data"] = data

        await self._apply_operation_delay()
        async with self._session.request(method, url, **kwargs) as response:
            if response.status != 200:
                error_text = await response.text()
                raise Exception(f"百度网盘API HTTP错误 ({response.status}): {error_text[:500]}")

            resp_data = await response.json()
            success, error_msg, errno_value = BaiduOpenApiHelper.check_success(resp_data)
            if not success:
                if not _retried and BaiduOpenApiHelper.is_token_expired(errno_value, error_msg):
                    self._log.info("百度网盘访问令牌失效，尝试自动刷新", driver_name="baidu_open")
                    await self._refresh_access_token_locked(force=False, expected_access_token=token_used)
                    return await self._api_request(operation, method, params=params, data=data, _retried=True)
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
                json={"driver_type": "百度网盘Open", "refresh_token": self.refresh_token},
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
        self._log.debug("百度网盘访问令牌刷新成功", driver_name="baidu_open")
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
            self._log.warning(f"百度网盘 Token 持久化失败: {e}", driver_name="baidu_open")

    async def _notify_direct_refresh_success(self) -> None:
        account_id = getattr(self, "_account_id", None) or getattr(self, "account_id", None)
        if not account_id or str(account_id) == "temp_test":
            return
        try:
            from core.auth_manager import sync_driver_refresh_success

            await sync_driver_refresh_success(int(account_id), self)
        except Exception as e:
            self._log.warning(f"百度网盘刷新成功后同步认证状态失败: {e}", driver_name="baidu_open")

    async def refresh_auth(self) -> bool:
        try:
            await self._refresh_access_token_locked(force=True, notify_success=False)
            self._log.info("✅ 百度网盘认证刷新成功", driver_name="baidu_open")
            return True
        except Exception as e:
            self._log.error(f"百度网盘认证刷新失败: {e}", driver_name="baidu_open")
            return False

    def _get_root_path(self) -> str:
        root_folder_id = str(self.config.root_folder_id or "/").strip() or "/"
        if root_folder_id in ("0", "/"):
            return "/"
        if not root_folder_id.startswith("/"):
            root_folder_id = f"/{root_folder_id}"
        return root_folder_id.rstrip("/") or "/"

    def _normalize_path(self, path: str) -> str:
        if path in (None, "", "0", "/"):
            return self._get_root_path()
        normalized = str(path)
        if not normalized.startswith("/"):
            normalized = f"/{normalized}"
        return normalized.rstrip("/") or "/"

    def _get_parent_path(self, path: str) -> str:
        normalized = self._normalize_path(path)
        root_path = self._get_root_path()
        if normalized == root_path:
            return root_path
        parent = dirname(normalized) or "/"
        return parent.rstrip("/") or "/"

    def _build_child_path(self, parent_path: str, name: str) -> str:
        normalized_parent = self._normalize_path(parent_path)
        if normalized_parent == "/":
            return f"/{name}"
        return f"{normalized_parent}/{name}"

    def _build_request_headers(self, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers = BaiduOpenApiHelper.build_headers()
        if extra_headers:
            headers.update(extra_headers)
        return headers

    @with_file_list_cache
    async def list_files(self, parent_id: str = "/") -> List[FileItem]:
        dir_path = self._normalize_path(parent_id)
        all_files: List[FileItem] = []
        start = 0
        limit = 1000

        while True:
            try:
                response = await self._api_request(
                    "file_list",
                    "GET",
                    params={
                        "dir": dir_path,
                        "folder": "0",
                        "start": str(start),
                        "limit": str(limit),
                        "order": "time",
                        "desc": "1",
                        "web": "1",
                        "showempty": "1",
                    },
                )
            except Exception as e:
                error_message = str(e)
                if "-7" in error_message and dir_path != self._get_root_path():
                    raise Exception(
                        f"{error_message}。当前目录 {dir_path} 可能不在应用授权范围内，"
                        "建议将根目录设置为应用目录，例如 /apps/应用名。"
                    )
                raise

            files_data = response.get("list", []) or []
            for file_data in files_data:
                item = BaiduOpenFile.from_list_api(
                    file_data,
                    fallback_path=self._build_child_path(dir_path, str(file_data.get("server_filename", ""))),
                ).to_file_item()
                all_files.append(item)

            if len(files_data) < limit:
                break
            start += len(files_data)

        return all_files

    @with_file_info_cache
    async def file_info(self, file_id: str) -> Optional[FileItem]:
        normalized_path = self._normalize_path(file_id)
        root_path = self._get_root_path()

        if normalized_path == root_path:
            return FileItem(
                id="/",
                name=root_path.rsplit("/", 1)[-1] or "根目录",
                path=root_path,
                is_dir=True,
                extra={"is_root": True},
            )

        if file_id.isdigit():
            try:
                result = await self._get_file_metas([file_id])
                if result:
                    return result[0]
            except Exception:
                pass

        parent_path = self._get_parent_path(normalized_path)
        # 不再强行把 root_path 转成 "0"
        parent_id = "/" if parent_path == "/" else parent_path
        for item in await self.list_files(parent_id):
            if self._normalize_path(item.id) == normalized_path:
                return item
        return None

    @auto_cleanup_cache("create_folder")
    async def create_folder(self, parent_id: str, name: str) -> OperationResult:
        folder_name = (name or "").strip()
        if not folder_name:
            return OperationResult(success=False, message="文件夹名称不能为空")

        parent_path = self._normalize_path(parent_id)
        target_path = self._build_child_path(parent_path, folder_name)

        try:
            response = await self._api_request(
                "file_create",
                "POST",
                data={
                    "path": target_path,
                    "isdir": "1",
                    "rtype": "0",
                },
            )
            folder_path = str(response.get("path") or target_path)
            return OperationResult(
                success=True,
                message=f"文件夹 '{folder_name}' 创建成功",
                data={
                    "folder_id": folder_path,
                    "folder_path": folder_path,
                    "fs_id": str(response.get("fs_id") or ""),
                    "parent_path": parent_path,
                    "folder_name": folder_name,
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"创建文件夹失败: {str(e)}")

    @auto_cleanup_cache("rename_file")
    async def rename_file(self, file_id: str, new_name: str) -> OperationResult:
        normalized_path = self._normalize_path(file_id)
        target_name = (new_name or "").strip()

        if not normalized_path:
            return OperationResult(success=False, message="文件ID不能为空")
        if normalized_path == "/":
            return OperationResult(success=False, message="根目录不支持重命名")
        if not target_name:
            return OperationResult(success=False, message="新名称不能为空")

        parent_path = self._get_parent_path(normalized_path)
        cache_parent_id = parent_path
        old_name = normalized_path.rsplit("/", 1)[-1]

        try:
            await self._api_request(
                "file_manager",
                "POST",
                params={"opera": "rename"},
                data={
                    "async": "0",
                    "ondup": "fail",
                    "filelist": json.dumps([{"path": normalized_path, "newname": target_name}], ensure_ascii=False),
                },
            )
            return OperationResult(
                success=True,
                message=f"文件重命名为 '{target_name}' 成功",
                data={
                    "file_id": normalized_path,
                    "parent_id": cache_parent_id,
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

    async def _delete_files(self, file_ids: List[str]) -> OperationResult:
        normalized_paths = []
        parent_ids = set()

        for file_id in file_ids:
            normalized_path = self._normalize_path(file_id)
            if normalized_path == "/":
                return OperationResult(success=False, message="根目录不支持删除")
            normalized_paths.append(normalized_path)

            parent_path = self._get_parent_path(normalized_path)
            parent_ids.add(parent_path)

        try:
            await self._api_request(
                "file_manager",
                "POST",
                params={"opera": "delete"},
                data={
                    "async": "0",
                    "filelist": json.dumps(normalized_paths, ensure_ascii=False),
                },
            )
            count = len(normalized_paths)
            message = f"已删除 {count} 个文件" if count > 1 else "删除成功"
            return OperationResult(
                success=True,
                message=message,
                data={
                    "deleted_count": count,
                    "file_ids": normalized_paths,
                    "parent_ids": list(parent_ids),
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"删除失败: {str(e)}")

    @auto_cleanup_cache("move_file")
    async def move_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        if not file_ids:
            return OperationResult(success=True, message="没有文件需要移动")

        target_parent_path = self._normalize_path(target_parent_id)
        # 不要强行转换为"0"，因为前端可能使用的是真实的挂载目录路径
        cache_target_parent_id = target_parent_id

        filelist = []
        source_parent_ids = set()

        for file_id in file_ids:
            normalized_path = self._normalize_path(file_id)
            if normalized_path == "/":
                return OperationResult(success=False, message="根目录不支持移动")

            parent_path = self._get_parent_path(normalized_path)
            source_parent_ids.add(parent_path)
            filelist.append(
                {
                    "path": normalized_path,
                    "dest": target_parent_path,
                    "newname": normalized_path.rsplit("/", 1)[-1],
                }
            )

        try:
            await self._api_request(
                "file_manager",
                "POST",
                params={"opera": "move"},
                data={
                    "async": "0",
                    "ondup": "newcopy",
                    "filelist": json.dumps(filelist, ensure_ascii=False),
                },
            )
            moved_count = len(filelist)
            return OperationResult(
                success=True,
                message=f"已移动 {moved_count} 个文件到目标目录",
                data={
                    "moved_count": moved_count,
                    "file_ids": [item["path"] for item in filelist],
                    "target_parent_id": cache_target_parent_id,
                    "source_parent_ids": list(source_parent_ids),
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"移动失败: {str(e)}")

    @auto_cleanup_cache("copy_file")
    async def copy_file(self, file_ids: List[str], target_parent_id: str, source_parent_id: str = None) -> OperationResult:
        if not file_ids:
            return OperationResult(success=True, message="没有文件需要复制")

        target_parent_path = self._normalize_path(target_parent_id)
        cache_target_parent_id = target_parent_id

        filelist = []
        source_parent_ids = set()

        for file_id in file_ids:
            normalized_path = self._normalize_path(file_id)
            if normalized_path == "/":
                return OperationResult(success=False, message="根目录不支持复制")

            parent_path = self._get_parent_path(normalized_path)
            if parent_path == target_parent_path:
                return OperationResult(success=False, message="百度网盘不支持复制到同一目录", data={"warning": True})
            source_parent_ids.add(parent_path)
            filelist.append(
                {
                    "path": normalized_path,
                    "dest": target_parent_path,
                    "newname": normalized_path.rsplit("/", 1)[-1],
                }
            )

        try:
            await self._api_request(
                "file_manager",
                "POST",
                params={"opera": "copy"},
                data={
                    "async": "0",
                    "ondup": "newcopy",
                    "filelist": json.dumps(filelist, ensure_ascii=False),
                },
            )
            copied_count = len(filelist)
            return OperationResult(
                success=True,
                message=f"已复制 {copied_count} 个文件到目标目录",
                data={
                    "copied_count": copied_count,
                    "file_ids": [item["path"] for item in filelist],
                    "target_parent_id": cache_target_parent_id,
                    "source_parent_ids": list(source_parent_ids),
                },
            )
        except Exception as e:
            return OperationResult(success=False, message=f"复制失败: {str(e)}")

    @auto_cleanup_cache("upload_file")
    async def upload_file(
        self,
        upload_file: UploadFile,
        parent_path: str = "0",
        conflict_policy: str = "overwrite",
    ) -> OperationResult:
        target_name = os.path.basename((upload_file.filename or "").strip())
        if not target_name:
            return OperationResult(success=False, message="上传文件名不能为空")

        if getattr(upload_file, "size", None) == 0:
            return OperationResult(success=False, message="暂不支持上传空文件")

        normalized_parent_path = self._normalize_path(parent_path)
        cache_parent_id = normalized_parent_path
        target_path = self._build_child_path(normalized_parent_path, target_name)
        temp_path = ""

        try:
            temp_path = await self._save_upload_to_tempfile(upload_file)
            upload_meta = await self._prepare_upload_metadata(temp_path)
            precreate_data = await self._precreate_upload(target_path, upload_meta, conflict_policy=conflict_policy)
            if precreate_data.get("rapid_upload"):
                return self._build_upload_success_result(
                    create_response=precreate_data.get("file_info", {}),
                    target_path=target_path,
                    target_name=target_name,
                    normalized_parent_path=normalized_parent_path,
                    cache_parent_id=cache_parent_id,
                    file_size=upload_meta["size"],
                    message=f"文件 '{target_name}' 秒传成功",
                )

            upload_host = await self._locate_upload_host(target_path, precreate_data["uploadid"])
            await self._upload_chunks(upload_host, target_path, precreate_data["uploadid"], temp_path, upload_meta)
            create_response = await self._create_uploaded_file(
                target_path,
                precreate_data,
                upload_meta,
                conflict_policy=conflict_policy,
            )
            return self._build_upload_success_result(
                create_response=create_response,
                target_path=target_path,
                target_name=target_name,
                normalized_parent_path=normalized_parent_path,
                cache_parent_id=cache_parent_id,
                file_size=upload_meta["size"],
            )
        except Exception as e:
            return OperationResult(success=False, message=f"上传文件失败: {str(e)}")
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
        if not local_path or not os.path.exists(local_path):
            return OperationResult(success=False, message="待上传文件不存在")

        normalized_parent_path = self._normalize_path(parent_path)
        cache_parent_id = normalized_parent_path
        target_path = self._build_child_path(normalized_parent_path, target_name)

        try:
            upload_meta = await self._prepare_upload_metadata(local_path)
            await self._notify_upload_progress(progress_callback, 0, upload_meta["size"], "正在预上传")
            precreate_data = await self._precreate_upload(target_path, upload_meta, conflict_policy=conflict_policy)
            if precreate_data.get("rapid_upload"):
                await self._notify_upload_progress(progress_callback, upload_meta["size"], upload_meta["size"], "秒传成功")
                return self._build_upload_success_result(
                    create_response=precreate_data.get("file_info", {}),
                    target_path=target_path,
                    target_name=target_name,
                    normalized_parent_path=normalized_parent_path,
                    cache_parent_id=cache_parent_id,
                    file_size=upload_meta["size"],
                    message=f"文件 '{target_name}' 秒传成功",
                )

            upload_host = await self._locate_upload_host(target_path, precreate_data["uploadid"])
            await self._upload_chunks(
                upload_host,
                target_path,
                precreate_data["uploadid"],
                local_path,
                upload_meta,
                progress_callback=progress_callback,
            )
            await self._notify_upload_progress(progress_callback, upload_meta["size"], upload_meta["size"], "正在写入网盘")
            create_response = await self._create_uploaded_file(
                target_path,
                precreate_data,
                upload_meta,
                conflict_policy=conflict_policy,
            )
            await self._notify_upload_progress(progress_callback, upload_meta["size"], upload_meta["size"], "上传成功")
            return self._build_upload_success_result(
                create_response=create_response,
                target_path=target_path,
                target_name=target_name,
                normalized_parent_path=normalized_parent_path,
                cache_parent_id=cache_parent_id,
                file_size=upload_meta["size"],
            )
        except Exception as e:
            return OperationResult(success=False, message=f"上传文件失败: {str(e)}")

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
        target_name = os.path.basename((file_name or "").strip())
        if not target_name:
            return OperationResult(success=False, message="上传文件名不能为空")
        if not local_path or not os.path.exists(local_path):
            return OperationResult(success=False, message="待上传文件不存在")

        normalized_parent_path = self._normalize_path(parent_path)
        cache_parent_id = normalized_parent_path
        target_path = self._build_child_path(normalized_parent_path, target_name)

        try:
            upload_meta = await self._prepare_upload_metadata(local_path)
            normalized_resume_state = self._normalize_upload_resume_state(
                resume_state,
                target_path=target_path,
                upload_meta=upload_meta,
            )

            if normalized_resume_state:
                await self._notify_upload_progress(
                    progress_callback,
                    normalized_resume_state["uploaded_bytes"],
                    upload_meta["size"],
                    "正在继续上传到百度网盘",
                )
                precreate_data = {
                    "rapid_upload": False,
                    "return_type": 0,
                    "uploadid": normalized_resume_state["uploadid"],
                    "block_list": upload_meta["block_list"],
                }
                upload_host = normalized_resume_state["upload_host"]
                completed_parts = normalized_resume_state["completed_parts"]
            else:
                await self._notify_upload_progress(progress_callback, 0, upload_meta["size"], "正在预上传")
                precreate_data = await self._precreate_upload(target_path, upload_meta, conflict_policy=conflict_policy)
                if precreate_data.get("rapid_upload"):
                    await self._notify_upload_progress(progress_callback, upload_meta["size"], upload_meta["size"], "秒传成功")
                    return self._build_upload_success_result(
                        create_response=precreate_data.get("file_info", {}),
                        target_path=target_path,
                        target_name=target_name,
                        normalized_parent_path=normalized_parent_path,
                        cache_parent_id=cache_parent_id,
                        file_size=upload_meta["size"],
                        message=f"文件 '{target_name}' 秒传成功",
                    )

                upload_host = await self._locate_upload_host(target_path, precreate_data["uploadid"])
                completed_parts = []

            await self._upload_chunks_with_resume(
                upload_host,
                target_path,
                precreate_data["uploadid"],
                local_path,
                upload_meta,
                progress_callback=progress_callback,
                state_callback=state_callback,
                completed_parts=completed_parts,
            )
            await self._notify_upload_progress(progress_callback, upload_meta["size"], upload_meta["size"], "正在写入网盘")
            create_response = await self._create_uploaded_file(
                target_path,
                precreate_data,
                upload_meta,
                conflict_policy=conflict_policy,
            )
            await self._notify_upload_progress(progress_callback, upload_meta["size"], upload_meta["size"], "上传成功")
            return self._build_upload_success_result(
                create_response=create_response,
                target_path=target_path,
                target_name=target_name,
                normalized_parent_path=normalized_parent_path,
                cache_parent_id=cache_parent_id,
                file_size=upload_meta["size"],
            )
        except Exception as e:
            return OperationResult(success=False, message=f"上传文件失败: {str(e)}")

    async def _save_upload_to_tempfile(self, upload_file: UploadFile) -> str:
        suffix = os.path.splitext(upload_file.filename or "")[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            upload_file.file.seek(0)
            while True:
                chunk = upload_file.file.read(1024 * 1024)
                if not chunk:
                    break
                temp_file.write(chunk)
            return temp_file.name

    async def _prepare_upload_metadata(self, temp_path: str) -> Dict[str, Any]:
        file_size = os.path.getsize(temp_path)
        if file_size <= 0:
            raise Exception("暂不支持上传空文件")

        chunk_size = BaiduOpenAPI.DEFAULT_UPLOAD_CHUNK_SIZE
        block_list: List[str] = []
        content_md5 = hashlib.md5()
        slice_hasher = hashlib.md5()
        first_slice_remaining = 256 * 1024

        with open(temp_path, "rb") as file_obj:
            while True:
                chunk = file_obj.read(chunk_size)
                if not chunk:
                    break
                content_md5.update(chunk)
                block_list.append(hashlib.md5(chunk).hexdigest())

                if first_slice_remaining > 0:
                    slice_part = chunk[:first_slice_remaining]
                    slice_hasher.update(slice_part)
                    first_slice_remaining -= len(slice_part)

        if not block_list:
            raise Exception("未生成有效的上传分片")

        return {
            "size": file_size,
            "chunk_size": chunk_size,
            "block_list": block_list,
            "content_md5": content_md5.hexdigest(),
            "slice_md5": slice_hasher.hexdigest(),
        }

    async def _notify_upload_progress(
        self,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]],
        uploaded_bytes: int,
        total_bytes: int,
        message: str,
    ) -> None:
        if progress_callback:
            await progress_callback(uploaded_bytes, total_bytes, message)

    def _normalize_upload_resume_state(
        self,
        resume_state: Optional[Dict[str, Any]],
        *,
        target_path: str,
        upload_meta: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(resume_state, dict):
            return None

        uploadid = str(resume_state.get("uploadid") or "").strip()
        upload_host = str(resume_state.get("upload_host") or "").strip()
        resume_path = str(resume_state.get("target_path") or "").strip()
        if not uploadid or not upload_host or resume_path != target_path:
            return None

        total_parts = len(upload_meta["block_list"])
        completed_parts = []
        for part in resume_state.get("completed_parts") or []:
            try:
                part_index = int(part)
            except (TypeError, ValueError):
                continue
            if 0 <= part_index < total_parts and part_index not in completed_parts:
                completed_parts.append(part_index)

        chunk_size = upload_meta["chunk_size"]
        total_bytes = upload_meta["size"]
        uploaded_bytes = 0
        for part_index in completed_parts:
            part_size = chunk_size
            if part_index == total_parts - 1:
                part_size = total_bytes - chunk_size * (total_parts - 1)
            uploaded_bytes += max(part_size, 0)

        progress = min(99, int(uploaded_bytes * 100 / max(total_bytes, 1))) if uploaded_bytes < total_bytes else 100
        return {
            "target_path": target_path,
            "uploadid": uploadid,
            "upload_host": upload_host,
            "completed_parts": sorted(completed_parts),
            "uploaded_bytes": uploaded_bytes,
            "progress": progress,
        }

    async def _persist_upload_resume_state(
        self,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]],
        *,
        target_path: str,
        uploadid: str,
        upload_host: str,
        completed_parts: List[int],
        uploaded_bytes: int,
        total_bytes: int,
    ) -> None:
        if not state_callback:
            return
        progress = min(99, int(uploaded_bytes * 100 / max(total_bytes, 1))) if uploaded_bytes < total_bytes else 100
        await state_callback({
            "target_path": target_path,
            "uploadid": uploadid,
            "upload_host": upload_host,
            "completed_parts": sorted(completed_parts),
            "uploaded_bytes": uploaded_bytes,
            "progress": progress,
        })

    def _normalize_conflict_policy(self, conflict_policy: Optional[str]) -> str:
        normalized = str(conflict_policy or "overwrite").strip().lower()
        if normalized not in {"fail", "overwrite", "rename"}:
            return "overwrite"
        return normalized

    def _get_upload_rtype(self, conflict_policy: Optional[str]) -> str:
        policy = self._normalize_conflict_policy(conflict_policy)
        if policy == "rename":
            return "1"
        if policy == "overwrite":
            return "3"
        return "0"

    async def _precreate_upload(
        self,
        target_path: str,
        upload_meta: Dict[str, Any],
        conflict_policy: str = "overwrite",
    ) -> Dict[str, Any]:
        rtype = self._get_upload_rtype(conflict_policy)
        response = await self._api_request(
            "file_precreate",
            "POST",
            data={
                "path": target_path,
                "size": str(upload_meta["size"]),
                "isdir": "0",
                "autoinit": "1",
                "rtype": rtype,
                "block_list": json.dumps(upload_meta["block_list"]),
                "content-md5": upload_meta["content_md5"],
                "slice-md5": upload_meta["slice_md5"],
            },
        )

        return_type = int(response.get("return_type") or 0)
        if return_type == 2:
            return {
                "rapid_upload": True,
                "return_type": return_type,
                "file_info": response.get("info") or {},
                "block_list": response.get("block_list") or [],
            }

        uploadid = str(response.get("uploadid") or "").strip()
        if not uploadid:
            raise Exception("预上传成功但未返回 uploadid")
        return {
            "rapid_upload": False,
            "return_type": return_type,
            "uploadid": uploadid,
            "block_list": response.get("block_list") or list(range(len(upload_meta["block_list"]))),
        }

    async def _locate_upload_host(self, target_path: str, uploadid: str) -> str:
        await self._ensure_session()
        url = BaiduOpenAPI.PCS_BASE_URL + BaiduOpenAPI.ENDPOINTS["locate_upload"]
        params = BaiduOpenApiHelper.build_params(
            "locate_upload",
            self.access_token,
            params={
                "appid": BaiduOpenAPI.UPLOAD_APP_ID,
                "path": target_path,
                "uploadid": uploadid,
                "upload_version": "2.0",
            },
        )

        await self._apply_operation_delay()
        async with self._session.get(url, params=params, headers=self._build_request_headers()) as response:
            if response.status != 200:
                error_text = await response.text()
                raise Exception(f"获取上传域名失败 ({response.status}): {error_text[:200]}")
            data = await response.json()

        error_code = data.get("error_code", 0)
        if error_code not in (0, "0", None):
            raise Exception(data.get("error_msg") or f"获取上传域名失败: {error_code}")

        for item in data.get("servers") or []:
            server = str(item.get("server") or "").strip()
            if server.startswith("https://"):
                return server

        host = str(data.get("host") or "").strip()
        if host:
            if host.startswith("http://") or host.startswith("https://"):
                return host
            return f"https://{host}"

        return "https://c3.pcs.baidu.com"

    async def _upload_chunks(
        self,
        upload_host: str,
        target_path: str,
        uploadid: str,
        temp_path: str,
        upload_meta: Dict[str, Any],
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    ) -> None:
        await self._ensure_session()
        url = f"{upload_host}{BaiduOpenAPI.ENDPOINTS['superfile_upload']}"
        chunk_size = upload_meta["chunk_size"]
        uploaded_bytes = 0
        total_bytes = upload_meta["size"]

        with open(temp_path, "rb") as file_obj:
            partseq = 0
            while True:
                chunk = file_obj.read(chunk_size)
                if not chunk:
                    break

                params = BaiduOpenApiHelper.build_params(
                    "superfile_upload",
                    self.access_token,
                    params={
                        "type": "tmpfile",
                        "path": target_path,
                        "uploadid": uploadid,
                        "partseq": str(partseq),
                    },
                )

                boundary = f"----LitePanBoundary{uuid.uuid4().hex}"
                body_prefix = (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="file"; filename="chunk-{partseq}"\r\n'
                    f"Content-Type: application/octet-stream\r\n\r\n"
                ).encode("utf-8")
                body_suffix = f"\r\n--{boundary}--\r\n".encode("utf-8")
                body = body_prefix + chunk + body_suffix
                headers = self._build_request_headers(
                    {
                        "Content-Type": f"multipart/form-data; boundary={boundary}",
                        "Content-Length": str(len(body)),
                    }
                )

                async with self._session.post(url, params=params, data=body, headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise Exception(f"上传分片 {partseq} 失败 ({response.status}): {error_text[:200]}")
                    response_text = await response.text()

                try:
                    response_data = json.loads(response_text)
                except json.JSONDecodeError:
                    raise Exception(f"上传分片 {partseq} 返回异常内容: {response_text[:200]}")

                uploaded_md5 = str(response_data.get("md5") or "").strip().lower()
                expected_md5 = upload_meta["block_list"][partseq].lower()
                if uploaded_md5 and uploaded_md5 != expected_md5:
                    raise Exception(f"上传分片 {partseq} 校验失败")
                uploaded_bytes += len(chunk)
                await self._notify_upload_progress(
                    progress_callback,
                    uploaded_bytes,
                    total_bytes,
                    f"正在上传到百度网盘，分片（{partseq + 1}/{len(upload_meta['block_list'])}）",
                )
                partseq += 1

    async def _upload_chunks_with_resume(
        self,
        upload_host: str,
        target_path: str,
        uploadid: str,
        temp_path: str,
        upload_meta: Dict[str, Any],
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        completed_parts: Optional[List[int]] = None,
    ) -> None:
        await self._ensure_session()
        url = f"{upload_host}{BaiduOpenAPI.ENDPOINTS['superfile_upload']}"
        chunk_size = upload_meta["chunk_size"]
        total_bytes = upload_meta["size"]
        total_parts = len(upload_meta["block_list"])
        completed_set = set(int(part) for part in (completed_parts or []))
        uploaded_bytes = 0

        for part_index in completed_set:
            part_size = chunk_size
            if part_index == total_parts - 1:
                part_size = total_bytes - chunk_size * (total_parts - 1)
            uploaded_bytes += max(part_size, 0)

        await self._persist_upload_resume_state(
            state_callback,
            target_path=target_path,
            uploadid=uploadid,
            upload_host=upload_host,
            completed_parts=list(completed_set),
            uploaded_bytes=uploaded_bytes,
            total_bytes=total_bytes,
        )

        with open(temp_path, "rb") as file_obj:
            partseq = 0
            while True:
                chunk = file_obj.read(chunk_size)
                if not chunk:
                    break

                if partseq in completed_set:
                    await self._notify_upload_progress(
                        progress_callback,
                        uploaded_bytes,
                        total_bytes,
                        f"正在继续上传到百度网盘，分片（{partseq + 1}/{total_parts}）",
                    )
                    partseq += 1
                    continue

                params = BaiduOpenApiHelper.build_params(
                    "superfile_upload",
                    self.access_token,
                    params={
                        "type": "tmpfile",
                        "path": target_path,
                        "uploadid": uploadid,
                        "partseq": str(partseq),
                    },
                )

                boundary = f"----LitePanBoundary{uuid.uuid4().hex}"
                body_prefix = (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="file"; filename="chunk-{partseq}"\r\n'
                    f"Content-Type: application/octet-stream\r\n\r\n"
                ).encode("utf-8")
                body_suffix = f"\r\n--{boundary}--\r\n".encode("utf-8")
                body = body_prefix + chunk + body_suffix
                headers = self._build_request_headers(
                    {
                        "Content-Type": f"multipart/form-data; boundary={boundary}",
                        "Content-Length": str(len(body)),
                    }
                )

                async with self._session.post(url, params=params, data=body, headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise Exception(f"上传分片 {partseq} 失败 ({response.status}): {error_text[:200]}")
                    response_text = await response.text()

                try:
                    response_data = json.loads(response_text)
                except json.JSONDecodeError:
                    raise Exception(f"上传分片 {partseq} 返回异常内容: {response_text[:200]}")

                uploaded_md5 = str(response_data.get("md5") or "").strip().lower()
                expected_md5 = upload_meta["block_list"][partseq].lower()
                if uploaded_md5 and uploaded_md5 != expected_md5:
                    raise Exception(f"上传分片 {partseq} 校验失败")

                completed_set.add(partseq)
                uploaded_bytes += len(chunk)
                await self._persist_upload_resume_state(
                    state_callback,
                    target_path=target_path,
                    uploadid=uploadid,
                    upload_host=upload_host,
                    completed_parts=list(completed_set),
                    uploaded_bytes=uploaded_bytes,
                    total_bytes=total_bytes,
                )
                await self._notify_upload_progress(
                    progress_callback,
                    uploaded_bytes,
                    total_bytes,
                    f"正在上传到百度网盘，分片（{partseq + 1}/{total_parts}）",
                )
                partseq += 1

    async def _create_uploaded_file(
        self,
        target_path: str,
        precreate_data: Dict[str, Any],
        upload_meta: Dict[str, Any],
        conflict_policy: str = "overwrite",
    ) -> Dict[str, Any]:
        rtype = self._get_upload_rtype(conflict_policy)
        return await self._api_request(
            "file_create",
            "POST",
            data={
                "path": target_path,
                "size": str(upload_meta["size"]),
                "isdir": "0",
                "rtype": rtype,
                "uploadid": precreate_data["uploadid"],
                "block_list": json.dumps(upload_meta["block_list"]),
            },
        )

    def _build_upload_success_result(
        self,
        *,
        create_response: Dict[str, Any],
        target_path: str,
        target_name: str,
        normalized_parent_path: str,
        cache_parent_id: str,
        file_size: int,
        message: Optional[str] = None,
    ) -> OperationResult:
        file_path = str(create_response.get("path") or target_path)
        file_name = str(create_response.get("server_filename") or target_name)
        return OperationResult(
            success=True,
            message=message or f"文件 '{file_name}' 上传成功",
            data={
                "file_id": file_path,
                "file_name": file_name,
                "parent_path": normalized_parent_path,
                "parent_id": cache_parent_id,
                "size": file_size,
                "fs_id": str(create_response.get("fs_id") or ""),
            },
        )

    async def get_download_info(self, file_id: str, user_agent: str = None) -> Dict[str, Any]:
        file_info = await self.file_info(file_id)
        if not file_info:
            raise Exception(f"文件 {file_id} 不存在")
        if file_info.is_dir:
            raise Exception("目录不支持下载")

        fs_id = str((file_info.extra or {}).get("fs_id") or "").strip()
        if not fs_id or not fs_id.isdigit():
            raise Exception(f"文件 {file_info.name} 缺少有效 fs_id")

        metas = await self._get_file_metas([fs_id], dlink=True)
        if not metas:
            raise Exception("获取文件下载信息失败")

        meta_info = metas[0]
        dlink = str((meta_info.extra or {}).get("dlink") or "").strip()
        if not dlink:
            raise Exception("未获取到百度下载链接")

        separator = "&" if "?" in dlink else "?"
        official_url = f"{dlink}{separator}access_token={self.access_token}"
        download_url = await self._resolve_download_url(official_url)

        return {
            "download_url": download_url,
            "file_name": meta_info.name or file_info.name or f"file_{file_id}",
            "size": int(meta_info.size or file_info.size or 0),
        }

    async def get_download_url(self, file_id: str, user_agent: str = None) -> str:
        info = await self.get_download_info(file_id, user_agent)
        return info["download_url"]

    async def get_download_headers(self, file_id: str, user_agent: str = None) -> Dict[str, str]:
        return {
            "User-Agent": BaiduOpenAPI.USER_AGENT,
        }

    @staticmethod
    def _extract_content_md5_from_headers(headers) -> str:
        for key in ("Content-MD5", "content-md5", "Etag", "etag"):
            value = headers.get(key)
            if not value:
                continue
            normalized = normalize_content_md5(str(value).strip().strip('"\''))
            if normalized:
                return normalized
        return ""

    async def _resolve_download_url(self, official_url: str) -> str:
        await self._ensure_session()
        async with self._session.head(
            official_url,
            headers={"User-Agent": BaiduOpenAPI.USER_AGENT},
            allow_redirects=False,
        ) as response:
            location = response.headers.get("Location") or response.headers.get("location")
            if location:
                return location
        return official_url

    async def _probe_content_md5_from_download_url(
        self,
        download_url: str,
        headers_override: Optional[Dict[str, str]] = None,
    ) -> str:
        await self._ensure_session()
        headers = dict(headers_override or {})
        headers.setdefault("User-Agent", BaiduOpenAPI.USER_AGENT)

        async with self._session.head(
            download_url,
            headers=headers,
            allow_redirects=True,
        ) as response:
            if response.status < 400:
                result = self._extract_content_md5_from_headers(response.headers)
                if result:
                    return result

        range_headers = dict(headers)
        range_headers["Range"] = "bytes=0-0"
        async with self._session.get(
            download_url,
            headers=range_headers,
            allow_redirects=True,
        ) as response:
            if response.status in {200, 206}:
                result = self._extract_content_md5_from_headers(response.headers)
                if result:
                    return result
        return ""

    async def _get_file_metas(self, fs_ids: List[str], dlink: bool = False) -> List[FileItem]:
        fsids_json = json.dumps([int(fid) for fid in fs_ids])
        response = await self._api_request(
            "file_metas",
            "GET",
            params={
                "fsids": fsids_json,
                "dlink": "1" if dlink else "0",
                "thumb": "1",
                "extra": "1",
            },
        )
        result: List[FileItem] = []
        for item_data in response.get("list", []):
            item = BaiduOpenFile.from_metas_api(item_data).to_file_item()
            result.append(item)
        return result

    async def _resolve_content_md5_from_download(self, item: FileItem) -> str:
        from core.driver_service import resolve_download

        extra = item.extra or {}
        cache_key = str(extra.get("fs_id") or item.id or "").strip()
        if cache_key and cache_key in self._transfer_md5_cache:
            return self._transfer_md5_cache[cache_key]

        if int(item.size or 0) <= 0:
            return ""

        download = await resolve_download(self, str(item.id), "", file_info=item)
        result = await self._probe_content_md5_from_download_url(
            download.download_url,
            download.headers,
        )
        if result and cache_key:
            self._transfer_md5_cache[cache_key] = result
        return result

    async def resolve_transfer_hash(self, item: FileItem, method: str, *, allow_stream: bool = False) -> str:
        if str(method or "").lower() != "md5" or not allow_stream:
            return ""

        try:
            return await self._resolve_content_md5_from_download(item)
        except Exception as exc:
            self._log.warning(
                f"百度获取 content-md5 失败 {item.name}: {exc}",
                driver_name="baidu_open",
            )
            return ""

    def __str__(self) -> str:
        masked_token = f"{self.access_token[:12]}..." if self.access_token else "empty"
        return f"<{self.__class__.__name__} access_token={masked_token}>"
