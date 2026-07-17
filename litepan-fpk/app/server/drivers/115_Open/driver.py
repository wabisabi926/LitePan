"""115 Open 驱动业务方法"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import tempfile
import time
import xml.etree.ElementTree as ET
from copy import deepcopy
from email.utils import formatdate
from typing import Any, Awaitable, Callable, Dict, List, Optional
from urllib.parse import quote

import aiohttp
from fastapi import UploadFile
from core.base import FileItem, OperationResult, DriverInfo
from core.driver_base import BaseDriver
from core.operation_wrapper import (
    auto_cleanup_cache, 
    with_file_list_cache, 
    with_file_info_cache, 
    with_performance_tracking,
    with_auth_retry,
    clear_operation_cache,
)
from .config import OneOneFiveOpenConfig
from .models import OneOneFiveFile, OneOneFiveRecycleFile
from .api import OneOneFiveAPI, OneOneFiveConstants, OneOneFiveApiHelper


class OneOneFiveOpenDriver(BaseDriver):
    def __init__(self, config: OneOneFiveOpenConfig):
        super().__init__(config)
        self.access_token = config.access_token
        self.refresh_token = config.refresh_token
        self.token_expires_at = 0
        self._session: Optional[aiohttp.ClientSession] = None
        self._refresh_lock = asyncio.Lock()

    def supports_parallel_range_download(self) -> bool:
        return False

    @classmethod
    def get_info(cls) -> DriverInfo:
        return DriverInfo(
            name="115网盘Open",
            display_name="115网盘Open",
            version="3.1.0",
            capabilities=["list", "info", "download", "create_folder", "delete", "batch_delete", "rename", "move", "copy", "upload"],
            description="115网盘官方API接入",
            author="LitePan"
        )

    async def init(self) -> None:
        if not self._session or self._session.closed:
            headers = OneOneFiveAPI.HEADERS.copy()
            self._session = aiohttp.ClientSession(headers=headers)

        self._log.debug("115网盘Open驱动初始化完成", driver_name="115_open")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._log.debug("115网盘Open驱动已关闭", driver_name="115_open")

    async def test_connection(self) -> OperationResult:
        try:
            self._log.debug("开始测试 115网盘Open 连接", driver_name="115_open")

            await self._api_request("file_list", "GET", params={
                "cid": self.config.root_folder_id,
                "limit": 1,
                "show_dir": "1"
            })

            self._log.debug("115网盘Open 连接测试成功", driver_name="115_open")
            return OperationResult(success=True, message="连接测试成功")

        except Exception as e:
            error_msg = f"连接测试失败: {str(e)}"
            self._log.error(f"115网盘Open {error_msg}", driver_name="115_open")
            return OperationResult(success=False, message=error_msg)

    async def _apply_operation_delay(self) -> None:
        await self.wait_for_request_interval()

    def _get_single_part_upload_limit(self) -> int:
        # 以 512MB 作为单次上传与分片上传的切换阈值。
        return 512 * 1024 * 1024

    def _clone_resume_state(self, resume_state: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """复制续传状态，避免原始对象被原地修改。"""
        return deepcopy(resume_state) if isinstance(resume_state, dict) else {}

    async def _persist_resume_state(
        self,
        resume_state: Dict[str, Any],
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        **updates: Any,
    ) -> None:
        """合并续传状态，并在需要时推送最新快照。"""
        resume_state.update({k: v for k, v in updates.items() if v is not None})
        if state_callback:
            await state_callback(deepcopy(resume_state))
    
    
    @with_auth_retry(max_retries=1)
    async def _api_request(self, operation: str, method: str, **kwargs) -> Dict[str, Any]:
        if not self._session:
            raise Exception("HTTP会话未初始化")

        if 'params' in kwargs:
            kwargs['params'] = OneOneFiveApiHelper.map_params(kwargs['params'], operation)
        if 'data' in kwargs:
            kwargs['data'] = OneOneFiveApiHelper.map_params(kwargs['data'], operation)
        
        url = OneOneFiveAPI.BASE_URL + OneOneFiveAPI.ENDPOINTS[operation]
        
        custom_headers = kwargs.pop('custom_headers', None)

        # HTML / 非 JSON / 网络抖动属于网关或网络层的瞬时问题：调用内退避重试，绝不触发 token 刷新。
        # 只有 115 返回的、能解析出的明确 JSON 认证错误，才向外层 @with_auth_retry 抛认证异常去统一刷新。
        # 连通性测试不重试，保持快速失败。
        backoffs = (0,) if self.is_connectivity_test() else (0, 2, 5)
        total = len(backoffs)
        last_transient = ""

        for attempt, backoff in enumerate(backoffs, start=1):
            if backoff:
                await asyncio.sleep(backoff)

            headers = custom_headers or OneOneFiveApiHelper.build_headers(
                self.access_token, OneOneFiveAPI.ENDPOINTS[operation]
            )
            await self._apply_operation_delay()
            try:
                async with self._session.request(method, url, headers=headers, **kwargs) as response:
                    status = response.status
                    content_type = response.headers.get('content-type', '')
                    text = await response.text()
            except Exception as e:
                last_transient = f"网络异常: {e}"
                self._log.warning(
                    f"🌐 数据接口瞬时失败({operation}, 第{attempt}/{total}次)，退避重试: {last_transient}",
                    driver_name="115_open"
                )
                continue

            # 能解析出 JSON 才算 115 的"明确答复"；否则是网关/限流页等瞬时情况
            try:
                data = json.loads(text)
            except Exception:
                data = None

            if not isinstance(data, dict):
                last_transient = self._summarize_response_body(text, content_type)
                self._log.warning(
                    f"🌐 数据接口返回非JSON({operation}, status={status})，"
                    f"退避重试(第{attempt}/{total}次): {last_transient}",
                    driver_name="115_open"
                )
                continue

            success, error_msg = OneOneFiveApiHelper.check_success(data)
            if success:
                return data

            # 115 给了明确 JSON 错误：直接抛出。若为认证类错误（含"Token认证失败"/access_token），
            # 外层 @with_auth_retry 会据此刷新 token 并重试整个请求；其余业务错误按原样抛出。
            raise Exception(error_msg)

        # 连续多次都是瞬时失败：抛出不含认证关键字的错误，避免外层 @with_auth_retry 误判为认证错误又去刷新
        raise Exception(
            f"115网盘API请求失败：网关或网络异常，已重试{total}次仍未获得有效响应（{operation}，详见日志）"
        )

    # 115 Open API 错误码分类
    _FATAL_ERROR_CODES = {40140115, 40140116, 40140119, 40140120}  # token 签名失败/无效/过期/被篡改
    _RETRYABLE_ERROR_CODES = {40140117, 40140121}                   # 刷新太频繁/刷新失败

    async def _ensure_session(self) -> None:
        if not self._session or self._session.closed:
            headers = OneOneFiveAPI.HEADERS.copy()
            self._session = aiohttp.ClientSession(headers=headers)

    async def _refresh_token_locked(self):
        async with self._refresh_lock:
            return await self._refresh_token()

    @staticmethod
    def _summarize_response_body(text: str, content_type: str = "") -> str:
        """把网关/限流返回的 HTML（或其它非 JSON）正文压成一行可读摘要，便于排错时一眼看出是什么。"""
        import re
        ct = content_type or "(未知)"
        if not text or not text.strip():
            return f"<空响应> content-type={ct}"
        flat = re.sub(r"\s+", " ", text.strip())
        title_match = re.search(r"<title[^>]*>(.*?)</title>", text, re.IGNORECASE | re.DOTALL)
        if title_match and title_match.group(1).strip():
            title = re.sub(r"\s+", " ", title_match.group(1)).strip()
            return f"content-type={ct} <title>={title!r} 正文片段={flat[:200]!r}"
        return f"content-type={ct} 正文片段={flat[:300]!r}"

    # 刷新返回 HTML/非 JSON/网络抖动时的调用内重试节奏（秒）：先快后慢，避免立刻判死进冷却
    _REFRESH_TRANSIENT_BACKOFFS = (0, 3, 6)

    async def _refresh_token(self):
        from core.auth_manager import RefreshOutcome

        if not self.refresh_token:
            self._log.error("没有 refresh_token，无法刷新", driver_name="115_open")
            return RefreshOutcome.FATAL

        last_transient = ""
        total = len(self._REFRESH_TRANSIENT_BACKOFFS)
        for attempt, backoff in enumerate(self._REFRESH_TRANSIENT_BACKOFFS, start=1):
            if backoff:
                await asyncio.sleep(backoff)

            try:
                await self._ensure_session()
                await self._apply_operation_delay()
                async with self._session.post(
                    OneOneFiveAPI.REFRESH_URL,
                    data={'refresh_token': self.refresh_token},
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                ) as response:
                    status = response.status
                    content_type = response.headers.get('content-type', '')
                    text = await response.text()
            except Exception as e:
                last_transient = f"网络异常: {e}"
                self._log.warning(
                    f"🌐 token 刷新瞬时失败(第{attempt}/{total}次)，将重试: {last_transient}",
                    driver_name="115_open"
                )
                continue

            # 能解析出 JSON 才算 115 给了"明确答复"；否则是网关/限流页等瞬时情况
            try:
                result = json.loads(text)
            except Exception:
                result = None

            if not isinstance(result, dict):
                last_transient = self._summarize_response_body(text, content_type)
                self._log.warning(
                    f"🌐 token 刷新返回非JSON(疑似网关/限流页, status={status})，"
                    f"按瞬时失败重试(第{attempt}/{total}次): {last_transient}",
                    driver_name="115_open"
                )
                continue

            if result.get('state') == 1 and result.get('data'):
                new_access_token = result['data'].get('access_token')
                new_refresh_token = result['data'].get('refresh_token')
                expires_in = result['data'].get('expires_in', OneOneFiveConstants.DEFAULT_TOKEN_EXPIRES)

                if new_access_token:
                    self.access_token = new_access_token
                    self.config.access_token = new_access_token
                if new_refresh_token:
                    self.refresh_token = new_refresh_token
                    self.config.refresh_token = new_refresh_token

                self.token_expires_at = time.time() + expires_in
                self.config.token_expires_at = self.token_expires_at
                return RefreshOutcome.SUCCESS

            # 115 给了明确错误码：致命码直接重新授权，其余（如 40140117 太频繁）交给上层正常冷却
            error_code = result.get('code', 0)
            error_msg = result.get('message', 'Unknown error')
            if error_code in self._FATAL_ERROR_CODES:
                self._log.error(f"token 刷新致命失败: code={error_code}, msg={error_msg}", driver_name="115_open")
                return RefreshOutcome.FATAL
            self._log.error(f"token 刷新失败: code={error_code}, msg={error_msg}", driver_name="115_open")
            return RefreshOutcome.RETRYABLE

        # 连续多次都是瞬时失败（没拿到 115 的明确答复）：退回上层冷却，等网关/限流恢复，token 未判死
        self._log.error(
            f"token 刷新连续 {total} 次瞬时失败，暂入冷却等待恢复（token 未判死）。最后一次: {last_transient}",
            driver_name="115_open"
        )
        return RefreshOutcome.RETRYABLE
    
    async def refresh_auth(self):
        try:
            from core.auth_manager import RefreshOutcome
            outcome = await self._refresh_token_locked()
            if outcome == RefreshOutcome.SUCCESS:
                self._log.info("✅ 115 网盘认证刷新成功", driver_name="115_open")
            elif outcome == RefreshOutcome.FATAL:
                self._log.error("❌ 115 网盘认证刷新致命失败，需要重新授权", driver_name="115_open")
            else:
                self._log.error("❌ 115 网盘认证刷新失败", driver_name="115_open")
            return outcome
        except Exception as e:
            self._log.error(f"❌ 115 网盘认证刷新异常: {e}", driver_name="115_open")
            from core.auth_manager import RefreshOutcome
            return RefreshOutcome.RETRYABLE
    
    def set_auth_manager(self, auth_manager):
        self._auth_manager = auth_manager

    def _sync_auth_from_config(self, source_config) -> None:
        if not source_config:
            return
        if hasattr(source_config, 'access_token') and source_config.access_token:
            self.access_token = source_config.access_token
            self.config.access_token = source_config.access_token
        if hasattr(source_config, 'refresh_token') and source_config.refresh_token:
            self.refresh_token = source_config.refresh_token
            self.config.refresh_token = source_config.refresh_token
        if hasattr(source_config, 'token_expires_at'):
            self.token_expires_at = getattr(source_config, 'token_expires_at') or self.token_expires_at

    async def _handle_auth_error(self, error_reason: str):
        """驱动内部的被动刷新：优先交给 auth_manager，失败才降级到本地刷新。"""
        try:
            if self.is_connectivity_test():
                return False
            self._log.warning(f"触发被动刷新: {error_reason}", driver_name="115_open")
            
            if hasattr(self, '_account_id'):
                try:
                    from core.auth_manager import auth_scheduler, handle_auth_error
                    normalized_account_id = int(self._account_id)
                    if normalized_account_id in auth_scheduler.auth_managers:
                        success = await handle_auth_error(normalized_account_id)
                        if success:
                            auth_manager = auth_scheduler.auth_managers.get(normalized_account_id)
                            self._sync_auth_from_config(getattr(auth_manager, 'config', None))
                            self._log.info("✅ 被动刷新成功 (通过认证管理器)", driver_name="115_open")
                            return True
                        self._log.warning("⚠️ 认证管理器刷新失败，本次不再追加直接刷新", driver_name="115_open")
                        return False
                except Exception as e:
                    self._log.warning(f"⚠️ 认证管理器刷新异常，尝试直接刷新: {e}", driver_name="115_open")
            
            self._log.info("📧 认证管理器不可用，直接调用本地刷新方法", driver_name="115_open")
            from core.auth_manager import RefreshOutcome
            outcome = await self.refresh_auth()
            if outcome == RefreshOutcome.SUCCESS:
                if hasattr(self, '_account_id'):
                    try:
                        from core.auth_manager import sync_driver_refresh_success
                        await sync_driver_refresh_success(self._account_id, self)
                    except Exception as sync_error:
                        self._log.warning(f"⚠️ 直接刷新成功后同步认证状态失败 {sync_error}", driver_name="115_open")
                self._log.info("✅ 被动刷新成功（通过直接调用）", driver_name="115_open")
            else:
                self._log.error("❌ 被动刷新失败", driver_name="115_open")
            return outcome == RefreshOutcome.SUCCESS
            
        except Exception as e:
            self._log.error(f"❌ 被动刷新异常: {e}", driver_name="115_open")
            return False
    
    
    @with_file_list_cache
    async def list_files(self, parent_id: str = "0") -> List[FileItem]:
        all_files = []
        offset = 0
        page_size = 300  # 115 网盘每页 300 条
        fetched = 0
        total_count = 0

        while True:
            response = await self._api_request("file_list", "GET", params={
                "cid": parent_id,
                "limit": page_size,
                "offset": offset,
                "show_dir": "1"
            })

            files_data = response.get("data", [])

            if not files_data:
                break

            if fetched == 0:
                total_count = int(response.get("count") or 0)

            for file_data in files_data:
                file_obj = OneOneFiveFile.from_dict(file_data)
                if not file_obj.is_trashed():
                    all_files.append(file_obj.to_file_item())

            fetched += len(files_data)

            if total_count > 0 and fetched >= total_count:
                break
            if len(files_data) < page_size:
                break

            offset += page_size

        return all_files

    async def fetch_folder_sizes(self, file_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """按 file_id 拉取目录占用；通常每次仅 1 个（用户进入文件夹时）。"""
        result: Dict[str, Dict[str, Any]] = {}
        for file_id in file_ids:
            if not file_id:
                continue
            try:
                detail = await self.file_info(file_id)
            except Exception:
                continue
            if not detail or not detail.is_dir or int(detail.size or 0) <= 0:
                continue
            entry: Dict[str, Any] = {"size": int(detail.size)}
            if detail.modified:
                entry["modified"] = detail.modified.isoformat()
            result[str(file_id)] = entry
        return result
    
    @with_file_info_cache
    async def file_info(self, file_id: str) -> Optional[FileItem]:
        response = await self._api_request("file_info", "GET", params={
            "file_id": file_id
        })

        file_data = response.get("data", {})
        if not file_data:
            return None
        
        file_obj = OneOneFiveFile.from_dict(file_data)
        return file_obj.to_file_item()
    
    @auto_cleanup_cache('create_folder')
    async def create_folder(self, parent_id: str, name: str) -> OperationResult:
        try:
            response = await self._api_request("create_folder", "POST", data={
                "file_name": name,
                "pid": parent_id
            })

            folder_data = response.get("data", {})
            folder_id = folder_data.get("cid") or folder_data.get("file_id")
            
            message = f"文件夹 '{name}' 创建成功"
            
            return OperationResult(
                success=True, 
                message=message, 
                data={"folder_id": folder_id, "parent_path": parent_id, "folder_name": name}
            )
            
        except Exception as e:
            return OperationResult(success=False, message=f"创建文件夹失败: {str(e)}")

    
    @auto_cleanup_cache('delete_file')
    async def delete_file(self, file_id: str) -> OperationResult:
        return await self._delete_files([file_id])

    @auto_cleanup_cache('batch_delete_file')
    async def batch_delete_file(self, file_ids: List[str]) -> OperationResult:
        if not file_ids:
            return OperationResult(success=True, message="没有文件需要删除")
        
        return await self._delete_files(file_ids)
    
    async def _delete_files(self, file_ids: List[str]) -> OperationResult:
        try:
            parent_ids = set()
            for file_id in file_ids:
                try:
                    file_info = await self.file_info(file_id)
                    if file_info and file_info.extra is not None:
                        parent_id = file_info.extra.get('parent_id')
                        if parent_id not in [None, ""]:
                            parent_ids.add(str(parent_id))
                except Exception:
                    pass

            if self.config.delete_mode == "delete":
                # 永久删除：115 没有直达永久删除接口，只能先入回收站再后台清空
                self._log.debug(f"永久删除模式：启动后台异步删除...", driver_name="115_open")

                self._log.debug(f"永久删除模式：快速移到回收站...", driver_name="115_open")
                await self._api_request("delete", "POST", data={
                    "file_ids": ','.join(file_ids)
                })

                self._log.debug(f"文件已移入回收站，永久删除将在后台完成", driver_name="115_open")
                asyncio.create_task(self._background_permanent_delete(file_ids, len(file_ids)))

                message = f"已永久删除 {len(file_ids)} 个文件"

            else:
                self._log.debug(f"回收站模式：准备将 {len(file_ids)} 个文件移入回收站...", driver_name="115_open")

                await self._api_request("delete", "POST", data={
                    "file_ids": ','.join(file_ids)
                })
                
                message = f"已将 {len(file_ids)} 个文件移到回收站"
            
            return OperationResult(
                success=True, 
                message=message, 
                data={
                    "deleted_count": len(file_ids),
                    "file_ids": file_ids,
                    "parent_ids": list(parent_ids)
                }
            )

        except Exception as e:
            error_msg = f"删除失败: {str(e)}"
            self._log.error(f"115网盘 {error_msg}", driver_name="115_open")
            return OperationResult(success=False, message=error_msg)
    
    async def _background_permanent_delete(self, original_file_ids: List[str], expected_count: int):
        """115 永久删除的第二阶段：轮询回收站、按 delete_time 匹配最近删除的记录再调用永久删除。"""
        try:
            self._log.debug(f"后台任务：开始永久删除 {expected_count} 个文件（原始ID: {original_file_ids[:3]}{'...' if len(original_file_ids) > 3 else ''}）...", driver_name="115_open")
            await asyncio.sleep(0.5)

            max_attempts = 8

            for attempt in range(max_attempts):
                try:
                    limit = min(expected_count + 3, 20)
                    recycle_response = await self._api_request("recycle_list", "GET", params={
                        "limit": limit,
                        "offset": 0
                    })
                    
                    recycle_data = recycle_response.get("data", {}) if isinstance(recycle_response, dict) else {}
                    recycle_files = []
                    
                    if isinstance(recycle_data, dict):
                        for file_id, file_info in recycle_data.items():
                            if isinstance(file_info, dict) and file_id not in ['offset', 'limit', 'count', 'rb_pass']:
                                simplified_file = {
                                    'id': file_info.get('id'),
                                    'file_name': file_info.get('file_name'),
                                    'delete_time': file_info.get('delete_time', 0)
                                }
                                recycle_files.append(simplified_file)
                    
                    if recycle_files and len(recycle_files) >= expected_count:
                        sorted_files = sorted(recycle_files,
                                            key=lambda x: x.get('delete_time', 0),
                                            reverse=True)

                        files_to_delete = sorted_files[:expected_count]
                        tids_to_delete = [f['id'] for f in files_to_delete if f.get('id')]

                        if len(tids_to_delete) == expected_count:
                            await self._api_request("delete_permanently", "POST", data={
                                "recycle_ids": tids_to_delete
                            })
                            
                            self._log.debug(f"后台任务：永久删除完成，用时约 {(attempt + 1) * 0.3 + 0.5:.1f} 秒", driver_name="115_open")
                            return
                        else:
                            self._log.debug(f"找到 {len(tids_to_delete)} 个文件，需要 {expected_count} 个", driver_name="115_open")
                    else:
                        self._log.debug(f"回收站文件数量不足: {len(recycle_files) if recycle_files else 0}", driver_name="115_open")
                    
                    if attempt < max_attempts - 1:
                        wait_time = 0.3 if attempt < 4 else 0.8
                        await asyncio.sleep(wait_time)

                except Exception as e:
                    self._log.error(f"后台任务第 {attempt + 1} 次检查失败: {e}", driver_name="115_open")
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(0.5)
            
            self._log.warning(f"后台任务：经过 {max_attempts} 次快速检查，永久删除未完成", driver_name="115_open")
            
        except Exception as e:
            self._log.error(f"后台永久删除任务异常: {e}", driver_name="115_open")
    
    @auto_cleanup_cache('rename_file')
    async def rename_file(self, file_id: str, new_name: str) -> OperationResult:
        try:
            response = await self._api_request("rename", "POST", data={
                "file_id": file_id,
                "file_name": new_name.strip()
            })

            message = f"文件重命名为 '{new_name}' 成功"
            
            return OperationResult(
                success=True, 
                message=message, 
                data={"file_id": file_id, "new_name": new_name}
            )
            
        except Exception as e:
            return OperationResult(success=False, message=f"重命名失败: {str(e)}")


    @auto_cleanup_cache('move_file')
    async def move_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        try:
            if not file_ids:
                return OperationResult(success=False, message="没有指定要移动的文件")

            self._log.debug(f"开始移动 {len(file_ids)} 个文件到目录 {target_parent_id}", driver_name="115_open")

            parent_ids = set()
            for file_id in file_ids:
                try:
                    file_info = await self.file_info(file_id)
                    if file_info and file_info.extra and 'parent_id' in file_info.extra:
                        parent_ids.add(file_info.extra['parent_id'])
                except Exception as e:
                    self._log.warning(f"获取文件 {file_id} 信息失败: {e}", driver_name="115_open")

            file_ids_str = ','.join(str(fid) for fid in file_ids)

            data = {
                'file_ids': file_ids_str,
                'to_cid': str(target_parent_id)
            }

            response = await self._api_request("move", "POST", data=data)

            if not response or not response.get('state'):
                error_msg = f"115网盘API返回错误: {response}"
                self._log.error(error_msg, driver_name="115_open")
                return OperationResult(success=False, message="移动失败，请稍后重试")

            message = f"已移动 {len(file_ids)} 个文件到目标目录"

            return OperationResult(
                success=True,
                message=message,
                data={
                    "moved_count": len(file_ids),
                    "file_ids": file_ids,
                    "target_parent_id": target_parent_id,
                    "source_parent_ids": list(parent_ids)
                }
            )

        except Exception as e:
            error_msg = f"移动失败: {str(e)}"
            self._log.error(f"115网盘Open {error_msg}", driver_name="115_open")
            return OperationResult(success=False, message=error_msg)
    
    @auto_cleanup_cache('copy_file')
    async def copy_file(self, file_ids: List[str], target_parent_id: str, source_parent_id: str = None) -> OperationResult:
        try:
            if not file_ids:
                return OperationResult(success=False, message="没有指定要复制的文件")

            self._log.debug(f"开始复制 {len(file_ids)} 个文件到目录 {target_parent_id}", driver_name="115_open")

            if source_parent_id and str(source_parent_id) == str(target_parent_id):
                return OperationResult(
                    success=False,
                    message="115网盘不支持复制到同一目录",
                    data={"warning": True}
                )

            data = {
                'file_id': ','.join(str(fid) for fid in file_ids),
                'pid': str(target_parent_id),
                'nodupli': 0,
            }

            response = await self._api_request("copy", "POST", data=data)

            if not response or not response.get('state'):
                error_msg = f"115网盘API返回错误: {response}"
                self._log.error(error_msg, driver_name="115_open")
                return OperationResult(success=False, message="复制失败，请稍后重试")

            message = f"已复制 {len(file_ids)} 个文件到目标目录"

            return OperationResult(
                success=True,
                message=message,
                data={
                    "copied_count": len(file_ids),
                    "file_ids": file_ids,
                    "target_parent_id": target_parent_id,
                    "source_parent_ids": [source_parent_id] if source_parent_id else []
                }
            )

        except Exception as e:
            error_msg = f"复制失败: {str(e)}"
            self._log.error(f"115网盘Open {error_msg}", driver_name="115_open")
            return OperationResult(success=False, message=error_msg)

    @with_performance_tracking
    async def get_download_url(self, file_id: str, user_agent: str = None) -> str:
        try:
            file_info = await self.file_info(file_id)
            if not file_info:
                raise Exception(f"文件 {file_id} 不存在")

            pick_code = file_info.extra.get('pick_code', '')
            if not pick_code:
                raise Exception(f"文件 {file_info.name} 缺少 pick_code")

            # download_mode 决定上层用 302 / 本地代理 / 代理服务器，这里只负责取 115 直链
            from core.dependency_container import get_cache_manager
            cache_manager = get_cache_manager()
            download_mode = "redirect"

            account_id = getattr(self, 'account_id', None)
            if not account_id:
                download_mode = self.config.download_mode
            elif cache_manager:
                cached_mode = await cache_manager.get_download_mode_cache(account_id)
                if cached_mode:
                    download_mode = cached_mode
                else:
                    download_mode = self.config.download_mode
                    await cache_manager.set_download_mode_cache(account_id, download_mode)
            else:
                download_mode = self.config.download_mode

            self._log.debug(f"文件 {file_info.name} 使用下载模式: {download_mode}", driver_name="115_open")

            return await self._get_115_download_url(pick_code, user_agent)
                    
        except Exception as e:
            self._log.error(f"获取下载链接失败: {str(e)}", driver_name="115_open")
            raise

    async def get_download_headers(self, file_id: str, user_agent: str = None) -> Dict[str, str]:
        headers = {
            "User-Agent": user_agent or OneOneFiveAPI.USER_AGENT,
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "identity",
            "Referer": "https://115.com/",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        return headers

    async def _get_115_download_url(self, pick_code: str, user_agent: str = None) -> str:
        default_headers = OneOneFiveApiHelper.build_headers(self.access_token, OneOneFiveAPI.ENDPOINTS["download"])

        # 下载直链对 UA 敏感，外部（WebDAV / strm）可传入客户端自己的 UA
        if user_agent:
            default_headers["User-Agent"] = user_agent
        
        response = await self._api_request("download", "POST", data={
            "pick_code": pick_code
        }, custom_headers=default_headers)
        
        if not response:
            raise Exception("获取下载链接失败")
        
        # 按当前响应结构解析下载链接
        if response.get('state') and response.get('data'):
            data_obj = response['data']
            
            if isinstance(data_obj, dict):
                if 'url' in data_obj:
                    download_url = data_obj['url']
                    if isinstance(download_url, dict) and 'url' in download_url:
                        download_url = download_url['url']
                    if download_url and isinstance(download_url, str):
                        download_url = download_url.strip().rstrip('\x00').strip('`').strip()
                        self._log.debug(f"获取下载链接成功（直接）: {download_url[:100]}...", driver_name="115_open")
                        return download_url
                
                for file_id_key, file_info in data_obj.items():
                    if isinstance(file_info, dict):
                        if 'url' in file_info:
                            url_info = file_info['url']
                            if isinstance(url_info, dict) and 'url' in url_info:
                                download_url = url_info['url']
                            elif isinstance(url_info, str):
                                download_url = url_info
                            else:
                                download_url = None
                                
                            if download_url:
                                download_url = download_url.strip().rstrip('\x00').strip('`').strip()
                                self._log.debug(f"获取下载链接成功（遍历）: {download_url[:100]}...", driver_name="115_open")
                                return download_url
                        
                        elif isinstance(file_info, str) and file_info.startswith('http'):
                            download_url = file_info.strip().rstrip('\x00').strip('`').strip()
                            self._log.debug(f"获取下载链接成功（字符串）: {download_url[:100]}...", driver_name="115_open")
                            return download_url
                
                for field in ['download_url', 'downloadUrl', 'link']:
                    if field in data_obj:
                        download_url = data_obj[field]
                        if download_url:
                            download_url = download_url.strip().rstrip('\x00').strip('`').strip()
                            self._log.debug(f"获取下载链接成功（其他字段）: {download_url[:100]}...", driver_name="115_open")
                            return download_url
            
            elif isinstance(data_obj, str):
                download_url = data_obj.strip().rstrip('\x00').strip('`').strip()
                if download_url.startswith('http'):
                    self._log.debug(f"获取下载链接成功（字符串）: {download_url[:100]}...", driver_name="115_open")
                    return download_url
        
        self._log.error(f"响应中未找到下载链接: {response}", driver_name="115_open")
        raise Exception("下载链接为空")

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

        temp_path = ""
        try:
            temp_path = await self._save_upload_to_tempfile(upload_file)
            return await self.upload_local_file(
                temp_path,
                target_name,
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

        file_size = os.path.getsize(local_path)
        if file_size <= 0:
            return OperationResult(success=False, message="暂不支持上传空文件")
        parent_id = str(parent_path or "0")
        single_part_limit = self._get_single_part_upload_limit()
        current_resume_state = self._clone_resume_state(resume_state)

        return await self._upload_local_file_impl(
            local_path=local_path,
            target_name=target_name,
            parent_id=parent_id,
            file_size=file_size,
            progress_callback=progress_callback,
            conflict_policy=conflict_policy,
            state_callback=state_callback,
            current_resume_state=current_resume_state,
            single_part_limit=single_part_limit,
        )

    async def _upload_local_file_impl(
        self,
        *,
        local_path: str,
        target_name: str,
        parent_id: str,
        file_size: int,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        conflict_policy: str = "overwrite",
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        current_resume_state: Optional[Dict[str, Any]] = None,
        single_part_limit: Optional[int] = None,
    ) -> OperationResult:
        single_part_limit = int(single_part_limit or self._get_single_part_upload_limit())
        resume_state = self._clone_resume_state(current_resume_state)
        prepared_context = await self._prepare_115_upload_context(
            local_path=local_path,
            target_name=target_name,
            parent_id=parent_id,
            file_size=file_size,
            conflict_policy=conflict_policy,
            progress_callback=progress_callback,
            resume_state=resume_state,
            state_callback=state_callback,
        )
        skipped_result = prepared_context.get("skipped_result")
        if skipped_result is not None:
            return skipped_result

        resolved_name = str(prepared_context["resolved_name"])
        file_sha1 = str(prepared_context["file_sha1"])
        preid = str(prepared_context["preid"])

        should_resume_multipart = (
            bool(resume_state.get("pick_code"))
            and (
                str(resume_state.get("upload_phase") or "") == "multipart"
                or bool(resume_state.get("oss_upload_id"))
                or file_size > single_part_limit
            )
        )
        if should_resume_multipart:
            await self._notify_upload_progress(
                progress_callback,
                int(resume_state.get("uploaded_bytes") or 0),
                file_size,
                "正在恢复 115 分片上传",
            )
            callback_result = await self._resume_multipart_upload(
                local_path=local_path,
                file_size=file_size,
                file_sha1=file_sha1,
                parent_id=parent_id,
                resolved_name=resolved_name,
                resume_state=resume_state,
                progress_callback=progress_callback,
                state_callback=state_callback,
            )
            await self._notify_upload_progress(progress_callback, file_size, file_size, "上传成功")
            return await self._build_upload_success_result(
                callback_result=callback_result,
                parent_id=parent_id,
                file_name=resolved_name,
                file_size=file_size,
                file_sha1=file_sha1,
            )

        init_data = await self._init_upload(
            file_name=resolved_name,
            file_size=file_size,
            parent_id=parent_id,
            file_sha1=file_sha1,
            preid=preid,
        )
        init_data = await self._complete_secondary_verification_if_needed(
            init_data=init_data,
            local_path=local_path,
            file_name=resolved_name,
            file_size=file_size,
            parent_id=parent_id,
            file_sha1=file_sha1,
            preid=preid,
        )
        await self._persist_resume_state(
            resume_state,
            state_callback,
            pick_code=str(init_data.get("pick_code") or resume_state.get("pick_code") or ""),
            upload_phase="single_part" if file_size <= single_part_limit else "multipart",
        )

        status = int(init_data.get("status") or 0)
        if status == 2:
            await self._notify_upload_progress(progress_callback, file_size, file_size, "秒传成功")
            return await self._build_rapid_upload_result(
                init_data=init_data,
                parent_id=parent_id,
                file_name=resolved_name,
                file_size=file_size,
                file_sha1=file_sha1,
            )
        if status != 1:
            raise Exception(f"115 上传初始化返回未知状态: {status}")

        await self._notify_upload_progress(progress_callback, 0, file_size, "正在获取上传凭证")
        token_data = await self._get_upload_token()
        if file_size <= single_part_limit:
            callback_result = await self._single_part_upload_to_oss(
                local_path=local_path,
                file_size=file_size,
                file_sha1=file_sha1,
                token_data=token_data,
                init_data=init_data,
                progress_callback=progress_callback,
            )
        else:
            callback_result = await self._multipart_upload_to_oss(
                local_path=local_path,
                file_size=file_size,
                file_sha1=file_sha1,
                token_data=token_data,
                init_data=init_data,
                progress_callback=progress_callback,
                resume_state=resume_state,
                state_callback=state_callback,
            )

        await self._notify_upload_progress(progress_callback, file_size, file_size, "上传成功")
        return await self._build_upload_success_result(
            callback_result=callback_result,
            parent_id=parent_id,
            file_name=resolved_name,
            file_size=file_size,
            file_sha1=file_sha1,
        )

    async def _prepare_115_upload_context(
        self,
        *,
        local_path: str,
        target_name: str,
        parent_id: str,
        file_size: int,
        conflict_policy: str,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        resume_state: Optional[Dict[str, Any]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    ) -> Dict[str, Any]:
        """准备 115 上传上下文，包括目标名、哈希和续传基础状态。"""
        current_resume_state = self._clone_resume_state(resume_state)
        await self._persist_resume_state(
            current_resume_state,
            state_callback,
            parent_id=parent_id,
            file_size=file_size,
            conflict_policy=conflict_policy,
            target=self._build_upload_target(parent_id),
        )
        resumed_uploaded_bytes = int(current_resume_state.get("uploaded_bytes") or 0)

        resolved_name = str(current_resume_state.get("resolved_name") or "").strip()
        if not resolved_name:
            await self._notify_upload_progress(progress_callback, resumed_uploaded_bytes, file_size, "正在检查目标目录")
            resolved_name, skipped_result = await self._resolve_upload_target_name(
                parent_id,
                target_name,
                conflict_policy,
            )
            if skipped_result is not None:
                await self._notify_upload_progress(progress_callback, file_size, file_size, skipped_result.message)
                return {"resume_state": current_resume_state, "skipped_result": skipped_result}
            await self._persist_resume_state(
                current_resume_state,
                state_callback,
                resolved_name=resolved_name,
            )

        file_sha1 = str(current_resume_state.get("file_sha1") or "").strip().upper()
        preid = str(current_resume_state.get("preid") or "").strip().upper()
        if not file_sha1 or not preid:
            await self._notify_upload_progress(progress_callback, resumed_uploaded_bytes, file_size, "正在计算文件哈希")
            file_sha1 = await asyncio.to_thread(self._calculate_file_sha1, local_path)
            preid = await asyncio.to_thread(self._calculate_partial_sha1, local_path)
            await self._persist_resume_state(
                current_resume_state,
                state_callback,
                file_sha1=file_sha1,
                preid=preid,
            )

        return {
            "resume_state": current_resume_state,
            "resolved_name": resolved_name,
            "file_sha1": file_sha1,
            "preid": preid,
            "skipped_result": None,
        }

    async def _save_upload_to_tempfile(self, upload_file: UploadFile) -> str:
        suffix = os.path.splitext(upload_file.filename or "")[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            while True:
                chunk = await upload_file.read(1024 * 1024)
                if not chunk:
                    break
                temp_file.write(chunk)
            return temp_file.name

    async def _resolve_upload_target_name(
        self,
        parent_id: str,
        file_name: str,
        conflict_policy: str,
    ) -> tuple[str, Optional[OperationResult]]:
        existing_items = await self.list_files(parent_id)
        name_map = {item.name.lower(): item for item in existing_items}
        existing_item = name_map.get(file_name.lower())
        if not existing_item:
            return file_name, None

        if existing_item.is_dir:
            raise Exception(f"目标目录已存在同名文件夹: {file_name}")

        normalized_policy = str(conflict_policy or "overwrite").strip().lower()
        if normalized_policy == "skip":
            return file_name, OperationResult(
                success=True,
                message=f"文件 '{file_name}' 已存在，已跳过",
                data={
                    "file_id": existing_item.id,
                    "file_name": existing_item.name,
                    "parent_id": parent_id,
                    "parent_path": parent_id,
                    "skipped": True,
                },
            )

        if normalized_policy == "keep_both":
            return self._generate_available_name(file_name, existing_items), None

        if normalized_policy != "overwrite":
            raise Exception(f"不支持的冲突处理策略: {conflict_policy}")

        delete_result = await self.delete_file(existing_item.id)
        if not delete_result.success:
            raise Exception(delete_result.message or "删除同名文件失败")
        return file_name, None

    def _generate_available_name(self, file_name: str, existing_items: List[FileItem]) -> str:
        existing_names = {item.name.lower() for item in existing_items}
        stem, ext = os.path.splitext(file_name)
        index = 1
        candidate = file_name
        while candidate.lower() in existing_names:
            candidate = f"{stem} ({index}){ext}"
            index += 1
        return candidate

    async def _init_upload(
        self,
        *,
        file_name: str,
        file_size: int,
        parent_id: str,
        file_sha1: str,
        preid: str,
        pick_code: str = "",
        sign_key: str = "",
        sign_val: str = "",
    ) -> Dict[str, Any]:
        data = {
            "file_name": file_name,
            "file_size": str(file_size),
            "target": self._build_upload_target(parent_id),
            "fileid": file_sha1,
            "preid": preid,
            "topupload": "0",
        }
        if pick_code:
            data["pick_code"] = pick_code
        if sign_key:
            data["sign_key"] = sign_key
        if sign_val:
            data["sign_val"] = sign_val

        response = await self._api_request(
            "upload_init",
            "POST",
            data=data,
        )
        return self._extract_response_data(response)

    async def _complete_secondary_verification_if_needed(
        self,
        *,
        init_data: Dict[str, Any],
        local_path: str,
        file_name: str,
        file_size: int,
        parent_id: str,
        file_sha1: str,
        preid: str,
    ) -> Dict[str, Any]:
        status = int(init_data.get("status") or 0)
        if status not in {6, 7, 8}:
            return init_data

        sign_check = str(
            init_data.get("sign_check")
            or init_data.get("signCheck")
            or ""
        ).strip()
        sign_key = str(
            init_data.get("sign_key")
            or init_data.get("signKey")
            or ""
        ).strip()
        if not sign_check or not sign_key:
            raise Exception("115 上传需要二次认证，但未返回 sign_check 或 sign_key")

        sign_val = await asyncio.to_thread(self._calculate_range_sha1, local_path, sign_check)
        return await self._init_upload(
            file_name=file_name,
            file_size=file_size,
            parent_id=parent_id,
            file_sha1=file_sha1,
            preid=preid,
            pick_code=str(init_data.get("pick_code") or ""),
            sign_key=sign_key,
            sign_val=sign_val,
        )

    async def _get_upload_token(self) -> Dict[str, Any]:
        last_error: Optional[Exception] = None
        for method in ("GET", "POST"):
            try:
                response = await self._api_request("upload_get_token", method)
                token_data = self._normalize_upload_token_data(self._extract_response_data(response))
                if token_data:
                    return token_data
            except Exception as exc:
                last_error = exc
        raise Exception(f"获取 115 上传凭证失败: {last_error or '未返回凭证数据'}")

    async def _resume_upload(
        self,
        *,
        file_size: int,
        parent_id: str,
        file_sha1: str,
        pick_code: str,
    ) -> Dict[str, Any]:
        response = await self._api_request(
            "upload_resume",
            "POST",
            data={
                "file_size": str(file_size),
                "target": self._build_upload_target(parent_id),
                "fileid": file_sha1,
                "pick_code": pick_code,
            },
        )
        return self._extract_response_data(response)

    async def _resume_multipart_upload(
        self,
        *,
        local_path: str,
        file_size: int,
        file_sha1: str,
        parent_id: str,
        resolved_name: str,
        resume_state: Dict[str, Any],
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    ) -> Dict[str, Any]:
        pick_code = str(resume_state.get("pick_code") or "").strip()
        if not pick_code:
            raise Exception("115 续传缺少 pick_code")

        resume_data = await self._resume_upload(
            file_size=file_size,
            parent_id=parent_id,
            file_sha1=file_sha1,
            pick_code=pick_code,
        )

        bucket = str(resume_data.get("bucket") or "")
        object_name = str(resume_data.get("object") or "")
        if not bucket or not object_name:
            raise Exception("115 续传返回的 OSS 目标不完整")

        resume_oss_upload_id = str(resume_state.get("oss_upload_id") or "").strip()
        saved_bucket = str(resume_state.get("bucket") or "").strip()
        saved_object = str(resume_state.get("object") or "").strip()
        if resume_oss_upload_id and (saved_bucket != bucket or saved_object != object_name):
            resume_oss_upload_id = ""

        merged_resume_state = deepcopy(resume_state)
        merged_resume_state.update(
            {
                "resolved_name": resolved_name,
                "parent_id": parent_id,
                "file_sha1": file_sha1,
                "pick_code": str(resume_data.get("pick_code") or pick_code),
                "bucket": bucket,
                "object": object_name,
                "upload_phase": "multipart",
                "oss_upload_id": resume_oss_upload_id,
            }
        )
        await self._persist_resume_state(merged_resume_state, state_callback)

        await self._notify_upload_progress(
            progress_callback,
            int(merged_resume_state.get("uploaded_bytes") or 0),
            file_size,
            "正在获取上传凭证",
        )
        token_data = await self._get_upload_token()
        return await self._multipart_upload_to_oss(
            local_path=local_path,
            file_size=file_size,
            file_sha1=file_sha1,
            token_data=token_data,
            init_data=resume_data,
            progress_callback=progress_callback,
            resume_state=merged_resume_state,
            state_callback=state_callback,
        )

    async def _build_rapid_upload_result(
        self,
        *,
        init_data: Dict[str, Any],
        parent_id: str,
        file_name: str,
        file_size: int,
        file_sha1: str,
    ) -> OperationResult:
        file_id = str(init_data.get("file_id") or "")
        if file_id:
            try:
                file_info = await self.file_info(file_id)
                if file_info:
                    return OperationResult(
                        success=True,
                        message=f"文件 '{file_name}' 秒传成功",
                        data=self._build_115_upload_result_data(
                            file_id=file_info.id,
                            file_name=file_info.name,
                            parent_id=str(file_info.extra.get("parent_id") or parent_id),
                            file_size=file_info.size,
                            file_sha1=str(file_info.extra.get("hash_info") or file_sha1),
                            pick_code=str(file_info.extra.get("pick_code") or ""),
                        ),
                    )
            except Exception:
                pass

        return OperationResult(
            success=True,
            message=f"文件 '{file_name}' 秒传成功",
            data=self._build_115_upload_result_data(
                file_id=file_id,
                file_name=file_name,
                parent_id=parent_id,
                file_size=file_size,
                file_sha1=file_sha1,
                pick_code=str(init_data.get("pick_code") or ""),
            ),
        )

    async def _build_upload_success_result(
        self,
        *,
        callback_result: Dict[str, Any],
        parent_id: str,
        file_name: str,
        file_size: int,
        file_sha1: str,
    ) -> OperationResult:
        if isinstance(callback_result, dict):
            success, error_msg = OneOneFiveApiHelper.check_success(callback_result)
            if not success:
                raise Exception(error_msg or "115 上传完成回调失败")

        file_data = self._extract_response_data(callback_result)
        if not file_data and isinstance(callback_result, dict):
            file_data = callback_result.get("data") or {}

        file_id = str(file_data.get("file_id") or file_data.get("fid") or "")
        resolved_name = str(file_data.get("file_name") or file_name)
        resolved_parent_id = str(file_data.get("cid") or file_data.get("parent_id") or parent_id)

        if file_id:
            try:
                file_info = await self.file_info(file_id)
                if file_info and not file_info.is_dir:
                    return OperationResult(
                        success=True,
                        message=f"文件 '{file_info.name}' 上传成功",
                        data=self._build_115_upload_result_data(
                            file_id=file_info.id,
                            file_name=file_info.name,
                            parent_id=str(file_info.extra.get("parent_id") or resolved_parent_id),
                            file_size=file_info.size,
                            file_sha1=str(file_info.extra.get("hash_info") or file_sha1),
                            pick_code=str(file_info.extra.get("pick_code") or file_data.get("pick_code") or ""),
                        ),
                    )
            except Exception:
                pass

        try:
            await clear_operation_cache(str(self.account_id), "directory_update", parent_id=resolved_parent_id)
            files = await self.list_files(resolved_parent_id)
            matched = None
            for item in files:
                if item.is_dir or item.name != resolved_name:
                    continue
                if item.size and item.size != file_size:
                    continue
                matched = item
                break
            if matched:
                return OperationResult(
                    success=True,
                    message=f"文件 '{matched.name}' 上传成功",
                    data=self._build_115_upload_result_data(
                        file_id=matched.id,
                        file_name=matched.name,
                        parent_id=resolved_parent_id,
                        file_size=matched.size or file_size,
                        file_sha1=str(matched.extra.get("hash_info") or file_sha1),
                        pick_code=str(matched.extra.get("pick_code") or file_data.get("pick_code") or ""),
                    ),
                )
        except Exception:
            pass

        raise Exception("115 上传完成后未在网盘中确认到文件，已阻止误报成功")

    def _build_115_upload_result_data(
        self,
        *,
        file_id: str,
        file_name: str,
        parent_id: str,
        file_size: int,
        file_sha1: str,
        pick_code: str,
    ) -> Dict[str, Any]:
        """构造统一的 115 上传结果数据。"""
        resolved_parent_id = str(parent_id or "0")
        return {
            "file_id": file_id,
            "file_name": file_name,
            "parent_id": resolved_parent_id,
            "parent_path": resolved_parent_id,
            "file_size": file_size,
            "sha1": file_sha1,
            "pick_code": pick_code,
        }

    async def _multipart_upload_to_oss(
        self,
        *,
        local_path: str,
        file_size: int,
        file_sha1: str,
        token_data: Dict[str, Any],
        init_data: Dict[str, Any],
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        resume_state: Optional[Dict[str, Any]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    ) -> Dict[str, Any]:
        endpoint = self._normalize_oss_endpoint(str(token_data.get("endpoint") or ""))
        bucket = str(init_data.get("bucket") or "")
        object_name = str(init_data.get("object") or "")
        if not endpoint or not bucket or not object_name:
            raise Exception("115 上传凭证不完整，缺少 endpoint、bucket 或 object")

        part_size = self._calculate_oss_part_size(file_size)
        total_parts = max(1, (file_size + part_size - 1) // part_size)
        current_resume_state = deepcopy(resume_state) if isinstance(resume_state, dict) else {}
        upload_id = ""
        uploaded_bytes = 0
        parts: List[Dict[str, str]] = []
        completed_part_numbers = set()

        candidate_upload_id = str(current_resume_state.get("oss_upload_id") or "").strip()
        saved_bucket = str(current_resume_state.get("bucket") or "").strip()
        saved_object = str(current_resume_state.get("object") or "").strip()
        if candidate_upload_id and saved_bucket == bucket and saved_object == object_name:
            try:
                existing_parts = await self._oss_list_uploaded_parts(
                    endpoint=endpoint,
                    bucket=bucket,
                    object_name=object_name,
                    upload_id=candidate_upload_id,
                    token_data=token_data,
                )
                upload_id = candidate_upload_id
                for part in existing_parts:
                    part_number = int(part["part_number"])
                    parts.append({"part_number": str(part_number), "etag": str(part["etag"])})
                    completed_part_numbers.add(part_number)
                    uploaded_bytes += int(part["size"])
                if uploaded_bytes > 0 and progress_callback:
                    await progress_callback(uploaded_bytes, file_size, f"正在继续上传到115网盘，分片（{min(len(completed_part_numbers) + 1, total_parts)}/{total_parts}）")
            except Exception:
                upload_id = ""
                uploaded_bytes = 0
                parts = []
                completed_part_numbers = set()

        if not upload_id:
            upload_id = await self._oss_initiate_multipart_upload(
                endpoint=endpoint,
                bucket=bucket,
                object_name=object_name,
                token_data=token_data,
            )

        await self._persist_resume_state(
            current_resume_state,
            state_callback,
            upload_phase="multipart",
            bucket=bucket,
            object=object_name,
            pick_code=str(current_resume_state.get("pick_code") or init_data.get("pick_code") or ""),
            file_sha1=file_sha1,
            oss_upload_id=upload_id,
            uploaded_bytes=uploaded_bytes,
            progress=min(99, int(uploaded_bytes * 100 / max(file_size, 1))) if uploaded_bytes < file_size else 100,
        )

        with open(local_path, "rb") as file_obj:
            part_number = 1
            offset = 0
            while offset < file_size:
                current_part_size = min(part_size, file_size - offset)
                if part_number in completed_part_numbers:
                    file_obj.seek(offset + current_part_size)
                    offset += current_part_size
                    part_number += 1
                    continue
                etag = await self._oss_upload_part(
                    endpoint=endpoint,
                    bucket=bucket,
                    object_name=object_name,
                    upload_id=upload_id,
                    part_number=part_number,
                    file_obj=file_obj,
                    part_size=current_part_size,
                    token_data=token_data,
                    uploaded_offset=offset,
                    total_size=file_size,
                    progress_callback=progress_callback,
                    total_parts=total_parts,
                )
                parts.append({"part_number": str(part_number), "etag": etag})
                completed_part_numbers.add(part_number)
                uploaded_bytes += current_part_size
                offset += current_part_size
                await self._persist_resume_state(
                    current_resume_state,
                    state_callback,
                    uploaded_bytes=uploaded_bytes,
                    progress=min(99, int(uploaded_bytes * 100 / max(file_size, 1))) if uploaded_bytes < file_size else 100,
                )
                part_number += 1

        return await self._oss_complete_multipart_upload(
            endpoint=endpoint,
            bucket=bucket,
            object_name=object_name,
            upload_id=upload_id,
            parts=parts,
            token_data=token_data,
            init_data=init_data,
            file_sha1=file_sha1,
        )

    async def _single_part_upload_to_oss(
        self,
        *,
        local_path: str,
        file_size: int,
        file_sha1: str,
        token_data: Dict[str, Any],
        init_data: Dict[str, Any],
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    ) -> Dict[str, Any]:
        endpoint = self._normalize_oss_endpoint(str(token_data.get("endpoint") or ""))
        bucket = str(init_data.get("bucket") or "")
        object_name = str(init_data.get("object") or "")
        if not endpoint or not bucket or not object_name:
            raise Exception("115 上传凭证不完整，缺少 endpoint、bucket 或 object")

        callback, callback_var = self._extract_oss_callback_headers(init_data, file_sha1=file_sha1)
        headers = self._build_oss_headers(
            method="PUT",
            bucket=bucket,
            object_name=object_name,
            access_key_id=str(token_data.get("access_key_id") or token_data.get("accessKeyId") or ""),
            access_key_secret=str(token_data.get("access_key_secret") or token_data.get("accessKeySecret") or ""),
            security_token=str(token_data.get("security_token") or token_data.get("securityToken") or ""),
            content_length=file_size,
            content_type="application/octet-stream",
            callback=callback,
            callback_var=callback_var,
        )
        url = self._build_oss_url(endpoint, bucket, object_name)

        async def payload():
            sent = 0
            with open(local_path, "rb") as file_obj:
                while True:
                    chunk = file_obj.read(1024 * 1024)
                    if not chunk:
                        break
                    sent += len(chunk)
                    if progress_callback:
                        await progress_callback(sent, file_size, "正在上传到115网盘，分片（1/1）")
                    yield chunk

        async with self._session.put(url, headers=headers, data=payload()) as response:
            body_bytes = await response.read()
            if response.status != 200:
                try:
                    text = body_bytes.decode("utf-8", errors="ignore")
                except Exception:
                    text = str(body_bytes)
                raise Exception(f"115 单次上传失败: HTTP {response.status}, {text}")

        return self._parse_json_bytes(body_bytes, "解析 115 单次上传回调结果失败")

    async def _oss_initiate_multipart_upload(
        self,
        *,
        endpoint: str,
        bucket: str,
        object_name: str,
        token_data: Dict[str, Any],
    ) -> str:
        query = {"uploads": None, "sequential": None}
        url = self._build_oss_url(endpoint, bucket, object_name, query)
        headers = self._build_oss_headers(
            method="POST",
            bucket=bucket,
            object_name=object_name,
            access_key_id=str(token_data.get("access_key_id") or token_data.get("accessKeyId") or ""),
            access_key_secret=str(token_data.get("access_key_secret") or token_data.get("accessKeySecret") or ""),
            security_token=str(token_data.get("security_token") or token_data.get("securityToken") or ""),
            subresources=query,
            content_length=0,
            content_type="application/octet-stream",
        )
        async with self._session.post(url, headers=headers, data=b"") as response:
            if response.status != 200:
                raise Exception(f"初始化 OSS 分片上传失败: HTTP {response.status}, {await response.text()}")
            xml_text = await response.text()

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            raise Exception(f"解析 OSS 初始化响应失败 {exc}") from exc

        upload_id = root.findtext(".//UploadId") or ""
        if not upload_id:
            raise Exception("OSS 初始化分片上传未返回 UploadId")
        return upload_id

    async def _oss_list_uploaded_parts(
        self,
        *,
        endpoint: str,
        bucket: str,
        object_name: str,
        upload_id: str,
        token_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        parts: List[Dict[str, Any]] = []
        part_number_marker = 0

        while True:
            query = {
                "uploadId": upload_id,
                "part-number-marker": str(part_number_marker),
            }
            url = self._build_oss_url(endpoint, bucket, object_name, query)
            headers = self._build_oss_headers(
                method="GET",
                bucket=bucket,
                object_name=object_name,
                access_key_id=str(token_data.get("access_key_id") or token_data.get("accessKeyId") or ""),
                access_key_secret=str(token_data.get("access_key_secret") or token_data.get("accessKeySecret") or ""),
                security_token=str(token_data.get("security_token") or token_data.get("securityToken") or ""),
                subresources=query,
            )
            async with self._session.get(url, headers=headers) as response:
                if response.status != 200:
                    raise Exception(f"获取 OSS 已上传分片失败 HTTP {response.status}, {await response.text()}")
                xml_text = await response.text()

            try:
                root = ET.fromstring(xml_text)
            except ET.ParseError as exc:
                raise Exception(f"解析 OSS 已上传分片响应失败 {exc}") from exc

            for part in root.findall(".//Part"):
                part_number = int(part.findtext("PartNumber") or 0)
                etag = str(part.findtext("ETag") or "").strip().strip('"')
                size = int(part.findtext("Size") or 0)
                if part_number > 0 and etag:
                    parts.append(
                        {
                            "part_number": part_number,
                            "etag": etag,
                            "size": size,
                        }
                    )

            is_truncated = str(root.findtext(".//IsTruncated") or "").strip().lower() == "true"
            if not is_truncated:
                break
            part_number_marker = int(root.findtext(".//NextPartNumberMarker") or 0)

        parts.sort(key=lambda item: int(item["part_number"]))
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
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        total_parts: int = 1,
    ) -> str:
        query = {
            "partNumber": str(part_number),
            "uploadId": upload_id,
        }
        url = self._build_oss_url(endpoint, bucket, object_name, query)
        headers = self._build_oss_headers(
            method="PUT",
            bucket=bucket,
            object_name=object_name,
            access_key_id=str(token_data.get("access_key_id") or token_data.get("accessKeyId") or ""),
            access_key_secret=str(token_data.get("access_key_secret") or token_data.get("accessKeySecret") or ""),
            security_token=str(token_data.get("security_token") or token_data.get("securityToken") or ""),
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
                if progress_callback:
                    await progress_callback(
                        uploaded_offset + sent,
                        total_size,
                        f"正在上传到115网盘，分片（{part_number}/{total_parts}）",
                    )
                yield chunk

        async with self._session.put(url, headers=headers, data=payload()) as response:
            if response.status != 200:
                raise Exception(
                    f"上传 115 OSS 分片失败(part {part_number}): HTTP {response.status}, {await response.text()}"
                )
            etag = response.headers.get("ETag", "").strip().strip('"')

        if not etag:
            raise Exception(f"上传 115 OSS 分片失败(part {part_number})，未返回 ETag")
        return etag

    async def _oss_complete_multipart_upload(
        self,
        *,
        endpoint: str,
        bucket: str,
        object_name: str,
        upload_id: str,
        parts: List[Dict[str, str]],
        token_data: Dict[str, Any],
        init_data: Dict[str, Any],
        file_sha1: str,
    ) -> Dict[str, Any]:
        root = ET.Element("CompleteMultipartUpload")
        for part in parts:
            part_node = ET.SubElement(root, "Part")
            ET.SubElement(part_node, "PartNumber").text = str(part["part_number"])
            ET.SubElement(part_node, "ETag").text = f"\"{part['etag']}\""
        body = ET.tostring(root, encoding="utf-8", xml_declaration=True)

        query = {"uploadId": upload_id}
        url = self._build_oss_url(endpoint, bucket, object_name, query)
        callback, callback_var = self._extract_oss_callback_headers(init_data, file_sha1=file_sha1)
        headers = self._build_oss_headers(
            method="POST",
            bucket=bucket,
            object_name=object_name,
            access_key_id=str(token_data.get("access_key_id") or token_data.get("accessKeyId") or ""),
            access_key_secret=str(token_data.get("access_key_secret") or token_data.get("accessKeySecret") or ""),
            security_token=str(token_data.get("security_token") or token_data.get("securityToken") or ""),
            subresources=query,
            content_length=len(body),
            content_type="application/xml",
            callback=callback,
            callback_var=callback_var,
        )
        async with self._session.post(url, headers=headers, data=body) as response:
            if response.status != 200:
                raise Exception(f"完成 115 OSS 分片上传失败: HTTP {response.status}, {await response.text()}")
            body_bytes = await response.read()

        return self._parse_json_bytes(body_bytes, "解析 115 OSS 回调结果失败")

    def _build_upload_target(self, parent_id: str) -> str:
        return f"U_1_{parent_id or '0'}"

    def _extract_response_data(self, response: Any) -> Dict[str, Any]:
        if isinstance(response, dict):
            data = response.get("data")
            if isinstance(data, dict):
                merged = {k: v for k, v in response.items() if k != "data"}
                merged.update(data)
                return merged
            return response
        return {}

    def _normalize_upload_token_data(self, token_data: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(token_data, dict):
            return {}

        normalized = dict(token_data)
        alias_map = {
            "access_key_id": ["access_key_id", "accessKeyId", "AccessKeyId"],
            "access_key_secret": ["access_key_secret", "accessKeySecret", "AccessKeySecret"],
            "security_token": ["security_token", "securityToken", "SecurityToken"],
            "endpoint": ["endpoint", "endPoint", "Endpoint", "EndPoint"],
        }
        for canonical_key, aliases in alias_map.items():
            if normalized.get(canonical_key):
                continue
            for alias in aliases:
                value = normalized.get(alias)
                if value:
                    normalized[canonical_key] = value
                    break
        return normalized

    async def _notify_upload_progress(
        self,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]],
        uploaded_bytes: int,
        total_bytes: int,
        message: str,
    ) -> None:
        if progress_callback:
            await progress_callback(uploaded_bytes, total_bytes, message)

    def _calculate_file_sha1(self, file_path: str) -> str:
        sha1 = hashlib.sha1()
        with open(file_path, "rb") as file_obj:
            while True:
                chunk = file_obj.read(1024 * 1024)
                if not chunk:
                    break
                sha1.update(chunk)
        return sha1.hexdigest().upper()

    def _calculate_partial_sha1(self, file_path: str, length: int = 128 * 1024) -> str:
        sha1 = hashlib.sha1()
        with open(file_path, "rb") as file_obj:
            sha1.update(file_obj.read(length))
        return sha1.hexdigest().upper()

    def _calculate_range_sha1(self, file_path: str, range_spec: str) -> str:
        start_str, end_str = str(range_spec).split("-", 1)
        start = int(start_str)
        end = int(end_str)
        if end < start:
            raise ValueError(f"非法的 sign_check 范围: {range_spec}")

        sha1 = hashlib.sha1()
        length = end - start + 1
        with open(file_path, "rb") as file_obj:
            file_obj.seek(start)
            remaining = length
            while remaining > 0:
                chunk = file_obj.read(min(1024 * 1024, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                sha1.update(chunk)
        return sha1.hexdigest().upper()

    def _calculate_oss_part_size(self, file_size: int) -> int:
        mb = 1024 * 1024
        gb = 1024 * mb
        tb = 1024 * gb
        part_size = 20 * mb
        if file_size > part_size:
            if file_size > tb:
                part_size = 5 * gb
            elif file_size > 768 * gb:
                part_size = 109951163
            elif file_size > 512 * gb:
                part_size = 82463373
            elif file_size > 384 * gb:
                part_size = 54975582
            elif file_size > 256 * gb:
                part_size = 41231687
            elif file_size > 128 * gb:
                part_size = 27487791
        return int(part_size)

    def _normalize_oss_endpoint(self, endpoint: str) -> str:
        normalized = str(endpoint or "").strip()
        if not normalized:
            return ""
        if not normalized.startswith(("http://", "https://")):
            normalized = f"https://{normalized}"
        return normalized.rstrip("/")

    def _build_oss_url(
        self,
        endpoint: str,
        bucket: str,
        object_name: str,
        query: Optional[Dict[str, Optional[str]]] = None,
    ) -> str:
        object_key = str(object_name or "").lstrip("/")
        encoded_object = quote(object_key, safe="/-_.~")
        scheme, host = endpoint.split("://", 1)
        url = f"{scheme}://{bucket}.{host}/{encoded_object}"
        if not query:
            return url

        items = []
        for key in sorted(query.keys()):
            value = query[key]
            if value is None or value == "":
                items.append(quote(str(key), safe="-_.~"))
            else:
                items.append(
                    f"{quote(str(key), safe='-_.~')}={quote(str(value), safe='-_.~')}"
                )
        return f"{url}?{'&'.join(items)}"

    def _build_oss_headers(
        self,
        *,
        method: str,
        bucket: str,
        object_name: str,
        access_key_id: str,
        access_key_secret: str,
        security_token: str,
        subresources: Optional[Dict[str, Optional[str]]] = None,
        content_length: Optional[int] = None,
        content_type: str = "",
        callback: str = "",
        callback_var: str = "",
    ) -> Dict[str, str]:
        if not access_key_id or not access_key_secret or not security_token:
            raise Exception("115 上传凭证缺少 AccessKey 或 SecurityToken")

        headers: Dict[str, str] = {
            "Date": formatdate(usegmt=True),
            "x-oss-security-token": security_token,
        }
        if content_type:
            headers["Content-Type"] = content_type
        if content_length is not None:
            headers["Content-Length"] = str(content_length)
        if callback:
            headers["x-oss-callback"] = base64.b64encode(callback.encode("utf-8")).decode("utf-8")
        if callback_var:
            headers["x-oss-callback-var"] = base64.b64encode(callback_var.encode("utf-8")).decode("utf-8")

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
        canonical_oss_headers = self._build_canonical_oss_headers(headers)
        canonical_resource = self._build_canonical_oss_resource(bucket, object_name, subresources)
        string_to_sign = "\n".join(
            [
                method.upper(),
                headers.get("Content-MD5", ""),
                headers.get("Content-Type", ""),
                headers.get("Date", ""),
                f"{canonical_oss_headers}{canonical_resource}",
            ]
        )
        digest = hmac.new(
            access_key_secret.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            hashlib.sha1,
        ).digest()
        signature = base64.b64encode(digest).decode("utf-8")
        return f"OSS {access_key_id}:{signature}"

    def _build_canonical_oss_headers(self, headers: Dict[str, str]) -> str:
        oss_headers = []
        for key, value in headers.items():
            lowered = key.lower().strip()
            if lowered.startswith("x-oss-"):
                normalized_value = " ".join(str(value).strip().split())
                oss_headers.append((lowered, normalized_value))
        oss_headers.sort(key=lambda item: item[0])
        return "".join(f"{key}:{value}\n" for key, value in oss_headers)

    def _build_canonical_oss_resource(
        self,
        bucket: str,
        object_name: str,
        subresources: Optional[Dict[str, Optional[str]]] = None,
    ) -> str:
        object_key = str(object_name or "").lstrip("/")
        resource = f"/{bucket}/{object_key}"
        if not subresources:
            return resource

        allowed = {"uploads", "uploadId", "partNumber", "sequential"}
        items = []
        for key in sorted(subresources.keys()):
            if key not in allowed:
                continue
            value = subresources[key]
            if value is None or value == "":
                items.append(str(key))
            else:
                items.append(f"{key}={value}")
        if items:
            resource = f"{resource}?{'&'.join(items)}"
        return resource

    def _extract_oss_callback_headers(self, init_data: Dict[str, Any], file_sha1: str = "") -> tuple[str, str]:
        callback_data = init_data.get("callback")
        if isinstance(callback_data, dict):
            if "callback" in callback_data or "callback_var" in callback_data:
                callback = str(callback_data.get("callback") or "")
                callback_var = str(callback_data.get("callback_var") or "")
                if file_sha1:
                    callback = callback.replace("${sha1}", file_sha1)
                return (callback, callback_var)
            value = callback_data.get("value")
            if isinstance(value, dict):
                callback = str(value.get("callback") or "")
                callback_var = str(value.get("callback_var") or "")
                if file_sha1:
                    callback = callback.replace("${sha1}", file_sha1)
                return (callback, callback_var)

        callback = str(init_data.get("callback") or "")
        callback_var = str(init_data.get("callback_var") or "")
        if file_sha1:
            callback = callback.replace("${sha1}", file_sha1)
        return (callback, callback_var)

    def _parse_json_bytes(self, body: bytes, error_message: str) -> Dict[str, Any]:
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            text = body.decode("utf-8", errors="ignore")

        try:
            return json.loads(text)
        except Exception as exc:
            raise Exception(f"{error_message}: {text}") from exc

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} access_token={self.config.access_token[:12]}...>"



