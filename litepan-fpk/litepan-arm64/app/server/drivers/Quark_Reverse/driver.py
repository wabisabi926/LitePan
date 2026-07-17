"""夸克网盘驱动核心业务方法"""

import asyncio
import base64
import hashlib
import json
import mimetypes
import aiohttp
import os
import re
import tempfile
import time
from http.cookies import SimpleCookie
from email.utils import formatdate
from typing import List, Optional, Dict, Any, Awaitable, Callable
from datetime import datetime, timezone
from fastapi import UploadFile
from core.base import FileItem, OperationResult, DriverInfo
from core.driver_base import BaseDriver
from core.operation_wrapper import (
    auto_cleanup_cache, 
    with_file_list_cache, 
    with_file_info_cache, 
    with_performance_tracking,
    with_auth_retry,
    clear_operation_cache
)
from .config import QuarkReverseConfig
from .models import QuarkFile
from .api import QuarkAPI, QuarkConstants, QuarkApiHelper


class QuarkReverseDriver(BaseDriver):
    def __init__(self, config: QuarkReverseConfig):
        super().__init__(config)
        self.cookie = config.cookie
        self._session: Optional[aiohttp.ClientSession] = None
        self._cookie_update_callback = None
        self._cookie_update_lock = asyncio.Lock()
        self._last_cookie_change_detected = False

    @classmethod
    def get_info(cls) -> DriverInfo:
        return DriverInfo(
            name="夸克网盘",
            display_name="夸克网盘",
            version="3.1.0",
            capabilities=["list", "info", "download", "create_folder", "delete", "batch_delete", "rename", "move", "copy", "upload"],
            description="夸克网盘逆向API接入",
            author="LitePan"
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
        if not self._session or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30.0)
            headers = QuarkApiHelper.build_headers(self.cookie)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers=headers,
                cookie_jar=aiohttp.DummyCookieJar()
            )
        await self.sync_runtime_auth_state()
        self._log.debug("夸克网盘驱动初始化完成", driver_name="quark_reverse")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._log.debug("夸克网盘驱动已关闭", driver_name="quark_reverse")

    async def sync_runtime_auth_state(self, config: Optional[Dict[str, Any]] = None) -> None:

        if isinstance(config, dict) and "cookie" in config:
            self.cookie = config.get("cookie") or ""
        elif not self.cookie and getattr(self.config, "cookie", None):
            self.cookie = self.config.cookie or ""

        self.config.cookie = self.cookie

        if self._session and not self._session.closed:
            if self.cookie:
                self._session.headers["Cookie"] = self.cookie
            else:
                self._session.headers.pop("Cookie", None)

    def _resolve_download_user_agent(self, user_agent: str = None) -> str:
        if user_agent:
            normalized_user_agent = user_agent.lower()
            if "quark-cloud-drive" in normalized_user_agent or "uc-cloud-drive" in normalized_user_agent:
                return user_agent
        return QuarkConstants.QUARK_USER_AGENT

    async def test_connection(self) -> OperationResult:
        try:
            self._log.debug("开始测试夸克网盘连接", driver_name="quark_reverse")

            if not self.cookie:
                return OperationResult(success=False, message="Cookie为空")

            list_response = await self._api_request("file_list", "GET", params={
                "pdir_fid": self.config.root_folder_id,
                "_page": 1,
                "_size": 20,
                "_fetch_total": 1
            })

            download_probe = await self._probe_download_channel(list_response)
            if not download_probe.success:
                return OperationResult(
                    success=True,
                    message=f"列表正常，但{download_probe.message}",
                    data={"warning": "download_channel_degraded", "detail": download_probe.message},
                )

            self._log.debug("夸克网盘连接测试成功", driver_name="quark_reverse")
            return OperationResult(success=True, message="夸克网盘连接测试成功")

        except Exception as e:
            error_msg = f"连接测试失败: {str(e)}"
            self._log.error(f"夸克网盘 {error_msg}", driver_name="quark_reverse")
            return OperationResult(success=False, message=error_msg)

    async def _probe_download_channel(self, list_response: Optional[Dict[str, Any]] = None) -> OperationResult:

        try:
            if list_response is None:
                return OperationResult(success=True, message="")

            sample_fid: Optional[str] = None
            for item in list_response.get("data", {}).get("list", []) or []:
                if not item:
                    continue
                if item.get("dir") or item.get("status") == 2:
                    continue
                fid_value = item.get("fid")
                if fid_value:
                    sample_fid = str(fid_value)
                    break

            if not sample_fid:
                return OperationResult(success=True, message="")

            try:
                await self._request_download_info_once(sample_fid)
                return OperationResult(success=True, message="")
            except Exception as e:
                return OperationResult(
                    success=False,
                    message=f"下载通道不可用（{str(e)[:80]}），建议重新抓取 Cookie",
                )

        except Exception as e:
            message = str(e)
            self._log.warning(f"下载通道探测异常: {message}", driver_name="quark_reverse")
            return OperationResult(
                success=False,
                message=f"下载通道异常（{message[:80]}），建议重新抓取 Cookie",
            )

    async def _apply_operation_delay(self) -> None:
        await self.wait_for_request_interval()

    @with_auth_retry(max_retries=1)
    async def _api_request(self, operation: str, method: str, benign_codes: Optional[set] = None, **kwargs) -> Dict[str, Any]:
        if not self._session:
            raise Exception("HTTP会话未初始化")
        self._last_cookie_change_detected = False
        
        if 'params' in kwargs:
            kwargs['params'] = QuarkApiHelper.map_params(kwargs['params'], operation)
        if 'json' in kwargs:
            kwargs['json'] = QuarkApiHelper.map_params(kwargs['json'], operation)
        
        if 'params' not in kwargs:
            kwargs['params'] = {}
        kwargs['params'].update(QuarkConstants.DEFAULT_PARAMS)

        op_params = QuarkConstants.OPERATION_PARAMS.get(operation)
        if op_params:
            for key, value in op_params.items():
                kwargs['params'].setdefault(key, value)

        if operation == "rename" and 'params' in kwargs:
            kwargs['params']['uc_param_str'] = ''
        
        url = QuarkAPI.BASE_URL + QuarkAPI.ENDPOINTS[operation]
        
        headers = {}
        custom_headers = kwargs.pop('custom_headers', {})
        if custom_headers:
            headers.update(custom_headers)
        
        if headers:
            kwargs['headers'] = headers
        
        await self._apply_operation_delay()
        async with self._session.request(method, url, **kwargs) as response:
            self._last_cookie_change_detected = await self._update_cookies_from_response(response)
            response_text = await response.text()

            # 良性业务码（如删除时“文件已删除”）短路返回，避免把幂等结果当错误打日志/抛异常
            if benign_codes:
                try:
                    import json as _json
                    _peek = _json.loads(response_text)
                except Exception:
                    _peek = None
                if isinstance(_peek, dict) and _peek.get("code") in benign_codes:
                    return _peek

            if response.status >= 400:
                self._log.error(f"API请求失败:", 
                            details={
                                "status": response.status,
                                "url": url,
                                "response_text": response_text[:500]
                            }, driver_name="quark_reverse")
                
                try:
                    import json as _json
                    err_data = _json.loads(response_text)
                    self._log.error(f"业务错误详情: {err_data}", driver_name="quark_reverse")
                except:
                    pass
                
                if response.status == 401:
                    if self.is_connectivity_test():
                        raise Exception("Cookie认证失败，请检查cookie是否有效或已过期")
                    await self._handle_auth_error("Cookie认证失败")
                    raise Exception("Cookie认证失败，请检查cookie是否有效或已过期")
                elif response.status == 403:
                    if self.is_connectivity_test():
                        raise Exception("访问被拒绝，可能是cookie权限不足")
                    await self._handle_auth_error("访问被拒绝")
                    raise Exception("访问被拒绝，可能是cookie权限不足")
                else:
                    raise Exception(f"HTTP {response.status}: {response_text}")
            
            try:
                import json as _json
                data = _json.loads(response_text)
            except Exception:
                raise Exception(f"API返回非JSON内容: {response_text[:200]}")
            
            success, error_msg = QuarkApiHelper.check_success(data)
            if not success:
                raise Exception(error_msg)
            
            return data
    
    
    @with_file_list_cache
    async def list_files(self, parent_id: str = "0") -> List[FileItem]:
        # "0" 上层约定成根目录，这里翻译成账号配置的真实根目录 ID
        api_parent_id = parent_id if parent_id != "0" else self.config.root_folder_id

        self._log.debug(f"获取文件列表: parent_id={parent_id}, api_parent_id={api_parent_id}", driver_name="quark_reverse")

        all_files = []
        page = 1
        page_size = 200  # 夸克每页上限

        while True:
            response = await self._api_request("file_list", "GET", params={
                "pdir_fid": api_parent_id,
                "_page": page,
                "_size": page_size,
                "_fetch_total": 1,
                "fetch_all_file": "1",
                "fetch_risk_file_name": "1"
            })
            
            files_data = response.get("data", {}).get("list", [])
            
            self._log.debug(f"文件列表响应: count={len(files_data)}, response_keys={list(response.keys())}", driver_name="quark_reverse")
            
            if not files_data:
                break
                
            for file_data in files_data:
                quark_file = QuarkFile.from_dict(file_data)
                if not quark_file.is_trashed():
                    all_files.append(quark_file.to_file_item())
            
            if len(files_data) < page_size:
                break
                
            page += 1
            
        return all_files
    
    @with_file_info_cache
    async def file_info(self, file_id: str) -> Optional[FileItem]:
        """夸克没有单文件详情接口，只能从根目录列表里命中；命中不到就退化成占位 FileItem，让下载流程继续。"""
        try:
            response = await self._api_request("file_list", "GET", params={
                "pdir_fid": self.config.root_folder_id,
                "_page": 1,
                "_size": 1000,
                "_fetch_total": 1
            })
            
            if response and 'data' in response:
                files_data = response.get("data", {}).get("list", [])
                for file_data in files_data:
                    if str(file_data.get('fid', '')) == str(file_id):
                        return QuarkFile.from_dict(file_data).to_file_item()
            
            self._log.debug(f"在根目录列表中未找到文件 {file_id}，返回简化信息", driver_name="quark_reverse")
            return FileItem(
                id=file_id,
                name=f"file_{file_id}",
                path="",
                size=0,
                is_dir=False,
                modified=datetime.now(timezone.utc),
                created=datetime.now(timezone.utc)
            )
            
        except Exception as e:
            self._log.error(f"获取文件信息失败: {str(e)}", driver_name="quark_reverse")
            return FileItem(
                id=file_id,
                name=f"file_{file_id}",
                path="",
                size=0,
                is_dir=False,
                modified=datetime.now(timezone.utc),
                created=datetime.now(timezone.utc)
            )
    
    @auto_cleanup_cache('create_folder')
    async def create_folder(self, parent_id: str, name: str) -> OperationResult:
        try:
            api_parent_id = parent_id if parent_id != "0" else self.config.root_folder_id
            
            self._log.debug(f"创建文件夹调试信息:", 
                          details={
                              "parent_id": parent_id,
                              "api_parent_id": api_parent_id,
                              "name": name,
                              "root_folder_id": self.config.root_folder_id
                          }, driver_name="quark_reverse")
            
            json_data = {
                "parent_id": api_parent_id,  # 使用映射前的参数名
                "name": name,                # 使用映射前的参数名
                "dir_init_lock": False,
                "dir_path": ""
            }
            
            mapped_data = QuarkApiHelper.map_params(json_data, "create_folder")
            self._log.debug(f"参数映射结果:", 
                          details={
                              "原始参数": json_data,
                              "映射后参数": mapped_data
                          }, driver_name="quark_reverse")
            
            response = await self._api_request("create_folder", "POST", json=json_data)
            
            folder_data = response.get("data", {})
            folder_id = folder_data.get("fid")
            
            # 状态收敛等待：目录创建成功后，网盘侧未必立刻可见，等待后再让上层刷新/回查。
            if self.config.operation_delay > 0:
                self._log.debug(f"目录创建后状态收敛等待 {self.config.operation_delay}ms", driver_name="quark_reverse")
                await asyncio.sleep(self.config.operation_delay / 1000.0)
            
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

    async def _collect_recycle_record_ids(self, file_ids: List[str]) -> List[str]:
        recycle_ids: List[str] = []
        target_ids = {str(file_id) for file_id in file_ids if file_id}
        if not target_ids:
            return recycle_ids

        page_size = 200
        for attempt in range(3):
            matched_ids: Dict[str, str] = {}
            page = 1

            while True:
                recycle_response = await self._api_request("recycle_list", "GET", params={
                    "_page": page,
                    "_size": page_size
                })
                recycle_files = recycle_response.get("data", {}).get("list", [])
                if not recycle_files:
                    break

                for recycle_file in recycle_files:
                    recycle_fid = str(recycle_file.get("fid") or "")
                    recycle_record_id = str(recycle_file.get("record_id") or "")
                    if recycle_fid in target_ids and recycle_record_id:
                        matched_ids[recycle_fid] = recycle_record_id

                if len(matched_ids) >= len(target_ids) or len(recycle_files) < page_size:
                    break
                page += 1

            if matched_ids:
                recycle_ids = list(matched_ids.values())
            if len(matched_ids) >= len(target_ids):
                return recycle_ids

            # 状态收敛等待：回收站记录落库存在异步延迟，轮询前逐步放宽等待时间。
            await asyncio.sleep(0.6 + attempt * 0.4)

        return recycle_ids

    # 夸克对“删除已不存在文件”返回该业务码，等价于幂等删除成功
    _ALREADY_DELETED_CODE = 23004

    def _already_deleted_result(self, file_ids: List[str], parent_ids: set) -> OperationResult:
        """目标文件已不存在：删除目的已达成，按成功返回，不打错误日志。"""
        self._log.debug(
            f"夸克网盘 {len(file_ids)} 个文件已不存在，视为删除成功",
            driver_name="quark_reverse",
        )
        return OperationResult(
            success=True,
            message="文件已不存在，视为删除成功",
            data={
                "deleted_count": len(file_ids),
                "file_ids": file_ids,
                "parent_ids": list(parent_ids),
            },
        )

    async def _delete_files(self, file_ids: List[str]) -> OperationResult:
        """删除文件，支持回收站模式和永久删除模式。"""
        try:
            parent_ids = set()
            for file_id in file_ids:
                try:
                    file_info = await self.file_info(file_id)
                    if file_info and file_info.extra and "parent_id" in file_info.extra:
                        parent_ids.add(file_info.extra["parent_id"])
                except Exception:
                    pass

            if self.config.delete_mode == "delete" and len(file_ids) > 1:
                failed_messages: List[str] = []
                for file_id in file_ids:
                    single_result = await self._delete_files([str(file_id)])
                    if not single_result.success:
                        failed_messages.append(single_result.message or f"文件 {file_id} 删除失败")
                if failed_messages:
                    return OperationResult(success=False, message="；".join(failed_messages))
                return OperationResult(
                    success=True,
                    message=f"已永久删除 {len(file_ids)} 个文件",
                    data={
                        "deleted_count": len(file_ids),
                        "file_ids": file_ids,
                        "parent_ids": list(parent_ids),
                    }
                )

            if self.config.delete_mode == "delete":
                self._log.debug(
                    f"永久删除模式：准备将 {len(file_ids)} 个文件移入回收站...",
                    driver_name="quark_reverse",
                )

                trash_resp = await self._api_request("trash", "POST", json={
                    "filelist": file_ids,
                    "action_type": 1,
                }, benign_codes={self._ALREADY_DELETED_CODE})
                if isinstance(trash_resp, dict) and trash_resp.get("code") == self._ALREADY_DELETED_CODE:
                    return self._already_deleted_result(file_ids, parent_ids)

                # 状态收敛等待：移动到回收站成功后，需等待回收站记录真正可查询。
                if self.config.operation_delay > 0:
                    self._log.debug(
                        f"回收站落库状态收敛等待 {self.config.operation_delay}ms",
                        driver_name="quark_reverse",
                    )
                    await asyncio.sleep(self.config.operation_delay / 1000.0)

                self._log.debug("永久删除模式：正在确认回收站记录...", driver_name="quark_reverse")
                recycle_ids = await self._collect_recycle_record_ids(file_ids)
                self._log.debug(
                    f"找到 {len(recycle_ids)} 个回收站文件记录",
                    driver_name="quark_reverse",
                )

                if recycle_ids:
                    self._log.debug(
                        f"永久删除模式：从回收站永久删除 {len(recycle_ids)} 个文件...",
                        driver_name="quark_reverse",
                    )
                    await self._api_request("delete", "POST", json={
                        "record_list": recycle_ids,
                        "select_mode": 2,
                    })

                    # 状态收敛等待：永久删除提交成功后，等待网盘后台真正完成删除。
                    if self.config.operation_delay > 0:
                        self._log.debug(
                            f"永久删除后状态收敛等待 {self.config.operation_delay}ms",
                            driver_name="quark_reverse",
                        )
                        await asyncio.sleep(self.config.operation_delay / 1000.0)

                    message = f"已永久删除 {len(file_ids)} 个文件"
                else:
                    message = f"已将 {len(file_ids)} 个文件移到回收站（未找到回收站记录，可能仍在同步中）"
            else:
                self._log.debug(
                    f"回收站模式：准备将 {len(file_ids)} 个文件移入回收站...",
                    driver_name="quark_reverse",
                )

                trash_resp = await self._api_request("trash", "POST", json={
                    "filelist": file_ids,
                    "action_type": 1,
                }, benign_codes={self._ALREADY_DELETED_CODE})
                if isinstance(trash_resp, dict) and trash_resp.get("code") == self._ALREADY_DELETED_CODE:
                    return self._already_deleted_result(file_ids, parent_ids)

                # 状态收敛等待：即使仅移到回收站，也需要等待网盘列表状态稳定。
                if self.config.operation_delay > 0:
                    self._log.debug(
                        f"回收站状态收敛等待 {self.config.operation_delay}ms",
                        driver_name="quark_reverse",
                    )
                    await asyncio.sleep(self.config.operation_delay / 1000.0)

                message = f"已将 {len(file_ids)} 个文件移到回收站"

            return OperationResult(
                success=True,
                message=message,
                data={
                    "deleted_count": len(file_ids),
                    "file_ids": file_ids,
                    "parent_ids": list(parent_ids),
                }
            )

        except Exception as e:
            error_msg = f"删除失败: {str(e)}"
            self._log.error(f"夸克网盘 {error_msg}", driver_name="quark_reverse")
            return OperationResult(success=False, message=error_msg)

    @auto_cleanup_cache('rename_file')
    async def rename_file(self, file_id: str, new_name: str) -> OperationResult:
        try:
            await self._api_request("rename", "POST", json={
                "fid": file_id,
                "file_name": new_name.strip()
            })
            
            # 状态收敛等待：重命名成功后，列表与缓存未必立刻反映新名称。
            if self.config.operation_delay > 0:
                self._log.debug(f"重命名后状态收敛等待 {self.config.operation_delay}ms", driver_name="quark_reverse")
                await asyncio.sleep(self.config.operation_delay / 1000.0)
            
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

            self._log.debug(f"开始移动 {len(file_ids)} 个文件到目录 {target_parent_id}", driver_name="quark_reverse")

            parent_ids = set()
            for file_id in file_ids:
                try:
                    file_info = await self.file_info(file_id)
                    if file_info and file_info.extra and 'parent_id' in file_info.extra:
                        parent_ids.add(file_info.extra['parent_id'])
                except Exception as e:
                    self._log.warning(f"获取文件 {file_id} 信息失败: {e}", driver_name="quark_reverse")

            json_data = {
                "action_type": 1,
                "exclude_fids": [],
                "filelist": file_ids,
                "to_pdir_fid": target_parent_id
            }

            await self._api_request("move", "POST", json=json_data)

            # 状态收敛等待：移动成功后，源/目标目录状态未必立刻同步完成。
            if self.config.operation_delay > 0:
                self._log.debug(f"移动后状态收敛等待 {self.config.operation_delay}ms", driver_name="quark_reverse")
                await asyncio.sleep(self.config.operation_delay / 1000.0)

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
            self._log.error(f"夸克网盘 {error_msg}", driver_name="quark_reverse")
            return OperationResult(success=False, message=error_msg)

    @auto_cleanup_cache('copy_file')
    async def copy_file(self, file_ids: List[str], target_parent_id: str, source_parent_id: str = None) -> OperationResult:
        try:
            if not file_ids:
                return OperationResult(success=True, message="没有文件需要复制")

            response = await self._api_request("copy", "POST", json={
                "filelist": file_ids,
                "to_pdir_fid": target_parent_id,
            })

            task_id = response.get("data", {}).get("task_id")
            if not task_id:
                return OperationResult(success=False, message="未获取到复制任务ID")

            for _ in range(30):
                await asyncio.sleep(2)
                task_response = await self._api_request("task", "GET", params={
                    "task_id": task_id,
                    "retry_index": "0",
                })
                task_status = task_response.get("data", {}).get("status")
                if task_status == 2:
                    return OperationResult(
                        success=True,
                        message=f"已复制 {len(file_ids)} 个文件到目标目录",
                        data={
                            "copied_count": len(file_ids),
                            "file_ids": file_ids,
                            "target_parent_id": target_parent_id,
                            "source_parent_ids": [source_parent_id] if source_parent_id else []
                        }
                    )

            return OperationResult(success=False, message="复制任务超时")

        except Exception as e:
            error_msg = f"复制失败: {str(e)}"
            self._log.error(f"夸克网盘 {error_msg}", driver_name="quark_reverse")
            return OperationResult(success=False, message=error_msg)

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

    @auto_cleanup_cache('upload_file')
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

    @auto_cleanup_cache('upload_file')
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
            resume_state=resume_state,
            state_callback=state_callback,
        )

    async def _upload_local_file_impl(
        self,
        *,
        local_path: str,
        file_name: str,
        parent_path: str = "0",
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        conflict_policy: str = "overwrite",
        resume_state: Optional[Dict[str, Any]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    ) -> OperationResult:
        try:
            target_name = os.path.basename((file_name or "").strip())
            if not target_name:
                return OperationResult(success=False, message="上传文件名不能为空")
            if not local_path or not os.path.exists(local_path):
                return OperationResult(success=False, message="待上传文件不存在")

            file_size = os.path.getsize(local_path)
            mime_type = self._guess_mime_type(target_name)
            file_md5, file_sha1 = await asyncio.to_thread(self._calculate_file_hashes, local_path)
            normalized_resume_state = self._normalize_quark_upload_resume_state(
                resume_state,
                parent_id=parent_path,
                target_name=target_name,
                file_size=file_size,
                file_md5=file_md5,
                file_sha1=file_sha1,
            )

            file_id = ""
            if normalized_resume_state:
                target_name = normalized_resume_state["target_name"]
                pre_data = normalized_resume_state["pre_data"]
                part_size = normalized_resume_state["part_size"]
                file_id = str(pre_data.get("fid") or "").strip()
                resumed_uploaded_bytes = normalized_resume_state["uploaded_bytes"]
                if resumed_uploaded_bytes > 0:
                    await self._notify_upload_progress(
                        progress_callback,
                        resumed_uploaded_bytes,
                        file_size,
                        "正在继续上传到夸克网盘",
                    )
            else:
                prepared = await self._prepare_quark_upload_target_name(parent_path, target_name, conflict_policy)
                if prepared.get("action") == "skip":
                    return OperationResult(
                        success=True,
                        message=f"文件 '{target_name}' 已存在，已跳过",
                        data={
                            "skipped": True,
                            "file_name": target_name,
                            "parent_id": parent_path,
                        },
                    )
                target_name = prepared.get("file_name") or target_name

                pre_response = await self._request_quark_upload_pre(
                    parent_id=parent_path,
                    target_name=target_name,
                    file_size=file_size,
                    mime_type=mime_type,
                    progress_callback=progress_callback,
                )

                pre_data = (pre_response or {}).get("data", {})
                pre_metadata = (pre_response or {}).get("metadata", {})
                task_id = str(pre_data.get("task_id") or "").strip()
                obj_key = str(pre_data.get("obj_key") or "").strip()
                upload_id = str(pre_data.get("upload_id") or "").strip()
                bucket = str(pre_data.get("bucket") or "").strip()
                upload_url = str(pre_data.get("upload_url") or "").strip()
                file_id = str(pre_data.get("fid") or "").strip()

                if not task_id:
                    raise Exception("夸克上传预处理未返回 task_id")

                hash_response = await self._api_request(
                    "update_hash",
                    "POST",
                    json={
                        "md5": file_md5,
                        "sha1": file_sha1,
                        "task_id": task_id,
                    },
                )

                hash_data = (hash_response or {}).get("data", {})
                if hash_data.get("finish"):
                    file_id = str(hash_data.get("fid") or file_id or "").strip()
                    resolved_item = await self._resolve_uploaded_file_in_parent(
                        parent_id=parent_path,
                        target_name=target_name,
                        file_size=file_size,
                        preferred_file_id=file_id,
                    )
                    if resolved_item:
                        file_id = str(resolved_item.id)
                        target_name = resolved_item.name

                    await self._notify_upload_progress(progress_callback, file_size, file_size, "秒传成功")
                    return self._build_quark_upload_success_result(
                        parent_id=parent_path,
                        target_name=target_name,
                        file_size=file_size,
                        file_id=file_id,
                        rapid_upload=True,
                    )

                if not bucket or not obj_key or not upload_id or not upload_url:
                    raise Exception("夸克上传预处理缺少 bucket、obj_key、upload_id 或 upload_url")

                part_size = int(pre_metadata.get("part_size") or 0) or (16 * 1024 * 1024)
                normalized_resume_state = None
                if state_callback:
                    await self._persist_quark_upload_resume_state(
                        state_callback=state_callback,
                        parent_id=parent_path,
                        target_name=target_name,
                        file_size=file_size,
                        file_md5=file_md5,
                        file_sha1=file_sha1,
                        part_size=part_size,
                        pre_data=pre_data,
                        completed_etags={},
                    )

            task_id = str(pre_data.get("task_id") or "").strip()
            obj_key = str(pre_data.get("obj_key") or "").strip()
            etags = await self._upload_quark_parts(
                local_path=local_path,
                file_size=file_size,
                mime_type=mime_type,
                pre_data=pre_data,
                part_size=part_size,
                progress_callback=progress_callback,
                state_callback=state_callback,
                parent_id=parent_path,
                target_name=target_name,
                file_md5=file_md5,
                file_sha1=file_sha1,
                completed_etags=normalized_resume_state["completed_etags"] if normalized_resume_state else None,
            )

            await self._complete_quark_upload(pre_data=pre_data, etags=etags)
            await self._api_request(
                "upload_finish",
                "POST",
                json={
                    "obj_key": obj_key,
                    "task_id": task_id,
                },
            )

            # 状态收敛等待：上传完成接口返回成功后，文件通常仍需短暂时间才能在目录中可见。
            await asyncio.sleep(1.0)
            resolved_item = await self._resolve_uploaded_file_in_parent(
                parent_id=parent_path,
                target_name=target_name,
                file_size=file_size,
                preferred_file_id=file_id,
            )
            if resolved_item:
                file_id = str(resolved_item.id)
                target_name = resolved_item.name

            await self._notify_upload_progress(progress_callback, file_size, file_size, "上传成功")
            return self._build_quark_upload_success_result(
                parent_id=parent_path,
                target_name=target_name,
                file_size=file_size,
                file_id=file_id,
            )
        except Exception as e:
            return OperationResult(success=False, message=f"上传文件失败: {str(e)}")

    async def _save_upload_to_tempfile(self, upload_file: UploadFile) -> str:
        suffix = os.path.splitext(upload_file.filename or "")[1]
        fd, temp_path = tempfile.mkstemp(prefix="litepan_quark_", suffix=suffix)
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

    async def _create_quark_upload_stream(
        self,
        *,
        local_path: str,
        offset: int,
        part_size: int,
        uploaded_base: int,
        total_bytes: int,
        message: str,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        stream_chunk_size: int = 1024 * 1024,
    ):
        sent_bytes = 0
        last_reported = uploaded_base

        with open(local_path, "rb") as fp:
            fp.seek(offset)
            while sent_bytes < part_size:
                chunk = fp.read(min(stream_chunk_size, part_size - sent_bytes))
                if not chunk:
                    break
                sent_bytes += len(chunk)
                current_uploaded = min(total_bytes, uploaded_base + sent_bytes)
                if current_uploaded > last_reported:
                    await self._notify_upload_progress(
                        progress_callback,
                        current_uploaded,
                        total_bytes,
                        message,
                    )
                    last_reported = current_uploaded
                yield chunk

    def _guess_mime_type(self, file_name: str) -> str:
        mime_type, _ = mimetypes.guess_type(file_name or "")
        return mime_type or "application/octet-stream"

    async def _request_quark_upload_pre(
        self,
        *,
        parent_id: str,
        target_name: str,
        file_size: int,
        mime_type: str,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    ) -> Dict[str, Any]:
        await self._notify_upload_progress(progress_callback, 0, file_size, "正在准备上传")
        api_parent_id = parent_id if parent_id != "0" else self.config.root_folder_id
        now_ms = int(time.time() * 1000)
        return await self._api_request(
            "upload_pre",
            "POST",
            json={
                "ccp_hash_update": True,
                "dir_name": "",
                "file_name": target_name,
                "format_type": mime_type,
                "l_created_at": now_ms,
                "l_updated_at": now_ms,
                "pdir_fid": api_parent_id,
                "size": file_size,
            },
        )

    def _build_quark_upload_success_result(
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
        return OperationResult(
            success=True,
            message=f"文件 '{target_name}' 上传成功",
            data=data,
        )

    def _calculate_file_hashes(self, local_path: str) -> tuple[str, str]:
        md5 = hashlib.md5()
        sha1 = hashlib.sha1()
        with open(local_path, "rb") as fp:
            for chunk in iter(lambda: fp.read(1024 * 1024), b""):
                if not chunk:
                    break
                md5.update(chunk)
                sha1.update(chunk)
        return md5.hexdigest(), sha1.hexdigest()

    def _calculate_quark_uploaded_bytes_by_parts(
        self,
        *,
        file_size: int,
        part_size: int,
        completed_parts: List[int],
    ) -> int:
        uploaded_bytes = 0
        for part_number in completed_parts:
            offset = max(0, (part_number - 1) * part_size)
            uploaded_bytes += min(part_size, max(file_size - offset, 0))
        return min(uploaded_bytes, file_size)

    def _normalize_quark_upload_resume_state(
        self,
        resume_state: Optional[Dict[str, Any]],
        *,
        parent_id: str,
        target_name: str,
        file_size: int,
        file_md5: str,
        file_sha1: str,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(resume_state, dict):
            return None

        pre_data = resume_state.get("pre_data")
        if not isinstance(pre_data, dict):
            return None

        resume_parent_id = str(resume_state.get("parent_id") or "").strip()
        resume_target_name = str(resume_state.get("target_name") or "").strip()
        resume_file_md5 = str(resume_state.get("file_md5") or "").strip().lower()
        resume_file_sha1 = str(resume_state.get("file_sha1") or "").strip().lower()
        resume_file_size = int(resume_state.get("file_size") or 0)
        part_size = int(resume_state.get("part_size") or 0)
        if (
            not resume_parent_id
            or resume_parent_id != str(parent_id)
            or not resume_target_name
            or resume_target_name != str(target_name)
            or resume_file_size != int(file_size)
            or resume_file_md5 != str(file_md5).lower()
            or resume_file_sha1 != str(file_sha1).lower()
            or part_size <= 0
        ):
            return None

        task_id = str(pre_data.get("task_id") or "").strip()
        upload_id = str(pre_data.get("upload_id") or "").strip()
        bucket = str(pre_data.get("bucket") or "").strip()
        obj_key = str(pre_data.get("obj_key") or "").strip()
        upload_url = str(pre_data.get("upload_url") or "").strip()
        if not task_id or not upload_id or not bucket or not obj_key or not upload_url:
            return None

        upload_nums = max(1, (file_size + part_size - 1) // part_size)
        raw_completed_etags = resume_state.get("completed_etags") or {}
        if not isinstance(raw_completed_etags, dict):
            raw_completed_etags = {}

        completed_etags: Dict[int, str] = {}
        for part_key, etag in raw_completed_etags.items():
            try:
                normalized_part = int(part_key)
            except (TypeError, ValueError):
                continue
            normalized_etag = self._normalize_oss_etag(str(etag or ""))
            if 1 <= normalized_part <= upload_nums and normalized_etag:
                completed_etags[normalized_part] = normalized_etag

        completed_parts = sorted(completed_etags.keys())
        uploaded_bytes = self._calculate_quark_uploaded_bytes_by_parts(
            file_size=file_size,
            part_size=part_size,
            completed_parts=completed_parts,
        )
        progress = min(99, int(uploaded_bytes * 100 / max(file_size, 1))) if uploaded_bytes < file_size else 100

        return {
            "parent_id": resume_parent_id,
            "target_name": resume_target_name,
            "file_size": file_size,
            "file_md5": resume_file_md5,
            "file_sha1": resume_file_sha1,
            "part_size": part_size,
            "pre_data": pre_data,
            "completed_parts": completed_parts,
            "completed_etags": completed_etags,
            "uploaded_bytes": uploaded_bytes,
            "progress": progress,
        }

    async def _persist_quark_upload_resume_state(
        self,
        *,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]],
        parent_id: str,
        target_name: str,
        file_size: int,
        file_md5: str,
        file_sha1: str,
        part_size: int,
        pre_data: Dict[str, Any],
        completed_etags: Dict[int, str],
    ) -> None:
        if not state_callback:
            return

        normalized_etags = {
            str(int(part_number)): self._normalize_oss_etag(str(etag))
            for part_number, etag in completed_etags.items()
            if int(part_number) > 0 and self._normalize_oss_etag(str(etag))
        }
        completed_parts = sorted(int(part_number) for part_number in normalized_etags.keys())
        uploaded_bytes = self._calculate_quark_uploaded_bytes_by_parts(
            file_size=file_size,
            part_size=part_size,
            completed_parts=completed_parts,
        )
        progress = min(99, int(uploaded_bytes * 100 / max(file_size, 1))) if uploaded_bytes < file_size else 100

        await state_callback({
            "parent_id": str(parent_id),
            "target_name": str(target_name),
            "file_size": int(file_size),
            "file_md5": str(file_md5).lower(),
            "file_sha1": str(file_sha1).lower(),
            "part_size": int(part_size),
            "pre_data": pre_data,
            "completed_parts": completed_parts,
            "completed_etags": normalized_etags,
            "uploaded_bytes": uploaded_bytes,
            "progress": progress,
        })

    async def _prepare_quark_upload_target_name(
        self,
        parent_id: str,
        file_name: str,
        conflict_policy: str,
    ) -> Dict[str, Any]:
        files = await self.list_files(parent_id or "0")
        exact_files = [item for item in files if not item.is_dir and item.name == file_name]
        exact_dirs = [item for item in files if item.is_dir and item.name == file_name]

        if exact_dirs:
            raise Exception(f"目标目录中已存在同名文件夹 '{file_name}'")
        if not exact_files:
            return {"action": "upload", "file_name": file_name}

        normalized_policy = str(conflict_policy or "overwrite").strip().lower()
        if normalized_policy == "skip":
            return {"action": "skip", "file_name": file_name}
        if normalized_policy == "fail":
            raise Exception(f"目标目录中已存在同名文件 '{file_name}'")
        if normalized_policy == "overwrite":
            delete_ids = [str(item.id) for item in exact_files]
            delete_result = await self.batch_delete_file(delete_ids)
            if not delete_result.success:
                raise Exception(delete_result.message or f"删除同名文件 '{file_name}' 失败")
            return {"action": "upload", "file_name": file_name}

        if normalized_policy in {"keep_both", "keep_both_new", "rename"}:
            existing_names = {item.name for item in files}
            return {"action": "upload", "file_name": self._generate_keep_both_name(file_name, existing_names)}

        return {"action": "upload", "file_name": file_name}

    def _generate_keep_both_name(self, original_name: str, existing_names: set[str]) -> str:
        if original_name not in existing_names:
            return original_name

        base_name, ext = os.path.splitext(original_name)
        index = 1
        while True:
            candidate = f"{base_name} ({index}){ext}"
            if candidate not in existing_names:
                return candidate
            index += 1

    async def _resolve_uploaded_file_in_parent(
        self,
        *,
        parent_id: str,
        target_name: str,
        file_size: int,
        preferred_file_id: str = "",
    ) -> Optional[FileItem]:
        account_id = str(getattr(self, "_account_id", "") or "")
        for _ in range(3):
            if account_id:
                try:
                    await clear_operation_cache(account_id, 'directory_update', parent_id=parent_id or "0")
                except Exception:
                    pass

            files = await self.list_files(parent_id or "0")
            candidates: List[FileItem] = []
            for item in files or []:
                if item.is_dir:
                    continue
                if preferred_file_id and str(item.id) == str(preferred_file_id):
                    return item
                if item.name == target_name and int(item.size or 0) == int(file_size or 0):
                    candidates.append(item)

            if len(candidates) == 1:
                return candidates[0]
            if len(candidates) > 1:
                candidates.sort(key=lambda current: current.modified or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
                return candidates[0]

            # 状态收敛等待：上传落盘后轮询目录，等待新文件真正可解析出来。
            await asyncio.sleep(0.4)

        return None

    def _build_quark_oss_time(self) -> str:
        return formatdate(timeval=None, localtime=False, usegmt=True)

    def _build_quark_upload_object_url(self, upload_url: str, bucket: str, obj_key: str) -> str:
        normalized_host = str(upload_url or "").strip()
        if normalized_host.startswith("https://"):
            normalized_host = normalized_host[8:]
        elif normalized_host.startswith("http://"):
            normalized_host = normalized_host[7:]
        normalized_host = normalized_host.rstrip("/")
        normalized_key = str(obj_key or "").lstrip("/")
        return f"https://{bucket}.{normalized_host}/{normalized_key}"

    async def _request_quark_upload_auth(self, auth_info: str, auth_meta: str, task_id: str) -> str:
        response = await self._api_request(
            "upload_auth",
            "POST",
            json={
                "auth_info": auth_info,
                "auth_meta": auth_meta,
                "task_id": task_id,
            },
        )
        auth_key = str((response or {}).get("data", {}).get("auth_key") or "").strip()
        if not auth_key:
            raise Exception("夸克上传未返回 auth_key")
        return auth_key

    def _normalize_oss_etag(self, etag: str) -> str:
        normalized = str(etag or "").strip().strip('"')
        return f'"{normalized}"' if normalized else ""

    async def _upload_quark_parts(
        self,
        *,
        local_path: str,
        file_size: int,
        mime_type: str,
        pre_data: Dict[str, Any],
        part_size: int,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        parent_id: str = "0",
        target_name: str = "",
        file_md5: str = "",
        file_sha1: str = "",
        completed_etags: Optional[Dict[int, str]] = None,
    ) -> List[str]:
        oss_user_agent = "aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit"
        upload_nums = max(1, (file_size + part_size - 1) // part_size)
        object_url = self._build_quark_upload_object_url(
            str(pre_data.get("upload_url") or ""),
            str(pre_data.get("bucket") or ""),
            str(pre_data.get("obj_key") or ""),
        )
        completed_etag_map: Dict[int, str] = {}
        for part_number, etag in (completed_etags or {}).items():
            normalized_etag = self._normalize_oss_etag(str(etag or ""))
            if 1 <= int(part_number) <= upload_nums and normalized_etag:
                completed_etag_map[int(part_number)] = normalized_etag
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=60, sock_read=None)

        async with aiohttp.ClientSession(cookie_jar=aiohttp.DummyCookieJar(), timeout=timeout) as session:
            for part_number in range(1, upload_nums + 1):
                offset = (part_number - 1) * part_size
                current_part_size = min(part_size, max(file_size - offset, 0))
                if current_part_size <= 0:
                    break

                if part_number in completed_etag_map:
                    await self._notify_upload_progress(
                        progress_callback,
                        self._calculate_quark_uploaded_bytes_by_parts(
                            file_size=file_size,
                            part_size=part_size,
                            completed_parts=sorted(completed_etag_map.keys()),
                        ),
                        file_size,
                        f"正在继续上传到夸克网盘，分片（{part_number}/{upload_nums}）",
                    )
                    continue

                time_str = self._build_quark_oss_time()
                auth_meta = (
                    f"PUT\n\n{mime_type}\n{time_str}\n"
                    f"x-oss-date:{time_str}\n"
                    f"x-oss-user-agent:{oss_user_agent}\n"
                    f"/{pre_data.get('bucket')}/{pre_data.get('obj_key')}?partNumber={part_number}&uploadId={pre_data.get('upload_id')}"
                )
                auth_key = await self._request_quark_upload_auth(
                    str(pre_data.get("auth_info") or ""),
                    auth_meta,
                    str(pre_data.get("task_id") or ""),
                )

                uploaded_base = self._calculate_quark_uploaded_bytes_by_parts(
                    file_size=file_size,
                    part_size=part_size,
                    completed_parts=sorted(completed_etag_map.keys()),
                )
                payload_stream = self._create_quark_upload_stream(
                    local_path=local_path,
                    offset=offset,
                    part_size=current_part_size,
                    uploaded_base=uploaded_base,
                    total_bytes=file_size,
                    message=f"正在上传到夸克网盘，分片（{part_number}/{upload_nums}）",
                    progress_callback=progress_callback,
                )

                async with session.put(
                    object_url,
                    params={
                        "partNumber": str(part_number),
                        "uploadId": str(pre_data.get("upload_id") or ""),
                    },
                    data=payload_stream,
                    headers={
                        "Authorization": auth_key,
                        "Content-Length": str(current_part_size),
                        "Content-Type": mime_type,
                        "Referer": f"{QuarkAPI.REFERER}/",
                        "x-oss-date": time_str,
                        "x-oss-user-agent": oss_user_agent,
                    },
                ) as response:
                    if response.status != 200:
                        body = await response.text()
                        if response.status == 409 and "PartAlreadyExist" in body:
                            part_etag_match = re.search(r"<PartEtag>(.*?)</PartEtag>", body, re.IGNORECASE | re.DOTALL)
                            existed_etag = self._normalize_oss_etag(part_etag_match.group(1) if part_etag_match else "")
                            if existed_etag:
                                completed_etag_map[part_number] = existed_etag
                                if state_callback:
                                    await self._persist_quark_upload_resume_state(
                                        state_callback=state_callback,
                                        parent_id=parent_id,
                                        target_name=target_name,
                                        file_size=file_size,
                                        file_md5=file_md5,
                                        file_sha1=file_sha1,
                                        part_size=part_size,
                                        pre_data=pre_data,
                                        completed_etags=completed_etag_map,
                                    )
                                self._log.info(
                                    f"夸克续传检测到分片 {part_number} 已存在，复用已上传分片",
                                    driver_name="quark_reverse",
                                )
                                continue
                        raise Exception(f"夸克上传分片 {part_number} 失败: HTTP {response.status}, {body}")
                    etag = self._normalize_oss_etag(response.headers.get("ETag") or response.headers.get("Etag") or "")
                    if not etag:
                        raise Exception(f"夸克上传分片 {part_number} 未返回 ETag")
                    completed_etag_map[part_number] = etag

                if state_callback:
                    await self._persist_quark_upload_resume_state(
                        state_callback=state_callback,
                        parent_id=parent_id,
                        target_name=target_name,
                        file_size=file_size,
                        file_md5=file_md5,
                        file_sha1=file_sha1,
                        part_size=part_size,
                        pre_data=pre_data,
                        completed_etags=completed_etag_map,
                    )

                uploaded_bytes = self._calculate_quark_uploaded_bytes_by_parts(
                    file_size=file_size,
                    part_size=part_size,
                    completed_parts=sorted(completed_etag_map.keys()),
                )
                await self._notify_upload_progress(
                    progress_callback,
                    uploaded_bytes,
                    file_size,
                    f"正在上传到夸克网盘，分片（{part_number}/{upload_nums}）",
                )

        return [completed_etag_map[part_number] for part_number in range(1, upload_nums + 1)]

    async def _complete_quark_upload(
        self,
        *,
        pre_data: Dict[str, Any],
        etags: List[str],
    ) -> None:
        oss_user_agent = "aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit"
        body_parts = ['<?xml version="1.0" encoding="UTF-8"?>', '<CompleteMultipartUpload>']
        for index, etag in enumerate(etags, start=1):
            body_parts.append("<Part>")
            body_parts.append(f"<PartNumber>{index}</PartNumber>")
            body_parts.append(f"<ETag>{etag}</ETag>")
            body_parts.append("</Part>")
        body_parts.append("</CompleteMultipartUpload>")
        body = "\n".join(body_parts)

        content_md5 = base64.b64encode(hashlib.md5(body.encode("utf-8")).digest()).decode("utf-8")
        callback_base64 = base64.b64encode(json.dumps(pre_data.get("callback") or {}, ensure_ascii=False).encode("utf-8")).decode("utf-8")
        time_str = self._build_quark_oss_time()
        auth_meta = (
            f"POST\n{content_md5}\napplication/xml\n{time_str}\n"
            f"x-oss-callback:{callback_base64}\n"
            f"x-oss-date:{time_str}\n"
            f"x-oss-user-agent:{oss_user_agent}\n"
            f"/{pre_data.get('bucket')}/{pre_data.get('obj_key')}?uploadId={pre_data.get('upload_id')}"
        )
        auth_key = await self._request_quark_upload_auth(
            str(pre_data.get("auth_info") or ""),
            auth_meta,
            str(pre_data.get("task_id") or ""),
        )

        object_url = self._build_quark_upload_object_url(
            str(pre_data.get("upload_url") or ""),
            str(pre_data.get("bucket") or ""),
            str(pre_data.get("obj_key") or ""),
        )

        timeout = aiohttp.ClientTimeout(total=None, sock_connect=60, sock_read=None)
        async with aiohttp.ClientSession(cookie_jar=aiohttp.DummyCookieJar(), timeout=timeout) as session:
            async with session.post(
                object_url,
                params={"uploadId": str(pre_data.get("upload_id") or "")},
                data=body.encode("utf-8"),
                headers={
                    "Authorization": auth_key,
                    "Content-MD5": content_md5,
                    "Content-Type": "application/xml",
                    "Referer": f"{QuarkAPI.REFERER}/",
                    "x-oss-callback": callback_base64,
                    "x-oss-date": time_str,
                    "x-oss-user-agent": oss_user_agent,
                },
            ) as response:
                if response.status != 200:
                    body_text = await response.text()
                    raise Exception(f"夸克完成分片上传失败: HTTP {response.status}, {body_text}")

    @with_performance_tracking
    async def get_download_info(self, file_id: str, user_agent: str = None) -> dict:
        """获取下载直链 + 对应文件名（夸克直链里不含原文件名，WebDAV 需要单独拿到）。"""
        last_error: Optional[Exception] = None
        try:
            for attempt in range(2):
                try:
                    return await self._request_download_info_once(file_id, user_agent)
                except Exception as e:
                    last_error = e
                    if attempt == 0 and self._last_cookie_change_detected:
                        self._log.info(
                            "检测到夸克下载接口返回新Cookie，使用新Cookie重试下载直链",
                            driver_name="quark_reverse"
                        )
                        continue
                    raise
            
        except Exception as e:
            if self._is_download_auth_failure(e):
                await self._mark_download_auth_fatal(str(e))
            self._log.error(f"获取下载信息失败: {str(e)}", driver_name="quark_reverse")
            raise

    async def _request_download_info_once(self, file_id: str, user_agent: str = None) -> dict:
        json_data = {
            "fids": [file_id]
        }

        headers = QuarkApiHelper.build_headers(self.cookie)
        headers['User-Agent'] = self._resolve_download_user_agent(user_agent)

        response = await self._api_request("download", "POST", json=json_data, custom_headers=headers)

        data = response.get('data', [])
        if not data:
            raise Exception("API返回数据为空")

        file_data = data[0]
        download_url = file_data.get('download_url')
        if not download_url:
            raise Exception("未找到下载链接")

        return {
            'download_url': download_url,
            'file_name': file_data.get('file_name', f'file_{file_id}'),
            'size': file_data.get('size', 0)
        }

    def _is_download_auth_failure(self, error: Exception) -> bool:
        error_text = str(error).lower()
        auth_markers = (
            "api返回数据为空",
            "未找到下载链接",
            "cookie认证失败",
            "访问被拒绝",
            "权限不足",
            "unauthorized",
            "forbidden",
            "401",
            "403",
        )
        return any(marker in error_text for marker in auth_markers)

    async def _mark_download_auth_fatal(self, reason: str) -> None:
        account_id = getattr(self, '_account_id', None)
        try:
            normalized_account_id = int(account_id)
        except (TypeError, ValueError):
            return

        message = f"夸克网盘下载认证已失效，需要重新获取 Cookie（{reason[:80]}）"

        try:
            from core.auth_manager import RefreshOutcome, auth_scheduler
            auth_manager = auth_scheduler.auth_managers.get(normalized_account_id)
            if auth_manager:
                await auth_manager._handle_passive_refresh_failure(RefreshOutcome.FATAL)
                return
        except Exception as e:
            self._log.warning(f"通过认证管理器标记夸克认证失效失败: {e}", driver_name="quark_reverse")

        try:
            from database.db import db
            from core.notification_manager import notification_manager
            from core.auth_manager import _pause_related_tasks_for_auth_failure

            account = await db.get_account(normalized_account_id)
            if account:
                config = account['config']
                config.update({
                    'auth_status': 'token_expired',
                    'error_message': message,
                    'refresh_attempts': 0,
                })
                await db.update_account(normalized_account_id, config=config)
                await _pause_related_tasks_for_auth_failure(normalized_account_id, message)
                await notification_manager.notify(
                    type="auth_expired",
                    level="error",
                    title="存储账号认证已失效",
                    message=f"「{account.get('name', f'账号{normalized_account_id}')}」{message}",
                    account_id=normalized_account_id,
                    action_label="前往设置",
                    action_route="/accounts",
                    dedup_key=f"auth_fatal_{normalized_account_id}"
                )
        except Exception as e:
            self._log.error(f"标记夸克下载认证失效失败: {e}", driver_name="quark_reverse")
    
    async def get_download_url(self, file_id: str, user_agent: str = None) -> str:
        try:
            info = await self.get_download_info(file_id, user_agent)
            return info['download_url']
        except Exception as e:
            self._log.error(f"获取下载链接失败: {str(e)}", driver_name="quark_reverse")
            raise

    async def get_download_headers(self, file_id: str, user_agent: str = None) -> Dict[str, str]:
        try:
            headers = {
                'Cookie': self.cookie,
                'Referer': 'https://pan.quark.cn/',
                'User-Agent': self._resolve_download_user_agent(user_agent),
            }

            return headers

        except Exception as e:
            self._log.error(f"获取下载请求头失败: {str(e)}", driver_name="quark_reverse")
            raise
    
    
    async def _update_cookies_from_response(self, response) -> bool:
        try:
            set_cookies = response.headers.getall('Set-Cookie', [])
            if not set_cookies:
                return False

            tracked_cookie_names = {"__puus", "__pus"}
            incoming_cookies: Dict[str, str] = {}

            for cookie_header in set_cookies:
                if not cookie_header:
                    continue
                parsed_cookie = SimpleCookie()
                try:
                    parsed_cookie.load(cookie_header)
                except Exception:
                    continue

                for cookie_name in tracked_cookie_names:
                    morsel = parsed_cookie.get(cookie_name)
                    if morsel and morsel.value:
                        incoming_cookies[cookie_name] = morsel.value

            if not incoming_cookies:
                return False

            async with self._cookie_update_lock:
                current_cookies: Dict[str, str] = {}
                if self.cookie:
                    for item in self.cookie.split(';'):
                        if '=' in item:
                            key, value = item.strip().split('=', 1)
                            current_cookies[key.strip()] = value.strip()

                updated_names = []
                for cookie_name, cookie_value in incoming_cookies.items():
                    if current_cookies.get(cookie_name) != cookie_value:
                        current_cookies[cookie_name] = cookie_value
                        updated_names.append(cookie_name)

                if not updated_names:
                    return False

                self.cookie = '; '.join(f"{k}={v}" for k, v in current_cookies.items())
                await self.sync_runtime_auth_state()

                self._log.info(
                    f"✅ 夸克网盘Cookie更新成功: {', '.join(sorted(updated_names))}",
                    driver_name="quark_reverse"
                )

                await self._persist_cookie_updates()

                try:
                    from core.auth_manager import notify_cookie_updated
                    account_id = getattr(self, '_account_id', 0) if hasattr(self, '_account_id') else 0
                    await notify_cookie_updated(account_id)
                except Exception as e:
                    self._log.error(f"通知认证调度器失败: {e}", driver_name="quark_reverse")
                return True
        except Exception as e:
            self._log.error(f"更新Cookie失败: {str(e)}", driver_name="quark_reverse")
            return False

    def set_cookie_update_callback(self, callback):
        self._cookie_update_callback = callback

    async def _persist_cookie_updates(self):
        if self._cookie_update_callback:
            try:
                await self._cookie_update_callback(self.cookie)
            except Exception as e:
                self._log.error(f"持久化Cookie时发生错误：{str(e)}", driver_name="quark_reverse")

    async def refresh_auth(self):
        try:
            await self._api_request("file_list", "GET", params={
                "pdir_fid": self.config.root_folder_id,
                "_page": 1,
                "_size": 1,
                "_fetch_total": 1
            })
            return True

        except Exception as api_error:
            error_str = str(api_error).lower()
            if "401" in error_str or "403" in error_str or "认证失败" in error_str:
                self._log.error("❌ 夸克网盘Cookie已失效，需要手动更新", driver_name="quark_reverse")
                from core.auth_manager import RefreshOutcome
                return RefreshOutcome.FATAL
            self._log.warning(f"⚠️ 夸克网盘健康检查遇到网络问题: {api_error}", driver_name="quark_reverse")
            return True
    
    def set_auth_manager(self, auth_manager):
        self._auth_manager = auth_manager

    async def _handle_auth_error(self, error_reason: str):
        """被动刷新：先交给 auth_manager 统一处理，失败才回退到驱动内刷新。"""
        try:
            if self.is_connectivity_test():
                return False
            self._log.warning(f"触发被动刷新: {error_reason}", driver_name="quark_reverse")
            
            if hasattr(self, '_account_id'):
                try:
                    from core.auth_manager import handle_auth_error
                    success = await handle_auth_error(self._account_id)
                    if success:
                        self._log.info("✅ 被动刷新成功 (通过认证管理器)", driver_name="quark_reverse")
                        return True
                    else:
                        self._log.warning("⚠️ 认证管理器刷新失败，尝试直接刷新", driver_name="quark_reverse")
                except Exception as e:
                    self._log.warning(f"⚠️ 认证管理器刷新异常，尝试直接刷新: {e}", driver_name="quark_reverse")

            self._log.warning("⚠️ 夸克网盘需要手动更新Cookie，被动刷新不适用", driver_name="quark_reverse")
            return False
            
        except Exception as e:
            self._log.error(f"❌ 被动刷新异常: {e}", driver_name="quark_reverse")
            return False
    
    def __str__(self) -> str:
        return f"<{self.__class__.__name__} root_folder_id={self.config.root_folder_id}>"
