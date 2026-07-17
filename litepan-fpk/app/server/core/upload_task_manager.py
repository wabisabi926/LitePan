"""上传任务管理器：内存级队列 + 并发槽位 + 临时文件清理。进程重启后任务不持久化。"""

import asyncio
import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from core.operation_wrapper import current_account_id

TEMP_UPLOAD_DIR = os.path.join("data", "upload_tasks")


@dataclass
class UploadTask:
    task_id: str
    account_id: int
    account_name: str
    driver_type: str
    file_name: str
    target_path: str
    local_path: str
    total_bytes: int
    target_display_path: str = ""
    client_task_id: str = ""
    conflict_policy: str = "overwrite"
    status: str = "pending"
    progress: int = 0
    uploaded_bytes: int = 0
    speed_bytes_per_second: float = 0.0
    message: str = "等待上传"
    error: str = ""
    result: Optional[Dict[str, Any]] = None
    resume_data: Optional[Dict[str, Any]] = None
    queue_order: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    _runner: Optional[asyncio.Task] = field(default=None, repr=False)
    _cancel_mode: Optional[str] = field(default=None, repr=False)
    _last_progress_emit_at: float = field(default=0.0, repr=False)
    _last_progress_emit_value: int = field(default=0, repr=False)
    _last_progress_emit_message: str = field(default="", repr=False)
    _last_speed_sample_at: float = field(default=0.0, repr=False)
    _last_speed_sample_bytes: int = field(default=0, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "client_task_id": self.client_task_id,
            "account_id": self.account_id,
            "account_name": self.account_name,
            "driver_type": self.driver_type,
            "file_name": self.file_name,
            "target_path": self.target_path,
            "target_display_path": self.target_display_path,
            "status": self.status,
            "progress": self.progress,
            "uploaded_bytes": self.uploaded_bytes,
            "speed_bytes_per_second": self.speed_bytes_per_second,
            "total_bytes": self.total_bytes,
            "message": self.message,
            "error": self.error,
            "result": deepcopy(self.result),
            "resume_data": deepcopy(self.resume_data),
            "queue_order": self.queue_order,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class UploadTaskManager:
    _PROGRESS_UPDATE_INTERVAL = 0.25
    _TEMP_UPLOAD_DIR = TEMP_UPLOAD_DIR
    _TEMP_FILE_CLEANUP_INTERVAL = 3600
    _TEMP_FILE_MAX_AGE = 24 * 3600

    def __init__(self):
        self._tasks: Dict[str, UploadTask] = {}
        self._lock = asyncio.Lock()
        self._concurrency_limit = 3
        self._running_count = 0
        self._concurrency_condition = asyncio.Condition()
        self._next_queue_order = 0
        self._subscribers: set[asyncio.Queue[str]] = set()
        self._temp_cleanup_task: Optional[asyncio.Task] = None

    def _allocate_queue_order(self) -> int:
        self._next_queue_order += 1
        return self._next_queue_order

    async def create_upload_task(
        self,
        *,
        client_task_id: str = "",
        account_id: int,
        account_name: str,
        driver_type: str,
        file_name: str,
        target_path: str,
        target_display_path: str = "",
        local_path: str,
        total_bytes: int,
        conflict_policy: str = "overwrite",
        display_name: str = "",
    ) -> Dict[str, Any]:
        task = UploadTask(
            task_id=uuid.uuid4().hex,
            client_task_id=client_task_id,
            account_id=account_id,
            account_name=account_name,
            driver_type=driver_type,
            file_name=display_name or file_name,
            target_path=target_path,
            target_display_path=target_display_path,
            local_path=local_path,
            total_bytes=total_bytes,
            conflict_policy=conflict_policy,
            queue_order=self._allocate_queue_order(),
        )

        async with self._lock:
            self._tasks[task.task_id] = task
            task._runner = asyncio.create_task(self._run_upload_task(task.task_id))
            self._prune_tasks_locked()
            task_data = task.to_dict()
        await self._broadcast_tasks_snapshot()
        return task_data

    async def list_tasks(self, account_id: Optional[int] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            tasks = list(self._tasks.values())

        if account_id is not None:
            tasks = [task for task in tasks if task.account_id == account_id]

        tasks.sort(key=lambda item: item.created_at, reverse=True)
        return [task.to_dict() for task in tasks]

    async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            task = self._tasks.get(task_id)
            return task.to_dict() if task else None

    async def pause_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        runner = None
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            if task.status not in {"pending", "running"}:
                return task.to_dict()

            task._cancel_mode = "pause"
            self._apply_task_state(
                task,
                status="paused",
                speed_bytes_per_second=0.0,
                message="上传已暂停",
                error="",
            )
            runner = task._runner

        await self._cancel_runner(runner)
        await self._broadcast_tasks_snapshot()
        return await self.get_task(task_id)

    async def resume_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            if task.status not in {"paused", "failed", "canceled"}:
                return task.to_dict()
            if not task.local_path or not os.path.exists(task.local_path):
                self._apply_task_state(
                    task,
                    status="failed",
                    message="上传失败",
                    error="本地临时文件不存在，无法继续上传",
                )
                return task.to_dict()

            resumed_progress_state = self._get_resumed_progress_state(task)
            task.status = "pending"
            task.queue_order = self._allocate_queue_order()
            if task.resume_data:
                task.progress = resumed_progress_state["progress"]
                task.uploaded_bytes = resumed_progress_state["uploaded_bytes"]
                task.message = "准备继续上传"
            else:
                task.progress = 0
                task.uploaded_bytes = 0
                task.message = "等待上传"
            task.speed_bytes_per_second = 0.0
            task.error = ""
            task.result = None
            task._cancel_mode = None
            task._runner = asyncio.create_task(self._run_upload_task(task.task_id))
            task.updated_at = time.time()
            task_data = task.to_dict()
        await self._broadcast_tasks_snapshot()
        return task_data

    async def delete_task(self, task_id: str, delete_uploaded_file: bool = False) -> bool:
        task = await self._get_task_object(task_id)
        if not task:
            return False

        runner = task._runner
        was_running = bool(runner and not runner.done())
        if runner and not runner.done():
            async with self._lock:
                current = self._tasks.get(task_id)
                if current:
                    current._cancel_mode = "delete"
            await self._cancel_runner(runner)

        task = await self._get_task_object(task_id) or task
        should_delete_uploaded_file = delete_uploaded_file or (was_running and task.status == "success")

        if should_delete_uploaded_file and task.status == "success":
            await self._delete_uploaded_file(task)

        await self._remove_local_file(task.local_path)

        async with self._lock:
            self._tasks.pop(task_id, None)
        await self._broadcast_tasks_snapshot()
        return True

    async def batch_delete_tasks(self, task_ids: List[str], delete_uploaded_file: bool = False) -> Dict[str, Any]:
        normalized_task_ids = []
        seen = set()
        for task_id in task_ids:
            normalized = str(task_id or "").strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                normalized_task_ids.append(normalized)

        if not normalized_task_ids:
            return {
                "deleted_task_ids": [],
                "failed_task_ids": [],
                "missing_task_ids": [],
                "failed_messages": {},
            }

        async with self._lock:
            existing_tasks = {task_id: self._tasks.get(task_id) for task_id in normalized_task_ids}

        missing_task_ids = [task_id for task_id, task in existing_tasks.items() if not task]
        tasks = [task for task in existing_tasks.values() if task]
        if not tasks:
            return {
                "deleted_task_ids": [],
                "failed_task_ids": [],
                "missing_task_ids": missing_task_ids,
                "failed_messages": {},
            }

        runners = []
        running_task_ids = set()
        async with self._lock:
            for task in tasks:
                if task._runner and not task._runner.done():
                    task._cancel_mode = "delete"
                    runners.append(task._runner)
                    running_task_ids.add(task.task_id)

        await self._cancel_runners(runners)

        failed_messages: Dict[str, str] = {}

        refreshed_tasks: List[UploadTask] = []
        for task in tasks:
            current_task = await self._get_task_object(task.task_id)
            refreshed_tasks.append(current_task or task)

        tasks = refreshed_tasks
        tasks_to_delete_uploaded_files = [
            task for task in tasks
            if task.status == "success" and (delete_uploaded_file or task.task_id in running_task_ids)
        ]

        if tasks_to_delete_uploaded_files:
            await self._batch_delete_uploaded_files(tasks_to_delete_uploaded_files, failed_messages)

        deleted_task_ids: List[str] = []
        async with self._lock:
            for task in tasks:
                if task.task_id in failed_messages:
                    continue
                await self._remove_local_file(task.local_path)
                self._tasks.pop(task.task_id, None)
                deleted_task_ids.append(task.task_id)

        failed_task_ids = list(failed_messages.keys())
        if deleted_task_ids:
            await self._broadcast_tasks_snapshot()
        return {
            "deleted_task_ids": deleted_task_ids,
            "failed_task_ids": failed_task_ids,
            "missing_task_ids": missing_task_ids,
            "failed_messages": failed_messages,
        }

    async def stop(self) -> None:
        async with self._lock:
            tasks = [task for task in self._tasks.values() if task._runner and not task._runner.done()]

        for task in tasks:
            task._cancel_mode = "shutdown"
        await self._cancel_runners([task._runner for task in tasks])
        await self.stop_temp_file_cleanup()

    async def cleanup_orphan_temp_files_on_startup(self) -> int:
        return await self._cleanup_temp_upload_files(max_age_seconds=0, cleanup_reason="startup")

    async def start_temp_file_cleanup(self) -> None:
        if self._temp_cleanup_task and not self._temp_cleanup_task.done():
            return
        self._temp_cleanup_task = asyncio.create_task(self._temp_file_cleanup_loop())

    async def stop_temp_file_cleanup(self) -> None:
        if not self._temp_cleanup_task:
            return
        self._temp_cleanup_task.cancel()
        try:
            await self._temp_cleanup_task
        except asyncio.CancelledError:
            pass
        finally:
            self._temp_cleanup_task = None

    async def apply_concurrency_limit(self, limit: Any) -> int:
        normalized_limit = self._normalize_concurrency_limit(limit)
        async with self._concurrency_condition:
            self._concurrency_limit = normalized_limit
            self._concurrency_condition.notify_all()
        return normalized_limit

    async def refresh_concurrency_limit(self) -> int:
        try:
            from config import config_manager

            limit = await config_manager.get_async("upload_task_concurrency")
        except Exception:
            limit = None
        return await self.apply_concurrency_limit(limit)

    async def _run_upload_task(self, task_id: str) -> None:
        task = await self._get_task_object(task_id)
        if not task:
            return

        try:
            await self.refresh_concurrency_limit()
            await self._update_task(
                task_id,
                status="pending",
                message="排队中",
                error="",
            )

            async with self._concurrency_slot(task_id):
                resumed_progress_state = self._get_resumed_progress_state(task)
                initial_progress = resumed_progress_state["progress"] if task.resume_data else 0
                initial_uploaded_bytes = resumed_progress_state["uploaded_bytes"] if task.resume_data else 0
                initial_message = "正在继续上传" if task.resume_data else "正在上传到网盘"
                speed_sample_started_at = time.time()
                task._last_speed_sample_at = speed_sample_started_at
                task._last_speed_sample_bytes = initial_uploaded_bytes

                await self._update_task(
                    task_id,
                    status="running",
                    progress=initial_progress,
                    uploaded_bytes=initial_uploaded_bytes,
                    speed_bytes_per_second=0.0,
                    message=initial_message,
                    error="",
                )

                from core.driver_service import get_account_driver_instance

                driver = await get_account_driver_instance(task.account_id)
                if not hasattr(driver, "upload_local_file") or not callable(getattr(driver, "upload_local_file")):
                    raise Exception("当前驱动暂不支持后台上传任务")

                current_account_id.set(str(task.account_id))

                async def progress_callback(uploaded_bytes: int, total_bytes: int, message: str = ""):
                    total = max(total_bytes or task.total_bytes, 1)
                    progress = min(99, int(uploaded_bytes * 100 / total)) if uploaded_bytes < total else 100
                    now = time.time()
                    speed_bytes_per_second = task.speed_bytes_per_second
                    if task._last_speed_sample_at > 0 and uploaded_bytes >= task._last_speed_sample_bytes:
                        elapsed_seconds = now - task._last_speed_sample_at
                        delta_bytes = uploaded_bytes - task._last_speed_sample_bytes
                        if elapsed_seconds > 0:
                            speed_bytes_per_second = delta_bytes / elapsed_seconds
                    elif uploaded_bytes <= 0:
                        speed_bytes_per_second = 0.0
                    normalized_message = message or "正在上传到网盘"
                    is_first = task._last_progress_emit_at <= 0
                    is_complete = uploaded_bytes >= total
                    progress_changed = progress != task._last_progress_emit_value
                    crossed_percent = progress >= task._last_progress_emit_value + 1
                    message_changed = normalized_message != task._last_progress_emit_message
                    interval_elapsed = (now - task._last_progress_emit_at) >= self._PROGRESS_UPDATE_INTERVAL

                    should_emit = (
                        is_first or
                        is_complete or
                        message_changed or
                        (progress_changed and crossed_percent) or
                        (progress_changed and interval_elapsed)
                    )

                    if not should_emit:
                        return

                    task._last_progress_emit_at = now
                    task._last_progress_emit_value = progress
                    task._last_progress_emit_message = normalized_message
                    task._last_speed_sample_at = now
                    task._last_speed_sample_bytes = uploaded_bytes
                    await self._update_task(
                        task_id,
                        status="running",
                        progress=progress,
                        uploaded_bytes=uploaded_bytes,
                        speed_bytes_per_second=speed_bytes_per_second,
                        total_bytes=total_bytes or task.total_bytes,
                        message=normalized_message,
                    )

                async def state_callback(resume_data: Dict[str, Any]):
                    await self._update_task(task_id, resume_data=deepcopy(resume_data))

                upload_method = getattr(driver, "upload_local_file_with_resume", None)
                if callable(upload_method):
                    result = await upload_method(
                        task.local_path,
                        task.file_name,
                        task.target_path,
                        progress_callback=progress_callback,
                        conflict_policy=task.conflict_policy,
                        resume_state=deepcopy(task.resume_data) if task.resume_data else None,
                        state_callback=state_callback,
                    )
                else:
                    result = await driver.upload_local_file(
                        task.local_path,
                        task.file_name,
                        task.target_path,
                        progress_callback=progress_callback,
                        conflict_policy=task.conflict_policy,
                    )

                if result.success:
                    await self._cleanup_directory_cache_after_success(task, result)
                    await self._update_task(
                        task_id,
                        status="success",
                        progress=100,
                        uploaded_bytes=task.total_bytes,
                        speed_bytes_per_second=0.0,
                        message=result.message or "上传成功",
                        result=deepcopy(result.data) if result.data else None,
                        error="",
                        resume_data=None,
                    )
                else:
                    await self._update_task(
                        task_id,
                        status="failed",
                        speed_bytes_per_second=0.0,
                        message="上传失败",
                        error=self._translate_error_message(result.message),
                    )
        except asyncio.CancelledError:
            current = await self._get_task_object(task_id)
            cancel_mode = current._cancel_mode if current else None
            if cancel_mode == "pause":
                await self._update_task(
                    task_id,
                    status="paused",
                    speed_bytes_per_second=0.0,
                    message="上传已暂停",
                    error="",
                )
            elif cancel_mode == "shutdown":
                await self._update_task(
                    task_id,
                    status="canceled",
                    speed_bytes_per_second=0.0,
                    message="上传任务已取消",
                    error="上传任务已取消",
                )
            raise
        except Exception as e:
            await self._update_task(
                task_id,
                status="failed",
                speed_bytes_per_second=0.0,
                message="上传失败",
                error=self._translate_error_message(str(e)),
            )
        finally:
            final_task = await self._get_task_object(task_id)
            if final_task and final_task.status == "success":
                await self._remove_local_file(final_task.local_path)
            if final_task:
                final_task._cancel_mode = None

    async def _delete_uploaded_file(self, task: UploadTask) -> None:
        file_id = self._get_task_uploaded_file_id(task)
        if not file_id:
            return

        from core.driver_service import get_account_driver_instance

        current_account_id.set(str(task.account_id))
        driver = await get_account_driver_instance(task.account_id)
        if hasattr(driver, "delete_file") and callable(getattr(driver, "delete_file")):
            await driver.delete_file(file_id)

    async def _batch_delete_uploaded_files(self, tasks: List[UploadTask], failed_messages: Dict[str, str]) -> None:
        account_groups: Dict[int, List[UploadTask]] = {}
        for task in tasks:
            if task.status != "success":
                continue
            file_id = self._get_task_uploaded_file_id(task)
            if not file_id:
                continue
            account_groups.setdefault(task.account_id, []).append(task)

        if not account_groups:
            return

        from core.driver_service import get_account_driver_instance

        for account_id, account_tasks in account_groups.items():
            current_account_id.set(str(account_id))
            driver = await get_account_driver_instance(account_id)
            file_ids = [self._get_task_uploaded_file_id(task) for task in account_tasks]
            file_ids = [file_id for file_id in file_ids if file_id]
            if not file_ids:
                continue

            try:
                if (
                    len(file_ids) > 1 and
                    hasattr(driver, "batch_delete_file") and
                    callable(getattr(driver, "batch_delete_file"))
                ):
                    result = await driver.batch_delete_file(file_ids)
                    if not result.success:
                        error_message = self._translate_error_message(result.message)
                        for task in account_tasks:
                            failed_messages[task.task_id] = error_message
                        continue
                else:
                    for task in account_tasks:
                        file_id = self._get_task_uploaded_file_id(task)
                        if not file_id:
                            continue
                        result = await driver.delete_file(file_id)
                        if not result.success:
                            failed_messages[task.task_id] = self._translate_error_message(result.message)
            except Exception as e:
                error_message = self._translate_error_message(str(e))
                for task in account_tasks:
                    failed_messages[task.task_id] = error_message

    async def _remove_local_file(self, local_path: str) -> None:
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except OSError:
                pass

    async def _temp_file_cleanup_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._TEMP_FILE_CLEANUP_INTERVAL)
                await self._cleanup_temp_upload_files(
                    max_age_seconds=self._TEMP_FILE_MAX_AGE,
                    cleanup_reason="scheduled",
                )
        except asyncio.CancelledError:
            raise

    async def _cleanup_temp_upload_files(self, *, max_age_seconds: int, cleanup_reason: str) -> int:
        temp_dir = self._TEMP_UPLOAD_DIR
        if not os.path.isdir(temp_dir):
            return 0

        active_paths = await self._get_active_temp_paths()
        now = time.time()
        deleted_count = 0

        try:
            entries = list(os.scandir(temp_dir))
        except OSError:
            return 0

        for entry in entries:
            if not entry.is_file():
                continue

            normalized_path = self._normalize_temp_path(entry.path)
            if normalized_path in active_paths:
                continue

            try:
                modified_at = entry.stat().st_mtime
            except OSError:
                continue

            if max_age_seconds > 0 and (now - modified_at) < max_age_seconds:
                continue

            try:
                os.remove(entry.path)
                deleted_count += 1
            except OSError:
                continue

        if deleted_count > 0:
            self._log_temp_cleanup(
                "info",
                f"上传临时文件清理完成: {cleanup_reason}，删除 {deleted_count} 个文件",
            )
        return deleted_count

    async def _get_active_temp_paths(self) -> set[str]:
        async with self._lock:
            local_paths = [task.local_path for task in self._tasks.values() if task.local_path]
        paths = {self._normalize_temp_path(path) for path in local_paths}
        try:
            from cross_transfer.relay_task_manager import relay_task_manager

            relay_paths = await relay_task_manager.get_active_local_paths()
            paths.update(self._normalize_temp_path(path) for path in relay_paths)
        except Exception:
            pass
        return paths

    def _normalize_temp_path(self, path: str) -> str:
        return os.path.normcase(os.path.abspath(path))

    def _log_temp_cleanup(self, level: str, message: str) -> None:
        try:
            from core.log_manager import LogModule, get_writer

            logger = get_writer(LogModule.FILE_OP)
            getattr(logger, level)(message)
        except Exception:
            pass

    async def _cleanup_directory_cache_after_success(self, task: UploadTask, result) -> None:
        try:
            from core.operation_wrapper import clear_operation_cache

            result_data = result.data or {}
            parent_id = result_data.get("parent_id") or result_data.get("parent_path") or task.target_path or "0"
            file_name = result_data.get("file_name") or task.file_name
            file_id = result_data.get("file_id")

            await clear_operation_cache(
                str(task.account_id),
                "file_created",
                parent_id=parent_id,
                file_name=file_name,
                file_id=file_id,
            )
        except Exception:
            # 缓存兜底清理失败不能反过来影响任务完成状态
            pass

    def _get_task_uploaded_file_id(self, task: UploadTask) -> Optional[str]:
        result = task.result or {}
        file_id = result.get("file_id")
        return str(file_id) if file_id not in (None, "") else None

    def _get_resumed_progress_state(self, task: UploadTask) -> Dict[str, int]:
        resume_data = task.resume_data or {}
        return {
            "progress": int(resume_data.get("progress") or task.progress or 0),
            "uploaded_bytes": int(resume_data.get("uploaded_bytes") or task.uploaded_bytes or 0),
        }

    def _apply_task_state(self, task: UploadTask, **updates: Any) -> None:
        for key, value in updates.items():
            if value is not None:
                setattr(task, key, value)
        task.updated_at = time.time()

    async def _cancel_runner(self, runner: Optional[asyncio.Task]) -> None:
        if not runner or runner.done():
            return
        runner.cancel()
        try:
            await runner
        except asyncio.CancelledError:
            pass

    async def _cancel_runners(self, runners: List[Optional[asyncio.Task]]) -> None:
        active_runners = [runner for runner in runners if runner and not runner.done()]
        for runner in active_runners:
            runner.cancel()
        for runner in active_runners:
            try:
                await runner
            except asyncio.CancelledError:
                pass

    async def _get_task_object(self, task_id: str) -> Optional[UploadTask]:
        async with self._lock:
            return self._tasks.get(task_id)

    async def _update_task(self, task_id: str, **updates) -> None:
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            for key, value in updates.items():
                if value is not None:
                    setattr(task, key, value)
            task.updated_at = time.time()
        await self._broadcast_tasks_snapshot()

    async def subscribe_task_stream(self) -> asyncio.Queue[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
        async with self._lock:
            self._subscribers.add(queue)
        initial_payload = await self._build_tasks_snapshot_payload()
        queue.put_nowait(initial_payload)
        return queue

    async def unsubscribe_task_stream(self, queue: asyncio.Queue[str]) -> None:
        async with self._lock:
            self._subscribers.discard(queue)

    async def _build_tasks_snapshot_payload(self) -> str:
        tasks = await self.list_tasks()
        return json.dumps({
            "type": "snapshot",
            "tasks": tasks,
        }, ensure_ascii=False)

    async def _broadcast_tasks_snapshot(self) -> None:
        async with self._lock:
            subscribers = list(self._subscribers)
        if not subscribers:
            return

        payload = await self._build_tasks_snapshot_payload()
        for queue in subscribers:
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                pass

    def _translate_error_message(self, message: Optional[str]) -> str:
        text = str(message or "").strip()
        if not text:
            return "上传失败"
        if "Server disconnected" in text:
            return "服务器连接已断开"
        if "Connection timeout" in text:
            return "连接服务器超时"
        if "Timeout" in text:
            return "请求超时"
        if "Network Error" in text:
            return "网络连接异常"
        if "Failed to fetch" in text:
            return "网络请求失败"
        return text

    def _is_next_pending_task_locked(self, task_id: str) -> bool:
        pending_tasks = [
            task for task in self._tasks.values()
            if task.status == "pending" and task._cancel_mode is None
        ]
        if not pending_tasks:
            return False
        pending_tasks.sort(key=lambda item: (item.queue_order, item.created_at))
        return pending_tasks[0].task_id == task_id

    async def _acquire_upload_slot(self, task_id: str) -> None:
        async with self._concurrency_condition:
            while (
                self._running_count >= self._concurrency_limit or
                not self._is_next_pending_task_locked(task_id)
            ):
                await self._concurrency_condition.wait()
            self._running_count += 1

    async def _release_upload_slot(self) -> None:
        async with self._concurrency_condition:
            if self._running_count > 0:
                self._running_count -= 1
            self._concurrency_condition.notify_all()

    @asynccontextmanager
    async def _concurrency_slot(self, task_id: str):
        await self._acquire_upload_slot(task_id)
        try:
            yield
        finally:
            await self._release_upload_slot()

    def _normalize_concurrency_limit(self, limit: Any) -> int:
        try:
            normalized = int(limit)
        except (TypeError, ValueError):
            normalized = 3
        return max(1, min(normalized, 5))

    def _prune_tasks_locked(self) -> None:
        if len(self._tasks) <= 100:
            return

        completed_statuses = {"success", "failed", "canceled", "skipped"}
        sorted_tasks = sorted(self._tasks.values(), key=lambda item: item.updated_at)
        for task in sorted_tasks:
            if len(self._tasks) <= 100:
                break
            if task.status in completed_statuses:
                self._tasks.pop(task.task_id, None)


upload_task_manager = UploadTaskManager()
