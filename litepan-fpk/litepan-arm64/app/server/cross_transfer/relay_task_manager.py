import asyncio
import json
import os
import time
import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from core.upload_task_manager import TEMP_UPLOAD_DIR
from cross_transfer.relay import download_source_to_file, upload_temp_to_target

_LEGACY_TEMP_DIR = os.path.join("data", "cross_transfer")


@dataclass
class RelayTask:
    task_id: str
    source_account_id: int
    source_account_name: str
    source_driver_type: str
    target_account_id: int
    target_account_name: str
    target_driver_type: str
    source_file_id: str
    file_name: str
    rel_path: str
    rel_dir: str
    target_parent_id: str
    target_display_path: str
    total_bytes: int
    method: str = "sha1"
    conflict_policy: str = "rename"
    status: str = "pending"
    phase: str = "pending"
    progress: int = 0
    downloaded_bytes: int = 0
    uploaded_bytes: int = 0
    speed_bytes_per_second: float = 0.0
    message: str = "等待中继"
    error: str = ""
    result: Optional[Dict[str, Any]] = None
    local_path: str = ""
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    _runner: Optional[asyncio.Task] = field(default=None, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "source_account_id": self.source_account_id,
            "source_account_name": self.source_account_name,
            "source_driver_type": self.source_driver_type,
            "target_account_id": self.target_account_id,
            "target_account_name": self.target_account_name,
            "target_driver_type": self.target_driver_type,
            "source_file_id": self.source_file_id,
            "file_name": self.file_name,
            "rel_path": self.rel_path,
            "rel_dir": self.rel_dir,
            "target_parent_id": self.target_parent_id,
            "target_display_path": self.target_display_path,
            "total_bytes": self.total_bytes,
            "method": self.method,
            "conflict_policy": self.conflict_policy,
            "status": self.status,
            "phase": self.phase,
            "progress": self.progress,
            "downloaded_bytes": self.downloaded_bytes,
            "uploaded_bytes": self.uploaded_bytes,
            "speed_bytes_per_second": self.speed_bytes_per_second,
            "message": self.message,
            "error": self.error,
            "result": deepcopy(self.result),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class RelayTaskManager:
    _TEMP_DIR = TEMP_UPLOAD_DIR
    _CONCURRENCY_LIMIT = 2
    _MAX_TASKS = 200

    def __init__(self):
        self._tasks: Dict[str, RelayTask] = {}
        self._lock = asyncio.Lock()
        self._running_count = 0
        self._condition = asyncio.Condition()
        self._subscribers: set[asyncio.Queue[str]] = set()

    async def create_task(self, **kwargs) -> Dict[str, Any]:
        os.makedirs(self._TEMP_DIR, exist_ok=True)
        task_id = uuid.uuid4().hex
        suffix = os.path.splitext(kwargs.get("file_name") or "")[1]
        local_path = os.path.join(self._TEMP_DIR, f"{task_id}{suffix}")
        task = RelayTask(task_id=task_id, local_path=local_path, **kwargs)

        async with self._lock:
            self._tasks[task_id] = task
            self._prune_locked()
            task._runner = asyncio.create_task(self._run_task(task_id))
            data = task.to_dict()
        await self._broadcast()
        return data

    async def list_tasks(self) -> List[Dict[str, Any]]:
        async with self._lock:
            tasks = list(self._tasks.values())
        tasks.sort(key=lambda item: item.created_at, reverse=True)
        return [task.to_dict() for task in tasks]

    async def delete_tasks(self, task_ids: List[str]) -> int:
        removed = 0
        async with self._lock:
            for task_id in task_ids:
                task = self._tasks.pop(task_id, None)
                if not task:
                    continue
                if task._runner and not task._runner.done():
                    task._runner.cancel()
                await self._remove_local_file(task.local_path)
                removed += 1
        if removed:
            await self._broadcast()
        return removed

    async def subscribe_task_stream(self) -> asyncio.Queue[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=8)
        async with self._lock:
            self._subscribers.add(queue)
        await queue.put(await self._snapshot_payload())
        return queue

    async def unsubscribe_task_stream(self, queue: asyncio.Queue[str]) -> None:
        async with self._lock:
            self._subscribers.discard(queue)

    async def get_active_local_paths(self) -> List[str]:
        async with self._lock:
            return [task.local_path for task in self._tasks.values() if task.local_path]

    async def cleanup_legacy_temp_dir_on_startup(self) -> int:
        if not os.path.isdir(_LEGACY_TEMP_DIR):
            return 0
        removed = 0
        for name in os.listdir(_LEGACY_TEMP_DIR):
            path = os.path.join(_LEGACY_TEMP_DIR, name)
            try:
                if os.path.isfile(path):
                    os.remove(path)
                    removed += 1
            except OSError:
                pass
        try:
            if not os.listdir(_LEGACY_TEMP_DIR):
                os.rmdir(_LEGACY_TEMP_DIR)
        except OSError:
            pass
        return removed

    async def stop(self) -> None:
        async with self._lock:
            runners = [task._runner for task in self._tasks.values() if task._runner and not task._runner.done()]
        for runner in runners:
            runner.cancel()
        if runners:
            await asyncio.gather(*runners, return_exceptions=True)

    async def _run_task(self, task_id: str) -> None:
        try:
            async with self._condition:
                while self._running_count >= self._CONCURRENCY_LIMIT:
                    await self._condition.wait()
                self._running_count += 1

            task = await self._get_object(task_id)
            if not task:
                return

            await self._update(task_id, status="running", phase="downloading", message="正在从源盘下载", progress=0)

            async def on_download(downloaded: int, total: int, message: str, speed: float):
                total = int(total or task.total_bytes or 0)
                progress = int(downloaded / total * 100) if total else 0
                await self._update(
                    task_id,
                    phase="downloading",
                    downloaded_bytes=downloaded,
                    progress=min(100, progress),
                    speed_bytes_per_second=speed,
                    message=message,
                )

            downloaded = await download_source_to_file(
                source_account_id=task.source_account_id,
                source_file_id=task.source_file_id,
                local_path=task.local_path,
                total_bytes=task.total_bytes,
                progress_callback=on_download,
            )
            if downloaded <= 0:
                raise RuntimeError("源盘下载为空文件")

            await self._update(
                task_id,
                phase="uploading",
                downloaded_bytes=downloaded,
                progress=0,
                speed_bytes_per_second=0.0,
                message="正在上传到目标盘",
            )

            target_folder_id = await self._resolve_target_folder(task)

            async def on_upload(uploaded: int, total: int, message: str, speed: float = 0.0):
                total = int(total or task.total_bytes or 0)
                ratio = (uploaded / total) if total else 0
                progress = int(ratio * 100)
                await self._update(
                    task_id,
                    phase="uploading",
                    uploaded_bytes=uploaded,
                    progress=min(99, progress),
                    message=message or "正在上传到目标盘",
                )

            data = await upload_temp_to_target(
                target_account_id=task.target_account_id,
                target_parent_id=target_folder_id,
                local_path=task.local_path,
                file_name=task.file_name,
                file_size=task.total_bytes,
                conflict_policy=task.conflict_policy,
                progress_callback=on_upload,
            )

            await self._update(
                task_id,
                status="success",
                phase="done",
                progress=100,
                uploaded_bytes=task.total_bytes,
                speed_bytes_per_second=0.0,
                message="兜底传输完成",
                result=data,
                error="",
            )
        except asyncio.CancelledError:
            await self._update(task_id, status="canceled", message="任务已取消", error="任务已取消")
            raise
        except Exception as exc:
            await self._update(
                task_id,
                status="failed",
                phase="failed",
                speed_bytes_per_second=0.0,
                message="兜底传输失败",
                error=str(exc),
            )
        finally:
            final = await self._get_object(task_id)
            if final and final.status == "success":
                await self._remove_local_file(final.local_path)
            async with self._condition:
                self._running_count = max(0, self._running_count - 1)
                self._condition.notify_all()

    async def _resolve_target_folder(self, task: RelayTask) -> str:
        if not task.rel_dir:
            return task.target_parent_id

        from cross_transfer.service import _ensure_target_dir
        from core.driver_service import get_account_driver_instance

        driver = await get_account_driver_instance(task.target_account_id)
        cache = {"": task.target_parent_id}
        return await _ensure_target_dir(driver, task.target_parent_id, task.rel_dir, cache)

    async def _get_object(self, task_id: str) -> Optional[RelayTask]:
        async with self._lock:
            return self._tasks.get(task_id)

    async def _update(self, task_id: str, **fields) -> None:
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            for key, value in fields.items():
                setattr(task, key, value)
            task.updated_at = time.time()
        await self._broadcast()

    async def _snapshot_payload(self) -> str:
        tasks = await self.list_tasks()
        return json.dumps({"tasks": tasks}, ensure_ascii=False)

    async def _broadcast(self) -> None:
        payload = await self._snapshot_payload()
        async with self._lock:
            subscribers = list(self._subscribers)
        for queue in subscribers:
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    queue.put_nowait(payload)
                except asyncio.QueueFull:
                    pass

    async def _remove_local_file(self, local_path: str) -> None:
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except OSError:
                pass

    def _prune_locked(self) -> None:
        if len(self._tasks) <= self._MAX_TASKS:
            return
        finished = [
            task for task in self._tasks.values()
            if task.status in {"success", "failed", "canceled"}
        ]
        finished.sort(key=lambda item: item.updated_at)
        for task in finished[: max(0, len(self._tasks) - self._MAX_TASKS)]:
            self._tasks.pop(task.task_id, None)


relay_task_manager = RelayTaskManager()
