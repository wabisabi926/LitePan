"""缓存保持任务的调度器：按 refresh_interval 周期性刷新指定目录，保证 list_files 缓存不过期。"""

import asyncio
import time
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from enum import Enum

from core.log_manager import get_writer, LogModule
from core.driver_base import _extra_api_delay
from core.driver_service import get_account_driver
from database.db import db


class TaskStatus(Enum):
    RUNNING = "running"
    PAUSED = "paused"
    ERROR = "error"
    WARNING = "warning"


@dataclass
class CacheRetentionTask:
    config_id: int
    account_id: int
    parent_id: str
    path: str
    recursive: bool
    scan_depth: int
    api_interval: int
    refresh_interval: int
    status: TaskStatus
    file_count: int = 0
    last_refresh: Optional[datetime] = None
    last_refresh_status: Optional[str] = None
    error_message: Optional[str] = None
    next_run_time: Optional[datetime] = None
    paused_reason: Optional[str] = None
    time_window_enabled: bool = False
    time_start: str = "00:00"
    time_end: str = "00:00"
    scanned_dirs: int = 0
    scanned_files: int = 0
    started_at: Optional[datetime] = None
    last_duration_ms: int = 0


MAX_SCAN_DEPTH = 5  # 产品上限：1..5 层，再深一律用无限递归(-1)，防止手工 API 传入超大层级把网盘打爆


def _normalize_scan_depth(raw, recursive_fallback) -> int:
    """归一化扫描层级：1..MAX_SCAN_DEPTH 为具体层级，-1 为无限递归。
    旧任务没有 scan_depth（raw 为 None）时按 recursive 回退：递归→无限(-1)，单层→1。"""
    if raw is None:
        return -1 if recursive_fallback else 1
    try:
        depth = int(raw)
    except (TypeError, ValueError):
        return -1 if recursive_fallback else 1
    if depth == -1:
        return -1
    if depth < 1:
        return 1
    return min(depth, MAX_SCAN_DEPTH)


class CacheRetentionManager:

    def __init__(self):
        self._tasks: Dict[int, CacheRetentionTask] = {}
        self._running_tasks: Set[int] = set()
        self._running_task_futures: Dict[int, asyncio.Task] = {}
        self._scheduler_task: Optional[asyncio.Task] = None
        self._logger = get_writer(LogModule.CACHE)
        self._is_running = False
        self._global_cache_enabled = True
        self._startup_ready_at: float = 0.0
        self._startup_delay_seconds: int = 100

    async def initialize(self):
        try:
            self._global_cache_enabled = await self._is_cache_globally_enabled()
            configs = await db.get_cache_retention_configs()

            for config in configs:
                task = CacheRetentionTask(
                    config_id=config['id'],
                    account_id=config['account_id'],
                    parent_id=config['parent_id'],
                    path=config['path'],
                    recursive=config['recursive'],
                    scan_depth=_normalize_scan_depth(config.get('scan_depth'), config.get('recursive')),
                    api_interval=config['api_interval'],
                    refresh_interval=config['refresh_interval'],
                    status=TaskStatus(config['status']),
                    file_count=config.get('file_count', 0),
                    last_refresh=datetime.fromisoformat(config['last_refresh'].replace('Z', '+00:00')) if config.get('last_refresh') else None,
                    last_refresh_status=config.get('last_refresh_status'),
                    paused_reason=config.get('paused_reason'),
                    time_window_enabled=bool(config.get('time_window_enabled') or False),
                    time_start=str(config.get('time_start') or '00:00'),
                    time_end=str(config.get('time_end') or '00:00'),
                )

                if task.status == TaskStatus.RUNNING and task.last_refresh:
                    task.next_run_time = task.last_refresh + timedelta(minutes=task.refresh_interval)
                elif task.status == TaskStatus.RUNNING:
                    task.next_run_time = datetime.now()
                else:
                    task.next_run_time = None

                self._tasks[config['id']] = task

            self._logger.debug(f"📦 缓存保持管理器初始化完成，加载了 {len(self._tasks)} 个任务")

        except Exception as e:
            self._logger.error(f"初始化缓存保持管理器失败: {e}")
            raise

    @property
    def startup_remaining(self) -> int:
        return max(0, int(self._startup_ready_at - time.time()))

    async def start(self):
        if self._is_running:
            return

        self._is_running = True
        self._startup_ready_at = time.time() + self._startup_delay_seconds
        self._logger.info(f"缓存保持调度器将在 {self._startup_delay_seconds}s 后开始执行任务")
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())

    async def stop(self):
        if not self._is_running:
            return

        self._is_running = False

        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        for future in list(self._running_task_futures.values()):
            if not future.done():
                future.cancel()
        if self._running_task_futures:
            await asyncio.gather(*self._running_task_futures.values(), return_exceptions=True)
            self._running_task_futures.clear()

        self._logger.debug("缓存保持管理器已停止")

    @staticmethod
    def _is_in_time_window(task: CacheRetentionTask) -> bool:
        if not task.time_window_enabled:
            return True
        try:
            now = datetime.now().time()
            start_h, start_m = map(int, task.time_start.split(":"))
            end_h, end_m = map(int, task.time_end.split(":"))
            start_t = datetime.now().replace(hour=start_h, minute=start_m, second=0, microsecond=0).time()
            end_t = datetime.now().replace(hour=end_h, minute=end_m, second=0, microsecond=0).time()
            if start_t <= end_t:
                return start_t <= now <= end_t
            else:
                return now >= start_t or now <= end_t
        except (ValueError, AttributeError):
            return True

    async def _scheduler_loop(self):
        while self._is_running:
            try:
                remaining = self.startup_remaining
                if remaining > 0:
                    await asyncio.sleep(1)
                    continue

                if not await self._is_cache_globally_enabled():
                    await asyncio.sleep(5)
                    continue

                now = datetime.now()

                strm_busy_account_ids: Set[int] = set()
                try:
                    from core.strm_sync_manager import strm_sync_manager
                    strm_busy_account_ids = strm_sync_manager.get_running_account_ids()
                except Exception:
                    pass

                tasks_to_run = []
                for task_id, task in self._tasks.items():
                    if (task.status == TaskStatus.RUNNING and
                        task.next_run_time and
                        task.next_run_time <= now):
                        if task_id in self._running_tasks:
                            continue
                        if not self._is_in_time_window(task):
                            continue
                        if task.account_id in strm_busy_account_ids:
                            continue
                        tasks_to_run.append(task_id)

                if tasks_to_run:
                    task_id = tasks_to_run[0]
                    if task_id not in self._running_tasks:
                        self._running_tasks.add(task_id)
                        self._tasks[task_id].next_run_time = None
                        future = asyncio.create_task(self._execute_task(task_id))
                        self._running_task_futures[task_id] = future
                await asyncio.sleep(5)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error(f"调度器循环出错: {e}")
                await asyncio.sleep(5)

    async def _is_cache_globally_enabled(self) -> bool:
        try:
            from config import config_manager
            cache_enabled = await config_manager.get_async('cache_enabled')
            return cache_enabled if cache_enabled is not None else True
        except Exception as e:
            self._logger.error(f"检查全局缓存开关失败: {e}")
            return True

    @staticmethod
    def _looks_like_auth_error(error_msg: str) -> bool:
        text = str(error_msg or "").lower()
        auth_error_keywords = [
            '认证', 'token', 'access_token', 'refresh_token', 'token invalid', 'invalid token', 'expired',
            '401', '403', 'cookie', '登录', '权限', 'permission', 'unauthorized',
            'unauthenticated', 'forbidden', 'not authorized'
        ]
        return any(keyword in text for keyword in auth_error_keywords)

    async def on_global_cache_changed(self, enabled: bool):
        try:
            old_state = self._global_cache_enabled
            self._global_cache_enabled = enabled

            if old_state != enabled:
                if enabled:
                    await self._resume_all_tasks()
                else:
                    await self._pause_all_tasks()

        except Exception as e:
            self._logger.error(f"处理全局缓存变更失败: {e}")

    async def _pause_all_tasks(self):
        try:
            paused_count = 0
            for task_id, task in self._tasks.items():
                if task.status == TaskStatus.RUNNING:
                    await self._pause_task(task, persist_status="paused")
                    paused_count += 1

            if paused_count > 0:
                self._logger.debug(f"已暂停 {paused_count} 个缓存保持任务")

        except Exception as e:
            self._logger.error(f"暂停所有任务失败: {e}")

    async def _resume_all_tasks(self):
        try:
            resumed_count = 0
            for task_id, task in self._tasks.items():
                if task.status == TaskStatus.PAUSED:
                    if not await self._should_pause_task_due_to_ttl(task):
                        self._resume_task(task)
                        await db.update_cache_retention_config(task_id, status="running", paused_reason=None)
                        resumed_count += 1

            if resumed_count > 0:
                self._logger.debug(f"已恢复 {resumed_count} 个缓存保持任务")

        except Exception as e:
            self._logger.error(f"恢复所有任务失败: {e}")

    async def _execute_task(self, task_id: int):
        task = self._tasks.get(task_id)
        if not task:
            self._logger.error(f"任务 {task_id} 不存在")
            return

        if task.status != TaskStatus.RUNNING:
            return

        task.scanned_dirs = 0
        task.scanned_files = 0
        task.started_at = datetime.now(timezone.utc)

        try:
            account = await db.get_account(task.account_id)
            if not account:
                await self._pause_task(
                    task,
                    refresh_status="error",
                    error_message="关联的账号已删除",
                    persist_status="paused",
                    persist_error_message="关联的账号已删除",
                    paused_reason="account_disabled"
                )
                return

            if not account.get('is_active', True):
                await self._pause_task(
                    task,
                    refresh_status="error",
                    error_message="关联的账号已禁用",
                    persist_status="paused",
                    persist_error_message="关联的账号已禁用",
                    paused_reason="account_disabled"
                )
                return

            account_config = account.get('config', {})
            auth_status = account_config.get('auth_status', 'active')
            if auth_status == 'failed':
                await self._pause_task(
                    task,
                    refresh_status="error",
                    error_message="关联的账号认证已失效",
                    persist_status="paused",
                    persist_error_message="关联的账号认证已失效，请重新配置账号认证信息",
                    paused_reason="auth_failure"
                )
                return

            if await self._should_pause_task_due_to_ttl(task):
                await self._pause_task_due_to_ttl(task)
                return

            file_count = await self._refresh_cache(task)

            task.last_refresh_status = "success"
            task.error_message = None
            task.file_count = file_count
            task.last_refresh = datetime.now()
            task.last_duration_ms = max(int((datetime.now(timezone.utc) - task.started_at).total_seconds() * 1000) if task.started_at else 0, 0)
            task.next_run_time = datetime.now() + timedelta(minutes=task.refresh_interval)

            await db.update_cache_retention_config(
                task_id,
                file_count=file_count,
                last_refresh=task.last_refresh.isoformat(),
                last_refresh_status=task.last_refresh_status,
                last_duration_ms=task.last_duration_ms,
                error_message=None
            )

        except asyncio.CancelledError:
            pass

        except Exception as e:
            error_msg = str(e)
            self._logger.error(f"缓存保持任务 {task_id} 执行失败: {error_msg}")

            if self._looks_like_auth_error(error_msg):
                await self._pause_task(
                    task,
                    refresh_status="error",
                    error_message=f"认证错误: {error_msg}",
                    persist_status="paused",
                    persist_error_message=f"认证错误，请检查账号配置: {error_msg[:100]}",
                    paused_reason="auth_failure"
                )
            else:
                task.last_refresh = datetime.now()
                task.last_refresh_status = "error"
                task.error_message = error_msg
                task.next_run_time = datetime.now() + timedelta(minutes=task.refresh_interval)

                await db.update_cache_retention_config(
                    task_id,
                    last_refresh=task.last_refresh.isoformat(),
                    last_refresh_status="error",
                    error_message=error_msg[:200]
                )

        finally:
            self._running_tasks.discard(task_id)
            if task_id in self._running_task_futures:
                del self._running_task_futures[task_id]
            t = self._tasks.get(task_id)
            if t:
                t.started_at = None

    async def _should_pause_task_due_to_ttl(self, task: CacheRetentionTask) -> bool:
        try:
            account = await db.get_account(task.account_id)
            if not account:
                return False

            account_config = account.get('config', {})
            cache_ttl = account_config.get('cache_ttl')

            if cache_ttl == 0:
                return True

            return False

        except Exception as e:
            self._logger.error(f"检查任务TTL设置失败: {e}")
            return False

    async def _pause_task_due_to_ttl(self, task: CacheRetentionTask):
        try:
            self._logger.warning(f"任务 {task.config_id} 的账号缓存已禁用(TTL=0)，自动暂停任务")
            await self._pause_task(task, persist_status="paused")

        except Exception as e:
            self._logger.error(f"暂停任务失败: {e}")

    async def _refresh_cache(self, task: CacheRetentionTask) -> int:
        driver = await get_account_driver(task.account_id)
        if not driver:
            raise RuntimeError("驱动实例不可用，可能是账号认证失效或账号已禁用")

        token = _extra_api_delay.set(task.api_interval)
        try:
            if task.scan_depth == 1:
                return await self._refresh_cache_single(driver, task.parent_id, task=task)
            max_depth = None if task.scan_depth == -1 else task.scan_depth
            return await self._refresh_cache_recursive(driver, task.parent_id, task=task, max_depth=max_depth)
        finally:
            _extra_api_delay.reset(token)

    async def _refresh_cache_single(self, driver, parent_id: str, task=None) -> int:
        try:
            files = await driver.list_files(parent_id)
            files_only = [f for f in files if not getattr(f, 'is_dir', True)]

            if task:
                task.scanned_dirs = 1
                task.scanned_files = len(files_only)

            return len(files_only)

        except Exception as e:
            self._logger.error(f"单层缓存刷新失败: {e}")
            raise

    async def _refresh_cache_recursive(self, driver, parent_id: str, task=None, max_depth: Optional[int] = None) -> int:
        total_files = 0
        total_dirs = 0

        try:
            # 队列元素为 (目录ID, 该目录所在层级)，根目录为第 1 层；
            # max_depth=None 表示无限递归，否则只下钻到第 max_depth 层。
            queue = [(parent_id, 1)]
            processed_dirs = set()

            while queue:
                current_dir, depth = queue.pop(0)

                if current_dir in processed_dirs:
                    continue

                processed_dirs.add(current_dir)

                try:
                    files = await driver.list_files(current_dir)

                    files_only = [f for f in files if not getattr(f, 'is_dir', True)]
                    dirs_only = [f for f in files if getattr(f, 'is_dir', False)]

                    total_files += len(files_only)
                    total_dirs += len(dirs_only)

                    if task:
                        task.scanned_dirs = len(processed_dirs)
                        task.scanned_files = total_files

                    if max_depth is None or depth < max_depth:
                        for file in dirs_only:
                            dir_id = getattr(file, 'id', '')
                            if dir_id:
                                queue.append((dir_id, depth + 1))

                except Exception as e:
                    if self._looks_like_auth_error(str(e)):
                        self._logger.error(f"递归缓存刷新遇到认证错误，停止任务: {e}")
                        raise
                    self._logger.warning(f"递归缓存刷新时目录 {current_dir} 失败: {e}")
                    continue

            self._logger.debug(f"递归缓存刷新完成，总共处理了 {len(processed_dirs)} 个目录，{total_files} 个文件，{total_dirs} 个子目录")
            return total_files

        except Exception as e:
            self._logger.error(f"递归缓存刷新失败: {e}")
            raise


    async def _pause_task(
        self,
        task: CacheRetentionTask,
        *,
        refresh_status: Optional[str] = None,
        error_message: Optional[str] = None,
        persist_status: str = "paused",
        persist_error_message: Optional[str] = None,
        paused_reason: Optional[str] = "user",
    ):
        future = self._running_task_futures.get(task.config_id)
        if future and future is not asyncio.current_task() and not future.done():
            future.cancel()
        task.status = TaskStatus.PAUSED
        task.next_run_time = None
        task.paused_reason = paused_reason
        if refresh_status is not None:
            task.last_refresh_status = refresh_status
        if error_message is not None:
            task.error_message = error_message
        self._running_tasks.discard(task.config_id)
        await db.update_cache_retention_config(
            task.config_id,
            status=persist_status,
            paused_reason=paused_reason,
            last_refresh_status=refresh_status,
            error_message=persist_error_message if persist_error_message is not None else error_message
        )

    def _resume_task(self, task: CacheRetentionTask):
        task.status = TaskStatus.RUNNING
        task.paused_reason = None
        task.next_run_time = datetime.now()

    async def pause_tasks_by_account(
        self,
        account_id: int,
        reason: str,
        message: Optional[str] = None,
    ) -> int:
        """账号禁用/认证失效时批量暂停该账号的任务，只对 RUNNING 生效，返回实际暂停数。"""
        if reason not in ("account_disabled", "auth_failure"):
            self._logger.warning(f"pause_tasks_by_account 收到未知 reason: {reason}")
            return 0

        paused = 0
        try:
            for task in list(self._tasks.values()):
                if task.account_id != account_id:
                    continue
                if task.status != TaskStatus.RUNNING:
                    continue

                await self._pause_task(
                    task,
                    refresh_status="error",
                    error_message=message,
                    persist_status="paused",
                    persist_error_message=message,
                    paused_reason=reason,
                )
                paused += 1

            if paused > 0:
                self._logger.info(
                    f"账号 {account_id} 状态变化({reason})，已暂停 {paused} 个缓存保持任务"
                )
        except Exception as e:
            self._logger.error(f"按账号暂停缓存保持任务失败: {e}")
        return paused

    async def resume_tasks_by_account(self, account_id: int) -> int:
        """账号恢复可用时只恢复 account_disabled/auth_failure 的任务，不触碰用户手动暂停的。"""
        resumed = 0
        try:
            for task in list(self._tasks.values()):
                if task.account_id != account_id:
                    continue
                if task.status != TaskStatus.PAUSED:
                    continue
                if task.paused_reason not in ("account_disabled", "auth_failure"):
                    continue
                if await self._should_pause_task_due_to_ttl(task):
                    continue

                self._resume_task(task)
                task.last_refresh_status = None
                task.error_message = None
                await db.update_cache_retention_config(
                    task.config_id,
                    status="running",
                    paused_reason=None,
                    last_refresh_status=None,
                    error_message=None,
                )
                resumed += 1

            if resumed > 0:
                self._logger.info(
                    f"账号 {account_id} 恢复可用，已自动恢复 {resumed} 个缓存保持任务"
                )
        except Exception as e:
            self._logger.error(f"按账号恢复缓存保持任务失败: {e}")
        return resumed

    async def add_task(self, config_id: int, **config_data) -> bool:
        try:
            task = CacheRetentionTask(
                config_id=config_id,
                account_id=config_data['account_id'],
                parent_id=config_data['parent_id'],
                path=config_data['path'],
                recursive=config_data['recursive'],
                scan_depth=_normalize_scan_depth(config_data.get('scan_depth'), config_data.get('recursive')),
                api_interval=config_data['api_interval'],
                refresh_interval=config_data['refresh_interval'],
                status=TaskStatus.RUNNING,
                next_run_time=datetime.now(),
                time_window_enabled=bool(config_data.get('time_window_enabled') or False),
                time_start=str(config_data.get('time_start') or '00:00'),
                time_end=str(config_data.get('time_end') or '00:00'),
            )

            self._tasks[config_id] = task
            self._logger.debug(f"添加缓存保持任务 {config_id}: {task.path}")
            return True

        except Exception as e:
            self._logger.error(f"添加任务失败: {e}")
            return False

    async def update_task(self, config_id: int, **config_data) -> bool:
        try:
            task = self._tasks.get(config_id)
            if not task:
                return False

            task.account_id = config_data['account_id']
            task.parent_id = config_data['parent_id']
            task.path = config_data['path']
            task.recursive = config_data['recursive']
            task.scan_depth = _normalize_scan_depth(config_data.get('scan_depth'), config_data.get('recursive'))
            task.api_interval = config_data['api_interval']
            task.refresh_interval = config_data['refresh_interval']
            task.time_window_enabled = bool(config_data.get('time_window_enabled') or False)
            task.time_start = str(config_data.get('time_start') or '00:00')
            task.time_end = str(config_data.get('time_end') or '00:00')

            if task.status == TaskStatus.RUNNING:
                if task.last_refresh:
                    task.next_run_time = task.last_refresh + timedelta(minutes=task.refresh_interval)
                else:
                    task.next_run_time = datetime.now()

            self._logger.debug(f"更新缓存保持任务 {config_id}: {task.path}")
            return True

        except Exception as e:
            self._logger.error(f"更新任务失败: {e}")
            return False

    async def remove_task(self, config_id: int, clear_cache: bool = False) -> bool:
        try:
            task = self._tasks.get(config_id)
            if not task:
                return False

            if clear_cache:
                await self._clear_task_cache(task)

            if config_id in self._running_tasks:
                self._running_tasks.discard(config_id)

            del self._tasks[config_id]

            self._logger.debug(f"移除缓存保持任务 {config_id}" + ("，已清理缓存" if clear_cache else ""))
            return True

        except Exception as e:
            self._logger.error(f"移除任务失败: {e}")
            return False

    async def remove_configs_by_account(self, account_id: int) -> int:
        try:
            config_ids = [task.config_id for task in self._tasks.values() if task.account_id == account_id]

            if not config_ids:
                return 0

            removed_count = 0
            for config_id in config_ids:
                if await self.remove_task(config_id, clear_cache=False):
                    removed_count += 1

            from database.db import db
            deleted = await db.delete_cache_retention_configs_by_account(account_id)

            self._logger.debug(f"已删除账号 {account_id} 的 {removed_count} 个缓存保持配置")
            return removed_count

        except Exception as e:
            self._logger.error(f"删除账号缓存保持配置失败: {e}")
            return 0

    async def _clear_task_cache(self, task: CacheRetentionTask):
        try:
            from core.dependency_container import get_cache_cleaner

            cache_cleaner = get_cache_cleaner()
            if not cache_cleaner:
                return

            await cache_cleaner._clear_directory_cache(str(task.account_id), task.parent_id)
            self._logger.debug(f"已清理任务 {task.config_id} 的根目录缓存: {task.parent_id}")

            if task.scan_depth != 1:
                self._logger.debug(f"任务 {task.config_id} 为多层扫描任务，建议手动清理子目录缓存")

        except Exception as e:
            self._logger.error(f"清理任务缓存失败: {e}")

    async def toggle_task(self, config_id: int) -> bool:
        try:
            task = self._tasks.get(config_id)
            if not task:
                return False

            if task.status == TaskStatus.RUNNING:
                await self._pause_task(task, persist_status="paused")
            else:
                self._resume_task(task)
                await db.update_cache_retention_config(config_id, status="running")

            return True

        except Exception as e:
            self._logger.error(f"切换任务状态失败: {e}")
            return False

    async def refresh_task_now(self, config_id: int) -> str:
        """返回 'running' | 'already_running' | 'blocked_by_strm' | 'missing'"""
        try:
            task = self._tasks.get(config_id)
            if not task:
                return "missing"

            if config_id in self._running_tasks:
                return "already_running"

            try:
                from core.strm_sync_manager import strm_sync_manager
                if task.account_id in strm_sync_manager.get_running_account_ids():
                    return "blocked_by_strm"
            except Exception:
                pass

            self._running_tasks.add(config_id)
            future = asyncio.create_task(self._execute_task(config_id))
            self._running_task_futures[config_id] = future
            return "running"

        except Exception as e:
            self._logger.error(f"立即刷新任务失败: {e}")
            return False

    async def force_stop_task(self, config_id: int) -> bool:
        """强制停止正在执行的任务，不改变任务状态，不影响下次调度。"""
        try:
            task = self._tasks.get(config_id)
            if not task:
                return False

            future = self._running_task_futures.get(config_id)
            if future and not future.done():
                future.cancel()

            self._running_tasks.discard(config_id)
            self._running_task_futures.pop(config_id, None)

            if task.next_run_time is None:
                task.next_run_time = datetime.now()

            self._logger.info(f"强制停止缓存保持任务 {config_id}（下次调度不受影响）")
            return True

        except Exception as e:
            self._logger.error(f"强制停止缓存保持任务失败: {e}")
            return False

    async def refresh_all_tasks(self) -> int:
        try:
            running_tasks = [task_id for task_id, task in self._tasks.items()
                           if task.status == TaskStatus.RUNNING]

            if not running_tasks:
                return 0

            started_count = 0
            for task_id in running_tasks:
                if task_id not in self._running_tasks:
                    self._running_tasks.add(task_id)
                    future = asyncio.create_task(self._execute_task(task_id))
                    self._running_task_futures[task_id] = future
                    started_count += 1
            self._logger.debug(f"已触发 {started_count} 个运行中任务")
            return started_count

        except Exception as e:
            self._logger.error(f"执行所有任务失败: {e}")
            return 0

    def get_task_status(self, config_id: int) -> Optional[Dict]:
        task = self._tasks.get(config_id)
        if not task:
            return None

        current_duration_ms = 0
        if task.started_at and config_id in self._running_tasks:
            current_duration_ms = max(
                int((datetime.now(timezone.utc) - task.started_at).total_seconds() * 1000),
                0,
            )

        return {
            'config_id': task.config_id,
            'status': task.status.value,
            'file_count': task.file_count,
            'last_refresh': task.last_refresh.isoformat() if task.last_refresh else None,
            'last_refresh_status': task.last_refresh_status,
            'next_run_time': task.next_run_time.isoformat() if task.next_run_time else None,
            'error_message': task.error_message,
            'scanned_dirs': task.scanned_dirs,
            'scanned_files': task.scanned_files,
            'started_at': task.started_at.isoformat() if task.started_at else None,
            'last_duration_ms': task.last_duration_ms,
            'current_duration_ms': current_duration_ms or task.last_duration_ms,
        }

    def get_all_tasks_status(self) -> List[Dict]:
        return [self.get_task_status(task_id) for task_id in self._tasks.keys()]

    def get_running_account_ids(self) -> Set[int]:
        """返回当前正在执行任务的账号 ID 集合，供 STRM 管理器跨管理器互斥。"""
        account_ids: Set[int] = set()
        for task_id in self._running_tasks:
            task = self._tasks.get(task_id)
            if task:
                account_ids.add(task.account_id)
        return account_ids


cache_retention_manager = CacheRetentionManager()
