"""应用生命周期管理。
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Awaitable, Callable, Iterable, Tuple

from fastapi import FastAPI

from cache import init_cache_system, start_cache_system, stop_cache_system
from config import APP_NAME, Settings
from core.log_manager import LogModule, get_writer, init_logging, start_logging, stop_logging
from core.operation_wrapper import init_operation_wrapper
from core.registry import init_drivers
from database.db import init_database


ShutdownStep = Tuple[str, Callable[[], Awaitable[None]], str]


class _LitepanUvicornInvalidHttpFilter(logging.Filter):

    _needle = "Invalid HTTP request received"

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            return self._needle not in record.getMessage()
        except Exception:
            return True


def silence_uvicorn_invalid_http_warnings() -> None:

    if os.getenv("LITEPAN_LOG_UVICORN_INVALID_HTTP", "").strip().lower() in {"1", "true", "yes", "on"}:
        return

    lg = logging.getLogger("uvicorn.error")
    if any(isinstance(f, _LitepanUvicornInvalidHttpFilter) for f in lg.filters):
        return
    lg.addFilter(_LitepanUvicornInvalidHttpFilter())


def _read_float_env(name: str, default: float, minimum: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        return max(minimum, float(raw))
    except ValueError:
        return default


def _log(system_log, level: str, message: str) -> None:
    try:
        getattr(system_log, level)(message)
    except Exception:
        print(message)


async def _run_shutdown_step(
    system_log,
    operation_name: str,
    operation_func: Callable[[], Awaitable[None]],
    success_msg: str,
    timeout: float,
) -> None:
    if timeout <= 0:
        _log(system_log, "warning", f"{operation_name}未执行，关闭时间预算已用完")
        return

    try:
        await asyncio.wait_for(operation_func(), timeout=timeout)
        _log(system_log, "info", success_msg)
    except asyncio.TimeoutError:
        _log(system_log, "warning", f"{operation_name}超时，已跳过等待")
    except asyncio.CancelledError:
        _log(system_log, "warning", f"{operation_name}被取消，已跳过等待")
    except Exception as e:
        _log(system_log, "error", f"{operation_name}时出错: {e}")


async def _run_shutdown_group(
    system_log,
    steps: Iterable[ShutdownStep],
    step_timeout: float,
    deadline: float,
) -> None:
    tasks = []
    for operation_name, operation_func, success_msg in steps:
        remaining = deadline - time.monotonic()
        timeout = min(step_timeout, remaining)
        tasks.append(_run_shutdown_step(system_log, operation_name, operation_func, success_msg, timeout))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_logging()
    silence_uvicorn_invalid_http_warnings()
    await start_logging()

    await init_database()

    from config import config_manager

    await config_manager.initialize_async()

    from core.log_manager import get_log_manager

    log_manager = get_log_manager()
    if log_manager:
        retention_days = await config_manager.get_async("log_retention_days") or Settings.LOG_RETENTION_DAYS
        await log_manager.start_auto_cleanup(retention_days)

    from core.upload_task_manager import upload_task_manager
    from cross_transfer.relay_task_manager import relay_task_manager

    await upload_task_manager.refresh_concurrency_limit()
    await upload_task_manager.cleanup_orphan_temp_files_on_startup()
    await upload_task_manager.start_temp_file_cleanup()
    await relay_task_manager.cleanup_legacy_temp_dir_on_startup()

    cache_manager = init_cache_system()
    await start_cache_system()

    init_drivers(cache_manager)

    from core.auth_manager import init_auth_system

    await init_auth_system()

    init_operation_wrapper()

    from core.cache_retention_manager import cache_retention_manager

    await cache_retention_manager.initialize()
    await cache_retention_manager.start()

    from core.plugin_system import plugin_manager

    await plugin_manager.initialize()

    from core.strm_sync_manager import strm_sync_manager

    await strm_sync_manager.initialize()
    await strm_sync_manager.start()

    from core.emby_proxy_server import emby_proxy_server_manager

    await emby_proxy_server_manager.initialize()

    system_log = get_writer(LogModule.SYSTEM)
    system_log.info(f"🎉 {APP_NAME} 启动完成！")

    try:
        yield
    finally:
        # 默认总预算必须短于 uvicorn 默认 graceful timeout，避免关闭阶段被上层强制取消。
        shutdown_total_timeout = _read_float_env("LITEPAN_SHUTDOWN_TOTAL_TIMEOUT", 2.0, 1.0)
        shutdown_step_timeout = _read_float_env("LITEPAN_SHUTDOWN_STEP_TIMEOUT", 0.8, 0.2)
        shutdown_deadline = time.monotonic() + shutdown_total_timeout

        try:
            system_log = get_writer(LogModule.SYSTEM)
        except Exception:
            system_log = None

        _log(system_log, "info", f"{APP_NAME} 正在关闭...")

        from core.auth_manager import stop_auth_system
        from core.cache_retention_manager import cache_retention_manager
        from core.emby_proxy_server import emby_proxy_server_manager
        from core.plugin_system import plugin_manager
        from core.strm_sync_manager import strm_sync_manager
        from core.upload_task_manager import upload_task_manager
        from cross_transfer.relay_task_manager import relay_task_manager

        # 调度器和后台服务彼此独立，关闭时并行取消，避免串行吃满容器退出时间。
        await _run_shutdown_group(
            system_log,
            (
                ("停止缓存保持管理器", cache_retention_manager.stop, "缓存保持管理器已停止"),
                ("停止STRM同步管理器", strm_sync_manager.stop, "STRM同步管理器已停止"),
                ("停止Emby反代监听", emby_proxy_server_manager.shutdown, "Emby反代监听已停止"),
                ("停止插件系统", plugin_manager.shutdown, "插件系统已停止"),
                ("停止认证系统", stop_auth_system, "认证系统已停止"),
                ("停止跨盘中继", relay_task_manager.stop, "跨盘中继已停止"),
                ("停止上传系统", upload_task_manager.stop, "上传系统已停止"),
                ("停止缓存系统", stop_cache_system, "缓存系统已停止"),
            ),
            shutdown_step_timeout,
            shutdown_deadline,
        )

        from core.registry import driver_registry

        await _run_shutdown_step(
            system_log,
            "关闭驱动实例",
            driver_registry.close_all_instances,
            "所有驱动实例已关闭",
            min(shutdown_step_timeout, shutdown_deadline - time.monotonic()),
        )

        from core.range_proxy import close_range_proxy_session

        await _run_shutdown_step(
            system_log,
            "关闭Range代理Session",
            close_range_proxy_session,
            "Range代理Session已关闭",
            min(shutdown_step_timeout, shutdown_deadline - time.monotonic()),
        )

        from database.db import db

        await _run_shutdown_step(
            system_log,
            "关闭数据库连接",
            db.close,
            "数据库连接已关闭",
            min(shutdown_step_timeout, shutdown_deadline - time.monotonic()),
        )

        # 日志系统放最后关闭，前面子系统的结束日志才能被正常写出。
        try:
            await asyncio.wait_for(stop_logging(), timeout=0.5)
        except Exception as e:
            print(f"停止日志管理器时出错: {e}")
