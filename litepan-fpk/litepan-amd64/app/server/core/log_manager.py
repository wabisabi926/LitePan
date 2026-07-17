"""统一日志"""

import asyncio
import json
import sys
from datetime import datetime, timedelta
from typing import Callable, List, Dict, Any, Optional
from pathlib import Path
from enum import Enum, IntEnum
from dataclasses import dataclass


class LogLevel(IntEnum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50

    @classmethod
    def from_string(cls, level_str: str) -> 'LogLevel':
        level_map = {
            'DEBUG': cls.DEBUG,
            'INFO': cls.INFO,
            'WARNING': cls.WARNING, 
            'WARN': cls.WARNING,
            'ERROR': cls.ERROR,
            'CRITICAL': cls.CRITICAL,
            'FATAL': cls.CRITICAL
        }
        return level_map.get(level_str.upper(), cls.INFO)

    def to_string(self) -> str:
        return self.name

    def to_emoji(self) -> str:
        emoji_map = {
            self.DEBUG: '🔍',
            self.INFO: 'ℹ️',
            self.WARNING: '⚠️',
            self.ERROR: '❌',
            self.CRITICAL: '🚨'
        }
        return emoji_map.get(self, 'ℹ️')


class LogModule(Enum):
    SYSTEM = "system"
    DRIVER = "driver"
    API = "api"
    WEB = "web"
    DRIVER_SYSTEM = "driver_system"
    CACHE = "cache"
    DATABASE = "database"
    AUTH = "auth"
    FILE_OP = "file_op"
    CONFIG = "config"
    WEBDAV = "webdav"

    def to_display_name(self) -> str:
        display_map = {
            self.SYSTEM: "系统",
            self.DRIVER: "驱动",
            self.API: "接口",
            self.WEB: "网页",
            self.DRIVER_SYSTEM: "驱动",
            self.CACHE: "缓存",
            self.DATABASE: "数据",
            self.AUTH: "认证",
            self.FILE_OP: "文件",
            self.CONFIG: "配置",
            self.WEBDAV: "WebDAV"
        }
        return display_map.get(self, self.value)

    def to_color(self) -> str:
        color_map = {
            self.SYSTEM: "#2196F3",
            self.DRIVER: "#4CAF50",
            self.API: "#FF9800",
            self.WEB: "#9C27B0",
            self.DRIVER_SYSTEM: "#00BCD4",
            self.CACHE: "#795548",
            self.DATABASE: "#3F51B5",
            self.AUTH: "#F44336",
            self.FILE_OP: "#009688",
            self.CONFIG: "#FF5722",
            self.WEBDAV: "#673AB7"
        }
        return color_map.get(self, "#666666")


class LogStorage:
    """按天切文件的日志落盘实现，写入走 asyncio.Queue，避免阻塞调用方。"""

    def __init__(self, log_dir: str = "log"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._write_queue = asyncio.Queue()
        self._writer_task = None
        self._fallback_file = self.log_dir / "_fallback.log"

    def _get_log_file_path(self, timestamp: str) -> Path:
        date_part = timestamp[:10] or datetime.now().strftime("%Y-%m-%d")
        return self.log_dir / f"{date_part}.log"

    def _serialize_entry(self, entry: Dict[str, Any]) -> str:
        normalized = self._normalize_entry(entry) or {}
        return json.dumps(normalized, ensure_ascii=False) + "\n"

    def _parse_log_line(self, line: str) -> Optional[Dict[str, Any]]:
        if not line:
            return None
        try:
            return self._normalize_entry(json.loads(line))
        except json.JSONDecodeError:
            return None

    def _normalize_level(self, value: Any) -> int:
        if isinstance(value, LogLevel):
            return int(value)
        if isinstance(value, int):
            try:
                return int(LogLevel(value))
            except ValueError:
                return int(LogLevel.INFO)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return int(LogLevel.INFO)
            try:
                return int(LogLevel(int(text)))
            except (ValueError, TypeError):
                return int(LogLevel.from_string(text))
        return int(LogLevel.INFO)

    def _normalize_module(self, value: Any) -> str:
        if isinstance(value, LogModule):
            return value.value
        if value is None:
            return LogModule.SYSTEM.value
        text = str(value).strip()
        return text or LogModule.SYSTEM.value

    def _normalize_details(self, value: Any) -> Optional[Dict[str, Any]]:
        if value is None or isinstance(value, dict):
            return value
        return {"value": value}

    def _normalize_entry(self, entry: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(entry, dict):
            return None

        normalized = dict(entry)
        normalized["timestamp"] = str(normalized.get("timestamp") or datetime.now().isoformat())
        normalized["level"] = self._normalize_level(normalized.get("level"))
        normalized["module"] = self._normalize_module(normalized.get("module"))
        normalized["message"] = str(normalized.get("message") or "")
        normalized["details"] = self._normalize_details(normalized.get("details"))
        return normalized

    async def start_writer(self):
        if self._writer_task is None or self._writer_task.done():
            self._writer_task = asyncio.create_task(self._writer_loop())

    async def stop_writer(self):
        if self._writer_task and not self._writer_task.done():
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass

    async def _writer_loop(self):
        # batch 10 条 / 1s 超时二选一触发落盘，避免频繁打开文件
        batch = []
        batch_size = 10
        timeout = 1.0

        try:
            while True:
                try:
                    log_entry = await asyncio.wait_for(self._write_queue.get(), timeout=timeout)
                    batch.append(log_entry)
                    if len(batch) >= batch_size:
                        await self._write_batch(batch)
                        batch.clear()
                except asyncio.TimeoutError:
                    if batch:
                        await self._write_batch(batch)
                        batch.clear()
        except asyncio.CancelledError:
            if batch:
                await self._write_batch(batch)
            raise

    async def _write_batch(self, batch: List[Dict]):
        if not batch:
            return

        try:
            await asyncio.to_thread(self._write_batch_sync, batch)
        except Exception as e:
            # 主路径写文件失败，落到 fallback 文件，保证至少不丢日志
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 日志写入失败，使用fallback: {e}")
            self._write_to_fallback(batch)

    def _write_batch_sync(self, batch: List[Dict]):
        grouped_lines: Dict[Path, List[str]] = {}
        for entry in batch:
            normalized = self._normalize_entry(entry)
            if not normalized:
                continue
            log_file = self._get_log_file_path(normalized['timestamp'])
            grouped_lines.setdefault(log_file, []).append(self._serialize_entry(normalized))

        for log_file, lines in grouped_lines.items():
            log_file.parent.mkdir(parents=True, exist_ok=True)
            with open(log_file, 'a', encoding='utf-8') as f:
                f.writelines(lines)

    def _write_to_fallback(self, batch: List[Dict]):
        try:
            with open(self._fallback_file, 'a', encoding='utf-8') as f:
                for entry in batch:
                    normalized = self._normalize_entry(entry)
                    if normalized:
                        f.write(self._serialize_entry(normalized))
                f.flush()
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Fallback文件写入失败: {e}")
            for entry in batch:
                print(f"EMERGENCY LOG: {entry['message']}")

    async def write_log(self, level: LogLevel, module: LogModule, message: str,
                       details: Optional[Dict] = None, **kwargs):
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'level': int(level),
            'module': module.value,
            'message': message,
            'details': details,
            'account_id': kwargs.get('account_id'),
            'driver_name': kwargs.get('driver_name')
        }
        try:
            self._write_queue.put_nowait(log_entry)
        except asyncio.QueueFull:
            # 队列满了宁可丢日志，也不阻塞业务
            pass

    def list_log_files(self) -> List[Path]:
        return sorted(
            [path for path in self.log_dir.glob("*.log") if path.name != self._fallback_file.name],
            key=lambda path: path.name,
            reverse=True
        )

    def read_all_logs(self) -> List[Dict[str, Any]]:
        logs: List[Dict[str, Any]] = []
        for log_file in self.list_log_files():
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        entry = self._parse_log_line(line.strip())
                        if entry:
                            logs.append(entry)
            except FileNotFoundError:
                continue
        logs.sort(key=lambda entry: entry.get('timestamp', ''), reverse=True)
        return logs

    def get_stats(self, ack_at: Optional[datetime] = None) -> Dict[str, Any]:
        """统计日志。

        - `recent_errors`：最近 24 小时**未确认**的 ERROR/CRITICAL 数量。
          仪表盘用它决定是否显示红色错误状态。当用户在仪表盘点击"已读"，
          上层调用方会把 ack_at 设为当时的时间戳，使 cutoff 前移，
          从而让已经看过的错误从这个数字里消失。
        - `recent_errors_total`：最近 24 小时全部 ERROR/CRITICAL 数量。
          不受 ack_at 影响，用于日志页面或对账场景显示真实计数。
        """
        logs = self.read_all_logs()
        by_level: Dict[Any, int] = {}
        by_module: Dict[str, int] = {}
        recent_errors_unack = 0
        recent_errors_total = 0
        cutoff_24h = datetime.now() - timedelta(days=1)
        ack_cutoff = max(cutoff_24h, ack_at) if ack_at else cutoff_24h

        for entry in logs:
            level = self._normalize_level(entry.get('level'))
            module = self._normalize_module(entry.get('module'))
            by_level[level] = by_level.get(level, 0) + 1
            by_module[module] = by_module.get(module, 0) + 1

            try:
                entry_time = datetime.fromisoformat(entry.get('timestamp'))
            except Exception:
                entry_time = None
            if entry_time and int(level) >= int(LogLevel.ERROR):
                if entry_time >= cutoff_24h:
                    recent_errors_total += 1
                if entry_time >= ack_cutoff:
                    recent_errors_unack += 1

        return {
            "total": len(logs),
            "by_level": by_level,
            "by_module": by_module,
            "recent_errors": recent_errors_unack,
            "recent_errors_total": recent_errors_total,
        }

    async def cleanup_old_logs(self, days: int) -> int:
        retention_days = int(days)
        if retention_days <= 0:
            return await self.clear_all_logs()

        keep_after = datetime.now().date() - timedelta(days=retention_days - 1)

        def cleanup_sync() -> int:
            deleted_count = 0
            for log_file in self.list_log_files():
                try:
                    file_date = datetime.strptime(log_file.stem, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if file_date < keep_after:
                    log_file.unlink(missing_ok=True)
                    deleted_count += 1
            return deleted_count

        return await asyncio.to_thread(cleanup_sync)

    async def clear_all_logs(self) -> int:
        """删除所有日志文件。返回删除的文件数。"""
        def clear_sync() -> int:
            deleted_count = 0
            for log_file in sorted(self.log_dir.glob("*.log")):
                try:
                    log_file.unlink(missing_ok=True)
                    deleted_count += 1
                except FileNotFoundError:
                    continue
            return deleted_count

        return await asyncio.to_thread(clear_sync)

    async def delete_matching_logs(self, matcher: Callable[[Dict[str, Any]], bool]) -> int:
        """按条件删除日志行。返回删除的日志条数。"""
        def delete_sync() -> int:
            deleted_count = 0
            for log_file in sorted(self.log_dir.glob("*.log")):
                kept_lines = []
                changed = False
                try:
                    with open(log_file, "r", encoding="utf-8") as f:
                        for line in f:
                            stripped = line.strip()
                            entry = self._parse_log_line(stripped)
                            if entry and matcher(entry):
                                deleted_count += 1
                                changed = True
                                continue
                            kept_lines.append(line)
                except FileNotFoundError:
                    continue

                if not changed:
                    continue
                if kept_lines:
                    with open(log_file, "w", encoding="utf-8") as f:
                        f.writelines(kept_lines)
                else:
                    log_file.unlink(missing_ok=True)
            return deleted_count

        return await asyncio.to_thread(delete_sync)


class LogWriter:
    def __init__(self, module: LogModule, manager: 'LogManager'):
        self.module = module
        self.manager = manager

    def debug(self, message: str, **kwargs):
        self.manager.log(LogLevel.DEBUG, self.module, message, **kwargs)

    def info(self, message: str, **kwargs):
        self.manager.log(LogLevel.INFO, self.module, message, **kwargs)

    def warning(self, message: str, **kwargs):
        self.manager.log(LogLevel.WARNING, self.module, message, **kwargs)

    def warn(self, message: str, **kwargs):
        self.warning(message, **kwargs)

    def error(self, message: str, **kwargs):
        self.manager.log(LogLevel.ERROR, self.module, message, **kwargs)

    def critical(self, message: str, **kwargs):
        self.manager.log(LogLevel.CRITICAL, self.module, message, **kwargs)

    def fatal(self, message: str, **kwargs):
        self.critical(message, **kwargs)


class LogManager:
    def __init__(self,
                 min_level: LogLevel = LogLevel.INFO,
                 console_output: bool = True,
                 log_dir: str = "log"):
        self.min_level = min_level
        self.console_output = console_output
        self.storage = LogStorage(log_dir)
        self._writers: Dict[LogModule, LogWriter] = {}
        self._started = False
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_retention_days = 30
        self._cleanup_interval_hours = 24

    async def start(self):
        if not self._started:
            await self.storage.start_writer()
            self._started = True

    async def stop(self):
        if self._started:
            await self.stop_auto_cleanup()
            await self.storage.stop_writer()
            self._started = False

    async def start_auto_cleanup(self, retention_days: int = 30, interval_hours: int = 24):
        self._cleanup_retention_days = max(1, int(retention_days))
        self._cleanup_interval_hours = max(1, int(interval_hours))

        if self._cleanup_task and not self._cleanup_task.done():
            return

        self._cleanup_task = asyncio.create_task(self._auto_cleanup_loop())

    async def stop_auto_cleanup(self):
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        self._cleanup_task = None

    async def _auto_cleanup_loop(self):
        try:
            await self._run_cleanup_once()

            while True:
                await asyncio.sleep(self._cleanup_interval_hours * 3600)
                await self._run_cleanup_once()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log(LogLevel.ERROR, LogModule.SYSTEM, f"自动日志清理任务异常: {e}")

    async def _run_cleanup_once(self):
        try:
            deleted_count = await self.storage.cleanup_old_logs(self._cleanup_retention_days)
            if deleted_count > 0:
                self.log(
                    LogLevel.INFO,
                    LogModule.SYSTEM,
                    f"自动清理旧日志完成，保留 {self._cleanup_retention_days} 天，已删除 {deleted_count} 个日志文件"
                )
            else:
                self.log(
                    LogLevel.DEBUG,
                    LogModule.SYSTEM,
                    f"自动清理旧日志完成，保留 {self._cleanup_retention_days} 天，无需删除"
                )
        except Exception as e:
            self.log(LogLevel.ERROR, LogModule.SYSTEM, f"自动清理旧日志失败: {e}")
    
    def get_writer(self, module: LogModule) -> LogWriter:
        if module not in self._writers:
            self._writers[module] = LogWriter(module, self)
        return self._writers[module]

    def log(self, level: LogLevel, module: LogModule, message: str,
            details: Optional[Dict] = None, **kwargs):
        if level < self.min_level:
            return

        try:
            if self.console_output:
                self._console_output(level, module, message, details)

            if self._started:
                asyncio.create_task(
                    self.storage.write_log(level, module, message, details, **kwargs)
                )
        except Exception as e:
            # 日志系统本身崩了也要尽量让消息走到 stdout，不能再 raise 出去
            try:
                timestamp = datetime.now().strftime("%H:%M:%S")
                level_emoji = level.to_emoji()
                module_name = module.to_display_name()
                emergency_msg = f"[{timestamp}] {level_emoji} {module_name} | {message}"
                print(emergency_msg, file=sys.stdout, flush=True)
                print(f"[{timestamp}] 系统 | 日志系统异常: {e}", file=sys.stdout, flush=True)
            except:
                pass

    def _console_output(self, level: LogLevel, module: LogModule,
                       message: str, details: Optional[Dict] = None):
        timestamp = datetime.now().strftime("%H:%M:%S")
        level_emoji = level.to_emoji()
        module_name = module.to_display_name()

        log_line = f"[{timestamp}] {level_emoji} {module_name} | {message}"

        if details:
            details_str = json.dumps(details, ensure_ascii=False, indent=2)
            log_line += f"\n  详情: {details_str}"

        print(log_line, file=sys.stdout, flush=True)

    def set_level(self, level: LogLevel):
        self.min_level = level

    def set_console_output(self, enabled: bool):
        self.console_output = enabled


_global_manager: Optional[LogManager] = None

def get_writer(module: LogModule) -> LogWriter:
    global _global_manager
    if _global_manager is None:
        raise RuntimeError("日志管理器未初始化，请先调用 init_log_manager()")

    return _global_manager.get_writer(module)

def init_log_manager(min_level: LogLevel = LogLevel.INFO,
                    console_output: bool = True,
                    log_dir: str = "log") -> LogManager:
    global _global_manager
    _global_manager = LogManager(min_level, console_output, log_dir)
    return _global_manager

async def start_log_manager():
    global _global_manager
    if _global_manager:
        await _global_manager.start()

async def stop_log_manager():
    global _global_manager
    if _global_manager:
        await _global_manager.stop()

def get_log_manager() -> Optional[LogManager]:
    return _global_manager

# 兼容别名，历史代码还在用 init_logging 等名字
init_logging = init_log_manager
start_logging = start_log_manager
stop_logging = stop_log_manager
