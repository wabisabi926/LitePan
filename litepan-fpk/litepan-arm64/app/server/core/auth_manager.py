"""账号认证：刷新、状态持久化、后台定时检查。"""

import time
import asyncio
from enum import Enum
from typing import Dict, Any, Optional
from core.log_manager import get_writer, LogModule
from core.account_utils import AUTH_RUNTIME_FIELDS, filter_runtime_config
from core.exceptions import AuthenticationError


class RefreshOutcome(Enum):
    SUCCESS = "success"
    RETRYABLE = "retryable"
    FATAL = "fatal"


AUTH_PERSIST_FIELDS = {
    'access_token', 'refresh_token', 'expires_at', 'token_expires_at',
    'last_refresh_time', 'auth_status', 'refresh_attempts'
}

FAILED_RETRY_SECONDS = 86400

_ACTIVE_COOLDOWN_STEPS = [
    (1, 60),
    (2, 120),
    (3, 300),
    (4, 1800),
]
_ACTIVE_FAILED_THRESHOLD = 5
# 被动刷新由业务请求触发，比主动刷新频率高，阈值放宽一些再判定 failed
_PASSIVE_FAILED_THRESHOLD = 10
_PASSIVE_COOLDOWN_SECONDS = 60
_PASSIVE_SUCCESS_REUSE_SECONDS = 20

def _stepped_cooldown(attempts: int) -> int:
    for threshold, cooldown in _ACTIVE_COOLDOWN_STEPS:
        if attempts <= threshold:
            return cooldown
    return 1200

def auth_log(level: str, message: str, **kwargs):
    try:
        logger = get_writer(LogModule.AUTH)
        getattr(logger, level)(message, **kwargs)
    except (RuntimeError, AttributeError):
        import time
        timestamp = time.strftime('%H:%M:%S')
        print(f"[{timestamp}] [{level.upper()}] {message}")


async def _pause_related_tasks_for_auth_failure(account_id: int, message: str) -> None:
    try:
        from core.cache_retention_manager import cache_retention_manager
        await cache_retention_manager.pause_tasks_by_account(account_id, "auth_failure", message)
    except Exception as e:
        auth_log("warning", f"暂停账号相关缓存保持任务失败: {e}", account_id=account_id)

    try:
        from core.strm_sync_manager import strm_sync_manager
        await strm_sync_manager.pause_tasks_by_account(account_id, "auth_failure", message)
    except Exception as e:
        auth_log("warning", f"暂停账号相关STRM任务失败: {e}", account_id=account_id)


async def _resume_related_tasks_after_auth_success(account_id: int) -> None:
    try:
        from core.cache_retention_manager import cache_retention_manager
        await cache_retention_manager.resume_tasks_by_account(account_id)
    except Exception as e:
        auth_log("warning", f"恢复账号相关缓存保持任务失败: {e}", account_id=account_id)

    try:
        from core.strm_sync_manager import strm_sync_manager
        await strm_sync_manager.resume_tasks_by_account(account_id)
    except Exception as e:
        auth_log("warning", f"恢复账号相关STRM任务失败: {e}", account_id=account_id)


def _read_runtime_value(auth_manager, config: Dict[str, Any], key: str, default=None):
    value = config.get(key) if isinstance(config, dict) else None
    if value not in (None, ''):
        return value

    manager_config = getattr(auth_manager, 'config', None)
    if manager_config is not None:
        value = getattr(manager_config, key, None)
        if value not in (None, ''):
            return value

    return default


async def _mark_auth_success(account_id: int, driver_instance=None) -> None:
    from database.db import db

    current_time = int(time.time())
    account = await db.get_account(account_id)
    if not account:
        return

    config = account['config']
    source_config = getattr(driver_instance, 'config', None) if driver_instance else None
    for field in AUTH_PERSIST_FIELDS:
        if source_config and hasattr(source_config, field):
            config[field] = getattr(source_config, field)

    config.update({
        'last_refresh_time': current_time,
        'auth_status': 'active',
        'refresh_attempts': 0,
    })
    await db.update_account(account_id, config=config)
    await _resume_related_tasks_after_auth_success(account_id)


async def get_auth_request_block(account_id: int) -> Optional[Dict[str, Any]]:
    """返回当前账号是否应阻断被动请求；只阻断已知认证失败状态，不处理普通网络波动。"""
    normalized_account_id = _normalize_account_id(account_id)
    if normalized_account_id is None:
        return None

    try:
        from database.db import db

        account = await db.get_account(normalized_account_id)
        config = account['config'] if account else {}
        auth_manager = auth_scheduler.auth_managers.get(normalized_account_id)

        auth_status = str(
            _read_runtime_value(auth_manager, config, 'auth_status', 'active') or 'active'
        ).lower()
        if auth_status not in ('cooldown', 'failed', 'token_expired'):
            return None

        current_time = int(time.time())
        last_refresh_time = int(_read_runtime_value(auth_manager, config, 'last_refresh_time', 0) or 0)

        if auth_status == 'token_expired':
            return {
                'account_id': normalized_account_id,
                'auth_status': auth_status,
                'remaining_seconds': 86400 * 365,
                'next_retry_time': current_time + 86400 * 365,
                'message': "账号认证令牌已失效，需要重新授权",
            }
        elif auth_status == 'cooldown':
            wait_seconds = int(
                _read_runtime_value(auth_manager, config, 'retry_cooldown_seconds', 1200) or 1200
            )
            status_desc = "冷却期"
        else:
            wait_seconds = int(
                _read_runtime_value(auth_manager, config, 'failed_retry_seconds', FAILED_RETRY_SECONDS)
                or FAILED_RETRY_SECONDS
            )
            status_desc = "长冷却"

        next_retry_time = last_refresh_time + max(1, wait_seconds)
        if last_refresh_time <= 0 or current_time >= next_retry_time:
            return None

        remaining = next_retry_time - current_time
        return {
            'account_id': normalized_account_id,
            'auth_status': auth_status,
            'remaining_seconds': remaining,
            'next_retry_time': next_retry_time,
            'message': f"账号认证处于{status_desc}，剩余{remaining}秒后允许再次尝试",
        }
    except Exception as e:
        auth_log("error", f"检查认证请求闸门失败: {e}", account_id=normalized_account_id)
        return None


async def ensure_auth_request_allowed(account_id: int) -> None:
    block = await get_auth_request_block(account_id)
    if not block:
        return

    auth_log("warning", block['message'], account_id=block['account_id'])
    raise AuthenticationError(block['message'])


class AuthManager:
    def __init__(self, account_id: int, driver_instance, config):
        self.account_id = account_id
        self.driver = driver_instance
        self.config = config
        self.auth_type = getattr(config, 'auth_type', 'unknown')
        self.refresh_attempts = getattr(config, 'refresh_attempts', 0)

    async def refresh_auth(self, caller: str = "active") -> RefreshOutcome:
        try:
            current_time = int(time.time())
            await self._update_auth_status(last_refresh_time=current_time)

            if hasattr(self.driver, 'refresh_auth'):
                result = await self.driver.refresh_auth()

                if isinstance(result, RefreshOutcome):
                    outcome = result
                elif result is True:
                    outcome = RefreshOutcome.SUCCESS
                else:
                    outcome = RefreshOutcome.RETRYABLE

                if outcome == RefreshOutcome.SUCCESS:
                    await self._update_auth_status(auth_status='active', refresh_attempts=0)
                    await self._persist_driver_config()
                    await _resume_related_tasks_after_auth_success(self.account_id)
                    return RefreshOutcome.SUCCESS
                else:
                    if caller == "active":
                        await self._handle_active_refresh_failure(outcome)
                    else:
                        await self._handle_passive_refresh_failure(outcome)
                    return outcome
            else:
                auth_log("error", f"驱动不支持认证刷新", account_id=self.account_id)
                return RefreshOutcome.RETRYABLE

        except Exception as e:
            auth_log("error", f"认证刷新异常: {e}", account_id=self.account_id)
            if caller == "active":
                await self._handle_active_refresh_failure(RefreshOutcome.RETRYABLE)
            else:
                await self._handle_passive_refresh_failure(RefreshOutcome.RETRYABLE)
            return RefreshOutcome.RETRYABLE

    async def _handle_active_refresh_failure(self, outcome: RefreshOutcome):
        if outcome == RefreshOutcome.FATAL:
            await self._update_auth_status(auth_status='token_expired')
            await _pause_related_tasks_for_auth_failure(
                self.account_id, "认证令牌已失效，需要重新授权"
            )
            await self._send_fatal_notification()
            return

        current_attempts = self.refresh_attempts + 1
        self.refresh_attempts = current_attempts

        if current_attempts >= _ACTIVE_FAILED_THRESHOLD:
            await self._update_auth_status(
                auth_status='failed',
                refresh_attempts=current_attempts
            )
            await _pause_related_tasks_for_auth_failure(
                self.account_id,
                "账号认证刷新连续失败，已暂停相关后台任务"
            )
        else:
            cooldown = _stepped_cooldown(current_attempts)
            await self._update_auth_status(
                auth_status='cooldown',
                refresh_attempts=current_attempts,
                retry_cooldown_seconds=cooldown
            )
            await _pause_related_tasks_for_auth_failure(
                self.account_id,
                f"账号认证刷新失败，进入{cooldown}秒冷却期"
            )

    async def _handle_passive_refresh_failure(self, outcome: RefreshOutcome):
        if outcome == RefreshOutcome.FATAL:
            await self._update_auth_status(auth_status='token_expired')
            await _pause_related_tasks_for_auth_failure(
                self.account_id, "认证令牌已失效，需要重新授权"
            )
            await self._send_fatal_notification()
            return

        # 累计失败次数，避免纯被动模式（主动刷新关闭）下死号永远在 active/cooldown 间振荡、永不 failed
        current_attempts = self.refresh_attempts + 1
        if current_attempts >= _PASSIVE_FAILED_THRESHOLD:
            await self._update_auth_status(
                auth_status='failed',
                refresh_attempts=current_attempts
            )
            await _pause_related_tasks_for_auth_failure(
                self.account_id, "账号认证连续失败，已暂停相关后台任务"
            )
        else:
            await self._update_auth_status(
                auth_status='cooldown',
                refresh_attempts=current_attempts,
                retry_cooldown_seconds=_PASSIVE_COOLDOWN_SECONDS
            )

    async def _send_fatal_notification(self):
        try:
            from database.db import db
            from core.notification_manager import notification_manager
            account = await db.get_account(self.account_id)
            account_name = account['name'] if account else f"账号{self.account_id}"

            await notification_manager.notify(
                type="auth_expired",
                level="error",
                title="存储账号认证已失效",
                message=f"「{account_name}」的认证令牌已失效，需要重新授权后才能继续使用。",
                account_id=self.account_id,
                action_label="前往设置",
                action_route="/accounts",
                dedup_key=f"auth_fatal_{self.account_id}"
            )
        except Exception as e:
            auth_log("error", f"发送通知失败: {e}", account_id=self.account_id)

    async def _update_auth_status(self, **kwargs):
        try:
            from database.db import db
            if 'refresh_attempts' in kwargs:
                self.refresh_attempts = kwargs['refresh_attempts']
            for key, value in kwargs.items():
                setattr(self.config, key, value)
            account = await db.get_account(self.account_id)
            if account:
                config = account['config']
                config.update(kwargs)
                await db.update_account(self.account_id, config=config)
        except Exception as e:
            auth_log("error", f"更新认证状态失败: {e}", account_id=self.account_id)
    
    async def _persist_driver_config(self):
        try:
            from database.db import db
            account = await db.get_account(self.account_id)
            if account:
                config = account['config']
                config_dict = {}
                for field in self.config.__dataclass_fields__:
                    if field in AUTH_PERSIST_FIELDS:
                        config_dict[field] = getattr(self.config, field)
                config.update(config_dict)
                await db.update_account(self.account_id, config=config)
        except Exception as e:
            auth_log("error", f"持久化驱动配置失败: {e}", account_id=self.account_id)

    async def sync_refresh_success(self, driver_instance=None):
        current_time = int(time.time())
        source_config = getattr(driver_instance, 'config', None) if driver_instance else None
        for field in AUTH_PERSIST_FIELDS:
            if source_config and hasattr(source_config, field):
                setattr(self.config, field, getattr(source_config, field))
        await self._update_auth_status(
            last_refresh_time=current_time,
            auth_status='active',
            refresh_attempts=0
        )
        await self._persist_driver_config()
        await _resume_related_tasks_after_auth_success(self.account_id)

class AuthScheduler:
    def __init__(self):
        self.auth_managers: Dict[int, AuthManager] = {}
        self._passive_refresh_locks: Dict[int, asyncio.Lock] = {}
        self._running = False
        self._background_task = None
        self._recalculate_event = asyncio.Event()
        self._active_refresh_enabled = True
        self._first_loop = True
        self._first_execution = True

    @staticmethod
    def _normalize_enabled(value) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).lower() != 'false'

    def _supports_refresh(self, driver_instance) -> bool:
        return bool(
            driver_instance and
            hasattr(driver_instance, 'config') and
            hasattr(driver_instance.config, 'supports_refresh') and
            driver_instance.config.supports_refresh
        )

    async def _register_auth_manager(self, account_id: int, driver_instance, config):
        if self._supports_refresh(driver_instance):
            await self.add_account(account_id, driver_instance, config)

    async def add_account(self, account_id: int, driver_instance, config):
        auth_manager = AuthManager(account_id, driver_instance, config)
        self.auth_managers[account_id] = auth_manager

        if hasattr(driver_instance, 'set_auth_manager'):
            driver_instance.set_auth_manager(auth_manager)
        setattr(driver_instance, '_account_id', account_id)

        await self._trigger_recalculation("账号已注册")

    async def remove_account(self, account_id: int):
        if account_id in self.auth_managers:
            del self.auth_managers[account_id]
            auth_log("debug", f"账号已从认证调度器中移除", account_id=account_id)
            await self._trigger_recalculation("账号已移除")

    async def start_background_check(self):
        if not self._active_refresh_enabled:
            return
        if self._running:
            return

        self._running = True
        self._background_task = asyncio.create_task(self._main_loop())

    async def stop_background_check(self):
        if not self._running:
            return
        self._running = False
        if self._background_task:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass
        
        auth_log("debug", "认证调度器已停止")

    async def set_active_refresh_enabled(self, enabled: bool):
        enabled = self._normalize_enabled(enabled)
        previous_enabled = self._active_refresh_enabled
        was_running = self._running
        self._active_refresh_enabled = enabled

        if enabled:
            if not previous_enabled:
                auth_log("info", "已启用主动认证刷新")
            await self.start_background_check()
            if was_running:
                await self._trigger_recalculation("主动刷新已启用")
            return

        if previous_enabled:
            auth_log("info", "已关闭主动认证刷新")
        await self.stop_background_check()
    
    async def _main_loop(self):
        auth_log("info", f"认证调度器启动，管理 {len(self.auth_managers)} 个账号")

        while self._running:
            try:
                next_check_time = await self._calculate_next_check_time()
                recalculate_needed = await self._wait_until_check_time_or_recalculate(next_check_time)
                if recalculate_needed:
                    continue

                await self._execute_check()
                if self._recalculate_event.is_set():
                    self._recalculate_event.clear()
                continue

            except asyncio.CancelledError:
                break
            except Exception as e:
                auth_log("error", f"认证调度器异常: {e}")
                await asyncio.sleep(60)

        auth_log("info", "认证调度器主循环结束")

    async def _calculate_next_check_time(self, emit_log: bool = True) -> float:
        if not self.auth_managers:
            return time.time() + 3600

        min_check_time = float('inf')
        current_time = time.time()
        shortest_account_name = None
        account_check_summaries = []
        from database.db import db

        first_loop = self._first_loop
        if self._first_loop:
            self._first_loop = False

        auth_managers_items = list(self.auth_managers.items())
        for account_id, auth_manager in auth_managers_items:
            try:
                check_time = await self._calculate_account_next_time(account_id, current_time, is_first_loop=first_loop)
                try:
                    account = await db.get_account(account_id)
                    account_name = account['name'] if account else f"账号{account_id}"
                    config = account['config'] if account else {}
                    auth_status = config.get('auth_status', 'active')
                except:
                    account_name = f"账号{account_id}"
                    auth_status = "unknown"
                check_time_str = time.strftime('%H:%M:%S', time.localtime(check_time))
                wait_seconds = max(0, int(check_time - current_time))
                if wait_seconds >= 60:
                    wait_desc = f"{wait_seconds // 60}分钟"
                else:
                    wait_desc = f"{wait_seconds}秒"
                account_check_summaries.append(
                    f"{account_name}[{auth_manager.auth_type}/{auth_status}]={check_time_str}({wait_desc})"
                )
                if check_time < min_check_time:
                    min_check_time = check_time
                    shortest_account_name = account_name
            except Exception as e:
                auth_log("error", f"计算账号检查时间失败: {e}", account_id=account_id)
        
        if min_check_time == float('inf'):
            return current_time + 3600

        if emit_log and shortest_account_name and min_check_time > current_time:
            wait_seconds = int(min_check_time - current_time)
            next_check_str = time.strftime('%H:%M:%S', time.localtime(min_check_time))
            if account_check_summaries:
                auth_log("debug", "各账号检查时间: " + " | ".join(account_check_summaries))

            # 首次循环里所有账号都进入 60-90s 的开机退避窗口（与其他同类程序错开，避免并发刷新冲突），
            # "最短的那个"只是随机摇到的最小数，没有单独的业务含义；
            # 退避结束后 _execute_check 会 force_all 全量检查并打印"首次启动，强制检查全部 N 个账号"，
            # 这里就不再重复输出，避免误导。
            if not first_loop:
                if wait_seconds >= 60:
                    wait_minutes = wait_seconds // 60
                    auth_log("info", f"重新计算检查时间，最短检查时间: {shortest_account_name}，下次检查: {next_check_str} (等待{wait_minutes}分钟)")
                else:
                    auth_log("info", f"重新计算检查时间，最短检查时间: {shortest_account_name}，下次检查: {next_check_str} (等待{wait_seconds}秒)")

        return min_check_time

    async def _calculate_account_next_time(self, account_id: int, current_time: float, is_first_loop: bool = False) -> float:
        try:
            from database.db import db
            account = await db.get_account(account_id)
            if not account:
                return current_time + 3600
            
            auth_manager = self.auth_managers[account_id]
            config = account['config']
            auth_status = config.get('auth_status', 'active')
            last_refresh_time = config.get('last_refresh_time', 0)

            import random

            if is_first_loop:
                # 开机退避窗口：与其他同类程序（如同机其它容器）错开，让它们先刷新网盘认证，
                # 避免开机瞬间多个程序并发刷新同一账号（尤其 115 一次性 refresh_token 会被烧掉）。
                return current_time + random.randint(60, 90)

            if auth_status == 'failed' or auth_status == 'token_expired':
                return last_refresh_time + 86400

            elif auth_status == 'cooldown':
                cooldown_seconds = getattr(auth_manager.config, 'retry_cooldown_seconds', 1200)
                return last_refresh_time + cooldown_seconds

            elif auth_status == 'active':
                if last_refresh_time == 0:
                    return current_time + random.randint(60, 90)
                else:
                    if auth_manager.auth_type in ['token', 'id_secret']:
                        token_expires = getattr(auth_manager.config, 'token_expires_seconds', 7200)
                        refresh_advance = getattr(auth_manager.config, 'refresh_advance_seconds', 1800)
                        return last_refresh_time + token_expires - refresh_advance
                    elif auth_manager.auth_type == 'cookie':
                        if hasattr(auth_manager.driver, 'config') and hasattr(auth_manager.driver.config, 'health_check_interval'):
                            check_interval = auth_manager.driver.config.health_check_interval
                        else:
                            check_interval = getattr(auth_manager.config, 'health_check_interval', 3600)
                        return last_refresh_time + check_interval

            return current_time + 3600

        except Exception as e:
            auth_log("error", f"计算账号时间异常: {e}", account_id=account_id)
            return current_time + 3600

    async def _wait_until_check_time_or_recalculate(self, target_time: float):
        current_time = time.time()
        if target_time <= current_time:
            return False

        wait_time = target_time - current_time

        try:
            await asyncio.wait_for(self._recalculate_event.wait(), timeout=wait_time)
            self._recalculate_event.clear()
            return True
        except asyncio.TimeoutError:
            return False

    async def _trigger_recalculation(self, reason: str = ""):
        if self._running:
            self._recalculate_event.set()
            if reason:
                auth_log("info", f"触发认证重算: {reason}")

    async def _execute_check(self):
        accounts_to_refresh = []
        current_time = time.time()
        tolerance_seconds = 30
        force_all = self._first_execution
        if self._first_execution:
            self._first_execution = False

        auth_managers_items = list(self.auth_managers.items())
        for account_id, auth_manager in auth_managers_items:
            try:
                next_check_time = await self._calculate_account_next_time(account_id, current_time)
                if force_all or next_check_time <= current_time + tolerance_seconds:
                    accounts_to_refresh.append((account_id, auth_manager, next_check_time))
            except Exception as e:
                auth_log("error", f"检查账号状态失败: {e}", account_id=account_id)

        if force_all and accounts_to_refresh:
            auth_log("info", f"首次启动，强制检查全部 {len(accounts_to_refresh)} 个账号认证状态")

        accounts_to_refresh.sort(key=lambda x: x[2])

        if accounts_to_refresh:
            success_count = 0
            attempted_count = 0
            skipped_count = 0
            total_count = len(accounts_to_refresh)

            for i, (account_id, auth_manager, next_check_time) in enumerate(accounts_to_refresh):
                try:
                    refresh_lock = self._passive_refresh_locks.setdefault(account_id, asyncio.Lock())
                    async with refresh_lock:
                        latest_time = time.time()
                        latest_next_check_time = await self._calculate_account_next_time(account_id, latest_time)
                        if latest_next_check_time > latest_time + tolerance_seconds:
                            auth_log(
                                "debug",
                                "跳过主动刷新：账号刷新时间已被其他流程更新",
                                account_id=account_id
                            )
                            skipped_count += 1
                            continue

                        attempted_count += 1
                        result = await auth_manager.refresh_auth(caller="active")
                        if result == RefreshOutcome.SUCCESS:
                            success_count += 1

                    if i < len(accounts_to_refresh) - 1:
                        await asyncio.sleep(2.0)

                except Exception as e:
                    auth_log("error", f"账号 {account_id} 刷新异常: {e}")

            # 首次启动会把全部账号塞进候选池，但锁内重算多半发现"还在窗口期内"
            # 然后全部 skip，此时打"X/N 个账号刷新成功"会让人误以为刷新失败；
            # 改成更准确的描述：真正尝试过的才计入分母。
            if attempted_count == 0:
                if force_all:
                    auth_log("info", f"首次启动健康检查完成: {total_count} 个账号当前认证均有效，无需刷新")
                # 普通周期里如果全 skip，说明窗口被其它流程提前刷过了，debug 即可，不必噪音
            else:
                auth_log("info", f"检查周期完成: {success_count}/{attempted_count} 个账号刷新成功")
        else:
            auth_log("info", "检查周期完成: 无账号需要更新")


auth_scheduler = AuthScheduler()


def _normalize_account_id(account_id) -> int | None:
    try:
        return int(account_id)
    except (TypeError, ValueError):
        return None

async def handle_auth_error(account_id: int) -> bool:
    """业务侧 401/403 的统一入口：按锁串行；被动刷新不累计失败计数。"""
    try:
        normalized_account_id = _normalize_account_id(account_id)
        if normalized_account_id is None:
            return False

        if normalized_account_id in auth_scheduler.auth_managers:
            refresh_lock = auth_scheduler._passive_refresh_locks.setdefault(normalized_account_id, asyncio.Lock())
            async with refresh_lock:
                auth_manager = auth_scheduler.auth_managers[normalized_account_id]
                block = await get_auth_request_block(normalized_account_id)
                if block:
                    auth_log("warning", f"跳过本次被动刷新：{block['message']}", account_id=normalized_account_id)
                    return False

                last_refresh_time = int(getattr(auth_manager.config, "last_refresh_time", 0) or 0)
                auth_status = str(getattr(auth_manager.config, "auth_status", "active") or "active").lower()
                if auth_status == "active" and last_refresh_time > 0:
                    elapsed = int(time.time()) - last_refresh_time
                    if 0 <= elapsed <= _PASSIVE_SUCCESS_REUSE_SECONDS:
                        auth_log(
                            "info",
                            f"复用刚刷新成功的认证结果，跳过重复被动刷新({elapsed}s)",
                            account_id=normalized_account_id
                        )
                        return True

                outcome = await auth_manager.refresh_auth(caller="passive")

                if outcome == RefreshOutcome.SUCCESS:
                    await auth_scheduler._trigger_recalculation("被动刷新成功")

                return outcome == RefreshOutcome.SUCCESS
        return False
    except Exception as e:
        auth_log("error", f"处理认证错误失败: {e}", account_id=account_id)
        return False


async def sync_driver_refresh_success(account_id: int, driver_instance=None) -> None:
    """驱动自己完成刷新后，同步认证状态并触发调度重算。"""
    try:
        normalized_account_id = _normalize_account_id(account_id)
        if normalized_account_id is None:
            return

        if normalized_account_id in auth_scheduler.auth_managers:
            auth_manager = auth_scheduler.auth_managers[normalized_account_id]
            await auth_manager.sync_refresh_success(driver_instance=driver_instance)
        else:
            await _mark_auth_success(normalized_account_id, driver_instance=driver_instance)

        await auth_scheduler._trigger_recalculation("Cookie自动更新")
    except Exception as e:
        auth_log("error", f"直接刷新成功后的状态同步失败: {e}", account_id=account_id)


async def notify_direct_refresh_success(account_id: int) -> None:
    await sync_driver_refresh_success(account_id)

async def _resend_fatal_notifications_on_startup(accounts: list) -> None:
    """启动时对已处于 token_expired 状态的账号补发通知。"""
    from core.notification_manager import notification_manager
    for account in accounts:
        config = account.get('config', {})
        if config.get('auth_status') != 'token_expired':
            continue
        account_name = account.get('name', f"账号{account.get('id')}")
        try:
            await notification_manager.notify(
                type="auth_expired",
                level="error",
                title="存储账号认证已失效",
                message=f"「{account_name}」的认证令牌已失效，需要重新授权后才能继续使用。",
                account_id=account.get('id'),
                action_label="前往设置",
                action_route="/accounts",
                dedup_key=f"auth_fatal_{account.get('id')}"
            )
        except Exception as e:
            auth_log("error", f"补发通知失败: {e}", account_id=account.get('id'))


async def init_auth_system():
    try:
        from database.db import db
        from core.driver_service import get_account_driver_instance
        from config import config_manager
        accounts = await db.list_accounts(include_inactive=False)

        for account in accounts:
            try:
                driver_config = filter_runtime_config(account['config'].copy())

                driver_instance = await get_account_driver_instance(
                    account_id=account['id'],
                    account=account,
                    require_active=False,
                    existing_driver_name=account['driver_type'],
                    config_override=driver_config
                )

                await auth_scheduler._register_auth_manager(account['id'], driver_instance, driver_instance.config)

            except Exception as e:
                auth_log("error", f"加载账号到认证管理器失败: {e}", account_id=account['id'])

        auth_log("debug", f"认证系统初始化完成，已加载 {len(accounts)} 个账号")

        await _resend_fatal_notifications_on_startup(accounts)

        active_refresh_enabled = await config_manager.get_async('auth_active_refresh_enabled')
        if active_refresh_enabled is None:
            active_refresh_enabled = True

        await auth_scheduler.set_active_refresh_enabled(
            AuthScheduler._normalize_enabled(active_refresh_enabled)
        )

    except Exception as e:
        auth_log("error", f"认证系统初始化失败: {e}")
        raise

async def stop_auth_system():
    try:
        auth_log("info", "正在停止认证系统")
        await auth_scheduler.stop_background_check()
        auth_log("info", "认证系统已停止")
    except Exception as e:
        auth_log("error", f"停止认证系统失败: {e}")

async def notify_cookie_updated(account_id: int):
    try:
        normalized_account_id = _normalize_account_id(account_id)
        if normalized_account_id is None:
            return
        await sync_driver_refresh_success(normalized_account_id)
    except Exception as e:
        auth_log("error", f"Cookie更新通知失败: {e}", account_id=account_id)
