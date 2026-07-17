"""操作包装器：统一缓存装饰器 + 写操作缓存清理 + 认证重试。"""

import time
from typing import List, Callable, Any, Optional
from functools import wraps
import asyncio
from contextvars import ContextVar

from core.base import OperationResult, FileItem
from core.log_manager import get_writer, LogModule
from cache.cache_keys import CacheKeyGenerator

# 通过 contextvar 透传当前账号 ID，让装饰器能在不改 API 签名的情况下识别出调用方账号
current_account_id: ContextVar[Optional[str]] = ContextVar('current_account_id', default=None)

from core.dependency_container import get_cache_manager, get_cache_cleaner, get_hit_tracker

def get_operation_logger():
    try:
        return get_writer(LogModule.FILE_OP)
    except RuntimeError:
        return None

def log_operation(level: str, message: str):
    logger = get_operation_logger()
    if logger:
        getattr(logger, level)(message)


async def _maybe_increment_api_call(driver) -> None:
    if hasattr(driver, '_increment_api_call_count'):
        await driver._increment_api_call_count()


async def _maybe_record_response_time(driver, start_time: float) -> None:
    if hasattr(driver, '_record_response_time'):
        await driver._record_response_time((time.time() - start_time) * 1000)


async def _record_cache_hit() -> None:
    hit_tracker = get_hit_tracker()
    if hit_tracker:
        await hit_tracker.record_hit()


async def _record_cache_miss() -> None:
    hit_tracker = get_hit_tracker()
    if hit_tracker:
        await hit_tracker.record_miss()


def _get_driver_account_id(driver):
    context_account_id = current_account_id.get()
    if context_account_id not in (None, ''):
        return context_account_id

    for attr_name in ('_account_id', 'account_id'):
        value = getattr(driver, attr_name, None)
        if value not in (None, ''):
            return value

    config = getattr(driver, 'config', None)
    value = getattr(config, 'account_id', None) if config is not None else None
    return value if value not in (None, '') else None


async def ensure_driver_auth_request_allowed(driver) -> None:
    account_id = _get_driver_account_id(driver)
    if account_id in (None, '', 'default', 'temp_test'):
        return

    from core.auth_manager import ensure_auth_request_allowed
    await ensure_auth_request_allowed(account_id)


async def _clear_directory_cache_prefix(cache_manager, account_id: str, parent_id: str) -> None:
    from cache.cache_keys import CacheKeyGenerator

    prefix = CacheKeyGenerator.directory_prefix(account_id, parent_id)
    await cache_manager.clear_by_prefix(prefix)


def _get_driver_cache_manager(driver):
    return getattr(driver, '_cache_manager', None)


async def _execute_driver_call(driver, func: Callable, *args, track_time: bool = True, **kwargs):
    await ensure_driver_auth_request_allowed(driver)
    await _maybe_increment_api_call(driver)
    if not track_time:
        return await func(driver, *args, **kwargs)

    start_time = time.time()
    result = await func(driver, *args, **kwargs)
    await _maybe_record_response_time(driver, start_time)
    return result


def _find_attr_value(args, kwargs, attr_name: str, default=None):
    for arg in args:
        if hasattr(arg, attr_name):
            return getattr(arg, attr_name)
    for value in kwargs.values():
        if hasattr(value, attr_name):
            return getattr(value, attr_name)
    return default


def _normalize_parent_ids(parent_ids) -> List[str]:
    return [str(parent_id) for parent_id in parent_ids if parent_id not in (None, '')]


class OperationWrapper:
    def __init__(self, cache_manager=None, cache_cleaner=None):
        self.cache_manager = cache_manager
        self.cache_cleaner = cache_cleaner

    def with_cache_cleanup(self, operation_type: str):
        """装饰器：写操作成功后按 operation_type 自动触发对应缓存清理。"""
        def decorator(func: Callable):
            @wraps(func)
            async def wrapper(driver, *args, **kwargs):
                try:
                    await ensure_driver_auth_request_allowed(driver)
                    result = await func(driver, *args, **kwargs)

                    if isinstance(result, OperationResult) and result.success:
                        await self._cleanup_cache_after_operation(
                            driver, operation_type, result, *args, **kwargs
                        )

                    return result

                except Exception as e:
                    log_operation("error", f"[{operation_type}] 操作异常: {str(e)}")
                    raise

            return wrapper
        return decorator

    async def _cleanup_cache_after_operation(self, driver, operation_type: str,
                                           result: OperationResult, *args, **kwargs):
        if not self.cache_cleaner:
            log_operation("warning", "缓存清理器未初始化，跳过清理")
            return

        try:
            # 优先用 contextvar 里的 account_id，其次再回退到驱动属性 / result.data
            account_id = current_account_id.get()
            if not account_id:
                account_id = getattr(driver, '_account_id', None) or \
                            getattr(driver.config, 'account_id', None) or \
                            result.data.get('account_id') if result.data else None

            if not account_id:
                log_operation("warning", f"无法获取账号ID，跳过缓存清理: {operation_type}")
                return
            handler = self._get_cleanup_handler(operation_type)
            if not handler:
                log_operation("warning", f"未知操作类型: {operation_type}")
                return

            await handler(str(account_id), result, *args, **kwargs)
        except Exception as e:
            log_operation("error", f"缓存清理失败 [{operation_type}]: {str(e)}")

    def _get_cleanup_handler(self, operation_type: str):
        return {
            'create_folder': self._cleanup_create_folder,
            'delete_file': self._cleanup_delete_file,
            'batch_delete_file': self._cleanup_batch_delete_file,
            'move_file': self._cleanup_move_file,
            'copy_file': self._cleanup_move_file,
            'rename_file': self._cleanup_rename_file,
            'upload_file': self._cleanup_upload_file,
        }.get(operation_type)

    async def _cleanup_create_folder(self, account_id: str, result: OperationResult, *args, **kwargs):
        parent_path = args[0] if args else kwargs.get('parent_path', '0')
        folder_name = args[1] if len(args) > 1 else kwargs.get('name', '')
        folder_id = result.data.get('folder_id') if result.data else None
        await self.cache_cleaner.on_file_created(
            account_id=account_id,
            parent_id=parent_path,
            file_name=folder_name,
            file_id=folder_id
        )

    async def _cleanup_delete_file(self, account_id: str, result: OperationResult, *args, **kwargs):
        file_id = args[0] if args else kwargs.get('file_id', '')
        parent_ids = result.data.get('parent_ids', []) if result.data else []
        normalized_parent_ids = _normalize_parent_ids(parent_ids)
        primary_parent_id = normalized_parent_ids[0] if normalized_parent_ids else None
        await self.cache_cleaner.on_file_deleted(
            account_id=account_id,
            file_id=file_id,
            parent_id=primary_parent_id
        )
        if len(normalized_parent_ids) > 1:
            await self._clear_parent_directories(account_id, normalized_parent_ids[1:])
        if not normalized_parent_ids:
            await self._force_clear_account_cache(account_id)

    async def _cleanup_batch_delete_file(self, account_id: str, result: OperationResult, *args, **kwargs):
        file_ids = args[0] if args else kwargs.get('file_ids', [])
        parent_ids = result.data.get('parent_ids', []) if result.data else []
        normalized_parent_ids = _normalize_parent_ids(parent_ids)
        shared_parent_id = normalized_parent_ids[0] if len(normalized_parent_ids) == 1 else None
        for file_id in file_ids:
            await self.cache_cleaner.on_file_deleted(
                account_id=account_id,
                file_id=file_id,
                parent_id=shared_parent_id
            )
        if len(normalized_parent_ids) > 1:
            await self._clear_parent_directories(account_id, normalized_parent_ids)
        if not normalized_parent_ids:
            await self._force_clear_account_cache(account_id)

    async def _cleanup_move_file(self, account_id: str, result: OperationResult, *args, **kwargs):
        file_ids = args[0] if args else kwargs.get('file_ids', [])
        fallback_target_parent_id = args[1] if len(args) > 1 else kwargs.get('target_parent_id', '')
        source_parent_ids = result.data.get('source_parent_ids', []) if result.data else []
        target_parent_id = result.data.get('target_parent_id', fallback_target_parent_id) if result.data else fallback_target_parent_id
        normalized_source_parent_ids = _normalize_parent_ids(source_parent_ids)
        normalized_target_parent_id = str(target_parent_id) if target_parent_id not in (None, '') else ''

        # 常见的移动场景源目录唯一可确定，直接复用统一的 cache_cleaner 入口，不在这里重写一套移动缓存逻辑
        if normalized_target_parent_id and len(normalized_source_parent_ids) == 1 and file_ids:
            old_parent_id = normalized_source_parent_ids[0]
            for file_id in file_ids:
                await self.cache_cleaner.on_file_moved(
                    account_id=account_id,
                    file_id=file_id,
                    old_parent_id=old_parent_id,
                    new_parent_id=normalized_target_parent_id
                )
            return

        await self._clear_parent_directories(account_id, normalized_source_parent_ids)
        if normalized_target_parent_id:
            await self.cache_cleaner._clear_directory_cache(account_id, normalized_target_parent_id)

        for file_id in file_ids:
            try:
                file_info_key = CacheKeyGenerator.file_info_key(account_id, file_id)
                await self.cache_cleaner.cache_manager.delete(file_info_key)
            except Exception as e:
                log_operation("warning", f"清理文件信息缓存失败: {file_id}, 错误: {str(e)}")

        if not normalized_source_parent_ids:
            await self._clear_all_directory_cache(account_id)

    async def _cleanup_rename_file(self, account_id: str, result: OperationResult, *args, **kwargs):
        file_path = args[0] if args else kwargs.get('path', '')
        new_name = args[1] if len(args) > 1 else kwargs.get('new_name', '')
        parent_id = result.data.get('parent_id') if result.data else '0'
        old_name = result.data.get('old_name', '') if result.data else ''
        await self.cache_cleaner.on_file_renamed(
            account_id=account_id,
            parent_id=parent_id,
            file_id=file_path,
            old_name=old_name,
            new_name=new_name
        )

    async def _cleanup_upload_file(self, account_id: str, result: OperationResult, *args, **kwargs):
        parent_path = '0'
        if result.data:
            parent_path = result.data.get('parent_id') or result.data.get('parent_path') or parent_path
        if parent_path == '0':
            if len(args) > 2:
                parent_path = args[2]
            elif len(args) > 1 and isinstance(args[1], str):
                parent_path = args[1]
            else:
                parent_path = kwargs.get('parent_path', '0')
        file_name = result.data.get('file_name', '') if result.data else ''
        file_id = result.data.get('file_id') if result.data else None
        await self.cache_cleaner.on_file_created(
            account_id=account_id,
            parent_id=parent_path,
            file_name=file_name,
            file_id=file_id
        )

    async def _clear_parent_directories(self, account_id: str, parent_ids):
        for parent_id in parent_ids:
            await self.cache_cleaner._clear_directory_cache(account_id, parent_id)
    
    async def _clear_all_directory_cache(self, account_id: str):
        cache_manager = self._get_cleaner_cache_manager()
        if not cache_manager:
            log_operation("warning", "缓存管理器未初始化，跳过目录缓存清理")
            return

        try:
            prefix = f"dir:{account_id}:"
            await cache_manager.clear_by_prefix(prefix)
        except Exception as e:
            log_operation("error", f"清理目录缓存失败: 账号{account_id}, 错误: {str(e)}")

    async def _force_clear_account_cache(self, account_id: str):
        cache_manager = self._get_cleaner_cache_manager()
        if not cache_manager:
            log_operation("warning", "缓存管理器未初始化，跳过强制缓存清理")
            return

        try:
            await cache_manager.clear_account_cache(account_id)
        except Exception as e:
            log_operation("error", f"强制清理账号缓存失败: 账号{account_id}, 错误: {str(e)}")

    def _get_cleaner_cache_manager(self):
        if not self.cache_cleaner:
            return None
        return getattr(self.cache_cleaner, "cache_manager", None)


operation_wrapper = OperationWrapper()


def init_operation_wrapper(cache_manager=None, cache_cleaner=None):
    global operation_wrapper

    if cache_manager is None:
        cache_manager = get_cache_manager()
    if cache_cleaner is None:
        cache_cleaner = get_cache_cleaner()

    operation_wrapper.cache_manager = cache_manager
    operation_wrapper.cache_cleaner = cache_cleaner


def with_file_list_cache(func: Callable) -> Callable:
    """list_files 的缓存装饰器：命中走缓存，未命中回源后回写（空列表也要缓存，避免空目录反复穿透）。"""
    @wraps(func)
    async def wrapper(self, parent_id: str = "0", *args, **kwargs) -> List[FileItem]:
        cache_manager = _get_driver_cache_manager(self)
        if not cache_manager:
            return await _execute_driver_call(self, func, parent_id, *args, **kwargs)

        try:
            account_id = str(getattr(self, 'account_id', 'default'))
            cached_result = await cache_manager.get_directory_cache(
                account_id=account_id,
                parent_id=parent_id,
                page=1
            )

            if cached_result is not None:
                if isinstance(cached_result, list):
                    # 空列表（合法空目录）或元素是 FileItem 才算有效缓存
                    if not cached_result or isinstance(cached_result[0], FileItem):
                        await _record_cache_hit()
                        return cached_result
                    else:
                        # 元素类型不对，可能是历史遗留的旧格式，清掉重新回源
                        log_operation("warning", f"驱动层缓存元素类型不匹配，清理旧缓存: account={account_id}, parent={parent_id}")
                        try:
                            await _clear_directory_cache_prefix(cache_manager, account_id, parent_id)
                        except Exception:
                            pass
                else:
                    # 不是 list（例如旧 dict 格式），直接清
                    log_operation("warning", f"驱动层缓存格式不匹配，清理旧缓存: account={account_id}, parent={parent_id}")
                    try:
                        await _clear_directory_cache_prefix(cache_manager, account_id, parent_id)
                    except Exception:
                        pass
        except Exception as e:
            log_operation("warning", f"缓存读取失败: {e}")

        await _record_cache_miss()
        result = await _execute_driver_call(self, func, parent_id, *args, **kwargs)

        if result is not None:
            try:
                account_id = str(getattr(self, 'account_id', 'default'))
                await cache_manager.set_directory_cache(
                    account_id=account_id,
                    parent_id=parent_id,
                    data=result,
                    page=1
                )

            except Exception as e:
                log_operation("warning", f"缓存设置失败: {e}")

        return result

    return wrapper


def with_file_info_cache(func: Callable) -> Callable:
    """file_info 的缓存装饰器。"""
    @wraps(func)
    async def wrapper(self, file_id: str, *args, **kwargs) -> Optional[FileItem]:
        cache_manager = _get_driver_cache_manager(self)
        if not cache_manager:
            return await _execute_driver_call(self, func, file_id, *args, track_time=False, **kwargs)

        try:
            account_id = str(getattr(self, 'account_id', 'default'))
            cached_result = await cache_manager.get_file_info_cache(
                account_id=account_id,
                file_id=file_id
            )

            if cached_result and isinstance(cached_result, FileItem):
                await _record_cache_hit()
                return cached_result
        except Exception as e:
            log_operation("warning", f"缓存读取失败: {e}")

        await _record_cache_miss()
        result = await _execute_driver_call(self, func, file_id, *args, track_time=False, **kwargs)

        if result:
            try:
                account_id = str(getattr(self, 'account_id', 'default'))
                await cache_manager.set_file_info_cache(
                    account_id=account_id,
                    file_id=file_id,
                    data=result
                )

            except Exception as e:
                log_operation("warning", f"缓存设置失败: {e}")

        return result

    return wrapper


def with_performance_tracking(func: Callable) -> Callable:
    """无缓存方法的性能统计装饰器。"""
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        await ensure_driver_auth_request_allowed(self)
        await _maybe_increment_api_call(self)

        start_time = time.time()
        result = await func(self, *args, **kwargs)

        await _maybe_record_response_time(self, start_time)

        return result

    return wrapper


def auto_cleanup_cache(operation_type: str):
    """写操作缓存清理装饰器。operation_type 取值见 OperationWrapper._get_cleanup_handler。"""
    return operation_wrapper.with_cache_cleanup(operation_type)


def auto_refresh_cache(account_id_func=None):
    """API 层强制刷新装饰器：调用原函数之前先清 (目录 / path_mapping / webdav_metadata) 缓存。"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if account_id_func:
                account_id = account_id_func(*args, **kwargs)
            else:
                account_id = _find_attr_value(args, kwargs, 'account_id')
                account_id = str(account_id) if account_id is not None else None

            parent_id = _find_attr_value(args, kwargs, 'parent_id', '0') or '0'

            if operation_wrapper.cache_cleaner and account_id:
                try:
                    await operation_wrapper.cache_cleaner._clear_directory_cache(account_id, parent_id)
                except Exception as e:
                    log_operation("warning", f"刷新前缓存清理失败: {str(e)}")
                try:
                    await operation_wrapper.cache_cleaner.cache_manager.clear_by_prefix(
                        CacheKeyGenerator.path_mapping_prefix(account_id)
                    )
                except Exception:
                    pass
                try:
                    await operation_wrapper.cache_cleaner.cache_manager.clear_by_prefix(
                        CacheKeyGenerator.webdav_metadata_prefix(account_id)
                    )
                except Exception:
                    pass

            result = await func(*args, **kwargs)

            return result
        return wrapper
    return decorator


async def clear_operation_cache(account_id: str, operation_type: str, **params):
    """业务代码用：按 operation_type 手动清理对应的缓存。"""
    if not operation_wrapper.cache_cleaner:
        log_operation("warning", "缓存清理器未初始化")
        return

    try:
        if operation_type == 'directory_update':
            parent_id = params.get('parent_id', '0')
            await operation_wrapper.cache_cleaner._clear_directory_cache(account_id, parent_id)

        elif operation_type == 'file_created':
            parent_id = params.get('parent_id', '0')
            file_name = params.get('file_name', '')
            file_id = params.get('file_id')
            await operation_wrapper.cache_cleaner.on_file_created(
                account_id=account_id,
                parent_id=parent_id,
                file_name=file_name,
                file_id=file_id,
            )

        elif operation_type == 'account_update':
            config_changed = params.get('config_changed', True)
            await operation_wrapper.cache_cleaner.on_account_updated(account_id, config_changed)

        elif operation_type == 'account_delete':
            await operation_wrapper.cache_cleaner.on_account_deleted(account_id)

        log_operation("debug", f"手动缓存清理完成: {operation_type}")

    except Exception as e:
        log_operation("error", f"手动缓存清理失败: {str(e)}")


def with_auth_retry(max_retries: int = 1):
    """认证重试装饰器：401/403 触发一次 handle_auth_error（刷新 token 等），再重试原请求。"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):
                await ensure_driver_auth_request_allowed(self)
                try:
                    return await func(self, *args, **kwargs)
                except Exception as e:
                    last_exception = e

                    error_str = str(e).lower()
                    is_auth_error = (
                        "401" in str(e) or
                        "403" in str(e) or
                        "unauthorized" in error_str or
                        "token" in error_str and ("invalid" in error_str or "expired" in error_str) or
                        "认证失败" in str(e) or
                        "token认证失败" in str(e)
                    )

                    if attempt == 0 and is_auth_error:
                        log_operation("warning", f"检测到认证错误，尝试刷新: {e}")

                        try:
                            from core.auth_manager import handle_auth_error
                            account_id = getattr(self, 'account_id', None)
                            if account_id:
                                auth_handled = await handle_auth_error(account_id)
                                if auth_handled:
                                    log_operation("info", "认证刷新成功，重试请求")
                                    continue
                                else:
                                    log_operation("error", "认证刷新失败")
                            else:
                                log_operation("error", "无法获取账号ID，跳过认证处理")
                        except Exception as auth_error:
                            log_operation("error", f"认证处理异常: {auth_error}")

                    break

            raise last_exception
        return wrapper
    return decorator


def get_operation_wrapper():
    return operation_wrapper
