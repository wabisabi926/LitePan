"""管理员 API：账号 CRUD、缓存、认证调度、系统配置。"""

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from typing import Dict, Any
from datetime import datetime, UTC
import time

from database.db import db
from core.registry import get_all_driver_info, get_config_schema, validate_driver_config, get_driver_names, driver_registry
from core.error_handler import raise_not_found, raise_api_error
from core.base import get_driver_capabilities, driver_supports
from core.log_manager import get_writer, LogModule
from api.deps import require_admin_auth
from api.responses import error_response as _error_response, success_response as _success_response
from core.account_utils import filter_runtime_config as _filter_driver_config, get_account_or_404
from core.driver_service import get_account_driver_instance
from core.security import generate_password_hash, assess_admin_credential_state
from core.session_manager import session_manager
router = APIRouter()

def _mask_sensitive_value(value: Any) -> str:
    if value is None:
        return "None"
    text = str(value)
    if not text:
        return "(empty)"
    if len(text) <= 8:
        return "*" * len(text)
    return f"{text[:3]}***{text[-3:]}"

from core.dependency_container import get_cache_manager, get_cache_cleaner, get_hit_tracker
from core.utils import normalize_bool

# ==================== 认证管理器通知函数 ====================

async def _notify_auth_manager_account_added(account_id: int, driver_name: str, config: Dict[str, Any]):
    try:
        from core.auth_manager import auth_scheduler
        logger = get_writer(LogModule.API)
        
        driver_instance = await get_account_driver_instance(
            account_id=account_id,
            require_active=False,
            account={
                "id": account_id,
                "driver_type": driver_name,
                "config": config,
                "is_active": True
            },
            existing_driver_name=driver_name,
            config_override=config
        )
        
        if driver_instance and hasattr(driver_instance.config, 'supports_refresh') and driver_instance.config.supports_refresh:
            await auth_scheduler.add_account(account_id, driver_instance, driver_instance.config)
            
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.warning(f"通知认证管理器失败: {e}")

async def _notify_auth_manager_account_updated(account_id: int, old_config: Dict[str, Any], new_config: Dict[str, Any], driver_name: str):
    try:
        from core.auth_manager import auth_scheduler
        from database.db import db
        from core.dependency_container import get_cache_cleaner
        import time
        import json
        
        logger = get_writer(LogModule.API)
        auth_fields = ['client_id', 'client_secret', 'access_token', 'refresh_token', 'username', 'password', 'cookie']
        
        auth_changed = False
        for field in auth_fields:
            old_value = old_config.get(field)
            new_value = new_config.get(field)
            if old_value != new_value:
                auth_changed = True
                break
        
        # 下载模式改了需要清掉旧 mode 的链接缓存，否则还会返回旧模式的链接
        old_download_mode = old_config.get('download_mode', 'redirect')
        new_download_mode = new_config.get('download_mode', 'redirect')
        if old_download_mode != new_download_mode:
            if logger:
                logger.debug(f"下载模式发生变化: {old_download_mode} -> {new_download_mode}，准备清理缓存")
            cache_cleaner = get_cache_cleaner()
            if cache_cleaner:
                await cache_cleaner.clear_download_mode_cache(str(account_id))

        if auth_changed:
            account = await db.get_account(account_id)
            if account:
                config = account['config']
                if isinstance(config, str):
                    config = json.loads(config)
                config.update({
                    'last_refresh_time': int(time.time()),
                    'auth_status': 'active',
                    'refresh_attempts': 0
                })
                await db.update_account(account_id, config=config)
            
            await auth_scheduler.remove_account(account_id)
            
            driver_instance = await get_account_driver_instance(
                account_id=account_id,
                require_active=False,
                account={
                    "id": account_id,
                    "driver_type": driver_name,
                    "config": new_config,
                    "is_active": True
                },
                existing_driver_name=driver_name,
                config_override=new_config
            )
            
            if driver_instance and hasattr(driver_instance.config, 'supports_refresh') and driver_instance.config.supports_refresh:
                await auth_scheduler.add_account(account_id, driver_instance, driver_instance.config)
        else:
            await auth_scheduler._trigger_recalculation("账号配置已更新")
            
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.warning(f"通知认证管理器账号更新失败: {e}")

async def _notify_auth_manager_account_deleted(account_id: int):
    try:
        from core.auth_manager import auth_scheduler
        await auth_scheduler.remove_account(account_id)
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.warning(f"通知认证管理器账号删除失败: {e}")

async def _notify_auth_manager_account_status_changed(account_id: int, enabled: bool):
    try:
        from core.auth_manager import auth_scheduler
        from database.db import db
        
        if enabled:
            account = await db.get_account(account_id)
            if account:
                driver_config = _filter_driver_config(account['config'])
                
                driver_instance = await get_account_driver_instance(
                    account_id=account_id,
                    account=account,
                    require_active=False,
                    existing_driver_name=account['driver_type'],
                    config_override=driver_config
                )
                    
                if driver_instance and hasattr(driver_instance.config, 'supports_refresh') and driver_instance.config.supports_refresh:
                    await auth_scheduler.add_account(account_id, driver_instance, driver_instance.config)
        else:
            await auth_scheduler.remove_account(account_id)
            
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.warning(f"通知认证管理器账号状态变更失败: {e}")


async def _pause_account_related_tasks(account_id: int, reason: str, message: str):
    """账号不可用时联动暂停 cache_retention / strm 任务（reason: account_disabled | auth_failure）。"""
    logger = get_writer(LogModule.API)
    try:
        from core.cache_retention_manager import cache_retention_manager
        await cache_retention_manager.pause_tasks_by_account(account_id, reason, message)
    except Exception as e:
        logger.warning(f"联动暂停缓存保持任务失败(account={account_id} reason={reason}): {e}")
    try:
        from core.strm_sync_manager import strm_sync_manager
        await strm_sync_manager.pause_tasks_by_account(account_id, reason, message)
    except Exception as e:
        logger.warning(f"联动暂停 STRM 任务失败(account={account_id} reason={reason}): {e}")


async def _resume_account_related_tasks(account_id: int):
    """账号重新可用时，只恢复因 account_disabled/auth_failure 被暂停的任务。"""
    logger = get_writer(LogModule.API)
    try:
        from core.cache_retention_manager import cache_retention_manager
        await cache_retention_manager.resume_tasks_by_account(account_id)
    except Exception as e:
        logger.warning(f"联动恢复缓存保持任务失败(account={account_id}): {e}")
    try:
        from core.strm_sync_manager import strm_sync_manager
        await strm_sync_manager.resume_tasks_by_account(account_id)
    except Exception as e:
        logger.warning(f"联动恢复 STRM 任务失败(account={account_id}): {e}")


async def _test_driver_connection(driver_name: str, config: Dict[str, Any], account_id: str = "temp_test"):
    driver = None
    try:
        driver = await driver_registry.get_driver_instance(
            account_id=account_id,
            driver_name=driver_name,
            config=config
        )
        result = await driver.test_connection()
        
        # test_connection 可能刷新了 token，回抓驱动里最新的 config
        import dataclasses
        updated_config = {}
        if hasattr(driver, 'config') and dataclasses.is_dataclass(driver.config):
            updated_config = dataclasses.asdict(driver.config)

        return result.success, (None if result.success else result.message), updated_config
    finally:
        if account_id == "temp_test" and driver is not None:
            await driver_registry._close_driver_instance(account_id)


def _raise_non_litepan_error(error: Exception, message: str):
    from core.exceptions import LitePanError

    if isinstance(error, LitePanError):
        raise error
    raise_api_error(f"{message}: {str(error)}", "internal_error")


# ==================== 驱动管理路由 ====================

@router.get("/drivers")
async def get_available_drivers(request: Request, session_data: dict = Depends(require_admin_auth)):
    try:
        drivers_info = get_all_driver_info()
        
        result_data = {}
        for name, info in drivers_info.items():
            result_data[name] = {
                "name": info["name"],
                "display_name": info["display_name"],
                "version": info["version"],
                "capabilities": info["capabilities"],
                "description": info["description"],
                "author": info["author"],
                "card_color": info.get("card_color"),
                "card_name": info.get("card_name"),
                "card_logo": info.get("card_logo"),
                "icon": info.get("icon"),
                "auto_oauth": info.get("auto_oauth", 0),
                "supports_qr_login": info.get("supports_qr_login", 0)
            }
        
        return _success_response(data=result_data, message="获取驱动列表成功")
    except Exception as e:
        return _error_response(message=str(e), data={})


@router.get("/drivers/{driver_name}/config-schema")
async def get_driver_config_schema(driver_name: str, session_data: dict = Depends(require_admin_auth)):
    schema = get_config_schema(driver_name)
    if not schema:
        raise_not_found(f"驱动 {driver_name} 配置结构")
    
    return _success_response(data=schema, message="获取驱动配置成功")


def _resolve_qr_login_driver(driver_name: str):
    driver_info = driver_registry.get_driver_info(driver_name)
    if not driver_info or not driver_info.get("supports_qr_login"):
        raise_api_error(f"驱动 {driver_name} 不支持扫码登录", "unsupported", 400)

    bundle = driver_registry._drivers.get(driver_name) or {}
    driver_class = bundle.get("driver_class")
    if driver_class is None or not hasattr(driver_class, "start_qr_login") or not hasattr(driver_class, "poll_qr_login"):
        raise_api_error(f"驱动 {driver_name} 未实现扫码登录接口", "unsupported", 400)
    return driver_class


@router.post("/qr-login/start")
async def qr_login_start(request: Request, session_data: dict = Depends(require_admin_auth)):
    try:
        body = await request.json()
    except Exception:
        body = {}
    driver_name = (body or {}).get("driver_type")
    if not driver_name:
        raise_api_error("缺少 driver_type", "validation", 400)

    driver_class = _resolve_qr_login_driver(driver_name)
    try:
        result = await driver_class.start_qr_login()
        return _success_response(data=result, message="二维码生成成功")
    except Exception as e:
        return _error_response(message=f"生成二维码失败: {str(e)}")


@router.get("/qr-login/status/{state_id}")
async def qr_login_status(
    state_id: str,
    driver_type: str,
    session_data: dict = Depends(require_admin_auth),
):
    driver_class = _resolve_qr_login_driver(driver_type)
    try:
        result = await driver_class.poll_qr_login(state_id)
        return _success_response(data=result, message="查询扫码状态成功")
    except Exception as e:
        return _error_response(message=f"查询扫码状态失败: {str(e)}")


@router.get("/accounts/{account_id}/capabilities")
async def get_account_capabilities(account_id: int, session_data: dict = Depends(require_admin_auth)):
    """鸭子类型：根据驱动实例动态探测真正支持的能力，前端据此控制按钮。"""
    account = await get_account_or_404(account_id)

    try:
        driver = await get_account_driver_instance(account_id, account=account)
        capabilities = get_driver_capabilities(driver)
        
        return _success_response(
            data={
                "account_id": account_id,
                "driver_type": account['driver_type'],
                "capabilities": capabilities,
                "supports": {
                    "list": driver_supports(driver, "list"),
                    "info": driver_supports(driver, "info"),
                    "download": driver_supports(driver, "download"),
                    "create_folder": driver_supports(driver, "create_folder"),
                    "delete": driver_supports(driver, "delete"),
                    "batch_delete": driver_supports(driver, "batch_delete"),
                    "rename": driver_supports(driver, "rename"),
                    "move": driver_supports(driver, "move"),
                    "upload": driver_supports(driver, "upload"),
                    "copy": driver_supports(driver, "copy"),
                    "share": driver_supports(driver, "share"),
                    "chunk_download": driver_supports(driver, "chunk_download"),
                    "resume_download": driver_supports(driver, "resume_download"),
                    "batch_share": driver_supports(driver, "batch_share")
                }
            },
            message="获取驱动能力成功"
        )
        
    except Exception as e:
        return _error_response(message=f"获取驱动能力失败: {str(e)}")


@router.get("/accounts")
async def get_accounts(request: Request, session_data: dict = Depends(require_admin_auth)):
    """返回所有账号（含禁用），默认账号排在前面，前端自己过滤。"""
    from database.db import db
    accounts = await db.list_accounts(include_inactive=True)

    processed_accounts = []
    for account in accounts:
        config = account['config']
        processed_accounts.append({
            **account,
            'status': config.get('status', 'unknown'),
            'error_message': config.get('error_message'),
            'last_tested': config.get('last_tested'),
            'enabled': account.get('is_active', True)
        })
    
    return _success_response(data=processed_accounts, message="获取账号列表成功")


@router.get("/accounts/{account_id}")
async def get_account(account_id: int, session_data: dict = Depends(require_admin_auth)):
    account = await get_account_or_404(account_id)
    return _success_response(data=account, message="获取账号详情成功")


@router.post("/accounts")
async def create_account(account_data: Dict[str, Any], session_data: dict = Depends(require_admin_auth)):
    try:
        required_fields = ["name", "driver_type", "config"]
        for field in required_fields:
            if field not in account_data:
                raise_api_error(f"缺少必需字段: {field}", "validation", 400)
        
        driver_name = account_data["driver_type"]
        if driver_name not in get_driver_names():
            raise_api_error(f"不支持的驱动类型: {driver_name}", "validation", 400)
        
        config = account_data["config"]
        is_valid, errors = validate_driver_config(driver_name, config)
        if not is_valid:
            raise_api_error(f"配置验证失败: {'; '.join(errors)}", "validation", 400)
        
        connection_success = False
        connection_error = None
        
        try:
            connection_success, connection_error, updated_config = await _test_driver_connection(
                driver_name=driver_name,
                config=config,
                account_id="temp_test"
            )
            if updated_config:
                config.update(updated_config)
        except Exception as e:
            connection_success = False
            connection_error = str(e)
        
        if not connection_success:
            from core.error_handler import raise_connection_test_error
            raise_connection_test_error(driver_name, connection_error)
        
        from database.db import db
        
        config_with_auth = config.copy()
        config_with_auth.update({
            'last_refresh_time': int(time.time()),
            'auth_status': 'active',
            'refresh_attempts': 0,
            'status': 'connected',
            'error_message': None
        })
        
        account_id = await db.add_account(
            name=account_data["name"],
            driver_type=driver_name,
            config=config_with_auth
        )
        
        account = await db.get_account(account_id)
        
        await _notify_auth_manager_account_added(account_id, driver_name, config)
        
        return {
            "success": True,
            "data": account,
            "message": "账号创建成功"
        }
        
    except Exception as e:
        _raise_non_litepan_error(e, "创建账号失败")


@router.put("/accounts/{account_id}")
async def update_account(
    account_id: int,
    account_data: Dict[str, Any],
    session_data: dict = Depends(require_admin_auth)
):
    from database.db import db
    account = await db.get_account(account_id)
    if not account:
        raise_not_found("账号")
    
    try:
        update_data = {}
        
        if "name" in account_data:
            update_data["name"] = account_data["name"]
        
        if "enabled" in account_data:
            update_data["is_active"] = account_data["enabled"]
        
        if "config" in account_data:
            new_config = account_data["config"]
            is_valid, errors = validate_driver_config(account['driver_type'], new_config)
            if not is_valid:
                raise_api_error(f"配置验证失败: {'; '.join(errors)}", "validation", 400)
            
            # 编辑 config 必须重跑连接测试，防止把坏 token 持久化
            connection_success = False
            connection_error = None
            
            try:
                connection_success, connection_error, updated_config = await _test_driver_connection(
                    driver_name=account['driver_type'],
                    config=new_config,
                    account_id="temp_test"
                )
                if updated_config:
                    new_config.update(updated_config)
            except Exception as e:
                connection_success = False
                connection_error = str(e)
            
            if not connection_success:
                from core.error_handler import raise_connection_test_error
                raise_connection_test_error(account['driver_type'], connection_error)
            
            update_data["config"] = new_config
        
        if update_data:
            success = await db.update_account(account_id, **update_data)
            if not success:
                raise_api_error("更新账号失败", "update_account", 500)
        
        updated_account = await db.get_account(account_id)
        
        if "config" in update_data:
            await _notify_auth_manager_account_updated(account_id, account['config'], update_data["config"], account['driver_type'])
        elif "is_active" in update_data:
            await _notify_auth_manager_account_status_changed(account_id, update_data["is_active"])

        final_enabled = bool(updated_account.get("is_active", True)) if updated_account else True
        old_enabled = bool(account.get("is_active", True))
        if "is_active" in update_data and old_enabled != final_enabled:
            if final_enabled:
                await _resume_account_related_tasks(account_id)
            else:
                await _pause_account_related_tasks(
                    account_id,
                    reason="account_disabled",
                    message="关联的账号已禁用",
                )
        elif "config" in update_data and final_enabled:
            # 账号已启用且刚通过连接测试更新 config，说明认证已修好，顺手恢复被暂停的任务
            await _resume_account_related_tasks(account_id)
        
        return {
            "success": True,
            "data": updated_account,
            "message": "账号更新成功"
        }
        
    except Exception as e:
        _raise_non_litepan_error(e, "更新账号失败")


@router.delete("/accounts/{account_id}")
async def delete_account(account_id: int, session_data: dict = Depends(require_admin_auth)):
    from database.db import db
    account = await db.get_account(account_id)
    if not account:
        raise_not_found("账号")
    
    try:
        success = await db.delete_account(account_id)
        
        await _notify_auth_manager_account_deleted(account_id)
        
        # 账号删了，顺带清掉它的 cache_retention / strm 任务，避免变成孤儿任务
        from core.cache_retention_manager import cache_retention_manager
        await cache_retention_manager.remove_configs_by_account(account_id)
        from core.strm_sync_manager import strm_sync_manager
        await strm_sync_manager.remove_tasks_by_account(account_id)
        
        return {
            "success": True,
            "message": "账号删除成功"
        }
        
    except Exception as e:
        raise_api_error(f"删除账号失败: {str(e)}", "internal_error")


@router.post("/accounts/{account_id}/test")
async def test_account_connection(account_id: int, session_data: dict = Depends(require_admin_auth)):
    from database.db import db
    account = await db.get_account(account_id)
    if not account:
        raise_not_found("账号")
    
    try:
        config = account['config']
        driver = await driver_registry.get_driver_instance(
            account_id=str(account_id),
            driver_name=account['driver_type'],
            config=config
        )
        
        result = await driver.test_connection()
        success = result.success
        error_msg = result.message if not success else None
        
        config = account['config']
        config['last_tested'] = datetime.now(UTC).isoformat()
        if success:
            config['status'] = "connected"
            config['error_message'] = None
        else:
            config['status'] = "failed"
            config['error_message'] = error_msg or "连接测试失败"
        
        await db.update_account(account_id, config=config)
        
        return {
            "success": True,
            "data": {
                "connected": success,
                "status": config['status'],
                "error_message": config.get('error_message'),
                "tested_at": config['last_tested']
            }
        }
        
    except Exception as e:
        config = account['config']
        config['status'] = "error"
        config['error_message'] = str(e)
        config['last_tested'] = datetime.now(UTC).isoformat()
        await db.update_account(account_id, config=config)
        
        return {
            "success": False,
            "data": {
                "connected": False,
                "status": config['status'],
                "error_message": config['error_message'],
                "tested_at": config['last_tested']
            }
        }


@router.post("/accounts/{account_id}/toggle")
async def toggle_account_status(account_id: int, session_data: dict = Depends(require_admin_auth)):
    from database.db import db
    account = await db.get_account(account_id)
    if not account:
        raise_not_found("账号")
    
    try:
        old_enabled = account['is_active']
        new_enabled = not old_enabled
        await db.update_account(account_id, is_active=new_enabled)
        
        if old_enabled != new_enabled:
            await _notify_auth_manager_account_status_changed(account_id, new_enabled)
            if new_enabled:
                await _resume_account_related_tasks(account_id)
            else:
                await _pause_account_related_tasks(
                    account_id,
                    reason="account_disabled",
                    message="关联的账号已禁用",
                )
        
        status_text = "启用" if new_enabled else "禁用"
        return {
            "success": True,
            "data": await db.get_account(account_id),
            "message": f"账号已{status_text}"
        }
        
    except Exception as e:
        raise_api_error(f"状态切换失败: {str(e)}", "internal_error")


@router.post("/accounts/{account_id}/set-default")
async def set_default_account(account_id: int, session_data: dict = Depends(require_admin_auth)):
    from database.db import db
    account = await db.get_account(account_id)
    if not account:
        raise_not_found("账号")
    
    try:
        success = await db.set_default_account(account_id)
        
        if success:
            return {
            "success": True,
            "data": account,
            "message": f"已将账号 '{account['name']}' 设为默认"
            }
        else:
            raise_api_error("设置默认账号失败", "internal_error")
        
    except Exception as e:
        raise_api_error(f"设置默认账号失败: {str(e)}", "internal_error") 

@router.get("/cache/info")
async def get_cache_info(session_data: dict = Depends(require_admin_auth)):
    try:
        cache_manager = get_cache_manager()
        cache_cleaner = get_cache_cleaner()
        
        if not cache_manager:
            return _error_response(message="缓存系统未初始化")
        
        cache_info = cache_manager.get_cache_info()
        cleaner_stats = cache_cleaner.get_cleanup_stats() if cache_cleaner else {}
        
        return _success_response(
            data={
                "cache_info": cache_info,
                "cleaner_stats": cleaner_stats,
                "system_status": "running"
            },
            message="获取缓存信息成功"
        )
    except Exception as e:
        return _error_response(message=f"获取缓存信息失败: {str(e)}")


@router.post("/cache/test")
async def test_cache(test_data: dict = None, session_data: dict = Depends(require_admin_auth)):
    try:
        from cache.cache_types import CacheType

        cache_manager = get_cache_manager()
        if not cache_manager:
            return _error_response(message="缓存系统未初始化")
        
        test_key = "test:cache:demo"
        test_value = test_data or {"message": "这是测试数据", "timestamp": str(datetime.now())}
        test_ttl = 60
        
        set_result = await cache_manager.set(test_key, test_value, test_ttl, CacheType.DIRECTORY)
        get_result = await cache_manager.get(test_key)
        
        # TTL=0 走不写缓存分支，回归一下这条路径
        no_cache_key = "test:no_cache"
        no_cache_result = await cache_manager.set(no_cache_key, test_value, 0, CacheType.DIRECTORY)
        
        return _success_response(
            data={
                "test_results": {
                    "set_cache": set_result,
                    "get_cache": get_result,
                    "cache_match": get_result == test_value,
                    "ttl_0_handling": no_cache_result,
                },
                "cache_stats": cache_manager.get_cache_info()
            },
            message="缓存测试完成"
        )
    except Exception as e:
        return _error_response(message=f"缓存测试失败: {str(e)}")


@router.post("/cache/clear")
async def clear_cache_by_type(clear_type: str = "all", session_data: dict = Depends(require_admin_auth)):
    try:
        cache_manager = get_cache_manager()
        cache_cleaner = get_cache_cleaner()
        
        if not cache_manager:
            return _error_response(message="缓存系统未初始化")
        
        cleared_count = 0
        
        if clear_type == "all":
            cleared_count = await cache_manager.clear_all()
        elif clear_type == "expired":
            if cache_cleaner:
                cleared_count = await cache_cleaner.cleanup_expired()
            else:
                return _error_response(message="缓存清理器未初始化")
        else:
            return _error_response(message=f"不支持的清理类型: {clear_type}")
        
        return _success_response(
            data={
                "cleared_count": cleared_count,
                "clear_type": clear_type
            },
            message="缓存清理完成"
        )
    except Exception as e:
        return _error_response(message=f"清理缓存失败: {str(e)}")


@router.get("/cache/stats/{account_id}")
async def get_account_cache_stats(account_id: int, session_data: dict = Depends(require_admin_auth)):
    try:
        cache_manager = get_cache_manager()
        if not cache_manager:
            return _error_response(message="缓存系统未初始化")
        
        account_cache_count = 0
        account_cache_size = 0
        
        async with cache_manager.lock:
            for key, item in cache_manager.cache.items():
                from cache.cache_keys import CacheKeyValidator
                extracted_account_id = CacheKeyValidator.extract_account_id(key)
                if extracted_account_id == account_id:
                    account_cache_count += 1
                    account_cache_size += item.size_bytes
        
        return _success_response(
            data={
                "account_id": account_id,
                "cache_count": account_cache_count,
                "cache_size_bytes": account_cache_size,
                "cache_size_mb": account_cache_size / (1024 * 1024)
            },
            message="获取账号缓存统计成功"
        )
    except Exception as e:
        return _error_response(message=f"获取账号缓存统计失败: {str(e)}")

@router.get("/cache-config")
async def get_cache_config(session_data: dict = Depends(require_admin_auth)):
    try:
        from config import config_manager
        from config import Settings
        
        # 数据库存秒、前端展示分钟，在这里做换算
        cache_enabled = await config_manager.get_async('cache_enabled')
        cache_ttl_seconds = await config_manager.get_async('cache_ttl') or Settings.CACHE_TTL
        cache_ttl_minutes = cache_ttl_seconds // 60
        cache_persistence_enabled = await config_manager.get_async('cache_persistence_enabled')
        cache_persistence_interval_seconds = await config_manager.get_async('cache_persistence_interval_seconds')
        cache_persistence_interval_minutes = (
            (cache_persistence_interval_seconds // 60)
            if cache_persistence_interval_seconds is not None
            else Settings.CACHE_PERSISTENCE_INTERVAL_SECONDS // 60
        )
        cache_max_items = await config_manager.get_async('cache_max_items')
        if cache_max_items is None:
            cache_max_items = Settings.CACHE_MAX_ITEMS
        cache_memory_limit_mb = await config_manager.get_async('cache_memory_limit_mb')
        if cache_memory_limit_mb is None:
            cache_memory_limit_mb = Settings.CACHE_MEMORY_LIMIT_MB

        return _success_response(
            data={
                "cache_enabled": cache_enabled if cache_enabled is not None else True,
                "cache_ttl": cache_ttl_minutes,
                "cache_persistence_enabled": (
                    cache_persistence_enabled
                    if cache_persistence_enabled is not None
                    else Settings.CACHE_PERSISTENCE_ENABLED
                ),
                "cache_persistence_interval_minutes": cache_persistence_interval_minutes,
                "cache_max_items": int(cache_max_items),
                "cache_memory_limit_mb": int(cache_memory_limit_mb),
            },
            message="获取缓存配置成功"
        )
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.error(f"获取缓存配置失败: {e}")
        return _error_response(message=f"获取缓存配置失败: {str(e)}")

@router.post("/update-cache-config")
async def update_cache_config(
    request: Request,
    session_data: dict = Depends(require_admin_auth)
):
    try:
        from config import Settings

        cache_data = await request.json()

        cache_enabled = cache_data.get('cache_enabled', True)
        cache_ttl = cache_data.get('cache_ttl', Settings.CACHE_TTL // 60)  # 前端传的是分钟
        cache_persistence_enabled = cache_data.get('cache_persistence_enabled', True)
        cache_persistence_interval_minutes = cache_data.get(
            'cache_persistence_interval_minutes',
            Settings.CACHE_PERSISTENCE_INTERVAL_SECONDS // 60,
        )
        cache_max_items = cache_data.get('cache_max_items', Settings.CACHE_MAX_ITEMS)
        cache_memory_limit_mb = cache_data.get('cache_memory_limit_mb', Settings.CACHE_MEMORY_LIMIT_MB)

        if not isinstance(cache_enabled, bool):
            return _error_response(message="缓存开关必须是布尔值")
        if not isinstance(cache_persistence_enabled, bool):
            return _error_response(message="缓存持久化开关必须是布尔值")
        if not isinstance(cache_ttl, (int, float)) or cache_ttl < 1 or cache_ttl > 1440:
            return _error_response(message="缓存过期时间必须在1-1440分钟之间")
        if (
            not isinstance(cache_persistence_interval_minutes, (int, float)) or
            cache_persistence_interval_minutes < 1 or
            cache_persistence_interval_minutes > 1440
        ):
            return _error_response(message="缓存持久化快照间隔必须在1-1440分钟之间")
        if (
            not isinstance(cache_max_items, (int, float)) or
            cache_max_items < 1000 or
            cache_max_items > 1000000
        ):
            return _error_response(message="缓存条目最大数量必须在1000-1000000之间")
        if (
            not isinstance(cache_memory_limit_mb, (int, float)) or
            cache_memory_limit_mb < 64 or
            cache_memory_limit_mb > 16384
        ):
            return _error_response(message="缓存内存上限必须在64-16384MB之间")

        from config import config_manager

        # 前端分钟 -> 数据库秒
        cache_ttl_seconds = int(cache_ttl * 60)
        cache_persistence_interval_seconds = int(cache_persistence_interval_minutes * 60)
        cache_max_items = int(cache_max_items)

        result1 = await config_manager.set_async('cache_enabled', cache_enabled)
        result2 = await config_manager.set_async('cache_ttl', cache_ttl_seconds)
        result3 = await config_manager.set_async('cache_persistence_enabled', cache_persistence_enabled)
        result4 = await config_manager.set_async('cache_persistence_interval_seconds', cache_persistence_interval_seconds)
        result5 = await config_manager.set_async('cache_max_items', cache_max_items)
        result6 = await config_manager.set_async('cache_memory_limit_mb', int(cache_memory_limit_mb))

        if result1 and result2 and result3 and result4 and result5 and result6:
            try:
                from core.cache_retention_manager import cache_retention_manager
                await cache_retention_manager.on_global_cache_changed(cache_enabled)
            except Exception as e:
                # 通知失败不回滚配置保存，只记日志
                logger = get_writer(LogModule.API)
                logger.warning(f"通知缓存保持管理器失败: {e}")

            try:
                from cache import apply_cache_runtime_settings

                await apply_cache_runtime_settings(
                    cache_enabled=cache_enabled,
                    persistence_enabled=cache_persistence_enabled,
                    persistence_interval_seconds=cache_persistence_interval_seconds,
                    max_items=cache_max_items,
                    memory_limit_mb=int(cache_memory_limit_mb),
                )
            except Exception as e:
                logger = get_writer(LogModule.API)
                logger.warning(f"应用缓存持久化配置失败: {e}")
            
            return _success_response(message="缓存配置更新成功")
        else:
            return _error_response(message="配置保存失败，请检查数据库连接")
    except Exception as e:

        return {"success": False, "message": f"更新缓存配置失败: {str(e)}"}

@router.get("/hit-rate/stats")
async def get_hit_rate_stats(session_data: dict = Depends(require_admin_auth)):
    try:
        hit_tracker = get_hit_tracker()
        if not hit_tracker:
            return _error_response(message="命中率追踪器未初始化")
        
        global_stats = await hit_tracker.get_global_stats()
        
        return _success_response(data=global_stats, message="获取命中率统计成功")
    except Exception as e:
        return _error_response(message=str(e))

@router.get("/cache/stats")
async def get_cache_stats(session_data: dict = Depends(require_admin_auth)):
    """缓存条目 / 大小 / 命中率 + cache_retention 任务数（只做调试展示，不计入缓存条目）。"""
    try:
        cache_manager = get_cache_manager()
        if not cache_manager:
            return _error_response(message="缓存管理器未初始化")
        
        cache_stats = cache_manager.get_stats()
        cache_info = cache_manager.get_cache_info()
        
        hit_tracker = get_hit_tracker()
        hit_rate_stats = {}
        if hit_tracker:
            hit_rate_stats = await hit_tracker.get_global_stats()
        
        cache_retention_stats = {}
        try:
            from core.cache_retention_manager import cache_retention_manager
            cache_retention_stats = {
                "total_tasks": len(cache_retention_manager._tasks),
                "running_tasks": len([t for t in cache_retention_manager._tasks.values() if t.status.value == 'running']),
                "paused_tasks": len([t for t in cache_retention_manager._tasks.values() if t.status.value == 'paused'])
            }
        except Exception as e:
            cache_retention_stats = {"error": str(e)}
        
        return _success_response(
            data={
                "total_keys": cache_info['total_items'],
                "total_size_bytes": cache_stats.total_size_bytes,
                "hit_rate": hit_rate_stats.get('hit_rate', 0),
                "cache_info": cache_info,
                "cache_retention": cache_retention_stats
            },
            message="获取缓存统计成功"
        )
    except Exception as e:
        return _error_response(message=str(e))

@router.get("/hit-rate/session")
async def get_session_hit_rate(session_id: str = "default", session_data: dict = Depends(require_admin_auth)):
    try:
        hit_tracker = get_hit_tracker()
        if not hit_tracker:
            return _error_response(message="命中率追踪器未初始化")
        
        session_stats = await hit_tracker.get_stats(session_id)
        
        if session_stats is None:
            return _error_response(message="会话不存在")
        
        return _success_response(data=session_stats, message="获取会话命中率成功")
    except Exception as e:
        return _error_response(message=str(e))

@router.post("/clear-cache")
async def clear_cache(session_data: dict = Depends(require_admin_auth)):
    try:
        cache_cleaner = get_cache_cleaner()
        if not cache_cleaner:
            return _error_response(message="缓存清理器未初始化")
        
        cleared_count = await cache_cleaner.clear_all_cache()
        
        hit_tracker = get_hit_tracker()
        if hit_tracker:
            await hit_tracker.reset_stats()
        
        return _success_response(
            data={"cleared_count": cleared_count},
            message=f"缓存已清空，清理了{cleared_count}个项目"
        )
    except Exception as e:
        return _error_response(message=str(e))

# ==================== 认证管理相关API ====================

@router.get("/accounts/{account_id}/auth_status")
async def get_auth_status(account_id: int, session_data: dict = Depends(require_admin_auth)):
    try:
        account = await db.get_account(account_id)
        if not account:
            raise_not_found("账号")
        
        from core.auth_manager import auth_scheduler
        auth_manager = auth_scheduler.auth_managers.get(account_id)
        
        if not auth_manager:
            config = account['config']
            return {
                "success": True,
                "data": {
                    "account_id": account_id,
                    "auth_status": config.get('auth_status', 'active'),
                    "auth_type": "unknown",
                    "last_refresh_time": config.get('last_refresh_time', 0),
                    "refresh_attempts": config.get('refresh_attempts', 0),
                    "next_refresh_time": 0,
                    "supports_refresh": False,
                    "managed_by_scheduler": False
                }
            }
        
        next_refresh_time = await auth_scheduler._calculate_account_next_time(account_id, time.time())
        
        config = account['config']
        return {
            "success": True,
            "data": {
                "account_id": account_id,
                "auth_status": config.get('auth_status', 'active'),
                "auth_type": getattr(auth_manager.config, 'auth_type', 'unknown'),
                "last_refresh_time": config.get('last_refresh_time', 0),
                "refresh_attempts": config.get('refresh_attempts', 0),
                "next_refresh_time": int(next_refresh_time),
                "supports_refresh": getattr(auth_manager.config, 'supports_refresh', False),
                "managed_by_scheduler": True,
                "config_params": {
                    "token_expires_seconds": getattr(auth_manager.config, 'token_expires_seconds', None),
                    "refresh_advance_seconds": getattr(auth_manager.config, 'refresh_advance_seconds', None),
                    "health_check_interval": getattr(auth_manager.config, 'health_check_interval', None),
                    "retry_cooldown_seconds": getattr(auth_manager.config, 'retry_cooldown_seconds', None)
                }
            }
        }
    except Exception as e:
        raise_api_error(str(e), "get_auth_status")

@router.post("/accounts/{account_id}/refresh_auth")
async def manual_refresh_auth(account_id: int, session_data: dict = Depends(require_admin_auth)):
    try:
        account = await db.get_account(account_id)
        if not account:
            raise_not_found("账号")
        
        from core.auth_manager import handle_auth_error
        success = await handle_auth_error(account_id)
        
        if success:
            return _success_response(message="认证刷新成功")
        else:
            return _error_response(message="认证刷新失败")
    except Exception as e:
        return _error_response(message=f"刷新异常: {str(e)}")

@router.get("/auth/scheduler_status")
async def get_scheduler_status(session_data: dict = Depends(require_admin_auth)):
    try:
        from core.auth_manager import auth_scheduler
        import time
        
        managed_accounts = len(auth_scheduler.auth_managers)
        is_running = auth_scheduler._running
        
        next_check_time = 0
        if is_running and managed_accounts > 0:
            next_check_time_float = await auth_scheduler._calculate_next_check_time(emit_log=False)
            next_check_time = int(next_check_time_float)
        
        accounts_status = []
        for account_id, auth_manager in auth_scheduler.auth_managers.items():
            try:
                next_refresh_time = await auth_scheduler._calculate_account_next_time(account_id, time.time())
                accounts_status.append({
                    "account_id": account_id,
                    "auth_type": auth_manager.auth_type,
                    "next_refresh_time": int(next_refresh_time),
                    "next_refresh_in_seconds": max(0, int(next_refresh_time - time.time()))
                })
            except Exception as e:
                accounts_status.append({
                    "account_id": account_id,
                    "auth_type": getattr(auth_manager, 'auth_type', 'unknown'),
                    "error": str(e)
                })
        
        return _success_response(
            data={
                "is_running": is_running,
                "managed_accounts": managed_accounts,
                "next_check_time": next_check_time,
                "accounts_status": accounts_status
            },
            message="获取调度器状态成功"
        )
    except Exception as e:
        raise_api_error(str(e), "get_auth_scheduler_status")

@router.post("/auth/scheduler/recalculate")
async def recalculate_scheduler(session_data: dict = Depends(require_admin_auth)):
    """兼容老前端的占位接口：调度器每轮自然重算，这里不做事。"""
    try:
        from core.auth_manager import auth_scheduler
        pass
        return _success_response(message="调度时间已重新计算")
    except Exception as e:
        return _error_response(message=f"重新计算失败: {str(e)}")

# ==================== 系统设置相关API ====================

@router.get("/test-credentials")
async def test_credentials(session_data: dict = Depends(require_admin_auth)):
    return _success_response(message="路由工作正常")

@router.post("/dashboard/ack-errors")
async def ack_dashboard_errors(session_data: dict = Depends(require_admin_auth)):
    """仪表盘"标记错误已读"。

    把当前时间戳写入 dashboard_errors_ack_at 配置，logs/stats 接口下次返回的
    recent_errors 会自动剔除该时间点之前的错误，仪表盘的红色状态也随之消失。
    日志本身**不会被删除**，依旧能在日志页和审计中看到。
    """
    try:
        from config import config_manager
        now_iso = datetime.now().isoformat()
        await config_manager.set_async('dashboard_errors_ack_at', now_iso)
        return _success_response(data={"ack_at": now_iso}, message="已确认最近错误日志")
    except Exception as e:
        return _error_response(message=f"标记已读失败: {str(e)}")


@router.get("/system-config")
async def get_system_config(session_data: dict = Depends(require_admin_auth)):
    try:
        from config import config_manager
        
        # 数据库存秒、前端显示小时
        session_timeout_seconds = await config_manager.get_async('session_timeout') or 7200
        session_timeout_hours = session_timeout_seconds / 3600
        
        webdav_enabled = await config_manager.get_async('webdav_enabled')
        webdav_smart_chunk = await config_manager.get_async('webdav_smart_chunk_enabled')
        webdav_chunk_size = await config_manager.get_async('webdav_chunk_size')
        webdav_cache_enabled = await config_manager.get_async('webdav_cache_enabled')
        auth_active_refresh_enabled = await config_manager.get_async('auth_active_refresh_enabled')
        public_index_enabled = await config_manager.get_async('public_index_enabled')
        index_account_switch_mode = await config_manager.get_async('index_account_switch_mode') or "dropdown"
        if index_account_switch_mode not in {"dropdown", "floating"}:
            index_account_switch_mode = "dropdown"
        admin_home_return_mode = await config_manager.get_async('admin_home_return_mode') or "top_icon"
        if admin_home_return_mode not in {"sidebar", "top_icon", "both"}:
            admin_home_return_mode = "top_icon"
        theme = await config_manager.get_async('theme') or "light"
        if theme not in {"light", "dark", "auto"}:
            theme = "light"
        
        # 兼容老数据：>8192 视为 bytes，其它按 KB；并回落到默认 256KB
        if webdav_chunk_size and webdav_chunk_size > 8192:
            webdav_chunk_size_kb = webdav_chunk_size // 1024
        else:
            webdav_chunk_size_kb = webdav_chunk_size or 256
        
        admin_username = await config_manager.get_async('admin_username') or "admin"
        admin_password = await config_manager.get_async('admin_password') or "admin"
        credential_state = assess_admin_credential_state(admin_username, admin_password)

        return _success_response(
            data={
                "admin_username": admin_username,
                "session_timeout": session_timeout_hours,
                "oauth_server_url": await config_manager.get_async('oauth_server_url') or "https://oauth.litepan.top",
                "public_index_enabled": normalize_bool(public_index_enabled, True),
                "index_account_switch_mode": index_account_switch_mode,
                "admin_home_return_mode": admin_home_return_mode,
                "theme": theme,
                "upload_task_concurrency": await config_manager.get_async('upload_task_concurrency') or 3,
                "log_retention_days": await config_manager.get_async('log_retention_days') or 30,
                "auth_active_refresh_enabled": normalize_bool(auth_active_refresh_enabled, True),
                "must_change_password": credential_state["must_change_password"],
                "password_change_reason": credential_state["password_change_reason"],
                "webdav_enabled": normalize_bool(webdav_enabled, True),
                "webdav_smart_chunk_enabled": webdav_smart_chunk if isinstance(webdav_smart_chunk, bool) else str(webdav_smart_chunk).lower() != 'false',
                "webdav_chunk_size": webdav_chunk_size_kb,
                "webdav_cache_enabled": normalize_bool(webdav_cache_enabled, True),
            },
            message="获取系统配置成功"
        )
    except Exception as e:
        return {
            "success": False,
            "message": f"获取配置失败: {str(e)}"
        }

@router.post("/theme")
async def update_theme(
    request: Request,
    session_data: dict = Depends(require_admin_auth)
):
    try:
        data = await request.json()
        theme = data.get('theme')
        if theme not in {"light", "dark", "auto"}:
            return _error_response(message="界面主题不正确")

        from config import config_manager
        await config_manager.set_async('theme', theme)
        return _success_response(data={"theme": theme}, message="界面主题已更新")
    except Exception as e:
        return _error_response(message=f"更新界面主题失败: {str(e)}")

@router.post("/update-credentials")
async def update_admin_credentials(
    request: Request,
    session_data: dict = Depends(require_admin_auth)
):
    try:
        credentials_data = await request.json()

        username = credentials_data.get('admin_username', '').strip()
        password = credentials_data.get('admin_password', '').strip()
        session_timeout = credentials_data.get('session_timeout')
        oauth_server_url = (credentials_data.get('oauth_server_url') or '').strip()
        public_index_enabled = credentials_data.get('public_index_enabled')
        upload_task_concurrency = credentials_data.get('upload_task_concurrency')
        log_retention_days = credentials_data.get('log_retention_days')
        auth_active_refresh_enabled = credentials_data.get('auth_active_refresh_enabled')
        index_account_switch_mode = credentials_data.get('index_account_switch_mode')
        admin_home_return_mode = credentials_data.get('admin_home_return_mode')
        theme = credentials_data.get('theme')

        if not username:
            return _error_response(message="用户名不能为空")

        import re
        if not re.match(r'^[a-zA-Z0-9_]+$', username):
            return _error_response(message="用户名只能包含字母、数字和下划线")

        from config import config_manager

        current_admin_password = await config_manager.get_async('admin_password') or "admin"
        current_credential_state = assess_admin_credential_state(username, current_admin_password)

        # 前端传小时，存库按秒
        if session_timeout is not None:
            if not isinstance(session_timeout, (int, float)) or session_timeout < 0.5 or session_timeout > 24:
                return _error_response(message="会话超时时间必须在0.5-24小时之间")
            session_timeout_seconds = int(session_timeout * 3600)

        if oauth_server_url:
            if not re.match(r'^https?://[^/]+$', oauth_server_url):
                return _error_response(message="OAuth服务地址格式不正确，示例：https://litepan.top")

        if upload_task_concurrency is not None:
            if not isinstance(upload_task_concurrency, int) or upload_task_concurrency < 1 or upload_task_concurrency > 5:
                return _error_response(message="上传任务并发数必须是 1-5 之间的整数")

        if log_retention_days is not None:
            if not isinstance(log_retention_days, int) or log_retention_days < 1 or log_retention_days > 365:
                return _error_response(message="日志保留天数必须是 1-365 之间的整数")

        if index_account_switch_mode is not None and index_account_switch_mode not in {"dropdown", "floating"}:
            return _error_response(message="首页账号切换方式不正确")

        if admin_home_return_mode is not None and admin_home_return_mode not in {"sidebar", "top_icon", "both"}:
            return _error_response(message="主页返回方式不正确")

        if theme is not None and theme not in {"light", "dark", "auto"}:
            return _error_response(message="界面主题不正确")

        async def apply_upload_task_concurrency():
            if upload_task_concurrency is None:
                return
            await config_manager.set_async('upload_task_concurrency', upload_task_concurrency)
            from core.upload_task_manager import upload_task_manager
            await upload_task_manager.apply_concurrency_limit(upload_task_concurrency)

        async def apply_public_index_enabled_setting():
            if public_index_enabled is None:
                return
            normalized_enabled = normalize_bool(public_index_enabled, True)
            await config_manager.set_async('public_index_enabled', normalized_enabled)

        async def apply_auth_active_refresh_setting():
            if auth_active_refresh_enabled is None:
                return
            normalized_enabled = normalize_bool(auth_active_refresh_enabled, True)
            await config_manager.set_async('auth_active_refresh_enabled', normalized_enabled)
            from core.auth_manager import auth_scheduler
            await auth_scheduler.set_active_refresh_enabled(normalized_enabled)

        async def apply_index_account_switch_mode():
            if index_account_switch_mode is None:
                return
            await config_manager.set_async('index_account_switch_mode', index_account_switch_mode)

        async def apply_admin_home_return_mode():
            if admin_home_return_mode is None:
                return
            await config_manager.set_async('admin_home_return_mode', admin_home_return_mode)

        async def apply_theme():
            if theme is None:
                return
            await config_manager.set_async('theme', theme)

        async def apply_log_retention_days():
            if log_retention_days is None:
                return
            await config_manager.set_async('log_retention_days', log_retention_days)
            from core.log_manager import get_log_manager
            log_manager = get_log_manager()
            if log_manager:
                await log_manager.start_auto_cleanup(log_retention_days)
                await log_manager.storage.cleanup_old_logs(log_retention_days)
        
        # 没传新密码视作不改密码，直接保存其他字段
        if not password:
            if current_credential_state["must_change_password"]:
                return _error_response(message="当前管理员密码需要升级，请先设置新密码后再保存其他设置")
            await config_manager.set_async('admin_username', username)
            if session_timeout is not None:
                await config_manager.set_async('session_timeout', session_timeout_seconds)
            if oauth_server_url:
                await config_manager.set_async('oauth_server_url', oauth_server_url.rstrip('/'))
            await apply_public_index_enabled_setting()
            await apply_upload_task_concurrency()
            await apply_auth_active_refresh_setting()
            await apply_index_account_switch_mode()
            await apply_admin_home_return_mode()
            await apply_theme()
            await apply_log_retention_days()
            response = JSONResponse(content=_success_response(message="管理员设置更新成功"))
            session_manager.create_session(
                response=response,
                user_data={
                    "is_admin": True,
                    "username": username,
                },
                remember=bool(session_data.get("remember")),
                request=request,
            )
            return response
        
        if len(password) < 6:
            return _error_response(message="密码长度至少6位")
        
        password_hash = generate_password_hash(password)
        
        await config_manager.set_async('admin_username', username)
        await config_manager.set_async('admin_password', password_hash)
        await config_manager.set_async("admin_temp_password_hash", "")
        await config_manager.set_async("admin_temp_password_expires_at", 0)
        if session_timeout is not None:
            await config_manager.set_async('session_timeout', session_timeout_seconds)
        if oauth_server_url:
            await config_manager.set_async('oauth_server_url', oauth_server_url.rstrip('/'))
        await apply_public_index_enabled_setting()
        await apply_upload_task_concurrency()
        await apply_auth_active_refresh_setting()
        await apply_index_account_switch_mode()
        await apply_admin_home_return_mode()
        await apply_theme()
        await apply_log_retention_days()

        response = JSONResponse(content=_success_response(message="管理员设置更新成功"))
        session_manager.create_session(
            response=response,
            user_data={
                "is_admin": True,
                "username": username,
            },
            remember=bool(session_data.get("remember")),
            request=request,
        )
        return response
        
    except Exception as e:
        return {
            "success": False,
            "message": f"更新失败: {str(e)}"
        }

@router.post("/accounts/{account_id}/clear-download-cache")
async def clear_account_download_cache(account_id: int, session_data: dict = Depends(require_admin_auth)):
    try:
        from core.dependency_container import get_cache_cleaner
        cache_cleaner = get_cache_cleaner()
        
        if cache_cleaner:
            await cache_cleaner.clear_download_mode_cache(str(account_id))
            return _success_response(message="下载模式缓存已清空")
        else:
            return _error_response(message="缓存清理器不可用")
            
    except Exception as e:
        return _error_response(message=f"清空缓存失败: {str(e)}")


@router.post("/webdav-config")
async def update_webdav_config(
    request: Request,
    session_data: dict = Depends(require_admin_auth)
):
    try:
        data = await request.json()
        from config import config_manager

        webdav_enabled = data.get('webdav_enabled')
        if webdav_enabled is not None:
            await config_manager.set_async('webdav_enabled', normalize_bool(webdav_enabled, True))

        smart_chunk = data.get('webdav_smart_chunk_enabled')
        if smart_chunk is not None:
            await config_manager.set_async('webdav_smart_chunk_enabled', smart_chunk)

        # 前端传 KB，库内存 bytes
        chunk_size_kb = data.get('webdav_chunk_size')
        if chunk_size_kb is not None:
            chunk_size_bytes = int(chunk_size_kb) * 1024
            await config_manager.set_async('webdav_chunk_size', chunk_size_bytes)

        webdav_cache_enabled = data.get('webdav_cache_enabled')
        if webdav_cache_enabled is not None:
            normalized_enabled = normalize_bool(webdav_cache_enabled, True)
            await config_manager.set_async('webdav_cache_enabled', normalized_enabled)

        if webdav_cache_enabled is not None:
            try:
                from webdav.server import clear_webdav_cache
                await clear_webdav_cache()
            except Exception as e:
                get_writer(LogModule.SYSTEM).warning(f"清除 WebDAV 缓存失败: {e}")
        
        return _success_response(message="WebDAV设置更新成功")
        
    except Exception as e:
        return _error_response(message=f"更新WebDAV配置失败: {str(e)}")
