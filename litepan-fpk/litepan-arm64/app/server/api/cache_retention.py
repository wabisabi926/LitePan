"""缓存保持配置的管理接口。"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
from pydantic import BaseModel

from database.db import db
from core.log_manager import get_writer, LogModule
from api.deps import require_admin_auth
from api.responses import error_response as _error_response, success_response as _success_response
from core.account_utils import get_account_or_404
from config import config_manager
from cache.cache_keys import CacheKeyGenerator
from core.dependency_container import get_cache_cleaner

router = APIRouter(prefix="/api/cache-retention", tags=["缓存保持"])

CACHE_RETENTION_MAX_CONFIGS = 6  # 硬上限，防止被人加几百条把网盘 API 打爆
CACHE_RETENTION_DEFAULT_EXTRA_API_INTERVAL = 200
CACHE_RETENTION_MAX_EXTRA_API_INTERVAL = 5000

class CacheRetentionConfig(BaseModel):
    account_id: int
    parent_id: str
    path: str
    recursive: bool = False
    scan_depth: Optional[int] = None
    api_interval: int = CACHE_RETENTION_DEFAULT_EXTRA_API_INTERVAL
    refresh_interval: int = 60
    time_window_enabled: bool = False
    time_start: str = "00:00"
    time_end: str = "00:00"

class CacheRetentionResponse(BaseModel):
    id: int
    account_id: int
    parent_id: str
    path: str
    account_name: str
    recursive: bool
    scan_depth: Optional[int] = None
    api_interval: int
    refresh_interval: int
    status: str
    file_count: int
    last_refresh: Optional[str]
    last_refresh_status: Optional[str]
    created_at: str

@router.get("/configs")
async def get_cache_retention_configs(session_data: dict = Depends(require_admin_auth)):
    try:
        configs = await db.get_cache_retention_configs()
        from core.cache_retention_manager import cache_retention_manager
        for config in configs:
            task_status = cache_retention_manager.get_task_status(config['id'])
            if task_status:
                config['scanned_dirs'] = task_status.get('scanned_dirs', 0)
                config['scanned_files'] = task_status.get('scanned_files', 0)
                config['started_at'] = task_status.get('started_at')
                config['last_duration_ms'] = task_status.get('last_duration_ms', 0)
        response = _success_response(data=configs, message=f"成功获取 {len(configs)} 个配置")
        response["startup_remaining"] = cache_retention_manager.startup_remaining
        return response
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.error(f"获取缓存保持配置失败: {e}")
        return _error_response(message=f"获取配置失败: {str(e)}", data=[])

@router.get("/configs/{config_id}")
async def get_cache_retention_config(config_id: int, session_data: dict = Depends(require_admin_auth)):
    try:
        config = await db.get_cache_retention_config(config_id)
        if not config:
            raise HTTPException(404, "配置不存在")
        return _success_response(data=config, message="获取配置成功")
    except HTTPException:
        raise
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.error(f"获取缓存保持配置失败: {e}")
        raise HTTPException(500, "获取配置失败")

@router.post("/configs")
async def create_cache_retention_config(config: CacheRetentionConfig, session_data: dict = Depends(require_admin_auth)):
    try:
        if config.api_interval < 0 or config.api_interval > CACHE_RETENTION_MAX_EXTRA_API_INTERVAL:
            return _error_response(message=f"API额外补偿间隔必须在 0-{CACHE_RETENTION_MAX_EXTRA_API_INTERVAL} 毫秒之间")
        current_count = await db.get_cache_retention_config_count()
        if current_count >= CACHE_RETENTION_MAX_CONFIGS:
            return _error_response(message=f"最多只能添加{CACHE_RETENTION_MAX_CONFIGS}个缓存保持配置")

        account = await get_account_or_404(config.account_id)
        if not account.get('is_active', True):
            return _error_response(message="账号未启用")

        from core.cache_retention_manager import cache_retention_manager, _normalize_scan_depth
        scan_depth = _normalize_scan_depth(config.scan_depth, config.recursive)
        recursive = scan_depth != 1  # 与 scan_depth 保持一致，供旧逻辑/展示回退使用

        config_id = await db.add_cache_retention_config(
            account_id=config.account_id,
            parent_id=config.parent_id,
            path=config.path,
            recursive=recursive,
            scan_depth=scan_depth,
            api_interval=config.api_interval,
            refresh_interval=config.refresh_interval,
            time_window_enabled=config.time_window_enabled,
            time_start=config.time_start,
            time_end=config.time_end,
        )

        task_data = config.dict()
        task_data['recursive'] = recursive
        task_data['scan_depth'] = scan_depth
        await cache_retention_manager.add_task(config_id, **task_data)
        trigger_state = await cache_retention_manager.refresh_task_now(config_id)

        status_message = "配置创建成功"
        if trigger_state == "running":
            status_message = "配置创建成功，已立即执行"
        elif trigger_state == "blocked_by_strm":
            status_message = "配置创建成功，等待 STRM 任务完成后自动执行"
        
        return _success_response(data={"id": config_id}, message=status_message)
        
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.error(f"创建缓存保持配置失败: {e}")
        return _error_response(message=f"创建配置失败: {str(e)}")

@router.put("/configs/{config_id}")
async def update_cache_retention_config(config_id: int, config: CacheRetentionConfig, session_data: dict = Depends(require_admin_auth)):
    try:
        if config.api_interval < 0 or config.api_interval > CACHE_RETENTION_MAX_EXTRA_API_INTERVAL:
            return _error_response(message=f"API额外补偿间隔必须在 0-{CACHE_RETENTION_MAX_EXTRA_API_INTERVAL} 毫秒之间")
        existing_config = await db.get_cache_retention_config(config_id)
        if not existing_config:
            raise HTTPException(404, "配置不存在")

        account = await get_account_or_404(config.account_id)
        if not account.get('is_active', True):
            return _error_response(message="账号未启用")

        from core.cache_retention_manager import cache_retention_manager, _normalize_scan_depth
        scan_depth = _normalize_scan_depth(config.scan_depth, config.recursive)
        recursive = scan_depth != 1

        success = await db.update_cache_retention_config(
            config_id,
            account_id=config.account_id,
            parent_id=config.parent_id,
            path=config.path,
            recursive=recursive,
            scan_depth=scan_depth,
            api_interval=config.api_interval,
            refresh_interval=config.refresh_interval,
            time_window_enabled=config.time_window_enabled,
            time_start=config.time_start,
            time_end=config.time_end,
        )

        if success:
            task_data = config.dict()
            task_data['recursive'] = recursive
            task_data['scan_depth'] = scan_depth
            await cache_retention_manager.update_task(config_id, **task_data)
            return _success_response(message="配置已更新")
        else:
            raise HTTPException(500, "更新配置失败")
    except HTTPException:
        raise
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.error(f"更新缓存保持配置失败: {e}")
        raise HTTPException(500, "更新配置失败")

@router.delete("/configs/{config_id}")
async def delete_cache_retention_config(config_id: int, clear_cache: bool = Query(False, description="是否清理缓存"), session_data: dict = Depends(require_admin_auth)):
    try:
        success = await db.delete_cache_retention_config(config_id)
        if success:
            from core.cache_retention_manager import cache_retention_manager
            await cache_retention_manager.remove_task(config_id, clear_cache=clear_cache)
            return _success_response(message="配置已删除" + ("，缓存已清理" if clear_cache else ""))
        else:
            raise HTTPException(404, "配置不存在")
    except HTTPException:
        raise
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.error(f"删除缓存保持配置失败: {e}")
        raise HTTPException(500, "删除配置失败")

@router.post("/configs/{config_id}/toggle")
async def toggle_cache_retention_config(config_id: int, session_data: dict = Depends(require_admin_auth)):
    try:
        from core.cache_retention_manager import cache_retention_manager
        success = await cache_retention_manager.toggle_task(config_id)
        if success:
            return _success_response(message="状态已切换")
        else:
            raise HTTPException(404, "配置不存在")
    except HTTPException:
        raise
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.error(f"切换缓存保持配置状态失败: {e}")
        raise HTTPException(500, "切换状态失败")

@router.post("/configs/{config_id}/refresh")
async def refresh_cache_retention_config(config_id: int, session_data: dict = Depends(require_admin_auth)):
    try:
        from core.cache_retention_manager import cache_retention_manager
        state = await cache_retention_manager.refresh_task_now(config_id)
        if state == "running":
            return _success_response(message="刷新任务已启动")
        elif state == "already_running":
            return _success_response(message="任务已在执行中")
        elif state == "blocked_by_strm":
            return _success_response(message="STRM 任务正在使用该账号，请稍后再试或等待 STRM 任务完成")
        else:
            raise HTTPException(404, "配置不存在")
    except HTTPException:
        raise
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.error(f"刷新缓存保持配置失败: {e}")
        raise HTTPException(500, "刷新失败")

@router.post("/configs/{config_id}/force-stop")
async def force_stop_cache_retention_config(config_id: int, session_data: dict = Depends(require_admin_auth)):
    try:
        from core.cache_retention_manager import cache_retention_manager
        success = await cache_retention_manager.force_stop_task(config_id)
        if success:
            return _success_response(message="任务已强制停止，下次调度不受影响")
        else:
            return _success_response(message="任务未在执行中")
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.error(f"强制停止缓存保持任务失败: {e}")
        raise HTTPException(500, "强制停止失败")

@router.post("/refresh-all")
async def refresh_all_cache_retention_configs(session_data: dict = Depends(require_admin_auth)):
    try:
        from core.cache_retention_manager import cache_retention_manager
        count = await cache_retention_manager.refresh_all_tasks()
        return _success_response(data={"count": count}, message=f"已启动{count}个刷新任务")
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.error(f"刷新所有缓存保持配置失败: {e}")
        raise HTTPException(500, "刷新失败")

@router.get("/stats")
async def get_cache_retention_stats(session_data: dict = Depends(require_admin_auth)):
    try:
        from core.cache_retention_manager import cache_retention_manager
        
        configs = await db.get_cache_retention_configs()
        
        total_count = len(configs)
        running_count = len([c for c in configs if c['status'] == 'running'])
        paused_count = len([c for c in configs if c['status'] == 'paused'])
        
        intervals = [c['refresh_interval'] for c in configs if c['refresh_interval']]
        average_interval = sum(intervals) / len(intervals) if intervals else 0
        
        running_task_ids = list(cache_retention_manager._running_tasks)
        
        return _success_response(
            data={
                "total_count": total_count,
                "running_count": running_count,
                "paused_count": paused_count,
                "max_configs": CACHE_RETENTION_MAX_CONFIGS,
                "average_interval": f"{average_interval:.1f}分钟" if average_interval > 0 else "无",
                "executing_task_ids": running_task_ids
            },
            message="获取统计成功"
        )
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.error(f"获取缓存保持统计失败: {e}")
        raise HTTPException(500, "获取统计失败")

@router.get("/defaults")
async def get_cache_retention_defaults(session_data: dict = Depends(require_admin_auth)):
    """获取缓存保持的全局默认值（从 config_manager 读取，兜底 config.py）。"""
    refresh_interval = await config_manager.get_async("cache_retention_default_refresh_interval") or 60
    api_interval = await config_manager.get_async("cache_retention_default_api_interval") or CACHE_RETENTION_DEFAULT_EXTRA_API_INTERVAL
    return _success_response(
        data={
            "refresh_interval": refresh_interval,
            "api_interval": api_interval,
        }
    )

@router.get("/accounts")
async def get_accounts(session_data: dict = Depends(require_admin_auth)):
    try:
        accounts = await db.list_accounts(include_inactive=False)

        processed_accounts = []
        for account in accounts:
            config = account['config']
            processed_account = {
                'id': account['id'],
                'name': account['name'],
                'driver_type': account['driver_type'],
                'is_active': account.get('is_active', True),
                'status': config.get('status', 'unknown'),
                'error_message': config.get('error_message'),
                'last_tested': config.get('last_tested')
            }
            processed_accounts.append(processed_account)
        
        return _success_response(
            data=processed_accounts,
            message=f"成功获取 {len(processed_accounts)} 个账号"
        )
        
    except Exception as e:
        logger = get_writer(LogModule.API)
        logger.error(f"获取账号列表失败: {e}")
        return _error_response(message=f"获取账号列表失败: {str(e)}", data=[])

@router.get("/accounts/{account_id}/directories")
async def get_directories(
    account_id: int,
    parent_id: str = "root",
    force_refresh: bool = False,
    session_data: dict = Depends(require_admin_auth),
):
    """前端目录选择器用。这里即使 list_files 抛错也不能把驱动实例标坏，交给 registry 自愈。"""
    logger = get_writer(LogModule.API)
    try:
        account = await get_account_or_404(account_id)
        if not account.get('is_active', True):
            logger.error(f"账号未启用: account_id={account_id}")
            raise HTTPException(400, "账号未启用")

        from core.driver_service import get_account_driver
        driver = None
        try:
            driver = await get_account_driver(account_id)

            # 前端给的 "root" 映射到大多数驱动的根目录 ID "0"
            if parent_id == "root":
                parent_id = "0"

            if force_refresh:
                cache_cleaner = get_cache_cleaner()
                if cache_cleaner:
                    await cache_cleaner._clear_directory_cache(str(account_id), parent_id)
                    try:
                        await cache_cleaner.cache_manager.clear_by_prefix(
                            CacheKeyGenerator.path_mapping_prefix(str(account_id))
                        )
                    except Exception as e:
                        logger.warning(f"清理路径映射缓存失败: {e}")
                    try:
                        await cache_cleaner.cache_manager.clear_by_prefix(
                            CacheKeyGenerator.webdav_metadata_prefix(str(account_id))
                        )
                    except Exception as e:
                        logger.warning(f"清理WebDAV元数据缓存失败: {e}")

            try:
                files = await driver.list_files(parent_id)
            except Exception as list_error:
                logger.error(f"获取文件列表失败，但不影响驱动实例: {list_error}")
                raise HTTPException(500, f"获取目录列表失败: {str(list_error)}")

            directories = []
            for file in files:
                if file.is_dir:
                    directories.append({
                        "id": file.id,
                        "name": file.name,
                        "path": file.path,
                        "type": "folder",
                        "size": file.size,
                        "modified_time": file.modified.isoformat() if file.modified else None
                    })
            
            return _success_response(data=directories, message="获取目录列表成功")
            
        except Exception as driver_error:
            logger.error(f"驱动操作失败 (account_id={account_id}, parent_id={parent_id}): {driver_error}")
            logger.error(f"错误类型: {type(driver_error).__name__}")
            logger.error(f"错误详情: {str(driver_error)}")
            raise HTTPException(500, f"驱动操作失败: {str(driver_error)}")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取目录列表失败 (account_id={account_id}, parent_id={parent_id}): {e}")
        logger.error(f"错误类型: {type(e).__name__}")
        logger.error(f"错误详情: {str(e)}")
        raise HTTPException(500, "获取目录列表失败") 
